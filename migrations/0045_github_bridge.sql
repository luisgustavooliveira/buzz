-- Optional GitHub bridge persistence (issue ↔ thread mapping + durable outbox).
--
-- The bridge maps a GitHub (repo, subject) pair to exactly one Buzz thread per
-- community, and durably queues outbound GitHub API actions (comment, open
-- issue) so a relay restart never loses an agent's decision to publish. The
-- whole feature is config-gated and inert when disabled; these tables stay
-- empty on deployments that never enable it.
--
-- Both tables are tenant-scoped (community_id NOT NULL, community-leading
-- keys) and receive the universal community write fence below, per the
-- migration-lint contract established in 0029.

-- ── Thread links ────────────────────────────────────────────────────────────
-- One row per (community, repo, subject_type, subject_number): the bridge's
-- answer to "which Buzz thread is GitHub issue N". `subject_type` is 'issue'
-- today; 'pr' is reserved for a later phase so the key space does not change.
-- `last_agent_pubkey` routes inbound GitHub replies to the agent who published
-- the last outbound comment; NULL until the first publish succeeds.

CREATE TABLE github_thread_links (
    community_id      UUID NOT NULL REFERENCES communities(id),
    repo_full_name    TEXT NOT NULL,
    subject_type      TEXT NOT NULL DEFAULT 'issue'
                      CHECK (subject_type IN ('issue', 'pr')),
    subject_number    INTEGER NOT NULL CHECK (subject_number > 0),
    channel_id        UUID NOT NULL,
    root_event_id     BYTEA NOT NULL CHECK (length(root_event_id) = 32),
    last_agent_pubkey BYTEA CHECK (last_agent_pubkey IS NULL OR length(last_agent_pubkey) = 32),
    status            TEXT NOT NULL DEFAULT 'open'
                      CHECK (status IN ('open', 'closed')),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (community_id, repo_full_name, subject_type, subject_number),
    FOREIGN KEY (community_id, channel_id)
        REFERENCES channels (community_id, id)
);

SELECT attach_community_write_fence('github_thread_links');

-- ── Outbox ──────────────────────────────────────────────────────────────────
-- One row per outbound GitHub action. Rows are inserted in the same
-- transaction as the kind:9 event that originated them (one action = one
-- atomic persist) and claimed by the bridge worker with FOR UPDATE SKIP
-- LOCKED. The claim bumps `attempts` and moves `next_retry_at` to the claim
-- lease, so a crashed worker's rows become claimable again when the lease
-- expires; `status` stays 'pending' across retries and only reaches the
-- terminal 'sent'/'failed' states via an explicit worker update.
-- `github_comment_id` is recorded on success so the webhook inbound path can
-- suppress the echo of a comment the bridge itself posted; it is unique per
-- community for that lookup.

CREATE TABLE github_outbox (
    id                UUID NOT NULL DEFAULT gen_random_uuid(),
    community_id      UUID NOT NULL REFERENCES communities(id),
    action            TEXT NOT NULL CHECK (action IN ('comment', 'open_issue')),
    link_repo         TEXT NOT NULL,
    -- NULL for open_issue: the issue number exists only after GitHub creates it.
    link_number       INTEGER CHECK (link_number IS NULL OR link_number > 0),
    event_id          BYTEA NOT NULL CHECK (length(event_id) = 32),
    author_pubkey     BYTEA NOT NULL CHECK (length(author_pubkey) = 32),
    payload           JSONB NOT NULL DEFAULT '{}'::jsonb,
    status            TEXT NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'sent', 'failed')),
    attempts          INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_retry_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error        TEXT,
    github_comment_id BIGINT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (community_id, id)
);

-- Worker claim scan: due pending rows ordered by retry time.
CREATE INDEX idx_github_outbox_pending
    ON github_outbox (next_retry_at, created_at)
    WHERE status = 'pending';

-- Echo suppression: has this community already published this comment id?
CREATE UNIQUE INDEX idx_github_outbox_comment_id
    ON github_outbox (community_id, github_comment_id)
    WHERE github_comment_id IS NOT NULL;

SELECT attach_community_write_fence('github_outbox');
