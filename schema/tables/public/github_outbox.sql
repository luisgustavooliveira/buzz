-- SNPS fork: GitHub bridge (migration 9001, originally 0045).
CREATE TABLE github_outbox (
    id                UUID NOT NULL DEFAULT gen_random_uuid(),
    community_id      UUID NOT NULL REFERENCES communities(id),
    action            TEXT NOT NULL CHECK (action IN ('comment', 'open_issue')),
    link_repo         TEXT NOT NULL,
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

CREATE INDEX idx_github_outbox_pending
    ON github_outbox (next_retry_at, created_at)
    WHERE status = 'pending';

CREATE UNIQUE INDEX idx_github_outbox_comment_id
    ON github_outbox (community_id, github_comment_id)
    WHERE github_comment_id IS NOT NULL;
