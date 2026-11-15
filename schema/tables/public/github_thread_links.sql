-- SNPS fork: GitHub bridge (migration 9001, originally 0045).
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
