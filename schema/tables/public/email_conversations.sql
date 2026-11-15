-- SNPS fork: V1 email bridge (migration 9002, originally 0046).
CREATE TABLE email_conversations (
    id                    UUID NOT NULL DEFAULT gen_random_uuid(),
    community_id          UUID NOT NULL REFERENCES communities(id),
    channel_id            UUID NOT NULL,
    root_event_id         BYTEA NOT NULL CHECK (length(root_event_id) = 32),
    status                TEXT NOT NULL DEFAULT 'active'
                          CHECK (status IN ('active', 'closed')),
    external_enabled      BOOLEAN NOT NULL DEFAULT FALSE,
    created_by            BYTEA NOT NULL CHECK (length(created_by) = 32),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at             TIMESTAMPTZ,
    PRIMARY KEY (community_id, id),
    UNIQUE (community_id, root_event_id),
    FOREIGN KEY (community_id, channel_id)
        REFERENCES channels (community_id, id)
);
