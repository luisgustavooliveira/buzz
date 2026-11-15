-- SNPS fork: V1 email bridge (migration 9002, originally 0046).
CREATE TABLE email_quarantine (
    id                    UUID NOT NULL DEFAULT gen_random_uuid(),
    community_id          UUID NOT NULL REFERENCES communities(id),
    mailbox_id            TEXT NOT NULL,
    graph_message_id      TEXT NOT NULL,
    internet_message_id   TEXT,
    sender_address        TEXT,
    reason                TEXT NOT NULL,
    references_seen       JSONB NOT NULL DEFAULT '[]'::jsonb,
    authentication        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (community_id, id),
    UNIQUE (community_id, mailbox_id, graph_message_id)
);
