-- SNPS fork: V1 email bridge (migration 9002, originally 0046).
CREATE TABLE email_outbox (
    id                    UUID NOT NULL DEFAULT gen_random_uuid(),
    community_id          UUID NOT NULL,
    conversation_id       UUID NOT NULL,
    event_id              BYTEA NOT NULL CHECK (length(event_id) = 32),
    author_pubkey         BYTEA NOT NULL CHECK (length(author_pubkey) = 32),
    internet_message_id   TEXT NOT NULL,
    payload               JSONB NOT NULL,
    status                TEXT NOT NULL DEFAULT 'pending'
                          CHECK (status IN ('pending', 'sent', 'failed')),
    attempts              INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_retry_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error            TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at               TIMESTAMPTZ,
    PRIMARY KEY (community_id, id),
    UNIQUE (community_id, internet_message_id),
    FOREIGN KEY (community_id, conversation_id)
        REFERENCES email_conversations (community_id, id)
);

CREATE INDEX idx_email_outbox_pending
    ON email_outbox (next_retry_at, created_at)
    WHERE status = 'pending';
