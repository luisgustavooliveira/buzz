-- SNPS fork: V1 email bridge (migration 9002, originally 0046).
CREATE TABLE email_messages (
    community_id          UUID NOT NULL,
    internet_message_id   TEXT NOT NULL,
    mailbox_id            TEXT NOT NULL,
    conversation_id       UUID NOT NULL,
    parent_message_id     TEXT,
    direction             TEXT NOT NULL CHECK (direction IN ('inbound', 'outbound')),
    sender_address        TEXT NOT NULL,
    graph_message_id      TEXT,
    buzz_event_id         BYTEA CHECK (buzz_event_id IS NULL OR length(buzz_event_id) = 32),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (community_id, internet_message_id, mailbox_id),
    FOREIGN KEY (community_id, conversation_id)
        REFERENCES email_conversations (community_id, id)
);

CREATE INDEX idx_email_messages_conversation
    ON email_messages (community_id, conversation_id, created_at);
