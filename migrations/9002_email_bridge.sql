-- Optional V1 email bridge: durable conversations, participants, messages,
-- outbound delivery and quarantine. External participants are represented in
-- the model but rejected by V1 policy; this avoids a V2 schema migration.

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

SELECT attach_community_write_fence('email_conversations');

CREATE TABLE email_participants (
    community_id          UUID NOT NULL,
    conversation_id       UUID NOT NULL,
    address               TEXT NOT NULL,
    participant_type      TEXT NOT NULL
                          CHECK (participant_type IN ('relay', 'external')),
    role                  TEXT NOT NULL
                          CHECK (role IN ('sender', 'to', 'cc', 'bcc')),
    agent_pubkey          BYTEA CHECK (agent_pubkey IS NULL OR length(agent_pubkey) = 32),
    approved_by           BYTEA CHECK (approved_by IS NULL OR length(approved_by) = 32),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (community_id, conversation_id, address, role),
    FOREIGN KEY (community_id, conversation_id)
        REFERENCES email_conversations (community_id, id)
);

SELECT attach_community_write_fence('email_participants');

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

SELECT attach_community_write_fence('email_messages');

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

SELECT attach_community_write_fence('email_outbox');

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

SELECT attach_community_write_fence('email_quarantine');
