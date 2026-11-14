-- SNPS fork: V1 email bridge (migration 9002, originally 0046).
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
