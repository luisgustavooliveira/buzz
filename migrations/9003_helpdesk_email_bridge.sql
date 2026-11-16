-- Optional helpdesk conversation routing. Existing relays remain compatible:
-- every column is nullable and existing writes need no backfill.
ALTER TABLE email_conversations
    ADD COLUMN helpdesk_ticket_id BIGINT,
    ADD COLUMN helpdesk_ticket_number TEXT,
    ADD COLUMN helpdesk_owner_agent_pubkey BYTEA
        CHECK (helpdesk_owner_agent_pubkey IS NULL OR length(helpdesk_owner_agent_pubkey) = 32);

CREATE UNIQUE INDEX idx_email_conversations_helpdesk_ticket
    ON email_conversations (community_id, helpdesk_ticket_id)
    WHERE helpdesk_ticket_id IS NOT NULL;
