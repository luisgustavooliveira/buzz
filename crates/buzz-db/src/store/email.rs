//! Optional V1 email bridge persistence.
//!
//! All routing reads use the writer pool. Event, conversation, participants and
//! outbox insertion share one transaction so an accepted send cannot lose its
//! durable delivery request.

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row, Transaction};
use uuid::Uuid;

use buzz_core::CommunityId;
use buzz_datastore_tracing::datastore_span;

use crate::error::Result;
use crate::Db;

/// Conversation state. Closed conversations remain readable but accept no mail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmailConversationStatus {
    /// Active conversation.
    Active,
    /// Closed conversation.
    Closed,
}

impl EmailConversationStatus {
    fn from_db(value: &str) -> Result<Self> {
        match value {
            "active" => Ok(Self::Active),
            "closed" => Ok(Self::Closed),
            other => Err(crate::error::DbError::InvalidData(format!(
                "unknown email conversation status: {other}"
            ))),
        }
    }
}

/// Conversation routing record.
#[derive(Debug, Clone)]
pub struct EmailConversation {
    /// Community owning the conversation.
    pub community_id: CommunityId,
    /// Stable conversation identifier, independent of email headers.
    pub id: Uuid,
    /// Buzz channel containing the mirrored thread.
    pub channel_id: Uuid,
    /// Buzz root event containing the thread.
    pub root_event_id: Vec<u8>,
    /// Lifecycle state.
    pub status: EmailConversationStatus,
    /// V2 switch; always false in V1.
    pub external_enabled: bool,
}

/// Participant inserted with a new conversation or a later outbound message.
#[derive(Debug, Clone)]
pub struct EmailParticipantInsert {
    /// Canonical lowercase email address.
    pub address: String,
    /// `relay` in V1; `external` is reserved for V2.
    pub participant_type: String,
    /// Sender, To, Cc or Bcc.
    pub role: String,
    /// Agent identity when the participant is an agent.
    pub agent_pubkey: Option<Vec<u8>>,
    /// Human who approved an external participant; unused in V1.
    pub approved_by: Option<Vec<u8>>,
}

/// Conversation data committed with an outbound event.
#[derive(Debug)]
pub struct EmailConversationInsert<'a> {
    /// Stable conversation ID.
    pub id: Uuid,
    /// Buzz channel.
    pub channel_id: Uuid,
    /// Buzz root event.
    pub root_event_id: &'a [u8],
    /// Event author.
    pub created_by: &'a [u8],
    /// Whether the row must be created.
    pub is_new: bool,
    /// Participants known after this outbound action.
    pub participants: &'a [EmailParticipantInsert],
}

/// Outbox data committed with an outbound event.
#[derive(Debug)]
pub struct EmailOutboxInsert<'a> {
    /// Conversation and participant data.
    pub conversation: EmailConversationInsert<'a>,
    /// Originating event.
    pub event_id: &'a [u8],
    /// Signing agent or human.
    pub author_pubkey: &'a [u8],
    /// Persistent, random RFC 5322 Message-ID.
    pub internet_message_id: &'a str,
    /// SMTP payload, including the derived sender and recipients.
    pub payload: &'a serde_json::Value,
}

/// Accepted inbound message persisted before its Buzz publication.
#[derive(Debug)]
pub struct InboundMessageInsert<'a> {
    /// RFC 5322 Message-ID.
    pub internet_message_id: &'a str,
    /// Receiving Exchange mailbox.
    pub mailbox_id: &'a str,
    /// Conversation ID.
    pub conversation_id: Uuid,
    /// Immediate parent Message-ID.
    pub parent_message_id: Option<&'a str>,
    /// Canonical sender address.
    pub sender_address: &'a str,
    /// Graph message ID.
    pub graph_message_id: &'a str,
}

/// Quarantine metadata; message bodies stay in Exchange.
#[derive(Debug)]
pub struct QuarantineInsert<'a> {
    /// Receiving mailbox.
    pub mailbox_id: &'a str,
    /// Graph message ID.
    pub graph_message_id: &'a str,
    /// Optional Message-ID.
    pub internet_message_id: Option<&'a str>,
    /// Optional sender address.
    pub sender_address: Option<&'a str>,
    /// Stable policy reason.
    pub reason: &'a str,
    /// Thread IDs observed.
    pub references_seen: &'a [String],
    /// Authentication metadata.
    pub authentication: &'a serde_json::Value,
}

/// Claimed outbound row.
#[derive(Debug, Clone)]
pub struct EmailOutboxRow {
    /// Outbox ID.
    pub id: Uuid,
    /// Community owning the row.
    pub community_id: CommunityId,
    /// Conversation ID.
    pub conversation_id: Uuid,
    /// Originating Buzz event.
    pub event_id: Vec<u8>,
    /// Signing identity.
    pub author_pubkey: Vec<u8>,
    /// Persistent Message-ID used across delivery retries.
    pub internet_message_id: String,
    /// SMTP payload.
    pub payload: serde_json::Value,
    /// Attempt number of this claim.
    pub attempts: i32,
}

fn row_to_conversation(row: &sqlx::postgres::PgRow) -> Result<EmailConversation> {
    Ok(EmailConversation {
        community_id: CommunityId::from_uuid(row.try_get("community_id")?),
        id: row.try_get("id")?,
        channel_id: row.try_get("channel_id")?,
        root_event_id: row.try_get("root_event_id")?,
        status: EmailConversationStatus::from_db(row.try_get("status")?)?,
        external_enabled: row.try_get("external_enabled")?,
    })
}

fn row_to_outbox(row: &sqlx::postgres::PgRow) -> Result<EmailOutboxRow> {
    Ok(EmailOutboxRow {
        id: row.try_get("id")?,
        community_id: CommunityId::from_uuid(row.try_get("community_id")?),
        conversation_id: row.try_get("conversation_id")?,
        event_id: row.try_get("event_id")?,
        author_pubkey: row.try_get("author_pubkey")?,
        internet_message_id: row.try_get("internet_message_id")?,
        payload: row.try_get("payload")?,
        attempts: row.try_get("attempts")?,
    })
}

/// Fetch a conversation by its Buzz thread root.
pub async fn get_conversation_by_root(
    pool: &PgPool,
    community_id: CommunityId,
    root_event_id: &[u8],
) -> Result<Option<EmailConversation>> {
    let row = sqlx::query(
        r#"SELECT community_id, id, channel_id, root_event_id, status, external_enabled
           FROM email_conversations
           WHERE community_id = $1 AND root_event_id = $2"#,
    )
    .bind(community_id.as_uuid())
    .bind(root_event_id)
    .fetch_optional(pool)
    .await?;
    row.map(|row| row_to_conversation(&row)).transpose()
}

/// Fetch a conversation by ID.
pub async fn get_conversation(
    pool: &PgPool,
    community_id: CommunityId,
    id: Uuid,
) -> Result<Option<EmailConversation>> {
    let row = sqlx::query(
        r#"SELECT community_id, id, channel_id, root_event_id, status, external_enabled
           FROM email_conversations WHERE community_id = $1 AND id = $2"#,
    )
    .bind(community_id.as_uuid())
    .bind(id)
    .fetch_optional(pool)
    .await?;
    row.map(|row| row_to_conversation(&row)).transpose()
}

/// Resolve all conversations referenced by Message-IDs. Zero or multiple rows
/// are both non-routable; the caller decides how to quarantine each case.
pub async fn find_conversations_for_message_ids(
    pool: &PgPool,
    community_id: CommunityId,
    message_ids: &[String],
) -> Result<Vec<EmailConversation>> {
    if message_ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        r#"SELECT DISTINCT c.community_id, c.id, c.channel_id, c.root_event_id,
                  c.status, c.external_enabled
           FROM email_conversations c
           JOIN email_messages m
             ON m.community_id = c.community_id AND m.conversation_id = c.id
           WHERE c.community_id = $1 AND m.internet_message_id = ANY($2)"#,
    )
    .bind(community_id.as_uuid())
    .bind(message_ids)
    .fetch_all(pool)
    .await?;
    rows.iter().map(row_to_conversation).collect()
}

/// Return the newest known Message-ID in a conversation for the next reply.
pub async fn latest_message_id(
    pool: &PgPool,
    community_id: CommunityId,
    conversation_id: Uuid,
) -> Result<Option<String>> {
    let row = sqlx::query(
        r#"SELECT internet_message_id FROM email_messages
           WHERE community_id = $1 AND conversation_id = $2
           ORDER BY created_at DESC, internet_message_id DESC LIMIT 1"#,
    )
    .bind(community_id.as_uuid())
    .bind(conversation_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|row| row.get("internet_message_id")))
}

/// Whether an agent is a relay participant of a conversation.
pub async fn conversation_has_agent(
    pool: &PgPool,
    community_id: CommunityId,
    conversation_id: Uuid,
    agent_pubkey: &[u8],
) -> Result<bool> {
    let exists = sqlx::query(
        r#"SELECT 1 FROM email_participants
           WHERE community_id = $1 AND conversation_id = $2
             AND agent_pubkey = $3 AND participant_type = 'relay'"#,
    )
    .bind(community_id.as_uuid())
    .bind(conversation_id)
    .bind(agent_pubkey)
    .fetch_optional(pool)
    .await?
    .is_some();
    Ok(exists)
}

/// Whether an inbound message was fully published to Buzz. A row with a NULL
/// Buzz event is retryable and must not suppress delivery.
pub async fn inbound_message_is_delivered(
    pool: &PgPool,
    community_id: CommunityId,
    internet_message_id: &str,
    mailbox_id: &str,
) -> Result<bool> {
    let exists = sqlx::query(
        r#"SELECT 1 FROM email_messages
           WHERE community_id = $1 AND internet_message_id = $2 AND mailbox_id = $3
             AND direction = 'inbound' AND buzz_event_id IS NOT NULL"#,
    )
    .bind(community_id.as_uuid())
    .bind(internet_message_id)
    .bind(mailbox_id)
    .fetch_optional(pool)
    .await?
    .is_some();
    Ok(exists)
}

/// Whether an address is already a participant in the conversation.
pub async fn conversation_has_address(
    pool: &PgPool,
    community_id: CommunityId,
    conversation_id: Uuid,
    address: &str,
) -> Result<bool> {
    let exists = sqlx::query(
        r#"SELECT 1 FROM email_participants
           WHERE community_id = $1 AND conversation_id = $2
             AND address = $3 AND participant_type = 'relay'"#,
    )
    .bind(community_id.as_uuid())
    .bind(conversation_id)
    .bind(address)
    .fetch_optional(pool)
    .await?
    .is_some();
    Ok(exists)
}

/// Count recent messages authored by agents in a conversation. Used as a hard
/// circuit breaker for agent-to-agent email loops.
pub async fn recent_agent_message_count(
    pool: &PgPool,
    community_id: CommunityId,
    conversation_id: Uuid,
    since: DateTime<Utc>,
) -> Result<i64> {
    let row = sqlx::query(
        r#"SELECT COUNT(*) AS count
           FROM email_messages m
           WHERE m.community_id = $1 AND m.conversation_id = $2
             AND m.created_at >= $3
             AND EXISTS (
               SELECT 1 FROM email_participants p
               WHERE p.community_id = m.community_id
                 AND p.conversation_id = m.conversation_id
                 AND p.address = m.sender_address
                 AND p.agent_pubkey IS NOT NULL
             )"#,
    )
    .bind(community_id.as_uuid())
    .bind(conversation_id)
    .bind(since)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get("count")?)
}

/// Mark an accepted inbound message as published after the Buzz event commits.
pub async fn mark_inbound_delivered(
    pool: &PgPool,
    community_id: CommunityId,
    internet_message_id: &str,
    mailbox_id: &str,
    buzz_event_id: &[u8],
) -> Result<()> {
    sqlx::query(
        r#"UPDATE email_messages SET buzz_event_id = $4
           WHERE community_id = $1 AND internet_message_id = $2
             AND mailbox_id = $3 AND direction = 'inbound'"#,
    )
    .bind(community_id.as_uuid())
    .bind(internet_message_id)
    .bind(mailbox_id)
    .bind(buzz_event_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Store an accepted inbound message before waking the agent.
pub async fn record_inbound_message(
    pool: &PgPool,
    community_id: CommunityId,
    insert: InboundMessageInsert<'_>,
) -> Result<bool> {
    let result = sqlx::query(
        r#"INSERT INTO email_messages
             (community_id, internet_message_id, mailbox_id, conversation_id,
              parent_message_id, direction, sender_address, graph_message_id)
           VALUES ($1, $2, $3, $4, $5, 'inbound', $6, $7)
           ON CONFLICT DO NOTHING"#,
    )
    .bind(community_id.as_uuid())
    .bind(insert.internet_message_id)
    .bind(insert.mailbox_id)
    .bind(insert.conversation_id)
    .bind(insert.parent_message_id)
    .bind(insert.sender_address)
    .bind(insert.graph_message_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Record a quarantined message without its body. The Exchange message remains
/// untouched, so an operator can inspect or recover it without Mail.ReadWrite.
pub async fn record_quarantine(
    pool: &PgPool,
    community_id: CommunityId,
    insert: QuarantineInsert<'_>,
) -> Result<bool> {
    let result = sqlx::query(
        r#"INSERT INTO email_quarantine
             (community_id, mailbox_id, graph_message_id, internet_message_id,
              sender_address, reason, references_seen, authentication)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
           ON CONFLICT DO NOTHING"#,
    )
    .bind(community_id.as_uuid())
    .bind(insert.mailbox_id)
    .bind(insert.graph_message_id)
    .bind(insert.internet_message_id)
    .bind(insert.sender_address)
    .bind(insert.reason)
    .bind(serde_json::to_value(insert.references_seen)?)
    .bind(insert.authentication)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Atomically persist an event, its thread/mentions, conversation,
/// participants and outbound delivery request.
pub async fn insert_event_with_email_outbox(
    pool: &PgPool,
    community_id: CommunityId,
    event: &nostr::Event,
    channel_id: Option<Uuid>,
    thread_meta: Option<crate::event::ThreadMetadataParams<'_>>,
    outbox: EmailOutboxInsert<'_>,
) -> Result<(buzz_core::StoredEvent, bool)> {
    let connection = crate::observability::acquire_writer(
        pool,
        crate::observability::WriterOperation::EventWrite,
    )
    .await?;
    let mut tx = sqlx::Transaction::begin(connection, None).await?;
    let (stored_event, was_inserted) = crate::event::insert_event_with_thread_metadata_tx(
        &mut tx,
        community_id,
        event,
        channel_id,
        thread_meta,
    )
    .await?;
    if was_inserted {
        crate::insert_mentions_in_transaction(&mut tx, community_id, event, channel_id).await?;
        insert_email_outbox_tx(&mut tx, community_id, outbox).await?;
    }
    tx.commit().await?;
    Ok((stored_event, was_inserted))
}

async fn insert_email_outbox_tx(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    community_id: CommunityId,
    outbox: EmailOutboxInsert<'_>,
) -> Result<()> {
    let conversation = outbox.conversation;
    if conversation.is_new {
        sqlx::query(
            r#"INSERT INTO email_conversations
                 (community_id, id, channel_id, root_event_id, created_by)
               VALUES ($1, $2, $3, $4, $5)"#,
        )
        .bind(community_id.as_uuid())
        .bind(conversation.id)
        .bind(conversation.channel_id)
        .bind(conversation.root_event_id)
        .bind(conversation.created_by)
        .execute(&mut **tx)
        .await?;
    }
    for participant in conversation.participants {
        sqlx::query(
            r#"INSERT INTO email_participants
                 (community_id, conversation_id, address, participant_type, role,
                  agent_pubkey, approved_by)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT DO NOTHING"#,
        )
        .bind(community_id.as_uuid())
        .bind(conversation.id)
        .bind(&participant.address)
        .bind(&participant.participant_type)
        .bind(&participant.role)
        .bind(&participant.agent_pubkey)
        .bind(&participant.approved_by)
        .execute(&mut **tx)
        .await?;
    }
    sqlx::query(
        r#"INSERT INTO email_outbox
             (community_id, conversation_id, event_id, author_pubkey,
              internet_message_id, payload)
           VALUES ($1, $2, $3, $4, $5, $6)"#,
    )
    .bind(community_id.as_uuid())
    .bind(conversation.id)
    .bind(outbox.event_id)
    .bind(outbox.author_pubkey)
    .bind(outbox.internet_message_id)
    .bind(outbox.payload)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Claim due outbound rows with a lease and FOR UPDATE SKIP LOCKED.
pub async fn claim_due_outbox(
    pool: &PgPool,
    now: DateTime<Utc>,
    limit: i64,
    lease_until: DateTime<Utc>,
) -> Result<Vec<EmailOutboxRow>> {
    let rows = sqlx::query(
        r#"WITH candidates AS (
             SELECT community_id, id FROM email_outbox
             WHERE status = 'pending' AND next_retry_at <= $1
               AND community_write_allowed(community_id)
             ORDER BY next_retry_at, created_at
             FOR UPDATE SKIP LOCKED LIMIT $2
           )
           UPDATE email_outbox o
           SET attempts = o.attempts + 1, next_retry_at = $3
           FROM candidates c
           WHERE o.community_id = c.community_id AND o.id = c.id
           RETURNING o.id, o.community_id, o.conversation_id, o.event_id,
                     o.author_pubkey, o.internet_message_id, o.payload, o.attempts"#,
    )
    .bind(now)
    .bind(limit.clamp(1, 100))
    .bind(lease_until)
    .fetch_all(pool)
    .await?;
    rows.iter().map(row_to_outbox).collect()
}

/// Mark a row for a later retry.
pub async fn mark_outbox_retry(
    pool: &PgPool,
    community_id: CommunityId,
    id: Uuid,
    error: &str,
    next_retry_at: DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        r#"UPDATE email_outbox SET last_error = $3, next_retry_at = $4
           WHERE community_id = $1 AND id = $2 AND status = 'pending'"#,
    )
    .bind(community_id.as_uuid())
    .bind(id)
    .bind(error)
    .bind(next_retry_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark a row terminally failed.
pub async fn mark_outbox_failed(
    pool: &PgPool,
    community_id: CommunityId,
    id: Uuid,
    error: &str,
) -> Result<()> {
    sqlx::query(
        r#"UPDATE email_outbox SET status = 'failed', last_error = $3
           WHERE community_id = $1 AND id = $2 AND status = 'pending'"#,
    )
    .bind(community_id.as_uuid())
    .bind(id)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark a row sent and register its Message-ID for inbound threading.
pub async fn mark_outbox_sent(pool: &PgPool, community_id: CommunityId, id: Uuid) -> Result<()> {
    let mut tx = pool.begin().await?;
    let row = sqlx::query(
        r#"UPDATE email_outbox SET status = 'sent', sent_at = now(), last_error = NULL
           WHERE community_id = $1 AND id = $2 AND status = 'pending'
           RETURNING conversation_id, event_id, internet_message_id, payload"#,
    )
    .bind(community_id.as_uuid())
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else {
        return Ok(());
    };
    let conversation_id: Uuid = row.try_get("conversation_id")?;
    let event_id: Vec<u8> = row.try_get("event_id")?;
    let internet_message_id: String = row.try_get("internet_message_id")?;
    let payload: serde_json::Value = row.try_get("payload")?;
    let mailbox_id = payload
        .get("mailbox_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            crate::error::DbError::InvalidData("email payload missing mailbox_id".into())
        })?;
    let sender = payload
        .get("from_address")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            crate::error::DbError::InvalidData("email payload missing from_address".into())
        })?;
    let parent = payload
        .get("in_reply_to")
        .and_then(serde_json::Value::as_str);
    sqlx::query(
        r#"INSERT INTO email_messages
             (community_id, internet_message_id, mailbox_id, conversation_id,
              parent_message_id, direction, sender_address, buzz_event_id)
           VALUES ($1, $2, $3, $4, $5, 'outbound', $6, $7)
           ON CONFLICT DO NOTHING"#,
    )
    .bind(community_id.as_uuid())
    .bind(&internet_message_id)
    .bind(mailbox_id)
    .bind(conversation_id)
    .bind(parent)
    .bind(sender)
    .bind(event_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

impl Db {
    /// Fetch an email conversation by thread root.
    #[datastore_span(name = "email_get_conversation_by_root", system = "postgresql")]
    pub async fn email_get_conversation_by_root(
        &self,
        community_id: CommunityId,
        root_event_id: &[u8],
    ) -> Result<Option<EmailConversation>> {
        get_conversation_by_root(&self.pool, community_id, root_event_id).await
    }

    /// Atomically insert an event and its email bridge records.
    #[datastore_span(name = "insert_event_with_email_outbox", system = "postgresql")]
    pub async fn insert_event_with_email_outbox(
        &self,
        community_id: CommunityId,
        event: &nostr::Event,
        channel_id: Option<Uuid>,
        thread_meta: Option<crate::event::ThreadMetadataParams<'_>>,
        outbox: EmailOutboxInsert<'_>,
    ) -> Result<(buzz_core::StoredEvent, bool)> {
        insert_event_with_email_outbox(
            &self.pool,
            community_id,
            event,
            channel_id,
            thread_meta,
            outbox,
        )
        .await
    }

    /// Fetch an email conversation by ID.
    pub async fn email_get_conversation(
        &self,
        community_id: CommunityId,
        id: Uuid,
    ) -> Result<Option<EmailConversation>> {
        get_conversation(&self.pool, community_id, id).await
    }

    /// Claim due email outbox rows.
    #[datastore_span(name = "email_claim_due_outbox", system = "postgresql")]
    pub async fn email_claim_due_outbox(
        &self,
        now: DateTime<Utc>,
        limit: i64,
        lease_until: DateTime<Utc>,
    ) -> Result<Vec<EmailOutboxRow>> {
        claim_due_outbox(&self.pool, now, limit, lease_until).await
    }

    /// Retry an email outbox row.
    pub async fn email_mark_outbox_retry(
        &self,
        community_id: CommunityId,
        id: Uuid,
        error: &str,
        next_retry_at: DateTime<Utc>,
    ) -> Result<()> {
        mark_outbox_retry(&self.pool, community_id, id, error, next_retry_at).await
    }

    /// Fail an email outbox row.
    pub async fn email_mark_outbox_failed(
        &self,
        community_id: CommunityId,
        id: Uuid,
        error: &str,
    ) -> Result<()> {
        mark_outbox_failed(&self.pool, community_id, id, error).await
    }

    /// Mark an email outbox row sent.
    pub async fn email_mark_outbox_sent(&self, community_id: CommunityId, id: Uuid) -> Result<()> {
        mark_outbox_sent(&self.pool, community_id, id).await
    }

    /// Resolve conversations by email threading IDs.
    pub async fn email_find_conversations_for_message_ids(
        &self,
        community_id: CommunityId,
        message_ids: &[String],
    ) -> Result<Vec<EmailConversation>> {
        find_conversations_for_message_ids(&self.pool, community_id, message_ids).await
    }

    /// Return the newest Message-ID in a conversation.
    pub async fn email_latest_message_id(
        &self,
        community_id: CommunityId,
        conversation_id: Uuid,
    ) -> Result<Option<String>> {
        latest_message_id(&self.pool, community_id, conversation_id).await
    }

    /// Check conversation agent membership.
    pub async fn email_conversation_has_agent(
        &self,
        community_id: CommunityId,
        conversation_id: Uuid,
        agent_pubkey: &[u8],
    ) -> Result<bool> {
        conversation_has_agent(&self.pool, community_id, conversation_id, agent_pubkey).await
    }

    /// Check whether inbound delivery already published a Buzz event.
    pub async fn email_inbound_message_is_delivered(
        &self,
        community_id: CommunityId,
        internet_message_id: &str,
        mailbox_id: &str,
    ) -> Result<bool> {
        inbound_message_is_delivered(&self.pool, community_id, internet_message_id, mailbox_id)
            .await
    }

    /// Check whether an address is a relay participant in a conversation.
    pub async fn email_conversation_has_address(
        &self,
        community_id: CommunityId,
        conversation_id: Uuid,
        address: &str,
    ) -> Result<bool> {
        conversation_has_address(&self.pool, community_id, conversation_id, address).await
    }

    /// Count recent agent messages for the loop circuit breaker.
    pub async fn email_recent_agent_message_count(
        &self,
        community_id: CommunityId,
        conversation_id: Uuid,
        since: DateTime<Utc>,
    ) -> Result<i64> {
        recent_agent_message_count(&self.pool, community_id, conversation_id, since).await
    }

    /// Mark inbound mail published after its Buzz event commits.
    pub async fn email_mark_inbound_delivered(
        &self,
        community_id: CommunityId,
        internet_message_id: &str,
        mailbox_id: &str,
        buzz_event_id: &[u8],
    ) -> Result<()> {
        mark_inbound_delivered(
            &self.pool,
            community_id,
            internet_message_id,
            mailbox_id,
            buzz_event_id,
        )
        .await
    }

    /// Record accepted inbound mail.
    pub async fn email_record_inbound_message(
        &self,
        community_id: CommunityId,
        insert: InboundMessageInsert<'_>,
    ) -> Result<bool> {
        record_inbound_message(&self.pool, community_id, insert).await
    }

    /// Record quarantined inbound mail without its body.
    pub async fn email_record_quarantine(
        &self,
        community_id: CommunityId,
        insert: QuarantineInsert<'_>,
    ) -> Result<bool> {
        record_quarantine(&self.pool, community_id, insert).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires Postgres with migration 0046"]
    async fn event_conversation_participants_and_outbox_are_atomic_and_idempotent() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&crate::test_support::database_url())
            .await
            .expect("connect email test pool");
        let community_uuid = Uuid::new_v4();
        sqlx::query("INSERT INTO communities (id, host) VALUES ($1, $2)")
            .bind(community_uuid)
            .bind(format!("email-bridge-{community_uuid}.example"))
            .execute(&pool)
            .await
            .expect("insert community");
        let community_id = CommunityId::from_uuid(community_uuid);
        let channel_id = Uuid::new_v4();
        let author = vec![0x42; 32];
        sqlx::query(
            r#"INSERT INTO channels (id, community_id, name, channel_type, visibility, created_by)
               VALUES ($1, $2, 'email-test', 'standard', 'open', $3)"#,
        )
        .bind(channel_id)
        .bind(community_uuid)
        .bind(&author)
        .execute(&pool)
        .await
        .expect("insert channel");
        let keys = nostr::Keys::generate();
        let event = nostr::EventBuilder::new(
            nostr::Kind::Custom(buzz_core::kind::KIND_STREAM_MESSAGE as u16),
            "email body",
        )
        .tags([nostr::Tag::parse(["h", &channel_id.to_string()]).expect("channel tag")])
        .sign_with_keys(&keys)
        .expect("sign event");
        let conversation_id = Uuid::new_v4();
        let message_id = "<random@example.com>";
        let participants = vec![EmailParticipantInsert {
            address: "owner@example.com".into(),
            participant_type: "relay".into(),
            role: "to".into(),
            agent_pubkey: None,
            approved_by: None,
        }];
        let payload =
            serde_json::json!({"mailbox_id": "mailbox", "from_address": "agent@example.com"});
        let insert = EmailOutboxInsert {
            conversation: EmailConversationInsert {
                id: conversation_id,
                channel_id,
                root_event_id: event.id.as_bytes(),
                created_by: event.pubkey.as_bytes(),
                is_new: true,
                participants: &participants,
            },
            event_id: event.id.as_bytes(),
            author_pubkey: event.pubkey.as_bytes(),
            internet_message_id: message_id,
            payload: &payload,
        };
        let (_, inserted) = insert_event_with_email_outbox(
            &pool,
            community_id,
            &event,
            Some(channel_id),
            None,
            insert,
        )
        .await
        .expect("atomic insert");
        assert!(inserted);
        let (_, inserted_again) = insert_event_with_email_outbox(
            &pool,
            community_id,
            &event,
            Some(channel_id),
            None,
            EmailOutboxInsert {
                conversation: EmailConversationInsert {
                    id: conversation_id,
                    channel_id,
                    root_event_id: event.id.as_bytes(),
                    created_by: event.pubkey.as_bytes(),
                    is_new: true,
                    participants: &participants,
                },
                event_id: event.id.as_bytes(),
                author_pubkey: event.pubkey.as_bytes(),
                internet_message_id: message_id,
                payload: &payload,
            },
        )
        .await
        .expect("duplicate insert");
        assert!(!inserted_again);
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM email_outbox WHERE community_id = $1")
                .bind(community_uuid)
                .fetch_one(&pool)
                .await
                .expect("count outbox");
        assert_eq!(count, 1);

        for table in [
            "email_outbox",
            "email_messages",
            "email_participants",
            "email_conversations",
            "email_quarantine",
        ] {
            let sql = match table {
                "email_outbox" => "DELETE FROM email_outbox WHERE community_id = $1",
                "email_messages" => "DELETE FROM email_messages WHERE community_id = $1",
                "email_participants" => "DELETE FROM email_participants WHERE community_id = $1",
                "email_conversations" => "DELETE FROM email_conversations WHERE community_id = $1",
                "email_quarantine" => "DELETE FROM email_quarantine WHERE community_id = $1",
                _ => unreachable!("fixed cleanup table list"),
            };
            sqlx::query(sql)
                .bind(community_uuid)
                .execute(&pool)
                .await
                .expect("clean email table");
        }
        sqlx::query("DELETE FROM communities WHERE id = $1")
            .bind(community_uuid)
            .execute(&pool)
            .await
            .expect("clean community");
    }
}
