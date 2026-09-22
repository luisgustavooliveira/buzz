//! GitHub bridge persistence -- `github_thread_links` and `github_outbox`.
//!
//! `github_thread_links` maps a GitHub `(repo, subject_type, subject_number)`
//! triple to the Buzz thread that mirrors it, scoped per community.
//! `github_outbox` is the durable queue of outbound GitHub API actions
//! (comment, open issue) that the bridge worker drains with retry/backoff.
//!
//! All IDs are native Postgres UUID columns; event ids and pubkeys are raw
//! 32-byte BYTEA. Never uses string interpolation for query values -- all user
//! data goes through bind parameters.
//!
//! Read-your-writes note: every read here influences a bridge write or routing
//! decision (thread lookup gates inbound replies; `is_known_comment_id` gates
//! echo suppression), so all queries run on the writer pool rather than the
//! routed-replica fast path.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use buzz_core::CommunityId;
use buzz_datastore_tracing::datastore_span;

use crate::error::{DbError, Result};
use crate::Db;

// -- Enums --------------------------------------------------------------------

/// Kind of GitHub subject a thread link points at.
///
/// Phase 1 handles issues only; `Pr` is reserved so the primary-key space does
/// not change when PR support lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GithubSubjectType {
    /// A GitHub issue.
    Issue,
    /// A GitHub pull request (reserved for a later phase).
    Pr,
}

impl fmt::Display for GithubSubjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GithubSubjectType::Issue => write!(f, "issue"),
            GithubSubjectType::Pr => write!(f, "pr"),
        }
    }
}

impl FromStr for GithubSubjectType {
    type Err = DbError;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "issue" => Ok(GithubSubjectType::Issue),
            "pr" => Ok(GithubSubjectType::Pr),
            other => Err(DbError::InvalidData(format!(
                "unknown github subject type: {other}"
            ))),
        }
    }
}

/// Lifecycle status of a thread link. A closed link still resolves inbound
/// lookups but no longer routes new GitHub comments to an agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GithubLinkStatus {
    /// The GitHub subject is open; inbound comments route to the last agent.
    Open,
    /// The GitHub subject is closed; inbound activity is recorded only.
    Closed,
}

impl fmt::Display for GithubLinkStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GithubLinkStatus::Open => write!(f, "open"),
            GithubLinkStatus::Closed => write!(f, "closed"),
        }
    }
}

impl FromStr for GithubLinkStatus {
    type Err = DbError;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "open" => Ok(GithubLinkStatus::Open),
            "closed" => Ok(GithubLinkStatus::Closed),
            other => Err(DbError::InvalidData(format!(
                "unknown github link status: {other}"
            ))),
        }
    }
}

/// Outbound GitHub action carried by an outbox row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GithubOutboxAction {
    /// Post a comment on the linked issue.
    Comment,
    /// Open a new issue; `link_number` is NULL until GitHub assigns one.
    OpenIssue,
}

impl fmt::Display for GithubOutboxAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GithubOutboxAction::Comment => write!(f, "comment"),
            GithubOutboxAction::OpenIssue => write!(f, "open_issue"),
        }
    }
}

impl FromStr for GithubOutboxAction {
    type Err = DbError;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "comment" => Ok(GithubOutboxAction::Comment),
            "open_issue" => Ok(GithubOutboxAction::OpenIssue),
            other => Err(DbError::InvalidData(format!(
                "unknown github outbox action: {other}"
            ))),
        }
    }
}

/// Delivery status of an outbox row: `pending` (claimable, possibly leased),
/// `sent` (terminal success), or `failed` (terminal, attempts exhausted or
/// non-retryable error).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GithubOutboxStatus {
    /// Awaiting delivery; `next_retry_at` is the next claim eligibility time.
    Pending,
    /// Delivered to GitHub; `github_comment_id` is set.
    Sent,
    /// Terminally failed; `last_error` records why.
    Failed,
}

impl fmt::Display for GithubOutboxStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GithubOutboxStatus::Pending => write!(f, "pending"),
            GithubOutboxStatus::Sent => write!(f, "sent"),
            GithubOutboxStatus::Failed => write!(f, "failed"),
        }
    }
}

impl FromStr for GithubOutboxStatus {
    type Err = DbError;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "pending" => Ok(GithubOutboxStatus::Pending),
            "sent" => Ok(GithubOutboxStatus::Sent),
            "failed" => Ok(GithubOutboxStatus::Failed),
            other => Err(DbError::InvalidData(format!(
                "unknown github outbox status: {other}"
            ))),
        }
    }
}

// -- Record types -------------------------------------------------------------

/// A `github_thread_links` row: the bridge's mapping from one GitHub subject
/// to one Buzz thread.
#[derive(Debug, Clone)]
pub struct GithubThreadLink {
    /// Server-resolved community that owns this link.
    pub community_id: CommunityId,
    /// Full repository name in `owner/repo` form.
    pub repo_full_name: String,
    /// Subject kind (`issue`; `pr` reserved).
    pub subject_type: GithubSubjectType,
    /// Issue/PR number within the repository.
    pub subject_number: i32,
    /// Channel hosting the mirrored thread.
    pub channel_id: Uuid,
    /// Raw 32-byte event id of the thread root.
    pub root_event_id: Vec<u8>,
    /// Raw pubkey of the agent that published the most recent outbound reply;
    /// inbound comments route to this agent. NULL until the first publish.
    pub last_agent_pubkey: Option<Vec<u8>>,
    /// Whether the GitHub subject is still open.
    pub status: GithubLinkStatus,
    /// When the link was created.
    pub created_at: DateTime<Utc>,
    /// When the link was last mutated.
    pub updated_at: DateTime<Utc>,
}

/// A `github_outbox` row: one durable outbound GitHub action.
#[derive(Debug, Clone)]
pub struct GithubOutboxRow {
    /// Row identifier (unique within its community).
    pub id: Uuid,
    /// Server-resolved community that owns this action.
    pub community_id: CommunityId,
    /// The outbound action to perform.
    pub action: GithubOutboxAction,
    /// Full repository name in `owner/repo` form.
    pub link_repo: String,
    /// Issue number for `comment`; NULL for `open_issue` until GitHub assigns
    /// one at creation time.
    pub link_number: Option<i32>,
    /// Raw 32-byte id of the kind:9 event that originated this action.
    pub event_id: Vec<u8>,
    /// Raw pubkey of the authoring agent; becomes the link's
    /// `last_agent_pubkey` on success.
    pub author_pubkey: Vec<u8>,
    /// Action-specific payload (`open_issue`: `{title, assignee, labels}`).
    pub payload: serde_json::Value,
    /// Delivery state.
    pub status: GithubOutboxStatus,
    /// Number of delivery attempts claimed so far.
    pub attempts: i32,
    /// Next time a pending row becomes claimable; doubles as the claim lease
    /// while a worker holds the row.
    pub next_retry_at: DateTime<Utc>,
    /// Error from the most recent failed attempt, if any.
    pub last_error: Option<String>,
    /// GitHub's comment id after a successful `comment` delivery; used for
    /// webhook echo suppression.
    pub github_comment_id: Option<i64>,
    /// When the row was created.
    pub created_at: DateTime<Utc>,
}

/// Parameters for inserting an outbox row. Used by both the transactional and
/// pool-based insert paths.
#[derive(Debug)]
pub struct GithubOutboxInsert<'a> {
    /// Server-resolved community that owns the originating event.
    pub community_id: CommunityId,
    /// The outbound action to perform.
    pub action: GithubOutboxAction,
    /// Full repository name in `owner/repo` form.
    pub link_repo: &'a str,
    /// Issue number for `comment`; None for `open_issue`.
    pub link_number: Option<i32>,
    /// Raw 32-byte id of the kind:9 event that originated the action.
    pub event_id: &'a [u8],
    /// Raw pubkey of the authoring agent.
    pub author_pubkey: &'a [u8],
    /// Action-specific payload.
    pub payload: &'a serde_json::Value,
}

// -- Thread links -------------------------------------------------------------

/// Create a thread link for a GitHub subject. Idempotent: a redelivered
/// `issues.opened` webhook that already has a link is a no-op.
///
/// Returns `Ok(true)` when the row was inserted, `Ok(false)` when a link for
/// `(community_id, repo_full_name, subject_type, subject_number)` already
/// existed.
pub async fn create_thread_link(
    pool: &PgPool,
    community_id: CommunityId,
    repo_full_name: &str,
    subject_type: GithubSubjectType,
    subject_number: i32,
    channel_id: Uuid,
    root_event_id: &[u8],
) -> Result<bool> {
    let result = sqlx::query(
        r#"
        INSERT INTO github_thread_links
            (community_id, repo_full_name, subject_type, subject_number,
             channel_id, root_event_id)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(community_id.as_uuid())
    .bind(repo_full_name)
    .bind(subject_type.to_string())
    .bind(subject_number)
    .bind(channel_id)
    .bind(root_event_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Fetch the thread link for a GitHub subject, scoped to its community.
///
/// Returns `Ok(None)` when the subject never passed through the bridge; the
/// inbound webhook path ignores such subjects.
pub async fn get_thread_link(
    pool: &PgPool,
    community_id: CommunityId,
    repo_full_name: &str,
    subject_type: GithubSubjectType,
    subject_number: i32,
) -> Result<Option<GithubThreadLink>> {
    let row = sqlx::query(
        r#"
        SELECT community_id, repo_full_name, subject_type, subject_number,
               channel_id, root_event_id, last_agent_pubkey, status,
               created_at, updated_at
        FROM github_thread_links
        WHERE community_id = $1
          AND repo_full_name = $2
          AND subject_type = $3
          AND subject_number = $4
        "#,
    )
    .bind(community_id.as_uuid())
    .bind(repo_full_name)
    .bind(subject_type.to_string())
    .bind(subject_number)
    .fetch_optional(pool)
    .await?;

    row.map(row_to_thread_link).transpose()
}

/// Record the agent that published the most recent outbound reply on a link.
///
/// Inbound GitHub comments route to this agent. Touches `updated_at`. Returns
/// `Ok(false)` when the link does not exist.
pub async fn set_last_agent(
    pool: &PgPool,
    community_id: CommunityId,
    repo_full_name: &str,
    subject_type: GithubSubjectType,
    subject_number: i32,
    agent_pubkey: &[u8],
) -> Result<bool> {
    let result = sqlx::query(
        r#"
        UPDATE github_thread_links
        SET last_agent_pubkey = $5,
            updated_at = now()
        WHERE community_id = $1
          AND repo_full_name = $2
          AND subject_type = $3
          AND subject_number = $4
        "#,
    )
    .bind(community_id.as_uuid())
    .bind(repo_full_name)
    .bind(subject_type.to_string())
    .bind(subject_number)
    .bind(agent_pubkey)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Set a link's lifecycle status (`open`/`closed`), touching `updated_at`.
///
/// Returns `Ok(false)` when the link does not exist.
pub async fn set_status(
    pool: &PgPool,
    community_id: CommunityId,
    repo_full_name: &str,
    subject_type: GithubSubjectType,
    subject_number: i32,
    status: GithubLinkStatus,
) -> Result<bool> {
    let result = sqlx::query(
        r#"
        UPDATE github_thread_links
        SET status = $5,
            updated_at = now()
        WHERE community_id = $1
          AND repo_full_name = $2
          AND subject_type = $3
          AND subject_number = $4
        "#,
    )
    .bind(community_id.as_uuid())
    .bind(repo_full_name)
    .bind(subject_type.to_string())
    .bind(subject_number)
    .bind(status.to_string())
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Fetch the thread link whose thread root is `root_event_id`, scoped to its
/// community — the reverse lookup the ingest hook uses to map a kind:9 reply's
/// thread to the GitHub issue it mirrors.
///
/// `root_event_id` is not unique across links (the primary key is by subject),
/// so a thread that ever mirrors two subjects resolves to the earliest link;
/// current writers only ever create one link per root.
///
/// Returns `Ok(None)` when no link references this root.
pub async fn get_thread_link_by_root(
    pool: &PgPool,
    community_id: CommunityId,
    root_event_id: &[u8],
) -> Result<Option<GithubThreadLink>> {
    let row = sqlx::query(
        r#"
        SELECT community_id, repo_full_name, subject_type, subject_number,
               channel_id, root_event_id, last_agent_pubkey, status,
               created_at, updated_at
        FROM github_thread_links
        WHERE community_id = $1
          AND root_event_id = $2
        ORDER BY created_at
        LIMIT 1
        "#,
    )
    .bind(community_id.as_uuid())
    .bind(root_event_id)
    .fetch_optional(pool)
    .await?;

    row.map(row_to_thread_link).transpose()
}

// -- Outbox -------------------------------------------------------------------

/// Insert an outbox row in a caller-owned transaction.
///
/// This is the transaction-composition seam for event ingest: the relay
/// inserts the kind:9 event and its outbox row atomically (one action = one
/// atomic persist). The caller owns commit or rollback. Returns the new row id.
pub async fn insert_outbox_in_transaction(
    tx: &mut Transaction<'_, Postgres>,
    params: GithubOutboxInsert<'_>,
) -> Result<Uuid> {
    let id = Uuid::new_v4();

    sqlx::query(
        r#"
        INSERT INTO github_outbox
            (id, community_id, action, link_repo, link_number,
             event_id, author_pubkey, payload)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(id)
    .bind(params.community_id.as_uuid())
    .bind(params.action.to_string())
    .bind(params.link_repo)
    .bind(params.link_number)
    .bind(params.event_id)
    .bind(params.author_pubkey)
    .bind(params.payload)
    .execute(&mut **tx)
    .await?;

    Ok(id)
}

/// Insert an outbox row outside an existing transaction. Ingest paths that
/// also write the originating event must use [`insert_outbox_in_transaction`]
/// instead so both writes commit atomically.
pub async fn insert_outbox(pool: &PgPool, params: GithubOutboxInsert<'_>) -> Result<Uuid> {
    let mut tx = pool.begin().await?;
    let id = insert_outbox_in_transaction(&mut tx, params).await?;
    tx.commit().await?;
    Ok(id)
}

/// Atomically insert a kind:9 event and the GitHub outbox row it originates.
///
/// One transaction: the event (with its thread metadata and reply-counter
/// updates), its `p`-tag mention rows, and the `github_outbox` row commit
/// together or not at all — one user action, one atomic persist. A duplicate
/// event (already stored) inserts neither mentions nor the outbox row and
/// returns `was_inserted = false`, so a redelivered publish can never enqueue
/// a second outbound comment.
///
/// Returns `(StoredEvent, was_inserted)`.
pub async fn insert_event_with_github_outbox(
    pool: &PgPool,
    community_id: CommunityId,
    event: &nostr::Event,
    channel_id: Option<Uuid>,
    thread_meta: Option<crate::event::ThreadMetadataParams<'_>>,
    outbox: GithubOutboxInsert<'_>,
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
        insert_outbox_in_transaction(&mut tx, outbox).await?;
    }
    tx.commit().await?;
    Ok((stored_event, was_inserted))
}

/// Claim up to `limit` due outbox rows across all active communities.
///
/// A claim is a single statement: due `pending` rows are locked with
/// `FOR UPDATE SKIP LOCKED` (the established outbox-claim pattern, see
/// `push::claim_due_wakes`), `attempts` is bumped, and `next_retry_at` is moved
/// to `lease_until`. The status stays `pending`: the lease is the fence, so a
/// crashed worker's rows become claimable again once `lease_until` passes,
/// giving at-least-once delivery without a separate claimed state.
///
/// Rows in communities under deletion are skipped via the fleet-wide
/// `community_write_allowed` admission filter, matching every other
/// cross-community sweep in this crate.
pub async fn claim_due_outbox(
    pool: &PgPool,
    now: DateTime<Utc>,
    limit: i64,
    lease_until: DateTime<Utc>,
) -> Result<Vec<GithubOutboxRow>> {
    let limit = limit.clamp(1, 1_000);
    let rows = sqlx::query(
        r#"
        WITH candidates AS (
            SELECT community_id, id
            FROM github_outbox
            WHERE status = 'pending'
              AND next_retry_at <= $1
              AND community_write_allowed(community_id)
            ORDER BY next_retry_at, created_at
            FOR UPDATE SKIP LOCKED
            LIMIT $2
        )
        UPDATE github_outbox o
        SET attempts = o.attempts + 1,
            next_retry_at = $3
        FROM candidates c
        WHERE o.community_id = c.community_id
          AND o.id = c.id
        RETURNING id, community_id, action, link_repo, link_number,
                  event_id, author_pubkey, payload, status, attempts,
                  next_retry_at, last_error, github_comment_id, created_at
        "#,
    )
    .bind(now)
    .bind(limit)
    .bind(lease_until)
    .fetch_all(pool)
    .await?;

    rows.into_iter().map(row_to_outbox_row).collect()
}

/// Mark an outbox row terminally delivered, recording GitHub's comment id for
/// webhook echo suppression and clearing the last error. Returns `Ok(false)`
/// when the row does not exist.
pub async fn mark_outbox_sent(
    pool: &PgPool,
    community_id: CommunityId,
    id: Uuid,
    github_comment_id: Option<i64>,
) -> Result<bool> {
    let result = sqlx::query(
        r#"
        UPDATE github_outbox
        SET status = 'sent',
            github_comment_id = $3,
            last_error = NULL
        WHERE community_id = $1 AND id = $2
        "#,
    )
    .bind(community_id.as_uuid())
    .bind(id)
    .bind(github_comment_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Record a failed attempt and reschedule the row for `next_retry_at`. The row
/// stays `pending`; the backoff schedule is the caller's job. Returns
/// `Ok(false)` when the row does not exist.
pub async fn mark_outbox_retry(
    pool: &PgPool,
    community_id: CommunityId,
    id: Uuid,
    error: &str,
    next_retry_at: DateTime<Utc>,
) -> Result<bool> {
    let result = sqlx::query(
        r#"
        UPDATE github_outbox
        SET status = 'pending',
            last_error = $3,
            next_retry_at = $4
        WHERE community_id = $1 AND id = $2
        "#,
    )
    .bind(community_id.as_uuid())
    .bind(id)
    .bind(error)
    .bind(next_retry_at)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Mark an outbox row terminally failed (attempts exhausted or a
/// non-retryable error). Returns `Ok(false)` when the row does not exist.
pub async fn mark_outbox_failed(
    pool: &PgPool,
    community_id: CommunityId,
    id: Uuid,
    error: &str,
) -> Result<bool> {
    let result = sqlx::query(
        r#"
        UPDATE github_outbox
        SET status = 'failed',
            last_error = $3
        WHERE community_id = $1 AND id = $2
        "#,
    )
    .bind(community_id.as_uuid())
    .bind(id)
    .bind(error)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Return whether `github_comment_id` was produced by this community's bridge.
///
/// Echo suppression for the inbound webhook path: a comment the bridge itself
/// posted arrives back as an `issue_comment.created` webhook and must not
/// re-route into the thread.
pub async fn is_known_comment_id(
    pool: &PgPool,
    community_id: CommunityId,
    github_comment_id: i64,
) -> Result<bool> {
    let known = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM github_outbox
            WHERE community_id = $1 AND github_comment_id = $2
        )
        "#,
    )
    .bind(community_id.as_uuid())
    .bind(github_comment_id)
    .fetch_one(pool)
    .await?;

    Ok(known)
}

// -- Row mappers --------------------------------------------------------------

fn row_to_thread_link(row: sqlx::postgres::PgRow) -> Result<GithubThreadLink> {
    let community_id: Uuid = row.try_get("community_id")?;
    let subject_type_str: String = row.try_get("subject_type")?;
    let status_str: String = row.try_get("status")?;

    Ok(GithubThreadLink {
        community_id: CommunityId::from_uuid(community_id),
        repo_full_name: row.try_get("repo_full_name")?,
        subject_type: subject_type_str.parse::<GithubSubjectType>()?,
        subject_number: row.try_get("subject_number")?,
        channel_id: row.try_get("channel_id")?,
        root_event_id: row.try_get("root_event_id")?,
        last_agent_pubkey: row.try_get("last_agent_pubkey")?,
        status: status_str.parse::<GithubLinkStatus>()?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn row_to_outbox_row(row: sqlx::postgres::PgRow) -> Result<GithubOutboxRow> {
    let community_id: Uuid = row.try_get("community_id")?;
    let action_str: String = row.try_get("action")?;
    let status_str: String = row.try_get("status")?;

    Ok(GithubOutboxRow {
        id: row.try_get("id")?,
        community_id: CommunityId::from_uuid(community_id),
        action: action_str.parse::<GithubOutboxAction>()?,
        link_repo: row.try_get("link_repo")?,
        link_number: row.try_get("link_number")?,
        event_id: row.try_get("event_id")?,
        author_pubkey: row.try_get("author_pubkey")?,
        payload: row.try_get("payload")?,
        status: status_str.parse::<GithubOutboxStatus>()?,
        attempts: row.try_get("attempts")?,
        next_retry_at: row.try_get("next_retry_at")?,
        last_error: row.try_get("last_error")?,
        github_comment_id: row.try_get("github_comment_id")?,
        created_at: row.try_get("created_at")?,
    })
}

// -- Db API -------------------------------------------------------------------

impl Db {
    /// Create a GitHub thread link. Returns `false` when the link already exists.
    #[datastore_span(name = "github_create_thread_link", system = "postgresql")]
    pub async fn github_create_thread_link(
        &self,
        community_id: CommunityId,
        repo_full_name: &str,
        subject_type: GithubSubjectType,
        subject_number: i32,
        channel_id: Uuid,
        root_event_id: &[u8],
    ) -> Result<bool> {
        crate::github::create_thread_link(
            &self.pool,
            community_id,
            repo_full_name,
            subject_type,
            subject_number,
            channel_id,
            root_event_id,
        )
        .await
    }

    /// Fetch the GitHub thread link for a subject, scoped to its community.
    #[datastore_span(name = "github_get_thread_link", system = "postgresql")]
    pub async fn github_get_thread_link(
        &self,
        community_id: CommunityId,
        repo_full_name: &str,
        subject_type: GithubSubjectType,
        subject_number: i32,
    ) -> Result<Option<GithubThreadLink>> {
        crate::github::get_thread_link(
            &self.pool,
            community_id,
            repo_full_name,
            subject_type,
            subject_number,
        )
        .await
    }

    /// Fetch the thread link whose thread root is `root_event_id` (ingest-hook
    /// reverse lookup). Returns the earliest link when several share a root.
    #[datastore_span(name = "github_get_thread_link_by_root", system = "postgresql")]
    pub async fn github_get_thread_link_by_root(
        &self,
        community_id: CommunityId,
        root_event_id: &[u8],
    ) -> Result<Option<GithubThreadLink>> {
        crate::github::get_thread_link_by_root(&self.pool, community_id, root_event_id).await
    }

    /// Record the agent that published the most recent outbound reply on a link.
    #[datastore_span(name = "github_set_last_agent", system = "postgresql")]
    pub async fn github_set_last_agent(
        &self,
        community_id: CommunityId,
        repo_full_name: &str,
        subject_type: GithubSubjectType,
        subject_number: i32,
        agent_pubkey: &[u8],
    ) -> Result<bool> {
        crate::github::set_last_agent(
            &self.pool,
            community_id,
            repo_full_name,
            subject_type,
            subject_number,
            agent_pubkey,
        )
        .await
    }

    /// Set a link's lifecycle status, touching `updated_at`.
    #[datastore_span(name = "github_set_link_status", system = "postgresql")]
    pub async fn github_set_link_status(
        &self,
        community_id: CommunityId,
        repo_full_name: &str,
        subject_type: GithubSubjectType,
        subject_number: i32,
        status: GithubLinkStatus,
    ) -> Result<bool> {
        crate::github::set_status(
            &self.pool,
            community_id,
            repo_full_name,
            subject_type,
            subject_number,
            status,
        )
        .await
    }

    /// Claim due GitHub outbox rows for the bridge worker.
    #[datastore_span(name = "github_claim_due_outbox", system = "postgresql")]
    pub async fn github_claim_due_outbox(
        &self,
        now: DateTime<Utc>,
        limit: i64,
        lease_until: DateTime<Utc>,
    ) -> Result<Vec<GithubOutboxRow>> {
        crate::github::claim_due_outbox(&self.pool, now, limit, lease_until).await
    }

    /// Mark an outbox row terminally delivered, recording GitHub's comment id.
    #[datastore_span(name = "github_mark_outbox_sent", system = "postgresql")]
    pub async fn github_mark_outbox_sent(
        &self,
        community_id: CommunityId,
        id: Uuid,
        github_comment_id: Option<i64>,
    ) -> Result<bool> {
        crate::github::mark_outbox_sent(&self.pool, community_id, id, github_comment_id).await
    }

    /// Record a failed outbox attempt and reschedule the row.
    #[datastore_span(name = "github_mark_outbox_retry", system = "postgresql")]
    pub async fn github_mark_outbox_retry(
        &self,
        community_id: CommunityId,
        id: Uuid,
        error: &str,
        next_retry_at: DateTime<Utc>,
    ) -> Result<bool> {
        crate::github::mark_outbox_retry(&self.pool, community_id, id, error, next_retry_at).await
    }

    /// Mark an outbox row terminally failed.
    #[datastore_span(name = "github_mark_outbox_failed", system = "postgresql")]
    pub async fn github_mark_outbox_failed(
        &self,
        community_id: CommunityId,
        id: Uuid,
        error: &str,
    ) -> Result<bool> {
        crate::github::mark_outbox_failed(&self.pool, community_id, id, error).await
    }

    /// Return whether a GitHub comment id was produced by this community's
    /// bridge (webhook echo suppression).
    #[datastore_span(name = "github_is_known_comment_id", system = "postgresql")]
    pub async fn github_is_known_comment_id(
        &self,
        community_id: CommunityId,
        github_comment_id: i64,
    ) -> Result<bool> {
        crate::github::is_known_comment_id(&self.pool, community_id, github_comment_id).await
    }

    /// Atomically insert a kind:9 event and the GitHub outbox row it
    /// originates (event + thread metadata + mentions + outbox in one
    /// transaction). Duplicates insert neither mentions nor the outbox row.
    #[datastore_span(name = "insert_event_with_github_outbox", system = "postgresql")]
    pub async fn insert_event_with_github_outbox(
        &self,
        community_id: CommunityId,
        event: &nostr::Event,
        channel_id: Option<Uuid>,
        thread_meta: Option<crate::event::ThreadMetadataParams<'_>>,
        outbox: GithubOutboxInsert<'_>,
    ) -> Result<(buzz_core::StoredEvent, bool)> {
        crate::github::insert_event_with_github_outbox(
            &self.pool,
            community_id,
            event,
            channel_id,
            thread_meta,
            outbox,
        )
        .await
    }
}

// -- Tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_type_display_and_parse_round_trip() {
        for (variant, text) in [
            (GithubSubjectType::Issue, "issue"),
            (GithubSubjectType::Pr, "pr"),
        ] {
            assert_eq!(variant.to_string(), text);
            assert_eq!(text.parse::<GithubSubjectType>().expect("parse"), variant);
        }
        assert!(matches!(
            "pull_request".parse::<GithubSubjectType>(),
            Err(DbError::InvalidData(_))
        ));
    }

    #[test]
    fn link_status_display_and_parse_round_trip() {
        for (variant, text) in [
            (GithubLinkStatus::Open, "open"),
            (GithubLinkStatus::Closed, "closed"),
        ] {
            assert_eq!(variant.to_string(), text);
            assert_eq!(text.parse::<GithubLinkStatus>().expect("parse"), variant);
        }
        assert!(matches!(
            "merged".parse::<GithubLinkStatus>(),
            Err(DbError::InvalidData(_))
        ));
    }

    #[test]
    fn outbox_action_display_and_parse_round_trip() {
        for (variant, text) in [
            (GithubOutboxAction::Comment, "comment"),
            (GithubOutboxAction::OpenIssue, "open_issue"),
        ] {
            assert_eq!(variant.to_string(), text);
            assert_eq!(text.parse::<GithubOutboxAction>().expect("parse"), variant);
        }
        assert!(matches!(
            "open-issue".parse::<GithubOutboxAction>(),
            Err(DbError::InvalidData(_))
        ));
    }

    #[test]
    fn outbox_status_display_and_parse_round_trip() {
        for (variant, text) in [
            (GithubOutboxStatus::Pending, "pending"),
            (GithubOutboxStatus::Sent, "sent"),
            (GithubOutboxStatus::Failed, "failed"),
        ] {
            assert_eq!(variant.to_string(), text);
            assert_eq!(text.parse::<GithubOutboxStatus>().expect("parse"), variant);
        }
        assert!(matches!(
            "delivered".parse::<GithubOutboxStatus>(),
            Err(DbError::InvalidData(_))
        ));
    }

    #[test]
    fn enum_db_spellings_match_migration_check_constraints() {
        // The CHECK constraints in migrations/0045_github_bridge.sql enumerate
        // the same literals the Rust enums serialize to; drift here means rows
        // written by the relay violate the constraint at insert time.
        let migration = include_str!("../../../../migrations/0045_github_bridge.sql");
        for sql_literal in ["'issue'", "'pr'"] {
            assert!(
                migration.contains(sql_literal),
                "subject_type CHECK must allow {sql_literal}"
            );
        }
        for sql_literal in ["'open'", "'closed'"] {
            assert!(
                migration.contains(sql_literal),
                "status CHECK must allow {sql_literal}"
            );
        }
        for sql_literal in ["'comment'", "'open_issue'"] {
            assert!(
                migration.contains(sql_literal),
                "action CHECK must allow {sql_literal}"
            );
        }
        for sql_literal in ["'pending'", "'sent'", "'failed'"] {
            assert!(
                migration.contains(sql_literal),
                "outbox status CHECK must allow {sql_literal}"
            );
        }
    }

    #[test]
    fn outbox_insert_params_serialize_action_as_db_literal() {
        let params = GithubOutboxInsert {
            community_id: CommunityId::from_uuid(Uuid::new_v4()),
            action: GithubOutboxAction::OpenIssue,
            link_repo: "SnpsGroup/SnpsGroup.QuantaPki",
            link_number: None,
            event_id: &[0x11; 32],
            author_pubkey: &[0x22; 32],
            payload: &serde_json::json!({"title": "t", "assignee": "a", "labels": ["x"]}),
        };
        assert_eq!(params.action.to_string(), "open_issue");
        assert!(params.link_number.is_none());
        assert_eq!(params.payload["title"], "t");
    }

    // -- Postgres-backed tests (ignored unless a database is available) --------

    /// Minimal fixture: a community, a channel, a stored top-level kind:9 root
    /// event, and a thread link pointing at that root. Returns
    /// `(pool, community, channel, root_event)`.
    async fn github_test_fixture() -> (sqlx::PgPool, CommunityId, Uuid, buzz_core::StoredEvent) {
        let database_url = crate::test_support::database_url();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await
            .expect("connect github bridge test pool");

        let community_uuid = Uuid::new_v4();
        sqlx::query("INSERT INTO communities (id, host) VALUES ($1, $2)")
            .bind(community_uuid)
            .bind(format!("github-bridge-{community_uuid}.example"))
            .execute(&pool)
            .await
            .expect("insert test community");
        let community_id = CommunityId::from_uuid(community_uuid);

        let channel_id = Uuid::new_v4();
        let creator: Vec<u8> = vec![0x42; 32];
        sqlx::query(
            r#"
            INSERT INTO channels
                (id, community_id, name, channel_type, visibility, created_by)
            VALUES ($1, $2, 'github-test', 'standard', 'open', $3)
            "#,
        )
        .bind(channel_id)
        .bind(community_uuid)
        .bind(&creator)
        .execute(&pool)
        .await
        .expect("insert test channel");

        let keys = nostr::Keys::generate();
        let root_event = nostr::EventBuilder::new(
            nostr::Kind::Custom(buzz_core::kind::KIND_STREAM_MESSAGE as u16),
            "root",
        )
        .tags([nostr::Tag::parse(["h", &channel_id.to_string()]).expect("h tag")])
        .sign_with_keys(&keys)
        .expect("sign root");
        let root_stored = crate::event::insert_event_with_thread_metadata(
            &pool,
            community_id,
            &root_event,
            Some(channel_id),
            None,
        )
        .await
        .expect("insert root")
        .0;

        create_thread_link(
            &pool,
            community_id,
            "SnpsGroup/QuantaPki",
            GithubSubjectType::Issue,
            104,
            channel_id,
            root_event.id.as_bytes(),
        )
        .await
        .expect("create link");

        (pool, community_id, channel_id, root_stored)
    }

    async fn cleanup_github_fixture(pool: &sqlx::PgPool, community_uuid: Uuid) {
        // FKs cascade from communities; github tables reference it directly.
        sqlx::query("DELETE FROM github_outbox WHERE community_id = $1")
            .bind(community_uuid)
            .execute(pool)
            .await
            .expect("clean outbox");
        sqlx::query("DELETE FROM github_thread_links WHERE community_id = $1")
            .bind(community_uuid)
            .execute(pool)
            .await
            .expect("clean links");
        sqlx::query("DELETE FROM communities WHERE id = $1")
            .bind(community_uuid)
            .execute(pool)
            .await
            .expect("clean community");
    }

    /// The combined insert must store event + mentions + outbox atomically, and
    /// a redelivered duplicate must not enqueue a second outbox row.
    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn insert_event_with_github_outbox_commits_all_three_or_nothing() {
        let (pool, community_id, channel_id, root) = github_test_fixture().await;
        let community_uuid = *community_id.as_uuid();

        let keys = nostr::Keys::generate();
        let root_hex = root.event.id.to_hex();
        let reply = nostr::EventBuilder::new(
            nostr::Kind::Custom(buzz_core::kind::KIND_STREAM_MESSAGE as u16),
            "reply via bridge",
        )
        .tags([
            nostr::Tag::parse(["h", &channel_id.to_string()]).expect("h tag"),
            nostr::Tag::parse(["e", &root_hex, "", "reply"]).expect("reply e tag"),
            nostr::Tag::parse(["p", &keys.public_key().to_hex()]).expect("p tag"),
        ])
        .sign_with_keys(&keys)
        .expect("sign reply");

        let author = keys.public_key().to_bytes().to_vec();
        let payload = serde_json::json!({});
        let params = GithubOutboxInsert {
            community_id,
            action: GithubOutboxAction::Comment,
            link_repo: "SnpsGroup/QuantaPki",
            link_number: Some(104),
            event_id: reply.id.as_bytes(),
            author_pubkey: &author,
            payload: &payload,
        };
        let thread_meta = crate::event::ThreadMetadataParams {
            event_id: reply.id.as_bytes(),
            event_created_at: chrono::Utc::now(),
            channel_id,
            parent_event_id: Some(root.event.id.as_bytes()),
            parent_event_created_at: None,
            root_event_id: Some(root.event.id.as_bytes()),
            root_event_created_at: None,
            depth: 1,
            broadcast: false,
        };

        let (_, inserted) = insert_event_with_github_outbox(
            &pool,
            community_id,
            &reply,
            Some(channel_id),
            Some(thread_meta),
            params,
        )
        .await
        .expect("combined insert");
        assert!(inserted, "fresh event must insert");

        let outbox_rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM github_outbox WHERE community_id = $1")
                .bind(community_uuid)
                .fetch_one(&pool)
                .await
                .expect("count outbox");
        assert_eq!(outbox_rows, 1, "one outbox row per accepted publish");

        let mention_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM event_mentions WHERE community_id = $1 AND event_id = $2",
        )
        .bind(community_uuid)
        .bind(reply.id.as_bytes())
        .fetch_one(&pool)
        .await
        .expect("count mentions");
        assert_eq!(mention_rows, 1, "p-tag mention row commits with the event");

        // Redelivery of the same event: no second outbox row.
        let payload2 = serde_json::json!({});
        let params2 = GithubOutboxInsert {
            community_id,
            action: GithubOutboxAction::Comment,
            link_repo: "SnpsGroup/QuantaPki",
            link_number: Some(104),
            event_id: reply.id.as_bytes(),
            author_pubkey: &author,
            payload: &payload2,
        };
        let (_, inserted_again) = insert_event_with_github_outbox(
            &pool,
            community_id,
            &reply,
            Some(channel_id),
            None,
            params2,
        )
        .await
        .expect("duplicate combined insert must not error");
        assert!(!inserted_again, "duplicate event must report not-inserted");
        let outbox_rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM github_outbox WHERE community_id = $1")
                .bind(community_uuid)
                .fetch_one(&pool)
                .await
                .expect("recount outbox");
        assert_eq!(outbox_rows, 1, "duplicate must not enqueue a second row");

        cleanup_github_fixture(&pool, community_uuid).await;
    }

    /// The ingest hook's reverse lookup: root_event_id → link, community-scoped.
    #[tokio::test]
    #[ignore = "requires Postgres"]
    async fn get_thread_link_by_root_resolves_and_scopes() {
        let (pool, community_id, _channel_id, root) = github_test_fixture().await;

        let link = get_thread_link_by_root(&pool, community_id, root.event.id.as_bytes()).await;
        let link = link.expect("lookup").expect("link exists for the root");
        assert_eq!(link.repo_full_name, "SnpsGroup/QuantaPki");
        assert_eq!(link.subject_number, 104);

        let other_community = CommunityId::from_uuid(Uuid::new_v4());
        let miss = get_thread_link_by_root(&pool, other_community, root.event.id.as_bytes())
            .await
            .expect("scoped lookup");
        assert!(miss.is_none(), "another community must not see the link");

        cleanup_github_fixture(&pool, *community_id.as_uuid()).await;
    }
}
