//! Durable outbound worker: drains `github_outbox` and delivers to GitHub.
//!
//! One action = one atomic persist upstream (the ingest hook wrote the event
//! and its outbox row in one transaction); this worker owns delivery:
//!
//! - claims a batch with `FOR UPDATE SKIP LOCKED` + a 2-minute lease
//!   (at-least-once: a crashed worker's rows become claimable again);
//! - `comment` → posts the (source-event or own) content as an issue comment,
//!   attributes it with the author's display name, marks `sent` with GitHub's
//!   comment id (webhook echo suppression), updates the link's
//!   `last_agent_pubkey`, and confirms in-thread with a relay-signed ✅
//!   reaction on the origin event;
//! - `open_issue` → creates the issue (assignee 422 falls back to a body
//!   @-mention), marks `sent`, creates the symmetric thread link pointing at
//!   the originating thread, and replies in-thread with the issue URL;
//! - failures retry with exponential backoff (30s·2^(n-1), capped 15min) for
//!   attempts 1–5; attempt ≥ 6 or a non-retryable error (installation removed,
//!   repo gone) marks the row `failed` **and** posts a relay-signed error
//!   reply mentioning the author — a terminal failure is always durable and
//!   visible, never a dropped log line.

use std::sync::Arc;
use std::time::Duration;

use buzz_core::kind::KIND_REACTION;
use buzz_core::tenant::TenantContext;
use buzz_db::github::{GithubOutboxAction, GithubOutboxRow, GithubSubjectType};
use buzz_db::ReactionEventInsertOutcome;
use chrono::Utc;
use nostr::{EventBuilder, Kind, Tag};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::state::AppState;

use super::{
    dispatch_bridge_event, display_name_for, post_bridge_message, truncate_chars, BridgeMention,
    GithubClientError, COMMENT_BODY_MAX_CHARS,
};

/// Idle poll cadence. The first pass runs immediately (interval's first tick).
const TICK: Duration = Duration::from_secs(5);
/// Rows claimed per pass (mirrors the admin outbox worker's batch sizing).
const BATCH_SIZE: i64 = 32;
/// Claim lease: a crashed worker's rows become claimable again after this.
const LEASE_SECS: i64 = 120;
/// Attempts 1..=5 are retried; attempt 6 is terminal.
const MAX_ATTEMPTS: i32 = 6;
/// Base retry backoff (30s·2^(n-1)).
const BASE_BACKOFF_SECS: u64 = 30;
/// Backoff ceiling.
const MAX_BACKOFF: Duration = Duration::from_secs(15 * 60);

/// Backoff after the `attempts`-th failed attempt (30s, 60s, 120s, … capped at
/// 15 minutes). Pure — unit-tested.
pub(crate) fn retry_delay(attempts: i32) -> Duration {
    let exp = (attempts.max(1) - 1).min(31) as u32;
    let secs = BASE_BACKOFF_SECS
        .saturating_mul(1u64.checked_shl(exp).unwrap_or(u64::MAX))
        .min(MAX_BACKOFF.as_secs());
    Duration::from_secs(secs)
}

/// Run the worker until `cancel` fires. Intended for `tokio::spawn`.
pub async fn run(state: Arc<AppState>, cancel: CancellationToken) {
    info!("GitHub outbox worker started");
    let mut interval = tokio::time::interval(TICK);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = cancel.cancelled() => {
                info!("GitHub outbox worker stopped");
                return;
            }
        }
        if let Err(e) = run_one_pass(&state).await {
            error!("github bridge: outbox claim failed: {e}");
        }
    }
}

async fn run_one_pass(state: &Arc<AppState>) -> Result<(), buzz_db::DbError> {
    let now = Utc::now();
    let lease_until = now + chrono::Duration::seconds(LEASE_SECS);
    let rows = state
        .db
        .github_claim_due_outbox(now, BATCH_SIZE, lease_until)
        .await?;
    for row in rows {
        deliver_row(state, &row).await;
    }
    Ok(())
}

/// Errors that can end a delivery attempt.
enum DeliveryError {
    /// GitHub said no — retryability via [`GithubClientError::is_retryable`].
    Github(GithubClientError),
    /// Local failure (DB, missing origin event, unresolvable tenant).
    /// Retried like a transport error: transient by default.
    Internal(String),
}

impl DeliveryError {
    fn is_retryable(&self) -> bool {
        match self {
            DeliveryError::Github(e) => e.is_retryable(),
            // A missing origin event or route is permanent, but distinguishing
            // it here only changes how long we wait before the terminal error
            // reply — let the attempt cap decide, uniformly.
            DeliveryError::Internal(_) => true,
        }
    }

    fn summary(&self) -> String {
        match self {
            DeliveryError::Github(e) => e.to_string(),
            DeliveryError::Internal(e) => e.clone(),
        }
    }
}

impl From<GithubClientError> for DeliveryError {
    fn from(e: GithubClientError) -> Self {
        DeliveryError::Github(e)
    }
}

/// Action-specific success effects, run after the row is marked `sent`.
enum Delivered {
    /// Comment posted; GitHub comment id recorded.
    Comment {
        comment_id: i64,
        origin: buzz_core::StoredEvent,
    },
    /// Issue created; symmetric link + in-thread confirmation follow.
    Issue {
        number: i32,
        html_url: String,
        assignee_fallback: bool,
        origin: buzz_core::StoredEvent,
    },
}

async fn deliver_row(state: &Arc<AppState>, row: &GithubOutboxRow) {
    let action_label = row.action.to_string();
    let result = match row.action {
        GithubOutboxAction::Comment => deliver_comment(state, row).await,
        GithubOutboxAction::OpenIssue => deliver_open_issue(state, row).await,
    };

    match result {
        Ok(delivered) => {
            metrics::counter!("buzz_github_outbox_total", "action" => action_label.clone(), "status" => "sent").increment(1);
            if let Err(e) = finish_delivery(state, row, delivered).await {
                // The GitHub side succeeded and the row is already marked sent;
                // these failures degrade the in-thread confirmation only.
                warn!(
                    outbox_id = %row.id,
                    event_id = %hex::encode(&row.event_id),
                    "github bridge: post-delivery confirmation failed: {e}"
                );
            }
        }
        Err(e) => {
            let terminal = !e.is_retryable() || row.attempts >= MAX_ATTEMPTS;
            let error_text = e.summary();
            if terminal {
                metrics::counter!("buzz_github_outbox_total", "action" => action_label.clone(), "status" => "failed").increment(1);
                error!(
                    outbox_id = %row.id,
                    event_id = %hex::encode(&row.event_id),
                    attempts = row.attempts,
                    error = %truncate_chars(&error_text, 256),
                    "github bridge: outbox delivery failed terminally"
                );
                if let Err(mark_err) = state
                    .db
                    .github_mark_outbox_failed(row.community_id, row.id, &error_text)
                    .await
                {
                    error!(outbox_id = %row.id, "github bridge: mark_failed DB call failed: {mark_err}");
                }
                if let Err(post_err) = post_error_reply(state, row, &error_text).await {
                    warn!(
                        outbox_id = %row.id,
                        "github bridge: terminal-failure reply could not be posted: {post_err}"
                    );
                }
            } else {
                metrics::counter!("buzz_github_outbox_total", "action" => action_label.clone(), "status" => "retry").increment(1);
                let next_retry_at = Utc::now()
                    + chrono::Duration::from_std(retry_delay(row.attempts))
                        .unwrap_or_else(|_| chrono::Duration::seconds(60));
                warn!(
                    outbox_id = %row.id,
                    event_id = %hex::encode(&row.event_id),
                    attempts = row.attempts,
                    error = %truncate_chars(&error_text, 256),
                    next_retry_at = %next_retry_at,
                    "github bridge: outbox delivery failed; will retry"
                );
                if let Err(mark_err) = state
                    .db
                    .github_mark_outbox_retry(row.community_id, row.id, &error_text, next_retry_at)
                    .await
                {
                    error!(outbox_id = %row.id, "github bridge: mark_retry DB call failed: {mark_err}");
                }
            }
        }
    }
}

/// Load the origin event, following `github:source-event` when the CLI
/// published an existing message instead of fresh content.
async fn resolve_comment_body_parts(
    state: &Arc<AppState>,
    row: &GithubOutboxRow,
) -> Result<(buzz_core::StoredEvent, String), DeliveryError> {
    let origin = state
        .db
        .get_event_by_id_for_event_write(row.community_id, &row.event_id)
        .await
        .map_err(|e| DeliveryError::Internal(format!("origin event load: {e}")))?
        .ok_or_else(|| DeliveryError::Internal("origin event no longer exists".into()))?;

    let body_source_hex = origin.event.tags.iter().find_map(|tag| {
        let values = tag.as_slice();
        if values.first().map(String::as_str) == Some("github:source-event") {
            values.get(1).cloned()
        } else {
            None
        }
    });
    let content = match body_source_hex {
        Some(source_hex) => {
            let Ok(source_bytes) = hex::decode(&source_hex) else {
                return Err(DeliveryError::Internal(
                    "github:source-event tag is not valid hex".into(),
                ));
            };
            state
                .db
                .get_event_by_id_for_event_write(row.community_id, &source_bytes)
                .await
                .map_err(|e| DeliveryError::Internal(format!("source event load: {e}")))?
                .ok_or_else(|| {
                    DeliveryError::Internal("github:source-event target missing".into())
                })?
                .event
                .content
        }
        None => origin.event.content.clone(),
    };
    Ok((origin, content))
}

async fn deliver_comment(
    state: &Arc<AppState>,
    row: &GithubOutboxRow,
) -> Result<Delivered, DeliveryError> {
    let Some(number) = row.link_number else {
        return Err(DeliveryError::Internal(
            "comment row has no issue number (link_number is NULL)".into(),
        ));
    };
    let (origin, content) = resolve_comment_body_parts(state, row).await?;
    let author_hex = hex::encode(&row.author_pubkey);
    let display = display_name_for(state, row.community_id, &author_hex).await;
    let body = format!(
        "**{display} (Buzz):**\n\n{}",
        truncate_chars(&content, COMMENT_BODY_MAX_CHARS)
    );

    let client = bridge_client(state)?;
    let comment_id = client
        .create_issue_comment(&row.link_repo, number, &body)
        .await?;

    if let Err(e) = state
        .db
        .github_mark_outbox_sent(row.community_id, row.id, Some(comment_id))
        .await
    {
        return Err(DeliveryError::Internal(format!("mark sent: {e}")));
    }
    Ok(Delivered::Comment { comment_id, origin })
}

async fn deliver_open_issue(
    state: &Arc<AppState>,
    row: &GithubOutboxRow,
) -> Result<Delivered, DeliveryError> {
    let origin = state
        .db
        .get_event_by_id_for_event_write(row.community_id, &row.event_id)
        .await
        .map_err(|e| DeliveryError::Internal(format!("origin event load: {e}")))?
        .ok_or_else(|| DeliveryError::Internal("origin event no longer exists".into()))?;

    let title = row
        .payload
        .get("title")
        .and_then(|v| v.as_str())
        .ok_or_else(|| DeliveryError::Internal("payload missing title".into()))?
        .to_string();
    let assignee = row
        .payload
        .get("assignee")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let labels: Vec<String> = row
        .payload
        .get("labels")
        .and_then(|v| v.as_array())
        .map(|entries| {
            entries
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

    let author_hex = hex::encode(&row.author_pubkey);
    let display = display_name_for(state, row.community_id, &author_hex).await;
    let mut body = format!(
        "**Aberto por {display} (Buzz)**\n\n{}",
        origin.event.content
    );
    if let Some(login) = assignee.as_deref() {
        body.push_str(&format!("\n\ncc @{login}"));
    }

    let assignees = assignee
        .clone()
        .map(|login| vec![login])
        .unwrap_or_default();
    let client = bridge_client(state)?;
    let outcome = client
        .create_issue(&row.link_repo, &title, &body, assignees, labels)
        .await?;

    if let Err(e) = state
        .db
        .github_mark_outbox_sent(row.community_id, row.id, None)
        .await
    {
        return Err(DeliveryError::Internal(format!("mark sent: {e}")));
    }
    Ok(Delivered::Issue {
        number: outcome.number,
        html_url: outcome.html_url,
        assignee_fallback: outcome.assignee_fallback,
        origin,
    })
}

fn bridge_client(state: &AppState) -> Result<Arc<super::GithubClient>, DeliveryError> {
    state
        .github
        .get()
        .map(|bridge| Arc::clone(&bridge.client))
        .ok_or_else(|| DeliveryError::Internal("github bridge state missing".into()))
}

/// Post-`sent` effects: link updates, in-thread confirmations, ✅ reaction.
/// Failures here are logged by the caller — the delivery itself succeeded.
async fn finish_delivery(
    state: &Arc<AppState>,
    row: &GithubOutboxRow,
    delivered: Delivered,
) -> Result<(), String> {
    let tenant = state
        .db
        .lookup_community_host(row.community_id)
        .await
        .map_err(|e| format!("host lookup: {e}"))?
        .map(|host| TenantContext::resolved(row.community_id, host))
        .ok_or_else(|| "community no longer mapped".to_string())?;

    match delivered {
        Delivered::Comment { comment_id, origin } => {
            info!(
                outbox_id = %row.id,
                comment_id,
                "github bridge: issue comment delivered"
            );
            state
                .db
                .github_set_last_agent(
                    row.community_id,
                    &row.link_repo,
                    GithubSubjectType::Issue,
                    row.link_number.unwrap_or_default(),
                    &row.author_pubkey,
                )
                .await
                .map_err(|e| format!("set last agent: {e}"))?;
            if let Some(channel_id) = origin.channel_id {
                react_confirmed(state, &tenant, channel_id, &origin.event.id.to_hex()).await;
            }
        }
        Delivered::Issue {
            number,
            html_url,
            assignee_fallback,
            origin,
        } => {
            // Symmetric link: the new issue mirrors the thread this event
            // started (or replied in), so the destination team's comments
            // route back into it.
            let channel_id = origin
                .channel_id
                .or_else(|| {
                    state
                        .config
                        .github
                        .as_ref()
                        .and_then(|g| g.route_for(&row.link_repo))
                        .map(|route| route.channel_id)
                })
                .ok_or_else(|| "no channel for link".to_string())?;
            state
                .db
                .github_create_thread_link(
                    row.community_id,
                    &row.link_repo,
                    GithubSubjectType::Issue,
                    number,
                    channel_id,
                    &row.event_id,
                )
                .await
                .map_err(|e| format!("create link: {e}"))?;
            state
                .db
                .github_set_last_agent(
                    row.community_id,
                    &row.link_repo,
                    GithubSubjectType::Issue,
                    number,
                    &row.author_pubkey,
                )
                .await
                .map_err(|e| format!("set last agent: {e}"))?;

            let author_hex = hex::encode(&row.author_pubkey);
            let mention = state
                .config
                .github
                .as_ref()
                .and_then(|g| g.route_for(&row.link_repo))
                .map(|route| BridgeMention {
                    agent_pubkey_hex: &author_hex,
                    owner_pubkey_hex: &route.owner_pubkey,
                });
            let fallback_note = if assignee_fallback {
                "\n(The requested assignee could not be set — they are @-mentioned in the issue body instead.)"
            } else {
                ""
            };
            let text = format!("Issue #{number} created: {html_url}{fallback_note}");
            post_bridge_message(
                &tenant,
                state,
                channel_id,
                &text,
                mention.as_ref(),
                Some(&hex::encode(&row.event_id)),
                &[],
            )
            .await?;
        }
    }
    Ok(())
}

/// Relay-signed ✅ on the origin event, deduped by the reaction row upsert.
async fn react_confirmed(
    state: &Arc<AppState>,
    tenant: &TenantContext,
    channel_id: uuid::Uuid,
    target_event_hex: &str,
) {
    let Ok(target_bytes) = hex::decode(target_event_hex) else {
        warn!("github bridge: reaction target id not hex");
        return;
    };
    let Ok(tag) = Tag::parse(["e", target_event_hex]) else {
        warn!("github bridge: reaction e tag build failed");
        return;
    };
    let Ok(event) = EventBuilder::new(Kind::from(KIND_REACTION as u16), "✅")
        .tags([tag])
        .sign_with_keys(&state.relay_keypair)
    else {
        warn!("github bridge: reaction signing failed");
        return;
    };
    let actor = state.relay_keypair.public_key().to_bytes().to_vec();
    match state
        .db
        .insert_reaction_event_with_thread_metadata(
            tenant.community(),
            &event,
            Some(channel_id),
            None,
            &target_bytes,
            &actor,
            "✅",
        )
        .await
    {
        Ok(ReactionEventInsertOutcome::Inserted { stored_event, .. }) => {
            dispatch_bridge_event(
                tenant,
                state,
                &stored_event,
                KIND_REACTION,
                &state.relay_keypair.public_key().to_hex(),
            )
            .await;
        }
        Ok(ReactionEventInsertOutcome::Duplicate) => {}
        Ok(ReactionEventInsertOutcome::TargetMissing) => {
            warn!(
                event_id = target_event_hex,
                "github bridge: reaction target missing"
            )
        }
        Err(e) => warn!(
            event_id = target_event_hex,
            "github bridge: reaction insert failed: {e}"
        ),
    }
}

/// Durable, visible terminal-failure notice in the originating thread,
/// mentioning the author (review-proven rule 1: never catch-log-and-drop).
async fn post_error_reply(
    state: &Arc<AppState>,
    row: &GithubOutboxRow,
    error_text: &str,
) -> Result<(), String> {
    let tenant = state
        .db
        .lookup_community_host(row.community_id)
        .await
        .map_err(|e| format!("host lookup: {e}"))?
        .map(|host| TenantContext::resolved(row.community_id, host))
        .ok_or_else(|| "community no longer mapped".to_string())?;

    let origin = state
        .db
        .get_event_by_id_for_event_write(row.community_id, &row.event_id)
        .await
        .map_err(|e| format!("origin event load: {e}"))?;

    let author_hex = hex::encode(&row.author_pubkey);
    let mention = state
        .config
        .github
        .as_ref()
        .and_then(|g| g.route_for(&row.link_repo))
        .map(|route| BridgeMention {
            agent_pubkey_hex: &author_hex,
            owner_pubkey_hex: &route.owner_pubkey,
        });

    let channel_id = origin
        .as_ref()
        .and_then(|o| o.channel_id)
        .or_else(|| {
            state
                .config
                .github
                .as_ref()
                .and_then(|g| g.route_for(&row.link_repo))
                .map(|route| route.channel_id)
        })
        .ok_or_else(|| "no channel for error reply".to_string())?;

    let action = row.action.to_string();
    let display = display_name_for(state, row.community_id, &author_hex).await;
    let text = format!(
        "@{display} ⚠️ GitHub {action} failed permanently and will NOT be retried. \
         Error: {}",
        truncate_chars(error_text, 256)
    );

    post_bridge_message(
        &tenant,
        state,
        channel_id,
        &text,
        mention.as_ref(),
        Some(&hex::encode(&row.event_id)),
        &[],
    )
    .await
    .map(|_| ())
}

// -- Tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_doubles_from_30s_and_caps_at_15_minutes() {
        let mut prev = Duration::ZERO;
        for attempt in 1..=12 {
            let delay = retry_delay(attempt);
            assert!(
                delay >= prev,
                "backoff must be monotonically non-decreasing"
            );
            assert!(delay <= MAX_BACKOFF, "backoff must respect the 15min cap");
            prev = delay;
        }
        assert_eq!(retry_delay(1), Duration::from_secs(30));
        assert_eq!(retry_delay(2), Duration::from_secs(60));
        assert_eq!(retry_delay(3), Duration::from_secs(120));
        assert_eq!(retry_delay(4), Duration::from_secs(240));
        assert_eq!(retry_delay(5), Duration::from_secs(480));
        assert_eq!(retry_delay(6), Duration::from_secs(900), "capped at 15min");
        assert_eq!(retry_delay(100), Duration::from_secs(900));
        assert_eq!(retry_delay(0), Duration::from_secs(30), "clamp, never zero");
    }

    #[test]
    fn delivery_error_retryability_follows_github_classification() {
        assert!(DeliveryError::Github(GithubClientError::Transport("t".into())).is_retryable());
        assert!(
            !DeliveryError::Github(GithubClientError::InstallationMissing { repo: "o/r".into() })
                .is_retryable()
        );
        assert!(DeliveryError::Internal("db blip".into()).is_retryable());
    }
}
