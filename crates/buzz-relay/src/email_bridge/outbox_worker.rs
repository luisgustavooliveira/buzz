//! Durable Postal outbox worker with bounded retries and visible failures.

use std::sync::Arc;
use std::time::Duration;

use buzz_core::tenant::TenantContext;
use chrono::Utc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::state::AppState;

use super::{log_error, post_email_note, truncate_chars};

const TICK: Duration = Duration::from_secs(5);
const BATCH_SIZE: i64 = 16;
const LEASE_SECS: i64 = 120;
const MAX_ATTEMPTS: i32 = 6;

/// Exponential retry delay, capped at 15 minutes.
pub(crate) fn retry_delay(attempts: i32) -> Duration {
    let exponent = (attempts.max(1) - 1).min(31) as u32;
    Duration::from_secs(
        30u64
            .saturating_mul(1u64.checked_shl(exponent).unwrap_or(u64::MAX))
            .min(15 * 60),
    )
}

/// Run until relay shutdown.
pub async fn run(state: Arc<AppState>, cancel: CancellationToken) {
    info!("email outbox worker started");
    let mut interval = tokio::time::interval(TICK);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_renewal: Option<tokio::time::Instant> = None;
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = cancel.cancelled() => {
                info!("email outbox worker stopped");
                return;
            }
        }
        if last_renewal.is_none_or(|last| last.elapsed() >= Duration::from_secs(30 * 60)) {
            renew_subscriptions(&state).await;
            last_renewal = Some(tokio::time::Instant::now());
        }
        let rows = state
            .db
            .email_claim_due_outbox(
                Utc::now(),
                BATCH_SIZE,
                Utc::now() + chrono::Duration::seconds(LEASE_SECS),
            )
            .await;
        match rows {
            Ok(rows) => {
                for row in rows {
                    deliver(&state, row).await;
                }
            }
            Err(error) => log_error("outbox claim failed", &error.to_string()),
        }
    }
}

async fn renew_subscriptions(state: &Arc<AppState>) {
    let Some(bridge) = state.email.get() else {
        return;
    };
    let Some(graph) = bridge.graph.as_ref() else {
        return;
    };
    let Some(config) = state.config.email.as_ref() else {
        return;
    };
    for account in &config.accounts {
        let Some(subscription_id) = account.graph_subscription_id else {
            continue;
        };
        match graph.renew_subscription(subscription_id).await {
            Ok(()) => {
                metrics::counter!("buzz_email_graph_subscription_renewal_total", "status" => "ok")
                    .increment(1);
            }
            Err(error) => {
                metrics::counter!("buzz_email_graph_subscription_renewal_total", "status" => "error").increment(1);
                error!(mailbox_id = %account.mailbox_id, error = %truncate_chars(&error, 256),
                    "email Graph subscription renewal failed; recreate it before expiry");
            }
        }
    }
}

async fn deliver(state: &Arc<AppState>, row: buzz_db::email::EmailOutboxRow) {
    let result = async {
        let bridge = state
            .email
            .get()
            .ok_or_else(|| "email bridge state missing".to_string())?;
        bridge.smtp.send(&row.payload).await
    }
    .await;
    match result {
        Ok(()) => {
            if let Err(error) = state
                .db
                .email_mark_outbox_sent(row.community_id, row.id)
                .await
            {
                // SMTP succeeded. Retrying the whole row could duplicate the
                // message; leave the durable sent state unresolved only when the
                // DB failed, with an explicit high-severity log for reconciliation.
                error!(outbox_id = %row.id, error = %truncate_chars(&error.to_string(), 256),
                    "email delivered but sent-state commit failed; manual reconciliation required");
                return;
            }
            metrics::counter!("buzz_email_outbox_total", "status" => "sent").increment(1);
            info!(outbox_id = %row.id, conversation_id = %row.conversation_id, "email delivered");
        }
        Err(error) => {
            let error = truncate_chars(&error, 500).to_string();
            if row.attempts < MAX_ATTEMPTS {
                let retry_at = Utc::now()
                    + chrono::Duration::from_std(retry_delay(row.attempts))
                        .unwrap_or_else(|_| chrono::Duration::minutes(1));
                if let Err(db_error) = state
                    .db
                    .email_mark_outbox_retry(row.community_id, row.id, &error, retry_at)
                    .await
                {
                    log_error("outbox retry update failed", &db_error.to_string());
                }
                metrics::counter!("buzz_email_outbox_total", "status" => "retry").increment(1);
                warn!(outbox_id = %row.id, attempts = row.attempts, retry_at = %retry_at,
                    "email delivery failed; retry scheduled");
                return;
            }
            if let Err(db_error) = state
                .db
                .email_mark_outbox_failed(row.community_id, row.id, &error)
                .await
            {
                log_error("outbox terminal update failed", &db_error.to_string());
            }
            metrics::counter!("buzz_email_outbox_total", "status" => "failed").increment(1);
            let conversation = state
                .db
                .email_get_conversation(row.community_id, row.conversation_id)
                .await;
            match conversation {
                Ok(Some(conversation)) => {
                    let tenant =
                        TenantContext::resolved(row.community_id, state.config.relay_url.clone());
                    let text = format!(
                        "⚠️ O envio de e-mail falhou definitivamente após {MAX_ATTEMPTS} tentativas. O erro foi registrado; não haverá nova tentativa automática."
                    );
                    if let Err(post_error) =
                        post_email_note(&tenant, state, &conversation, &text).await
                    {
                        log_error("terminal failure note failed", &post_error);
                    }
                }
                Ok(None) => {
                    error!(outbox_id = %row.id, "email conversation missing on terminal failure")
                }
                Err(db_error) => log_error(
                    "conversation lookup on terminal failure failed",
                    &db_error.to_string(),
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_backoff_is_bounded() {
        assert_eq!(retry_delay(1), Duration::from_secs(30));
        assert_eq!(retry_delay(2), Duration::from_secs(60));
        assert_eq!(retry_delay(6), Duration::from_secs(15 * 60));
        assert_eq!(retry_delay(100), Duration::from_secs(15 * 60));
    }
}
