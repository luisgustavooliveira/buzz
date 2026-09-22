//! GitHub webhook endpoint: `POST /integrations/github/webhook`.
//!
//! Verification order (each failure indistinguishable from a missing route
//! where possible): bridge enabled → tenant bind by `Host` (generic 404) →
//! HMAC-SHA256 `X-Hub-Signature-256` over the raw body (generic 401) →
//! `X-GitHub-Delivery` dedup (Redis `SET NX EX` 48h; a Redis outage fails
//! closed with 503 so GitHub retries rather than double-processing) → event
//! filter (`issues` / `issue_comment`; everything else 200 "ignored").
//!
//! The handler then answers 202 immediately and processes the payload in a
//! spawned task (same shape as `workflow_webhook`). GitHub retries hard on
//! 4xx/5xx, so anything that is not our failure — unknown repo, no link,
//! echo — logs and answers 2xx.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Json;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use tracing::{info, warn};

use crate::github_bridge::inbound::{
    process_issue_comment_event, process_issues_event, IssueCommentEventPayload, IssuesEventPayload,
};
use crate::state::AppState;

/// Redis TTL for delivery-id dedup keys. GitHub redelivers for up to ~a day;
/// 48h covers it with margin.
const DELIVERY_DEDUP_TTL_SECS: u64 = 48 * 60 * 60;

/// Verify `X-Hub-Signature-256: sha256=<hex>` against HMAC-SHA256 of the raw
/// body. Constant-time via `hmac`'s `verify_slice` (subtle). Pure — tested.
pub(crate) fn verify_github_signature(secret: &[u8], header_value: &str, body: &[u8]) -> bool {
    let Some(hex_sig) = header_value.strip_prefix("sha256=") else {
        return false;
    };
    let Ok(signature) = hex::decode(hex_sig) else {
        return false;
    };
    if signature.len() != 32 {
        return false;
    }
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret) else {
        return false;
    };
    mac.update(body);
    mac.verify_slice(&signature).is_ok()
}

/// Record `X-GitHub-Delivery` as seen. `Ok(true)` = first sighting (process),
/// `Ok(false)` = duplicate (skip), `Err` = Redis unavailable (fail closed so
/// GitHub retries instead of double-processing).
async fn mark_delivery_seen(
    pool: &deadpool_redis::Pool,
    community: buzz_core::CommunityId,
    delivery: &str,
) -> Result<bool, String> {
    let key = format!("github:webhook:delivery:{}:{delivery}", community.as_uuid());
    let mut conn = pool.get().await.map_err(|e| format!("redis get: {e}"))?;
    // SET key 1 NX EX <ttl>: Some("OK") on first claim, None when seen.
    let result: Option<String> = redis::cmd("SET")
        .arg(&key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(DELIVERY_DEDUP_TTL_SECS)
        .query_async(&mut *conn)
        .await
        .map_err(|e| format!("redis SET NX EX: {e}"))?;
    match result.as_deref() {
        Some("OK") => Ok(true),
        None => Ok(false),
        Some(other) => Err(format!("unexpected SET NX EX reply: {other}")),
    }
}

/// GitHub webhook receiver. See module docs for the verification order.
#[allow(clippy::type_complexity)]
pub async fn github_webhook(
    State(state): State<std::sync::Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<serde_json::Value>)> {
    // Disabled bridge: indistinguishable from an unknown route.
    let Some(bridge) = state.github.get() else {
        return Err(super::not_found("not found"));
    };

    // Row zero: bind the request to its community from the Host header before
    // any tenant-scoped work (same fail-closed generic 404 as workflow_webhook).
    let raw_host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let tenant = crate::tenant::bind_community(&state.db, raw_host)
        .await
        .map_err(|_| super::not_found("not found"))?;

    // HMAC over the raw body, constant-time.
    let signature_header = headers
        .get("x-hub-signature-256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !verify_github_signature(&bridge.webhook_secret, signature_header, &body) {
        warn!("github webhook: invalid or missing signature");
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "authentication failed"})),
        ));
    }

    let event_name = headers
        .get("x-github-event")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if event_name.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "missing X-GitHub-Event"})),
        ));
    }

    // Delivery dedup. Redis unavailable ⇒ 503 so GitHub retries once Redis is
    // back, instead of risking double-processing a redelivered comment.
    let delivery = headers
        .get("x-github-delivery")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if delivery.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "missing X-GitHub-Delivery"})),
        ));
    }
    match mark_delivery_seen(&state.redis_pool, tenant.community(), delivery).await {
        Ok(true) => {}
        Ok(false) => {
            info!(delivery, "github webhook: duplicate delivery ignored");
            return Ok((
                StatusCode::OK,
                Json(serde_json::json!({"status": "duplicate"})),
            ));
        }
        Err(e) => {
            warn!("github webhook: delivery dedup unavailable: {e}");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "temporarily unavailable"})),
            ));
        }
    }

    if event_name != "issues" && event_name != "issue_comment" {
        info!(event = event_name, "github webhook: event ignored");
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({"status": "ignored"})),
        ));
    }

    // Parse before answering so malformed bodies surface as 400 (GitHub will
    // not retry them; the payload is corrupt regardless of our state).
    let outcome = match event_name {
        "issues" => serde_json::from_slice::<IssuesEventPayload>(&body)
            .map(WebhookWork::Issues)
            .map_err(|e| e.to_string()),
        _ => serde_json::from_slice::<IssueCommentEventPayload>(&body)
            .map(WebhookWork::Comment)
            .map_err(|e| e.to_string()),
    };
    let work = outcome
        .map_err(|e| super::api_error(StatusCode::BAD_REQUEST, &format!("invalid payload: {e}")))?;

    // 202 now; process async (workflow_webhook pattern).
    let task_state = std::sync::Arc::clone(&state);
    tokio::spawn(async move {
        match &work {
            WebhookWork::Issues(payload) => {
                process_issues_event(&tenant, &task_state, payload).await
            }
            WebhookWork::Comment(payload) => {
                process_issue_comment_event(&tenant, &task_state, payload).await
            }
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"status": "accepted"})),
    ))
}

enum WebhookWork {
    Issues(IssuesEventPayload),
    Comment(IssueCommentEventPayload),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sign(secret: &[u8], body: &[u8]) -> String {
        use hmac::Mac;
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac accepts any key len");
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    const SECRET: &[u8] = b"webhook-secret";
    const BODY: &[u8] = br#"{"action":"opened"}"#;

    #[test]
    fn signature_verify_accepts_a_valid_mac() {
        assert!(verify_github_signature(SECRET, &sign(SECRET, BODY), BODY));
    }

    #[test]
    fn signature_verify_rejects_tampering_and_wrong_secret() {
        let sig = sign(SECRET, BODY);
        assert!(!verify_github_signature(b"other-secret", &sig, BODY));
        assert!(!verify_github_signature(
            SECRET,
            &sig,
            b"{\"action\":\"closed\"}"
        ));
    }

    #[test]
    fn signature_verify_rejects_missing_malformed_and_truncated_headers() {
        assert!(!verify_github_signature(SECRET, "", BODY), "missing header");
        assert!(
            !verify_github_signature(SECRET, "sha1=abcdef", BODY),
            "wrong algorithm prefix"
        );
        assert!(
            !verify_github_signature(SECRET, "sha256=zz-not-hex", BODY),
            "non-hex digest"
        );
        let truncated = &sign(SECRET, BODY)[..20];
        assert!(
            !verify_github_signature(SECRET, truncated, BODY),
            "truncated digest"
        );
        // A valid sha256= signature for a DIFFERENT body must fail.
        let other = sign(SECRET, b"other");
        assert!(!verify_github_signature(SECRET, &other, BODY));
    }

    #[test]
    fn signature_verify_accepts_empty_body_when_signed_empty() {
        assert!(verify_github_signature(SECRET, &sign(SECRET, b""), b""));
    }
}
