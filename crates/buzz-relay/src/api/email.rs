//! Microsoft Graph email webhook: `POST /integrations/email/graph`.

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use subtle::ConstantTimeEq;
use tracing::warn;

use crate::email_bridge::inbound::{
    mailbox_from_resource, process_notification, GraphNotificationPayload,
};
use crate::state::AppState;

/// Graph subscription validation query.
#[derive(Deserialize)]
pub struct GraphWebhookQuery {
    /// Microsoft echoes this value during endpoint validation.
    #[serde(rename = "validationToken")]
    pub validation_token: Option<String>,
}

/// Receive Graph notifications. Processing errors return 5xx so Microsoft can
/// retry; policy rejections are persisted in quarantine and return 202.
pub async fn graph_webhook(
    State(state): State<std::sync::Arc<AppState>>,
    Query(query): Query<GraphWebhookQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(bridge) = state.email.get() else {
        return super::not_found("not found").into_response();
    };
    if let Some(token) = query.validation_token {
        return (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; charset=utf-8",
            )],
            token,
        )
            .into_response();
    }
    let Some(expected_state) = bridge.graph_client_state.as_deref() else {
        return super::not_found("not found").into_response();
    };
    let raw_host = headers
        .get(axum::http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let tenant = match crate::tenant::bind_community(&state.db, raw_host).await {
        Ok(tenant) => tenant,
        Err(_) => return super::not_found("not found").into_response(),
    };
    let payload: GraphNotificationPayload = match serde_json::from_slice(&body) {
        Ok(payload) => payload,
        Err(_) => {
            return super::api_error(StatusCode::BAD_REQUEST, "invalid JSON body").into_response()
        }
    };
    let Some(config) = state.config.email.as_ref() else {
        return super::not_found("not found").into_response();
    };
    let Some(graph) = config.graph.as_ref() else {
        return super::not_found("not found").into_response();
    };
    for notification in &payload.value {
        if !bool::from(notification.client_state.as_bytes().ct_eq(expected_state)) {
            warn!("email Graph webhook: invalid clientState");
            return super::api_error(StatusCode::UNAUTHORIZED, "authentication failed")
                .into_response();
        }
        if notification.tenant_id != graph.tenant_id {
            warn!("email Graph webhook: unexpected tenant");
            return super::api_error(StatusCode::UNAUTHORIZED, "authentication failed")
                .into_response();
        }
    }
    for notification in &payload.value {
        let Some(mailbox_id) = mailbox_from_resource(&notification.resource) else {
            continue;
        };
        let Some(account) = config.account_for_mailbox(&mailbox_id) else {
            continue;
        };
        let expected_subscription = account.graph_subscription_id.map(|id| id.to_string());
        if expected_subscription.as_deref() != Some(notification.subscription_id.as_str()) {
            warn!(mailbox_id = %account.mailbox_id, "email Graph webhook: unexpected subscription");
            continue;
        }
        if let Err(error) = process_notification(&tenant, &state, account, notification).await {
            warn!(error = %crate::email_bridge::truncate_chars(&error, 256),
                "email Graph notification processing failed");
            return super::api_error(StatusCode::INTERNAL_SERVER_ERROR, "temporarily unavailable")
                .into_response();
        }
    }
    (
        StatusCode::ACCEPTED,
        axum::Json(serde_json::json!({"status": "accepted"})),
    )
        .into_response()
}
