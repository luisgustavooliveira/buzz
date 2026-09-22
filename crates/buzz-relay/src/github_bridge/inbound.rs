//! Inbound GitHub webhook payloads and GitHub → Buzz routing.
//!
//! Payload structs are deliberately default-tolerant (GitHub adds fields and
//! nulls optional ones without notice); routing decisions are pure functions
//! over those structs so every branch is unit-testable without a webhook.
//! All GitHub-supplied text is untrusted: it is capped before entering event
//! content and never logged untruncated.

use std::sync::Arc;

use buzz_core::tenant::TenantContext;
use buzz_db::github::{GithubLinkStatus, GithubSubjectType};
use tracing::{info, warn};

use crate::state::AppState;

use super::{
    display_name_for, post_bridge_message, truncate_chars, BridgeMention, COMMENT_BODY_MAX_CHARS,
    ISSUE_BODY_EXCERPT_MAX_CHARS,
};

// -- Payload structs ----------------------------------------------------------

/// A GitHub account reference (`login`).
#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WebhookUser {
    /// Account login (empty when GitHub omitted the object).
    #[serde(default)]
    pub login: String,
}

/// The repository block every webhook carries.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WebhookRepository {
    /// `owner/repo` full name (empty when absent).
    #[serde(default)]
    pub full_name: String,
}

/// The issue block of `issues` / `issue_comment` events. `body` is nullable.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WebhookIssue {
    /// Issue number.
    #[serde(default)]
    pub number: i64,
    /// Issue title.
    #[serde(default)]
    pub title: String,
    /// Browser URL.
    #[serde(default)]
    pub html_url: String,
    /// Issue body; GitHub sends `null` for empty bodies.
    #[serde(default)]
    pub body: Option<String>,
    /// Opening user.
    #[serde(default)]
    pub user: Option<WebhookUser>,
}

/// The comment block of `issue_comment` events. `body` is nullable.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WebhookComment {
    /// GitHub comment id (echo suppression key).
    #[serde(default)]
    pub id: i64,
    /// Comment body; nullable.
    #[serde(default)]
    pub body: Option<String>,
    /// Browser URL.
    #[serde(default)]
    pub html_url: String,
    /// Commenting user.
    #[serde(default)]
    pub user: Option<WebhookUser>,
}

/// Payload of an `issues` event.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct IssuesEventPayload {
    /// `opened` / `closed` / `reopened` / …
    #[serde(default)]
    pub action: String,
    /// The issue the event is about.
    #[serde(default)]
    pub issue: WebhookIssue,
    /// The repository.
    #[serde(default)]
    pub repository: WebhookRepository,
    /// The acting user.
    #[serde(default)]
    pub sender: Option<WebhookUser>,
}

/// Payload of an `issue_comment` event.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct IssueCommentEventPayload {
    /// `created` / `edited` / `deleted` / …
    #[serde(default)]
    pub action: String,
    /// The issue the comment is on.
    #[serde(default)]
    pub issue: WebhookIssue,
    /// The comment itself.
    #[serde(default)]
    pub comment: WebhookComment,
    /// The repository.
    #[serde(default)]
    pub repository: WebhookRepository,
    /// The acting user.
    #[serde(default)]
    pub sender: Option<WebhookUser>,
}

// -- Pure routing decisions ---------------------------------------------------

/// Why an inbound webhook was ignored (logged, never an error response —
/// GitHub retries hard on 4xx/5xx and an unroutable event is not our failure).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IgnoreReason {
    /// Action we do not handle (e.g. `issues.assigned`).
    UnhandledAction,
    /// Bot echo: the sender is this bridge's own bot identity.
    BotEcho,
    /// Echo: the comment id was published by this bridge.
    KnownCommentId,
    /// The repository is not in the config allowlist.
    UnknownRepo,
    /// No thread link exists for the issue.
    NoLink,
}

/// What an inbound webhook asks the relay to do. Produced by
/// [`classify_issues_event`] / [`classify_issue_comment_event`]; the async
/// processing functions resolve config routes and DB links.
#[derive(Debug, Clone, PartialEq)]
pub enum InboundRoute {
    /// Do nothing (see [`IgnoreReason`]).
    Ignore(IgnoreReason),
    /// Mirror a newly opened issue as a new thread.
    OpenIssue {
        /// `owner/repo`.
        repo: String,
        /// Issue number.
        number: i64,
        /// Issue title.
        title: String,
        /// Issue body (`""` for null).
        body: String,
        /// Issue URL.
        html_url: String,
        /// Opening user's login.
        author: String,
    },
    /// Note an issue closure in the linked thread.
    CloseIssue {
        /// `owner/repo`.
        repo: String,
        /// Issue number.
        number: i64,
        /// Closing user's login.
        actor: String,
    },
    /// Note an issue reopening in the linked thread.
    ReopenIssue {
        /// `owner/repo`.
        repo: String,
        /// Issue number.
        number: i64,
        /// Reopening user's login.
        actor: String,
    },
    /// Route a new issue comment into the linked thread.
    Comment {
        /// `owner/repo`.
        repo: String,
        /// Issue number.
        number: i64,
        /// GitHub comment id (echo key).
        comment_id: i64,
        /// Comment body (`""` for null).
        body: String,
        /// Comment URL.
        html_url: String,
        /// Commenting user's login.
        author: String,
    },
}

/// Classify an `issues` event payload.
pub fn classify_issues_event(payload: &IssuesEventPayload) -> InboundRoute {
    let repo = payload.repository.full_name.clone();
    let number = payload.issue.number;
    match payload.action.as_str() {
        "opened" => InboundRoute::OpenIssue {
            repo,
            number,
            title: payload.issue.title.clone(),
            body: payload.issue.body.clone().unwrap_or_default(),
            html_url: payload.issue.html_url.clone(),
            author: payload
                .issue
                .user
                .as_ref()
                .map(|u| u.login.clone())
                .unwrap_or_default(),
        },
        "closed" => InboundRoute::CloseIssue {
            repo,
            number,
            actor: actor_login(payload),
        },
        "reopened" => InboundRoute::ReopenIssue {
            repo,
            number,
            actor: actor_login(payload),
        },
        _ => InboundRoute::Ignore(IgnoreReason::UnhandledAction),
    }
}

fn actor_login(payload: &IssuesEventPayload) -> String {
    payload
        .sender
        .as_ref()
        .map(|u| u.login.clone())
        .unwrap_or_default()
}

/// Classify an `issue_comment` event payload. The payload-only echo check
/// (`sender` is the bridge bot) is applied here; the comment-id echo check
/// needs the DB and runs in [`process_issue_comment_event`].
pub fn classify_issue_comment_event(
    app_slug: &str,
    payload: &IssueCommentEventPayload,
) -> InboundRoute {
    if payload.action != "created" {
        return InboundRoute::Ignore(IgnoreReason::UnhandledAction);
    }
    let sender = payload
        .sender
        .as_ref()
        .map(|u| u.login.clone())
        .unwrap_or_default();
    if is_bridge_bot_login(app_slug, &sender) {
        return InboundRoute::Ignore(IgnoreReason::BotEcho);
    }
    InboundRoute::Comment {
        repo: payload.repository.full_name.clone(),
        number: payload.issue.number,
        comment_id: payload.comment.id,
        body: payload.comment.body.clone().unwrap_or_default(),
        html_url: payload.comment.html_url.clone(),
        author: payload
            .comment
            .user
            .as_ref()
            .map(|u| u.login.clone())
            .unwrap_or_default(),
    }
}

/// The bot login this bridge posts as (`<app-slug>[bot]`).
pub fn bridge_bot_login(app_slug: &str) -> String {
    format!("{app_slug}[bot]")
}

/// Whether `sender_login` is this bridge's own bot identity — the first half
/// of echo suppression (a comment the bridge itself posted must not route
/// back into the thread and re-wake the agent in a loop).
pub fn is_bridge_bot_login(app_slug: &str, sender_login: &str) -> bool {
    sender_login == bridge_bot_login(app_slug)
}

/// Cap an issue number to the DB's `i32` domain; `None` when out of range.
fn issue_number_i32(number: i64) -> Option<i32> {
    i32::try_from(number).ok().filter(|n| *n > 0)
}

// -- Async processing ---------------------------------------------------------

/// Process one verified `issues` webhook payload (called from the 202 path).
pub(crate) async fn process_issues_event(
    tenant: &TenantContext,
    state: &Arc<AppState>,
    payload: &IssuesEventPayload,
) {
    let Some(github) = state.config.github.clone() else {
        return;
    };
    match classify_issues_event(payload) {
        InboundRoute::OpenIssue {
            repo,
            number,
            title,
            body,
            html_url,
            author,
        } => {
            let Some(route) = github.route_for(&repo) else {
                info!(repo = %truncate_chars(&repo, 80), "github bridge: issue opened on unrouted repo; ignoring");
                return;
            };
            let Some(number_i32) = issue_number_i32(number) else {
                warn!("github bridge: issue number {number} out of range");
                return;
            };
            // Idempotency: a redelivered `issues.opened` must not duplicate
            // the mirror message. An existing link means this issue was
            // already mirrored — skip. The create below is ON CONFLICT DO
            // NOTHING, so a concurrent delivery loses the race and does not
            // double-create.
            match state
                .db
                .github_get_thread_link(
                    tenant.community(),
                    &repo,
                    GithubSubjectType::Issue,
                    number_i32,
                )
                .await
            {
                Ok(Some(_)) => {
                    info!(repo = %repo, number, "github bridge: link exists; skipping duplicate mirror");
                    return;
                }
                Ok(None) => {}
                Err(e) => {
                    warn!("github bridge: link lookup failed: {e}");
                    return;
                }
            }

            let agent = route.default_agent_pubkey.clone();
            let agent_display = display_name_for(state, tenant.community(), &agent).await;
            let excerpt = truncate_chars(&body, ISSUE_BODY_EXCERPT_MAX_CHARS);
            let text =
                format!("**#{number} {title}** (opened by {author})\n{excerpt}\n\n{html_url}\n\n@{agent_display}");
            let mirror_ref = format!("{repo}#{number}");
            match post_bridge_message(
                tenant,
                state,
                route.channel_id,
                &text,
                Some(&BridgeMention {
                    agent_pubkey_hex: &agent,
                    owner_pubkey_hex: &route.owner_pubkey,
                }),
                None,
                &[&["buzz:github-mirror", mirror_ref.as_str()]],
            )
            .await
            {
                Ok(event_hex) => {
                    let Ok(root_bytes) = hex::decode(&event_hex) else {
                        warn!("github bridge: posted event id not hex");
                        return;
                    };
                    match state
                        .db
                        .github_create_thread_link(
                            tenant.community(),
                            &repo,
                            GithubSubjectType::Issue,
                            number_i32,
                            route.channel_id,
                            &root_bytes,
                        )
                        .await
                    {
                        Ok(_) => {
                            info!(repo = %repo, number, event_id = %event_hex, "github bridge: issue mirrored")
                        }
                        Err(e) => {
                            // The thread exists but is not linked: inbound
                            // comments will not route. Loud, with the exact
                            // repair needed.
                            warn!(
                                repo = %repo,
                                number,
                                event_id = %event_hex,
                                "github bridge: LINK CREATE FAILED after mirror post — thread is NOT linked; re-run the webhook or insert the link manually: {e}"
                            );
                        }
                    }
                }
                Err(e) => super::log_bridge_post_failure("issue mirror", &e),
            }
        }
        InboundRoute::CloseIssue {
            repo,
            number,
            actor,
        }
        | InboundRoute::ReopenIssue {
            repo,
            number,
            actor,
        } => {
            let closing = payload.action == "closed";
            let Some(route) = github.route_for(&repo) else {
                info!(repo = %truncate_chars(&repo, 80), "github bridge: lifecycle event on unrouted repo; ignoring");
                return;
            };
            let Some(number_i32) = issue_number_i32(number) else {
                return;
            };
            let Some(link) = state
                .db
                .github_get_thread_link(
                    tenant.community(),
                    &repo,
                    GithubSubjectType::Issue,
                    number_i32,
                )
                .await
                .ok()
                .flatten()
            else {
                info!(repo = %repo, number, "github bridge: no link for lifecycle event; ignoring");
                return;
            };
            let status = if closing {
                GithubLinkStatus::Closed
            } else {
                GithubLinkStatus::Open
            };
            if let Err(e) = state
                .db
                .github_set_link_status(
                    tenant.community(),
                    &repo,
                    GithubSubjectType::Issue,
                    number_i32,
                    status,
                )
                .await
            {
                warn!("github bridge: link status update failed: {e}");
            }
            let text = if closing {
                format!("Issue #{number} closed by {actor} — thread archived")
            } else {
                format!("Issue #{number} reopened by {actor}")
            };
            // Lifecycle notes address nobody: no p/owner/mention tags, so no
            // agent is woken by a closure or reopening.
            let root_hex = hex::encode(&link.root_event_id);
            if let Err(e) = post_bridge_message(
                tenant,
                state,
                route.channel_id,
                &text,
                None,
                Some(&root_hex),
                &[],
            )
            .await
            {
                super::log_bridge_post_failure("lifecycle note", &e);
            }
        }
        InboundRoute::Ignore(reason) => {
            info!(?reason, "github bridge: issues event ignored");
        }
        InboundRoute::Comment { .. } => {
            // classify_issues_event never produces this variant.
        }
    }
}

/// Process one verified `issue_comment` webhook payload.
pub(crate) async fn process_issue_comment_event(
    tenant: &TenantContext,
    state: &Arc<AppState>,
    payload: &IssueCommentEventPayload,
) {
    let Some(github) = state.config.github.clone() else {
        return;
    };
    let decision = classify_issue_comment_event(&github.app_slug, payload);
    let InboundRoute::Comment {
        repo,
        number,
        comment_id,
        body,
        html_url,
        author,
    } = decision
    else {
        match decision {
            InboundRoute::Ignore(IgnoreReason::BotEcho) => {
                info!("github bridge: own bot comment echo suppressed");
            }
            InboundRoute::Ignore(reason) => {
                info!(?reason, "github bridge: comment event ignored");
            }
            _ => {}
        }
        return;
    };

    // Echo suppression part 2: the comment id is one this bridge posted
    // (recorded in github_outbox at delivery time).
    match state
        .db
        .github_is_known_comment_id(tenant.community(), comment_id)
        .await
    {
        Ok(true) => {
            info!(
                comment_id,
                "github bridge: known comment id echo suppressed"
            );
            return;
        }
        Ok(false) => {}
        Err(e) => {
            // Fail closed: without the echo check we may loop a comment back
            // into the thread. Skipping is safe — GitHub redelivers nothing
            // for a 202, but a manual re-send is possible.
            warn!("github bridge: echo check failed, skipping comment: {e}");
            return;
        }
    }

    let Some(route) = github.route_for(&repo) else {
        info!(repo = %truncate_chars(&repo, 80), "github bridge: comment on unrouted repo; ignoring");
        return;
    };
    let Some(number_i32) = issue_number_i32(number) else {
        return;
    };
    let Some(link) = state
        .db
        .github_get_thread_link(
            tenant.community(),
            &repo,
            GithubSubjectType::Issue,
            number_i32,
        )
        .await
        .ok()
        .flatten()
    else {
        info!(repo = %repo, number, "github bridge: comment on unlinked issue; ignoring");
        return;
    };

    let root_hex = hex::encode(&link.root_event_id);
    let capped = truncate_chars(&body, COMMENT_BODY_MAX_CHARS);
    let text = format!("**{author}** commented on #{number}: {capped}\n{html_url}");

    if link.status == GithubLinkStatus::Closed {
        // Record the comment but wake nobody on an archived thread.
        if let Err(e) = post_bridge_message(
            tenant,
            state,
            route.channel_id,
            &text,
            None,
            Some(&root_hex),
            &[],
        )
        .await
        {
            super::log_bridge_post_failure("closed-thread comment note", &e);
        }
        return;
    }

    let target_agent = link
        .last_agent_pubkey
        .as_ref()
        .map(hex::encode)
        .unwrap_or_else(|| route.default_agent_pubkey.clone());
    if let Err(e) = post_bridge_message(
        tenant,
        state,
        route.channel_id,
        &text,
        Some(&BridgeMention {
            agent_pubkey_hex: &target_agent,
            owner_pubkey_hex: &route.owner_pubkey,
        }),
        Some(&root_hex),
        &[],
    )
    .await
    {
        super::log_bridge_post_failure("comment mirror", &e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(login: &str) -> WebhookUser {
        WebhookUser {
            login: login.to_string(),
        }
    }

    fn comment_payload(action: &str, sender: &str, comment_id: i64) -> IssueCommentEventPayload {
        IssueCommentEventPayload {
            action: action.to_string(),
            issue: WebhookIssue {
                number: 104,
                title: "QuantaPKI signature-validation API".into(),
                html_url: "https://github.com/SnpsGroup/QuantaPki/issues/104".into(),
                body: None,
                user: Some(user("argus-dev")),
            },
            comment: WebhookComment {
                id: comment_id,
                body: Some("Status update: evidence link schema is ready.".into()),
                html_url: format!(
                    "https://github.com/SnpsGroup/QuantaPki/issues/104#issuecomment-{comment_id}"
                ),
                user: Some(user("argus-dev")),
            },
            repository: WebhookRepository {
                full_name: "SnpsGroup/QuantaPki".into(),
            },
            sender: Some(user(sender)),
        }
    }

    #[test]
    fn parses_real_shaped_issue_comment_payload_and_routes() {
        let comment_id: i64 = 2625068123;
        let raw = serde_json::json!({
            "action": "created",
            "issue": {
                "number": 104,
                "title": "QuantaPKI signature-validation API",
                "html_url": "https://github.com/SnpsGroup/QuantaPki/issues/104",
                "body": null,
                "user": { "login": "argus-dev" },
                "locked": false,
                "labels": []
            },
            "comment": {
                "id": comment_id,
                "body": "Status update: evidence link schema is ready.",
                "html_url": "https://github.com/SnpsGroup/QuantaPki/issues/104#issuecomment-2625068123",
                "user": { "login": "argus-dev" },
                "created_at": "2026-09-22T12:00:00Z"
            },
            "repository": { "full_name": "SnpsGroup/QuantaPki", "private": true },
            "sender": { "login": "argus-dev" },
            "installation": { "id": 42 }
        });
        let payload: IssueCommentEventPayload =
            serde_json::from_value(raw).expect("real-shaped payload parses");
        assert_eq!(payload.comment.id, comment_id);
        assert_eq!(payload.issue.body, None, "null body tolerated");

        match classify_issue_comment_event("snps-buzz-bridge", &payload) {
            InboundRoute::Comment {
                repo,
                number,
                comment_id,
                body,
                author,
                ..
            } => {
                assert_eq!(repo, "SnpsGroup/QuantaPki");
                assert_eq!(number, 104);
                assert_eq!(comment_id, 2625068123i64);
                assert_eq!(body, "Status update: evidence link schema is ready.");
                assert_eq!(author, "argus-dev");
            }
            other => panic!("expected Comment, got {other:?}"),
        }
    }

    #[test]
    fn parses_issues_opened_and_classifies_all_lifecycle_actions() {
        let raw = serde_json::json!({
            "action": "opened",
            "issue": {
                "number": 7,
                "title": "New demand",
                "html_url": "https://github.com/o/r/issues/7",
                "body": "Please advise",
                "user": { "login": "octocat" }
            },
            "repository": { "full_name": "o/r" },
            "sender": { "login": "octocat" }
        });
        let payload: IssuesEventPayload = serde_json::from_value(raw).expect("parses");
        match classify_issues_event(&payload) {
            InboundRoute::OpenIssue {
                repo,
                number,
                title,
                body,
                author,
                ..
            } => {
                assert_eq!(
                    (repo.as_str(), number, author.as_str()),
                    ("o/r", 7, "octocat")
                );
                assert_eq!(title, "New demand");
                assert_eq!(body, "Please advise");
            }
            other => panic!("expected OpenIssue, got {other:?}"),
        }

        let mut lifecycle = payload;
        lifecycle.issue.body = None;
        lifecycle.action = "closed".into();
        lifecycle.sender = Some(user("closer"));
        assert_eq!(
            classify_issues_event(&lifecycle),
            InboundRoute::CloseIssue {
                repo: "o/r".into(),
                number: 7,
                actor: "closer".into()
            }
        );

        lifecycle.action = "reopened".into();
        assert_eq!(
            classify_issues_event(&lifecycle),
            InboundRoute::ReopenIssue {
                repo: "o/r".into(),
                number: 7,
                actor: "closer".into()
            }
        );

        lifecycle.action = "assigned".into();
        assert_eq!(
            classify_issues_event(&lifecycle),
            InboundRoute::Ignore(IgnoreReason::UnhandledAction)
        );
    }

    #[test]
    fn echo_suppression_catches_only_this_bridges_bot_sender() {
        assert_eq!(
            classify_issue_comment_event(
                "snps-buzz-bridge",
                &comment_payload("created", "snps-buzz-bridge[bot]", 1)
            ),
            InboundRoute::Ignore(IgnoreReason::BotEcho)
        );
        // A different app's bot is a legitimate external commenter.
        assert!(matches!(
            classify_issue_comment_event(
                "snps-buzz-bridge",
                &comment_payload("created", "some-other-app[bot]", 1)
            ),
            InboundRoute::Comment { .. }
        ));
        assert!(matches!(
            classify_issue_comment_event(
                "snps-buzz-bridge",
                &comment_payload("created", "argus-dev", 1)
            ),
            InboundRoute::Comment { .. }
        ));
    }

    #[test]
    fn comment_edits_and_deletes_are_unhandled() {
        assert_eq!(
            classify_issue_comment_event("slug", &comment_payload("edited", "x", 1)),
            InboundRoute::Ignore(IgnoreReason::UnhandledAction)
        );
        assert_eq!(
            classify_issue_comment_event("slug", &comment_payload("deleted", "x", 1)),
            InboundRoute::Ignore(IgnoreReason::UnhandledAction)
        );
    }

    #[test]
    fn empty_payload_object_parses_via_defaults_and_is_ignored() {
        let payload: IssuesEventPayload = serde_json::from_str("{}").expect("empty object parses");
        assert_eq!(
            classify_issues_event(&payload),
            InboundRoute::Ignore(IgnoreReason::UnhandledAction)
        );
    }
}
