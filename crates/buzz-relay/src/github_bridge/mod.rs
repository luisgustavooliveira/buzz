//! GitHub bridge core: inbound webhook routing, outbound outbox worker, and
//! the atomic ingest hook.
//!
//! # Flow
//!
//! **Inbound (GitHub → Buzz):** GitHub delivers `issues` / `issue_comment`
//! webhooks to `POST /integrations/github/webhook` (`api::github`), which
//! verifies the HMAC signature, dedups on `X-GitHub-Delivery`, and responds
//! 202 before spawning the async processing ([`inbound::process_issues_event`]
//! / [`inbound::process_issue_comment_event`]). Routing:
//!
//! - `issues.opened` → relay-signed kind:9 top-level message in the repo's
//!   configured channel + idempotent `github_thread_links` row. A redelivered
//!   webhook (link exists) never posts a duplicate message.
//! - `issue_comment.created` → echo-suppressed when the sender is this bridge
//!   (`<app-slug>[bot]`) or the comment id is one the bridge itself posted;
//!   otherwise a relay-signed reply in the linked thread mentioning the last
//!   publishing agent (fallback: the repo's default agent). Comments on a
//!   closed link post a plain note without waking any agent.
//! - `issues.closed` / `issues.reopened` → lifecycle note + link status.
//!
//! **Outbound (Buzz → GitHub):** an agent posts a kind:9 event carrying
//! `buzz:github-publish` (comment on the issue mirrored by the thread) or
//! `buzz:github-action=open-issue` (new issue). [`try_ingest_hook`] validates
//! the event against the config allowlist and thread links and returns an
//! outbox plan; `handlers::ingest` persists the event, its mentions, and the
//! `github_outbox` row in **one transaction**. [`outbox_worker`] drains the
//! table with a lease, delivers via the GitHub App (comment / issue creation),
//! and confirms in-thread (✅ reaction or reply) — every terminal failure
//! posts a durable, visible error reply mentioning the author; nothing is
//! dropped silently.
//!
//! **Agent wake authority:** every bridge-authored kind:9 message that should
//! wake an agent is signed by the relay key and carries exactly the
//! `buzz:bridge`/`github` marker, `buzz:bridge-owner`, and `buzz:bridge-mention`
//! tags that `buzz-acp`'s inbound author gate verifies against the relay's
//! NIP-11 `self` key — a normal user cannot forge them.
//!
//! The whole feature is config-gated: with `BUZZ_GITHUB_ENABLED` unset, the
//! webhook route 404s, the ingest hook rejects tagged events with an
//! actionable message, and the worker never starts.

pub mod client;
pub mod inbound;
pub mod outbox_worker;

use std::sync::Arc;

use buzz_core::kind::KIND_STREAM_MESSAGE;
use buzz_core::tenant::TenantContext;
use buzz_core::CommunityId;
use nostr::{Event, EventBuilder, Kind, Tag};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::GitHubConfig;
use crate::handlers::ingest::{IngestError, ThreadMetadataOwned};
use crate::state::AppState;

pub use client::{CreateIssueOutcome, GithubClient, GithubClientError};

/// Cap for issue-body excerpts mirrored into a thread message.
pub(crate) const ISSUE_BODY_EXCERPT_MAX_CHARS: usize = 2_000;
/// Cap for GitHub comment bodies mirrored into a thread reply.
pub(crate) const COMMENT_BODY_MAX_CHARS: usize = 4_000;

/// Runtime state for an enabled GitHub bridge, set once on `AppState` at
/// startup (`state.github`). Loading is fail-closed: a missing/unreadable
/// webhook secret aborts relay startup rather than serving an unauthenticated
/// webhook route.
pub struct BridgeState {
    /// Raw webhook secret bytes (HMAC-SHA256 key for `X-Hub-Signature-256`).
    pub webhook_secret: Vec<u8>,
    /// GitHub App API client shared by the webhook and outbox paths.
    pub client: Arc<GithubClient>,
}

impl BridgeState {
    /// Load the webhook secret and build the API client from config.
    ///
    /// # Errors
    /// Fails when the secret file cannot be read or is empty — the operator
    /// asked for the bridge, so a broken secret must stop the relay, not
    /// disable signature checks.
    pub fn load(config: &GitHubConfig) -> Result<Self, String> {
        // Trim edge whitespace: editors and CLI redirects routinely terminate
        // secret files with a newline, and that byte must not become part of
        // the HMAC key (GitHub signs with the pasted value, no newline).
        let mut webhook_secret = std::fs::read(&config.webhook_secret_path).map_err(|e| {
            format!(
                "BUZZ_GITHUB_WEBHOOK_SECRET_PATH={} could not be read: {e}",
                config.webhook_secret_path.display()
            )
        })?;
        while webhook_secret
            .last()
            .is_some_and(|b| b.is_ascii_whitespace())
        {
            webhook_secret.pop();
        }
        while webhook_secret
            .first()
            .is_some_and(|b| b.is_ascii_whitespace())
        {
            webhook_secret.remove(0);
        }
        if webhook_secret.is_empty() {
            return Err(format!(
                "BUZZ_GITHUB_WEBHOOK_SECRET_PATH={} is empty — refusing to run the \
                 webhook endpoint without a signature secret",
                config.webhook_secret_path.display()
            ));
        }
        Ok(BridgeState {
            webhook_secret,
            client: Arc::new(GithubClient::new(
                &config.api_base_url,
                config.app_id,
                config.private_key_path.clone(),
            )),
        })
    }
}

/// Truncate to at most `max_chars` Unicode scalar values, mirroring the
/// `truncate_reason` byte-cap pattern but for event content caps.
pub(crate) fn truncate_chars(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        Some((idx, _)) => &s[..idx],
        None => s,
    }
}

/// Resolve a member's display name for `@`-mention text; falls back to a
/// short hex prefix when the pubkey has no stored profile.
pub(crate) async fn display_name_for(
    state: &AppState,
    community: CommunityId,
    pubkey_hex: &str,
) -> String {
    let Ok(pubkey) = nostr::PublicKey::from_hex(pubkey_hex) else {
        return pubkey_hex.chars().take(8).collect();
    };
    let bytes = pubkey.to_bytes().to_vec();
    match state
        .db
        .get_users_bulk_for_event_write(community, &[bytes])
        .await
    {
        Ok(users) => users
            .first()
            .and_then(|u| u.display_name.clone())
            .unwrap_or_else(|| pubkey_hex.chars().take(8).collect()),
        Err(e) => {
            warn!("github bridge: display-name lookup failed: {e}");
            pubkey_hex.chars().take(8).collect()
        }
    }
}

/// Who a bridge message is addressed to. `Some(..)` emits the full delegation
/// tag set (p + owner + mention) so buzz-acp can wake the agent with the
/// owner's authority; `None` emits a plain system note that mentions nobody.
pub(crate) struct BridgeMention<'a> {
    /// Hex pubkey of the agent being addressed (p + buzz:bridge-mention).
    pub agent_pubkey_hex: &'a str,
    /// Hex pubkey of the configured route owner (buzz:bridge-owner).
    pub owner_pubkey_hex: &'a str,
}

/// Build the tag list for a bridge message. Pure — unit-tested.
///
/// With `mention: Some(..)` the message carries the full delegation family
/// (`p`, `buzz:bridge-owner`, `buzz:bridge-mention`, marker `buzz:bridge` =
/// `github`) in the exact cardinality buzz-acp's `verified_relay_delegation`
/// gate verifies; with `mention: None` none of those tags are emitted, so the
/// note can never wake or mention-feed an agent. `reply_root_hex` adds the
/// NIP-10 `e` reply tag.
pub(crate) fn build_bridge_tags(
    mention: Option<&BridgeMention<'_>>,
    channel_id: Uuid,
    extra_tags: &[&[&str]],
    reply_root_hex: Option<&str>,
) -> Result<Vec<Tag>, String> {
    let mut tags: Vec<Tag> = Vec::with_capacity(4 + extra_tags.len());
    let push = |tags: &mut Vec<Tag>, values: &[&str]| -> Result<(), String> {
        tags.push(
            Tag::parse(values.to_vec()).map_err(|e| format!("tag build {}: {e}", values[0]))?,
        );
        Ok(())
    };

    if let Some(m) = mention {
        push(&mut tags, &["p", m.agent_pubkey_hex])?;
        push(&mut tags, &["buzz:bridge-owner", m.owner_pubkey_hex])?;
        push(&mut tags, &["buzz:bridge-mention", m.agent_pubkey_hex])?;
    }
    let channel_hex = channel_id.to_string();
    push(&mut tags, &["h", &channel_hex])?;
    push(&mut tags, &["buzz:bridge", "github"])?;
    for extra in extra_tags {
        push(&mut tags, extra)?;
    }
    if let Some(root_hex) = reply_root_hex {
        push(&mut tags, &["e", root_hex, "", "reply"])?;
    }
    Ok(tags)
}

/// Post a relay-signed kind:9 message on behalf of the bridge.
///
/// Mirrors the verified workflow sink (`workflow_sink.rs`): tags (`p`, `h`,
/// delegation family, NIP-10 `e` root/reply), relay-keypair signing, thread
/// metadata persistence, post-commit fan-out, and a live thread summary for
/// replies. Returns the new event id hex.
///
/// `parent_event_hex` = `Some(root_hex)` posts a reply to the thread root; a
/// system note that should address nobody passes `mention: None`.
pub(crate) async fn post_bridge_message(
    tenant: &TenantContext,
    state: &Arc<AppState>,
    channel_id: Uuid,
    text: &str,
    mention: Option<&BridgeMention<'_>>,
    parent_event_hex: Option<&str>,
    extra_tags: &[&[&str]],
) -> Result<String, String> {
    if text.trim().is_empty() {
        return Err("bridge message content is empty".to_string());
    }

    let channel = state
        .db
        .get_channel_for_event_write(tenant.community(), channel_id)
        .await
        .map_err(|e| format!("channel lookup: {e}"))?;
    if channel.archived_at.is_some() {
        return Err(format!("channel {channel_id} is archived"));
    }

    let reply_ancestry = match parent_event_hex {
        Some(parent_hex) => Some(
            crate::handlers::ingest::resolve_relay_reply_thread_meta(
                tenant.community(),
                parent_hex,
                channel_id,
                state,
            )
            .await
            .map_err(|e| format!("thread ancestry: {e}"))?,
        ),
        None => None,
    };
    // A reply to the root emits a single `reply` tag (matches
    // `buzz_sdk::builders::thread_tags` and the workflow sink).
    let reply_root_hex = reply_ancestry.as_ref().map(|a| a.root_hex());
    let tags = build_bridge_tags(mention, channel_id, extra_tags, reply_root_hex.as_deref())?;

    let event: Event = EventBuilder::new(Kind::from(KIND_STREAM_MESSAGE as u16), text)
        .tags(tags)
        .sign_with_keys(&state.relay_keypair)
        .map_err(|e| format!("signing: {e}"))?;

    let event_id_bytes = event.id.as_bytes().to_vec();
    let event_created_at = chrono::DateTime::from_timestamp(event.created_at.as_secs() as i64, 0)
        .unwrap_or_else(chrono::Utc::now);

    let thread_meta_owned = reply_ancestry.map(|ancestry| {
        ancestry.into_thread_meta(event_id_bytes.clone(), event_created_at, channel_id)
    });
    let thread_meta = Some(match &thread_meta_owned {
        Some(owned) => owned.as_params(),
        None => buzz_db::event::ThreadMetadataParams {
            event_id: &event_id_bytes,
            event_created_at,
            channel_id,
            parent_event_id: None,
            parent_event_created_at: None,
            root_event_id: None,
            root_event_created_at: None,
            depth: 0,
            broadcast: false,
        },
    });

    let (stored_event, was_inserted) = state
        .db
        .insert_event_with_thread_metadata(
            tenant.community(),
            &event,
            Some(channel_id),
            thread_meta,
        )
        .await
        .map_err(|e| format!("persist: {e}"))?;

    if !was_inserted {
        return Err("bridge message was a duplicate".to_string());
    }

    let actor_hex = state.relay_keypair.public_key().to_hex();
    dispatch_bridge_event(
        tenant,
        state,
        &stored_event,
        KIND_STREAM_MESSAGE,
        &actor_hex,
    )
    .await;

    if let Some(owned) = &thread_meta_owned {
        crate::handlers::side_effects::emit_live_thread_summary(
            tenant,
            state,
            channel_id,
            owned.root_event_id.clone(),
        );
    }

    Ok(event.id.to_hex())
}

/// Post-commit fan-out for a bridge-authored event (audit, Redis publish,
/// local fan-out) — the same dispatch the ingest path uses.
pub(crate) async fn dispatch_bridge_event(
    tenant: &TenantContext,
    state: &Arc<AppState>,
    stored_event: &buzz_core::StoredEvent,
    kind_u32: u32,
    actor_pubkey_hex: &str,
) {
    let _ = crate::handlers::event::dispatch_persistent_event(
        tenant,
        state,
        stored_event,
        kind_u32,
        actor_pubkey_hex,
        None,
    )
    .await;
}

// -- Ingest hook --------------------------------------------------------------

/// A validated outbound action attached to an ingesting kind:9 event.
///
/// Produced by [`try_ingest_hook`]; `handlers::ingest` feeds [`Self::as_insert`]
/// into [`buzz_db::Db::insert_event_with_github_outbox`] so the event, its
/// mentions, and the outbox row commit atomically.
pub struct GithubOutboxPlan {
    action: buzz_db::github::GithubOutboxAction,
    link_repo: String,
    link_number: Option<i32>,
    payload: serde_json::Value,
}

impl GithubOutboxPlan {
    /// Borrow the plan as buzz-db insert parameters. The event id and author
    /// pubkey come from the ingesting event itself.
    pub fn as_insert<'a>(
        &'a self,
        community_id: CommunityId,
        event: &'a Event,
    ) -> buzz_db::github::GithubOutboxInsert<'a> {
        buzz_db::github::GithubOutboxInsert {
            community_id,
            action: self.action,
            link_repo: &self.link_repo,
            link_number: self.link_number,
            event_id: event.id.as_bytes(),
            author_pubkey: event.pubkey.as_bytes(),
            payload: &self.payload,
        }
    }
}

/// Extract the single value of tag `name` (error on duplicates).
fn single_tag_value<'a>(event: &'a Event, name: &str) -> Result<Option<&'a str>, IngestError> {
    let mut found: Option<&'a str> = None;
    for tag in event.tags.iter() {
        let values = tag.as_slice();
        if values.first().map(String::as_str) == Some(name) {
            if found.is_some() {
                return Err(IngestError::Rejected(format!(
                    "invalid: event carries more than one {name:?} tag"
                )));
            }
            let value = values.get(1).map(String::as_str).unwrap_or("");
            found = Some(value);
        }
    }
    Ok(found)
}

/// All values of tag `name`.
fn tag_values<'a>(event: &'a Event, name: &str) -> Vec<&'a str> {
    event
        .tags
        .iter()
        .filter_map(|tag| {
            let values = tag.as_slice();
            if values.first().map(String::as_str) == Some(name) {
                Some(values.get(1).map(String::as_str).unwrap_or(""))
            } else {
                None
            }
        })
        .collect()
}

/// Parsed `buzz:github-action=open-issue` metadata from CLI-shaped tags.
/// Pure — unit-tested against the exact tag shapes `buzz-cli` emits.
#[derive(Debug, PartialEq)]
pub(crate) struct OpenIssueParams {
    /// Target `owner/repo`.
    pub repo: String,
    /// Issue title.
    pub title: String,
    /// Optional assignee login.
    pub assignee: Option<String>,
    /// Repeated labels.
    pub labels: Vec<String>,
}

/// Parse `github:repo` / `github:title` / `github:assignee` / `github:label`
/// tags off an open-issue event. Singleton tags must appear at most once.
pub(crate) fn parse_open_issue_tags(event: &Event) -> Result<OpenIssueParams, IngestError> {
    let repo = single_tag_value(event, "github:repo")?
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            IngestError::Rejected(
                "invalid: buzz:github-action=open-issue requires a github:repo tag".into(),
            )
        })?;
    let title = single_tag_value(event, "github:title")?
        .map(str::to_string)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| {
            IngestError::Rejected(
                "invalid: buzz:github-action=open-issue requires a non-empty github:title tag"
                    .into(),
            )
        })?;
    let assignee = single_tag_value(event, "github:assignee")?
        .map(str::to_string)
        .filter(|s| !s.is_empty());
    let labels = tag_values(event, "github:label")
        .into_iter()
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .collect();
    Ok(OpenIssueParams {
        repo,
        title,
        assignee,
        labels,
    })
}

/// Validate a kind:9 event carrying `buzz:github-publish` / `buzz:github-action`
/// tags and produce the outbox row it must commit with.
///
/// Returns `Ok(None)` for ordinary kind:9 events (the caller falls through to
/// the generic insert). Validation is fail-closed with actionable messages:
///
/// - bridge disabled → rejected;
/// - `buzz:github-publish` outside a thread that mirrors a GitHub issue →
///   rejected (the agent is told to publish inside a mirrored thread);
/// - `buzz:github-action=open-issue` with a repo outside the config allowlist
///   or malformed metadata → rejected.
pub(crate) async fn try_ingest_hook(
    tenant: &TenantContext,
    state: &AppState,
    event: &Event,
    thread_meta: Option<&ThreadMetadataOwned>,
) -> Result<Option<GithubOutboxPlan>, IngestError> {
    let is_publish = single_tag_value(event, "buzz:github-publish")?.is_some();
    let action = single_tag_value(event, "buzz:github-action")?;
    let open_issue = action == Some("open-issue");
    if !is_publish && !open_issue {
        return Ok(None);
    }

    let Some(github) = state.config.github.as_ref() else {
        return Err(IngestError::Rejected(
            "invalid: github bridge is not enabled on this relay".into(),
        ));
    };

    if is_publish {
        if open_issue {
            return Err(IngestError::Rejected(
                "invalid: event carries both buzz:github-publish and \
                 buzz:github-action tags; use exactly one"
                    .into(),
            ));
        }
        // The publish target is the issue mirrored by this event's thread.
        let root_id: Vec<u8> = match thread_meta {
            Some(meta) => meta.root_event_id.clone(),
            None => event.id.as_bytes().to_vec(),
        };
        let link = state
            .db
            .github_get_thread_link_by_root(tenant.community(), &root_id)
            .await
            .map_err(|e| IngestError::Internal(format!("error: github link lookup: {e}")))?
            .ok_or_else(|| {
                IngestError::Rejected(
                    "invalid: buzz:github-publish must be sent inside a thread that mirrors a \
                     GitHub issue; open a thread from a bridged issue first"
                        .into(),
                )
            })?;
        if link.status == buzz_db::github::GithubLinkStatus::Closed {
            return Err(IngestError::Rejected(
                "invalid: the GitHub issue mirrored by this thread is closed — publishing \
                 is disabled until it is reopened"
                    .into(),
            ));
        }
        info!(
            event_id = %event.id,
            repo = %link.repo_full_name,
            number = link.subject_number,
            "github bridge: publish enqueued"
        );
        return Ok(Some(GithubOutboxPlan {
            action: buzz_db::github::GithubOutboxAction::Comment,
            link_repo: link.repo_full_name,
            link_number: Some(link.subject_number),
            payload: serde_json::json!({}),
        }));
    }

    // open-issue
    let params = parse_open_issue_tags(event)?;
    if github.route_for(&params.repo).is_none() {
        return Err(IngestError::Rejected(format!(
            "invalid: repository {:?} is not in this relay's github allowlist \
             (BUZZ_GITHUB_REPOS)",
            params.repo
        )));
    }
    let OpenIssueParams {
        repo,
        title,
        assignee,
        labels,
    } = params;
    info!(event_id = %event.id, repo = %repo, "github bridge: open-issue enqueued");
    Ok(Some(GithubOutboxPlan {
        action: buzz_db::github::GithubOutboxAction::OpenIssue,
        link_repo: repo,
        link_number: None,
        payload: serde_json::json!({ "title": title, "assignee": assignee, "labels": labels }),
    }))
}

/// Log a bridge posting failure uniformly (event/row context by the caller).
pub(crate) fn log_bridge_post_failure(context: &str, error: &str) {
    error!("github bridge: {context} failed: {error}");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pk(nibble: char) -> String {
        std::iter::repeat_n(nibble, 64).collect()
    }

    fn tag_strings(tags: &[Tag]) -> Vec<Vec<String>> {
        tags.iter().map(|t| t.as_slice().to_vec()).collect()
    }

    /// The delegation family must match buzz-acp's `verified_relay_delegation`
    /// contract exactly: one canonical marker, one owner tag, one mention tag
    /// naming the same agent as the `p` tag, plus the channel `h` tag.
    #[test]
    fn build_bridge_tags_addressed_message_carries_exact_delegation_family() {
        let agent = pk('a');
        let owner = pk('b');
        let channel = Uuid::nil();
        let tags = build_bridge_tags(
            Some(&BridgeMention {
                agent_pubkey_hex: &agent,
                owner_pubkey_hex: &owner,
            }),
            channel,
            &[],
            None,
        )
        .expect("tags build");
        let values = tag_strings(&tags);
        assert!(values.contains(&vec!["p".to_string(), agent.clone()]));
        assert!(values.contains(&vec!["buzz:bridge-owner".to_string(), owner]));
        assert!(values.contains(&vec!["buzz:bridge-mention".to_string(), agent]));
        assert!(values.contains(&vec!["buzz:bridge".to_string(), "github".to_string()]));
        assert!(values.contains(&vec!["h".to_string(), channel.to_string()]));
        // Cardinality the gate enforces: exactly one marker, one owner.
        assert_eq!(values.iter().filter(|t| t[0] == "buzz:bridge").count(), 1);
        assert_eq!(
            values
                .iter()
                .filter(|t| t[0] == "buzz:bridge-owner")
                .count(),
            1
        );
    }

    /// A note that addresses nobody must not carry any authority-bearing tag:
    /// without the marker+owner+mention triple the buzz-acp gate rejects it,
    /// which is exactly the intent for lifecycle/closed-thread notes.
    #[test]
    fn build_bridge_tags_unaddressed_note_has_no_delegation_tags() {
        let tags = build_bridge_tags(None, Uuid::nil(), &[&["buzz:github-mirror", "o/r#7"]], None)
            .expect("tags build");
        let values = tag_strings(&tags);
        for forbidden in ["p", "buzz:bridge-owner", "buzz:bridge-mention"] {
            assert!(
                !values.iter().any(|t| t[0] == *forbidden),
                "unaddressed note must not carry {forbidden}"
            );
        }
        assert!(values.contains(&vec!["buzz:github-mirror".to_string(), "o/r#7".to_string()]));
    }

    #[test]
    fn build_bridge_tags_reply_carries_nip10_reply_tag() {
        let root = pk('e');
        let tags = build_bridge_tags(None, Uuid::nil(), &[], Some(&root)).expect("tags build");
        let values = tag_strings(&tags);
        assert!(values.contains(&vec![
            "e".to_string(),
            root,
            String::new(),
            "reply".to_string()
        ]));
    }

    fn tagged_event(tag_values: &[&[&str]]) -> Event {
        use nostr::Tag;
        let tags: Vec<Tag> = tag_values
            .iter()
            .map(|values| Tag::parse(values.to_vec()).expect("tag parses"))
            .collect();
        EventBuilder::new(Kind::from(KIND_STREAM_MESSAGE as u16), "body")
            .tags(tags)
            .sign_with_keys(&nostr::Keys::generate())
            .expect("sign")
    }

    /// The outbox payload must round-trip the exact tag shapes the CLI emits.
    #[test]
    fn parse_open_issue_tags_reads_cli_shaped_tags() {
        let event = tagged_event(&[
            &["buzz:github-action", "open-issue"],
            &["github:repo", "SnpsGroup/Argus"],
            &["github:title", "Adjust evidence link validation"],
            &["github:assignee", "fulano.argus"],
            &["github:label", "bug"],
            &["github:label", "priority"],
        ]);
        assert_eq!(
            parse_open_issue_tags(&event).expect("parse"),
            OpenIssueParams {
                repo: "SnpsGroup/Argus".into(),
                title: "Adjust evidence link validation".into(),
                assignee: Some("fulano.argus".into()),
                labels: vec!["bug".into(), "priority".into()],
            }
        );

        let bare = tagged_event(&[
            &["buzz:github-action", "open-issue"],
            &["github:repo", "SnpsGroup/Argus"],
            &["github:title", "T"],
        ]);
        assert_eq!(
            parse_open_issue_tags(&bare).expect("parse"),
            OpenIssueParams {
                repo: "SnpsGroup/Argus".into(),
                title: "T".into(),
                assignee: None,
                labels: vec![],
            }
        );
    }

    #[test]
    fn parse_open_issue_tags_rejects_missing_and_duplicated_metadata() {
        let missing_repo = tagged_event(&[
            &["buzz:github-action", "open-issue"],
            &["github:title", "T"],
        ]);
        assert!(matches!(
            parse_open_issue_tags(&missing_repo),
            Err(IngestError::Rejected(m)) if m.contains("github:repo")
        ));

        let missing_title = tagged_event(&[
            &["buzz:github-action", "open-issue"],
            &["github:repo", "o/r"],
        ]);
        assert!(matches!(
            parse_open_issue_tags(&missing_title),
            Err(IngestError::Rejected(m)) if m.contains("github:title")
        ));

        let empty_title = tagged_event(&[
            &["buzz:github-action", "open-issue"],
            &["github:repo", "o/r"],
            &["github:title", "   "],
        ]);
        assert!(
            parse_open_issue_tags(&empty_title).is_err(),
            "blank title rejected"
        );

        let dup_repo = tagged_event(&[
            &["buzz:github-action", "open-issue"],
            &["github:repo", "o/r"],
            &["github:repo", "o/r2"],
            &["github:title", "T"],
        ]);
        assert!(matches!(
            parse_open_issue_tags(&dup_repo),
            Err(IngestError::Rejected(m)) if m.contains("more than one")
        ));
    }

    #[test]
    fn truncate_chars_caps_at_char_boundaries() {
        // 4-char string, cap 2.
        assert_eq!(truncate_chars("héllo", 2), "hé");
        assert_eq!(truncate_chars("héllo", 10), "héllo");
        assert_eq!(truncate_chars("", 5), "");
    }
}
