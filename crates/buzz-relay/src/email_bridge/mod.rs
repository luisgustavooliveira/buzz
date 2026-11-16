//! V1 email bridge: Buzz/Postal outbound and Microsoft Graph inbound.
//!
//! The feature is config-gated. Outbound kind:9 events use
//! `buzz:email-action=send`; the relay derives From from the signer and commits
//! event, conversation, participants and outbox atomically. V1 rejects external
//! senders even when they present valid threading headers.

pub mod helpdesk;
pub mod inbound;
pub mod outbox_worker;
pub mod smtp;

use std::sync::Arc;

use nostr::{Event, EventBuilder, Kind, Tag};
use tracing::{info, warn};
use uuid::Uuid;

use buzz_core::kind::KIND_STREAM_MESSAGE;
use buzz_core::tenant::TenantContext;

use crate::config::EmailConfig;
use crate::handlers::ingest::{IngestError, ThreadMetadataOwned};
use crate::state::AppState;

/// Maximum inbound plain-text body mirrored to Buzz.
pub(crate) const INBOUND_BODY_MAX_CHARS: usize = 20_000;

/// Runtime secrets and clients for the enabled email bridge.
pub struct BridgeState {
    /// Postal SMTP client.
    pub smtp: Arc<smtp::PostalClient>,
    /// Graph client when inbound is enabled.
    pub graph: Option<Arc<inbound::GraphClient>>,
    /// Zammad client when shared-mailbox helpdesk ingress is configured.
    pub helpdesk: Option<Arc<helpdesk::ZammadClient>>,
    /// Expected bearer token for Zammad app article webhooks.
    pub helpdesk_webhook_token: Option<Vec<u8>>,
    /// Expected Graph webhook clientState.
    pub graph_client_state: Option<Vec<u8>>,
}

impl BridgeState {
    /// Load secrets fail-closed without exposing their values in logs.
    pub fn load(config: &EmailConfig) -> Result<Self, String> {
        let username = read_secret(&config.smtp_username_path)?;
        let password = read_secret(&config.smtp_password_path)?;
        let smtp = smtp::PostalClient::new(
            config.smtp_host.clone(),
            config.smtp_port,
            username,
            password,
        );
        let (graph, graph_client_state) = match &config.graph {
            Some(graph) => {
                let secret = read_secret(&graph.client_secret_path)?;
                let state = read_secret(&graph.client_state_path)?;
                (
                    Some(Arc::new(inbound::GraphClient::new(graph, secret))),
                    Some(state.into_bytes()),
                )
            }
            None => (None, None),
        };
        let helpdesk = config
            .helpdesk
            .as_ref()
            .map(|helpdesk| {
                read_secret(&helpdesk.api_token_path)
                    .map(|token| Arc::new(helpdesk::ZammadClient::new(helpdesk, token)))
            })
            .transpose()?;
        let helpdesk_webhook_token = config
            .helpdesk
            .as_ref()
            .map(|helpdesk| read_secret(&helpdesk.webhook_token_path).map(String::into_bytes))
            .transpose()?;
        Ok(Self {
            smtp: Arc::new(smtp),
            graph,
            helpdesk,
            helpdesk_webhook_token,
            graph_client_state,
        })
    }
}

fn read_secret(path: &std::path::Path) -> Result<String, String> {
    let value = std::fs::read_to_string(path)
        .map_err(|e| format!("email secret {} could not be read: {e}", path.display()))?
        .trim()
        .to_string();
    if value.is_empty() {
        return Err(format!("email secret {} is empty", path.display()));
    }
    Ok(value)
}

/// Validated outbound email ready for atomic persistence.
pub struct EmailOutboxPlan {
    conversation_id: Uuid,
    channel_id: Uuid,
    root_event_id: Vec<u8>,
    created_by: Vec<u8>,
    is_new: bool,
    participants: Vec<buzz_db::email::EmailParticipantInsert>,
    internet_message_id: String,
    payload: serde_json::Value,
}

impl EmailOutboxPlan {
    /// Borrow the plan for buzz-db.
    pub fn as_insert<'a>(&'a self, event: &'a Event) -> buzz_db::email::EmailOutboxInsert<'a> {
        buzz_db::email::EmailOutboxInsert {
            conversation: buzz_db::email::EmailConversationInsert {
                id: self.conversation_id,
                channel_id: self.channel_id,
                root_event_id: &self.root_event_id,
                created_by: &self.created_by,
                is_new: self.is_new,
                participants: &self.participants,
                helpdesk_owner_agent_pubkey: None,
            },
            event_id: event.id.as_bytes(),
            author_pubkey: event.pubkey.as_bytes(),
            internet_message_id: &self.internet_message_id,
            payload: &self.payload,
        }
    }
}

fn single_tag_value<'a>(event: &'a Event, name: &str) -> Result<Option<&'a str>, IngestError> {
    let mut result = None;
    for tag in event.tags.iter() {
        let values = tag.as_slice();
        if values.first().map(String::as_str) == Some(name) {
            if result.is_some() {
                return Err(IngestError::Rejected(format!(
                    "invalid: duplicate {name} tag"
                )));
            }
            result = Some(values.get(1).map(String::as_str).unwrap_or(""));
        }
    }
    Ok(result)
}

fn tag_values(event: &Event, name: &str) -> Vec<String> {
    event
        .tags
        .iter()
        .filter_map(|tag| {
            let values = tag.as_slice();
            (values.first().map(String::as_str) == Some(name))
                .then(|| values.get(1).map(String::as_str).unwrap_or("").to_string())
        })
        .collect()
}

fn validate_addresses(values: Vec<String>) -> Result<Vec<String>, IngestError> {
    values
        .into_iter()
        .map(|value| {
            crate::config::canonical_email_address(&value).ok_or_else(|| {
                IngestError::Rejected(format!("invalid: malformed email recipient {value:?}"))
            })
        })
        .collect()
}

/// Validate and plan an outbound V1 email.
pub(crate) async fn try_ingest_hook(
    tenant: &TenantContext,
    state: &AppState,
    event: &Event,
    thread_meta: Option<&ThreadMetadataOwned>,
) -> Result<Option<EmailOutboxPlan>, IngestError> {
    let action = single_tag_value(event, "buzz:email-action")?;
    if action != Some("send") {
        if action.is_none() {
            return helpdesk::try_ingest_public_note(tenant, state, event, thread_meta).await;
        }
        return Ok(None);
    }
    let Some(config) = state.config.email.as_ref() else {
        return Err(IngestError::Rejected(
            "invalid: email bridge is not enabled on this relay".into(),
        ));
    };
    let author = event.pubkey.to_hex();
    let account = config.account_for_agent(&author);
    let allowed_sender = config
        .allowed_senders
        .iter()
        .find(|sender| sender.pubkey == author);
    let (from_address, display_name, mailbox_id) = match account {
        Some(account) => (
            account.address.clone(),
            account.display_name.clone(),
            account.mailbox_id.clone(),
        ),
        None => {
            let sender = allowed_sender.ok_or_else(|| {
                IngestError::Rejected(
                    "invalid: signing identity has no authorized email address".into(),
                )
            })?;
            (
                sender.address.clone(),
                sender.address.clone(),
                String::new(),
            )
        }
    };
    let subject = single_tag_value(event, "email:subject")?
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| IngestError::Rejected("invalid: email:subject is required".into()))?;
    if subject.chars().count() > 256 || subject.contains(['\r', '\n']) {
        return Err(IngestError::Rejected(
            "invalid: email subject must be one line with at most 256 characters".into(),
        ));
    }
    if event.content.trim().is_empty() || event.content.chars().count() > 60_000 {
        return Err(IngestError::Rejected(
            "invalid: email body must be non-empty and at most 60000 characters".into(),
        ));
    }
    let channel_id = single_tag_value(event, "h")?
        .ok_or_else(|| IngestError::Rejected("invalid: email event requires a channel".into()))?
        .parse::<Uuid>()
        .map_err(|_| IngestError::Rejected("invalid: malformed email channel".into()))?;
    let root_event_id = thread_meta
        .map(|meta| meta.root_event_id.clone())
        .unwrap_or_else(|| event.id.as_bytes().to_vec());
    let existing = state
        .db
        .email_get_conversation_by_root(tenant.community(), &root_event_id)
        .await
        .map_err(|e| IngestError::Internal(format!("error: email conversation lookup: {e}")))?;
    if thread_meta.is_some() && existing.is_none() {
        return Err(IngestError::Rejected(
            "invalid: email replies require an email-backed Buzz thread".into(),
        ));
    }
    if existing
        .as_ref()
        .is_some_and(|c| c.status != buzz_db::email::EmailConversationStatus::Active)
    {
        return Err(IngestError::Rejected(
            "invalid: email conversation is closed".into(),
        ));
    }
    if existing
        .as_ref()
        .is_some_and(|c| c.channel_id != channel_id)
    {
        return Err(IngestError::Rejected(
            "invalid: email conversation channel mismatch".into(),
        ));
    }

    let to = validate_addresses(tag_values(event, "email:to"))?;
    let cc = validate_addresses(tag_values(event, "email:cc"))?;
    let bcc = validate_addresses(tag_values(event, "email:bcc"))?;
    if to.is_empty() && cc.is_empty() && bcc.is_empty() {
        return Err(IngestError::Rejected(
            "invalid: email requires at least one recipient".into(),
        ));
    }
    let helpdesk_conversation = existing.as_ref().is_some_and(|conversation| {
        config
            .helpdesk
            .as_ref()
            .is_some_and(|helpdesk| conversation.channel_id == helpdesk.channel_id)
    });
    let participant = |address: &str, role: &str| {
        let account = config.account_for_address(address);
        let allowed = config.allowed_sender(address);
        let internal_recipient = address
            .rsplit_once('@')
            .is_some_and(|(_, domain)| domain.eq_ignore_ascii_case(&config.message_id_domain));
        let Some((participant_type, pubkey)) = (match (account, allowed) {
            (Some(account), _) => Some(("relay", Some(account.agent_pubkey.clone()))),
            (None, Some(sender)) => Some(("relay", Some(sender.pubkey.clone()))),
            (None, None) if helpdesk_conversation && internal_recipient => Some(("external", None)),
            (None, None) => None,
        }) else {
            return Err(IngestError::Rejected(format!(
                "invalid: recipient {address} is not an authorized relay participant"
            )));
        };
        Ok(buzz_db::email::EmailParticipantInsert {
            address: address.to_string(),
            participant_type: participant_type.to_string(),
            role: role.to_string(),
            agent_pubkey: pubkey.map(hex::decode).transpose().map_err(|_| {
                IngestError::Rejected("invalid: participant pubkey is malformed".into())
            })?,
            approved_by: None,
        })
    };
    let mut participants = vec![buzz_db::email::EmailParticipantInsert {
        address: from_address.clone(),
        participant_type: "relay".into(),
        role: "sender".into(),
        agent_pubkey: account.map(|_| event.pubkey.to_bytes().to_vec()),
        approved_by: None,
    }];
    for (role, addresses) in [("to", &to), ("cc", &cc), ("bcc", &bcc)] {
        for address in addresses {
            if helpdesk_conversation
                && config
                    .helpdesk
                    .as_ref()
                    .is_some_and(|helpdesk| address == &helpdesk.inbox_address)
            {
                continue;
            }
            participants.push(participant(address, role)?);
        }
    }

    let conversation_id = existing.as_ref().map(|c| c.id).unwrap_or_else(Uuid::new_v4);
    let parent_message_id = match &existing {
        Some(conversation) => state
            .db
            .email_latest_message_id(tenant.community(), conversation.id)
            .await
            .map_err(|e| IngestError::Internal(format!("error: email parent lookup: {e}")))?,
        None => None,
    };
    let internet_message_id = format!("<{}@{}>", Uuid::new_v4(), config.message_id_domain);
    let references = if helpdesk_conversation {
        serde_json::json!(parent_message_id
            .as_ref()
            .map(|id| vec![id.clone()])
            .unwrap_or_default())
    } else {
        serde_json::json!(parent_message_id)
    };
    let payload = serde_json::json!({
        "operation": if helpdesk_conversation { "zammad_add_article" } else { "smtp_send" },
        "from_address": from_address,
        "display_name": display_name.clone(),
        "sender_name": display_name,
        "sender_is_agent": account.is_some(),
        "mailbox_id": if helpdesk_conversation { config.helpdesk.as_ref().map(|helpdesk| helpdesk.mailbox_id.clone()).unwrap_or(mailbox_id) } else { mailbox_id },
        "subject": subject,
        "body": event.content,
        "to": to,
        "cc": cc,
        "bcc": bcc,
        "internet_message_id": internet_message_id,
        "in_reply_to": parent_message_id,
        "references": references,
        "conversation_id": conversation_id,
    });
    info!(event_id = %event.id, %conversation_id, "email bridge: send enqueued");
    Ok(Some(EmailOutboxPlan {
        conversation_id,
        channel_id,
        root_event_id,
        created_by: event.pubkey.to_bytes().to_vec(),
        is_new: existing.is_none(),
        participants,
        internet_message_id,
        payload,
    }))
}

/// Truncate untrusted text without splitting UTF-8.
pub(crate) fn truncate_chars(value: &str, max_chars: usize) -> &str {
    match value.char_indices().nth(max_chars) {
        Some((index, _)) => &value[..index],
        None => value,
    }
}

/// Post a relay-signed message carrying the authenticated email delegation.
pub(crate) async fn post_email_message(
    tenant: &TenantContext,
    state: &Arc<AppState>,
    conversation: &buzz_db::email::EmailConversation,
    text: &str,
    agent_pubkey_hex: &str,
    owner_pubkey_hex: &str,
) -> Result<String, String> {
    post_email_event(
        tenant,
        state,
        conversation,
        text,
        Some((agent_pubkey_hex, owner_pubkey_hex)),
    )
    .await
}

/// Post a system note without waking an agent.
pub(crate) async fn post_email_note(
    tenant: &TenantContext,
    state: &Arc<AppState>,
    conversation: &buzz_db::email::EmailConversation,
    text: &str,
) -> Result<String, String> {
    post_email_event(tenant, state, conversation, text, None).await
}

async fn post_email_event(
    tenant: &TenantContext,
    state: &Arc<AppState>,
    conversation: &buzz_db::email::EmailConversation,
    text: &str,
    mention: Option<(&str, &str)>,
) -> Result<String, String> {
    if text.trim().is_empty() {
        return Err("email bridge message content is empty".into());
    }
    let root_hex = hex::encode(&conversation.root_event_id);
    let reply_ancestry = crate::handlers::ingest::resolve_relay_reply_thread_meta(
        tenant.community(),
        &root_hex,
        conversation.channel_id,
        &state.relay_keypair.public_key().to_bytes(),
        state,
    )
    .await
    .map_err(|e| format!("email thread ancestry: {e}"))?;
    let reply_root = reply_ancestry.root_hex();
    let channel = conversation.channel_id.to_string();
    let mut tags = Vec::new();
    if let Some((agent, owner)) = mention {
        tags.push(Tag::parse(["p", agent]).map_err(|e| e.to_string())?);
        tags.push(Tag::parse(["buzz:bridge-owner", owner]).map_err(|e| e.to_string())?);
        tags.push(Tag::parse(["buzz:bridge-mention", agent]).map_err(|e| e.to_string())?);
    }
    tags.push(Tag::parse(["h", &channel]).map_err(|e| e.to_string())?);
    tags.push(Tag::parse(["buzz:bridge", "email"]).map_err(|e| e.to_string())?);
    tags.push(Tag::parse(["e", &reply_root, "", "reply"]).map_err(|e| e.to_string())?);
    let event = EventBuilder::new(Kind::from(KIND_STREAM_MESSAGE as u16), text)
        .tags(tags)
        .sign_with_keys(&state.relay_keypair)
        .map_err(|e| format!("email event signing: {e}"))?;
    let event_id = event.id.to_hex();
    let event_created_at = chrono::DateTime::from_timestamp(event.created_at.as_secs() as i64, 0)
        .unwrap_or_else(chrono::Utc::now);
    let thread_meta = reply_ancestry.into_thread_meta(
        event.id.as_bytes().to_vec(),
        event_created_at,
        conversation.channel_id,
    );
    let (stored, was_inserted) = state
        .db
        .insert_event_with_thread_metadata(
            tenant.community(),
            &event,
            Some(conversation.channel_id),
            Some(thread_meta.as_params()),
        )
        .await
        .map_err(|e| format!("email event insert: {e}"))?;
    if was_inserted {
        crate::handlers::event::dispatch_persistent_event(
            tenant,
            state,
            &stored,
            KIND_STREAM_MESSAGE,
            &state.relay_keypair.public_key().to_hex(),
            None,
        )
        .await;
    }
    Ok(event_id)
}

/// Log a bounded bridge error.
pub(crate) fn log_error(context: &str, error: &str) {
    warn!("email bridge: {context}: {}", truncate_chars(error, 256));
}
