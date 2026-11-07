//! `buzz email` — V1 email bridge commands.
//!
//! The CLI never accepts a From address. The relay derives it from the event
//! signer and enforces the account and participant policy before enqueueing.

use nostr::{EventBuilder, Tag};

use crate::client::{normalize_write_response, BuzzClient};
use crate::error::CliError;
use crate::validate::{parse_uuid, read_or_stdin, sdk_err, validate_hex64};

use super::messages::resolve_thread_ref;

/// Maximum subject length.
const MAX_SUBJECT_CHARS: usize = 256;
/// Maximum plain-text body length.
const MAX_BODY_CHARS: usize = 60_000;

/// Validate and canonicalize one address.
pub fn validate_email_address(value: &str) -> Result<String, CliError> {
    let value = value.trim().to_lowercase();
    let valid = value.len() <= 254
        && !value.chars().any(char::is_whitespace)
        && value.split_once('@').is_some_and(|(local, domain)| {
            !local.is_empty()
                && !domain.is_empty()
                && !domain.contains('@')
                && !local.starts_with('.')
                && !local.ends_with('.')
                && !local.contains("..")
                && domain.contains('.')
        });
    if !valid {
        return Err(CliError::Usage(format!("invalid email address {value:?}")));
    }
    Ok(value)
}

fn validate_subject(subject: &str) -> Result<(), CliError> {
    let chars = subject.chars().count();
    if subject.trim().is_empty() || chars > MAX_SUBJECT_CHARS || subject.contains(['\r', '\n']) {
        return Err(CliError::Usage(format!(
            "email subject must be one non-empty line with at most {MAX_SUBJECT_CHARS} characters"
        )));
    }
    Ok(())
}

fn validate_body(body: &str) -> Result<(), CliError> {
    let chars = body.chars().count();
    if body.trim().is_empty() || chars > MAX_BODY_CHARS {
        return Err(CliError::Usage(format!(
            "email body must be non-empty and at most {MAX_BODY_CHARS} characters"
        )));
    }
    Ok(())
}

fn attach_tags(builder: EventBuilder, tags: &[Vec<String>]) -> Result<EventBuilder, CliError> {
    let tags = tags
        .iter()
        .map(|tag| {
            Tag::parse(tag).map_err(|e| CliError::Other(format!("invalid email tag {tag:?}: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(builder.tags(tags))
}

/// Parameters for `buzz email send`.
pub struct EmailSendParams {
    /// Buzz channel for the mirrored thread.
    pub channel: String,
    /// Existing email thread root or reply; omit for a new conversation.
    pub reply_to: Option<String>,
    /// Subject for a new conversation; ignored only when the relay confirms a reply.
    pub subject: String,
    /// Plain-text body, or `-` for stdin.
    pub content: String,
    /// To recipients.
    pub to: Vec<String>,
    /// Cc recipients.
    pub cc: Vec<String>,
    /// Bcc recipients.
    pub bcc: Vec<String>,
}

/// Publish a signed Buzz event that atomically enqueues an email.
pub async fn cmd_email_send(client: &BuzzClient, p: EmailSendParams) -> Result<(), CliError> {
    validate_subject(&p.subject)?;
    let body = read_or_stdin(&p.content)?;
    validate_body(&body)?;
    let channel = parse_uuid(&p.channel)?;
    let thread_ref = match p.reply_to.as_deref() {
        Some(value) => {
            validate_hex64(value)?;
            Some(resolve_thread_ref(client, value).await?)
        }
        None => None,
    };
    let to =
        p.to.iter()
            .map(|v| validate_email_address(v))
            .collect::<Result<Vec<_>, _>>()?;
    let cc =
        p.cc.iter()
            .map(|v| validate_email_address(v))
            .collect::<Result<Vec<_>, _>>()?;
    let bcc = p
        .bcc
        .iter()
        .map(|v| validate_email_address(v))
        .collect::<Result<Vec<_>, _>>()?;
    if to.is_empty() && cc.is_empty() && bcc.is_empty() {
        return Err(CliError::Usage(
            "at least one --to, --cc or --bcc recipient is required".into(),
        ));
    }

    let mut tags = vec![
        vec!["buzz:email-action".into(), "send".into()],
        vec!["email:subject".into(), p.subject],
    ];
    for (role, addresses) in [("to", to), ("cc", cc), ("bcc", bcc)] {
        tags.extend(
            addresses
                .into_iter()
                .map(|address| vec![format!("email:{role}"), address]),
        );
    }
    let builder =
        buzz_sdk::build_message(channel, &body, thread_ref.as_ref(), &[], false, &[], &[])
            .map_err(sdk_err)?;
    let event = client.sign_event(attach_tags(builder, &tags)?)?;
    let response = client.submit_event(event).await?;
    println!("{}", normalize_write_response(&response));
    Ok(())
}

/// Dispatch an email subcommand.
pub async fn dispatch(cmd: crate::EmailCmd, client: &BuzzClient) -> Result<(), CliError> {
    match cmd {
        crate::EmailCmd::Send {
            channel,
            reply_to,
            subject,
            content,
            to,
            cc,
            bcc,
        } => {
            cmd_email_send(
                client,
                EmailSendParams {
                    channel,
                    reply_to,
                    subject,
                    content,
                    to,
                    cc,
                    bcc,
                },
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_are_canonical_and_malformed_input_fails() {
        assert_eq!(
            validate_email_address(" Melissa.Ferraz@SNPSGroup.com ").unwrap(),
            "melissa.ferraz@snpsgroup.com"
        );
        for invalid in ["", "external", "@snpsgroup.com", "a@@b.com", "a b@b.com"] {
            assert!(
                validate_email_address(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }

    #[test]
    fn subject_rejects_header_injection() {
        assert!(validate_subject("Status").is_ok());
        assert!(validate_subject("Status\r\nBcc: attacker@example.com").is_err());
        assert!(validate_subject(" ").is_err());
    }
}
