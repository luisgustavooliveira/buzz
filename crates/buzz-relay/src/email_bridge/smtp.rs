//! Postal SMTP delivery with persistent Message-ID and standard threading.

use lettre::message::header::{InReplyTo, MessageId, References};
use lettre::message::{header, Mailbox, Message};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{SmtpTransport, Transport};

/// SMTP client configuration. Secrets are never included in Debug output.
pub struct PostalClient {
    host: String,
    port: u16,
    username: String,
    password: String,
}

impl PostalClient {
    /// Build a Postal client.
    pub fn new(host: String, port: u16, username: String, password: String) -> Self {
        Self {
            host,
            port,
            username,
            password,
        }
    }

    /// Send one outbox payload through Postal STARTTLS.
    pub async fn send(&self, payload: &serde_json::Value) -> Result<(), String> {
        let message = build_message(payload)?;
        let credentials = Credentials::new(self.username.clone(), self.password.clone());
        let host = self.host.clone();
        let port = self.port;
        tokio::task::spawn_blocking(move || {
            let transport = SmtpTransport::starttls_relay(&host)
                .map_err(|e| format!("SMTP transport: {e}"))?
                .port(port)
                .credentials(credentials)
                .timeout(Some(std::time::Duration::from_secs(20)))
                .build();
            transport
                .send(&message)
                .map_err(|e| format!("SMTP delivery: {e}"))?;
            Ok(())
        })
        .await
        .map_err(|e| format!("SMTP task: {e}"))?
    }
}

fn required<'a>(payload: &'a serde_json::Value, field: &str) -> Result<&'a str, String> {
    payload
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("email payload missing {field}"))
}

fn addresses(payload: &serde_json::Value, field: &str) -> Result<Vec<Mailbox>, String> {
    payload
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("email payload missing {field}"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| format!("email {field} contains a non-string"))?
                .parse::<Mailbox>()
                .map_err(|e| format!("email {field} mailbox: {e}"))
        })
        .collect()
}

/// Build the exact RFC 5322 message used by the worker. Pure and unit-tested.
pub fn build_message(payload: &serde_json::Value) -> Result<Message, String> {
    let from_address = required(payload, "from_address")?;
    let display_name = required(payload, "display_name")?;
    let from = Mailbox::new(
        Some(display_name.to_string()),
        from_address
            .parse()
            .map_err(|e| format!("From address: {e}"))?,
    );
    let message_id = required(payload, "internet_message_id")?.to_string();
    let subject = required(payload, "subject")?;
    let body = required(payload, "body")?;
    let conversation_id = required(payload, "conversation_id")?;
    let mut builder = Message::builder()
        .from(from.clone())
        .reply_to(from)
        .subject(subject)
        .header(MessageId::from(message_id))
        .raw_header(header::HeaderValue::new(
            header::HeaderName::new_from_ascii_str("Auto-Submitted"),
            "auto-generated".to_string(),
        ))
        .raw_header(header::HeaderValue::new(
            header::HeaderName::new_from_ascii_str("X-Buzz-Conversation-ID"),
            conversation_id.to_string(),
        ));
    for address in addresses(payload, "to")? {
        builder = builder.to(address);
    }
    for address in addresses(payload, "cc")? {
        builder = builder.cc(address);
    }
    for address in addresses(payload, "bcc")? {
        builder = builder.bcc(address);
    }
    if let Some(parent) = payload
        .get("in_reply_to")
        .and_then(serde_json::Value::as_str)
    {
        builder = builder.header(InReplyTo::from(parent.to_string()));
    }
    if let Some(references) = payload
        .get("references")
        .and_then(serde_json::Value::as_str)
    {
        builder = builder.header(References::from(references.to_string()));
    }
    builder
        .body(body.to_string())
        .map_err(|e| format!("email message build: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload() -> serde_json::Value {
        serde_json::json!({
            "from_address": "melissa.ferraz@snpsgroup.com",
            "display_name": "Melissa Ferraz",
            "internet_message_id": "<random@snpsgroup.com>",
            "conversation_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
            "subject": "Status",
            "body": "Concluído",
            "to": ["luis@snpsgroup.com"],
            "cc": ["helena.vega@snpsgroup.com"],
            "bcc": ["caio.andrade@snpsgroup.com"],
            "in_reply_to": "<parent@snpsgroup.com>",
            "references": "<root@snpsgroup.com> <parent@snpsgroup.com>"
        })
    }

    #[test]
    fn message_has_identity_threading_and_expected_recipients() {
        let message = build_message(&payload()).unwrap();
        assert!(message.envelope().to().iter().any(|address| {
            address
                .to_string()
                .eq_ignore_ascii_case("caio.andrade@snpsgroup.com")
        }));
        let formatted = message.formatted();
        let text = String::from_utf8_lossy(&formatted);
        assert!(text.contains("Message-ID: <random@snpsgroup.com>"));
        assert!(text.contains("In-Reply-To: <parent@snpsgroup.com>"));
        assert!(text.contains("References: <root@snpsgroup.com> <parent@snpsgroup.com>"));
        assert!(text.contains("X-Buzz-Conversation-ID: f47ac10b-58cc-4372-a567-0e02b2c3d479"));
        assert!(text.contains("To: luis@snpsgroup.com"));
        assert!(text.contains("Cc: helena.vega@snpsgroup.com"));
        assert!(!text.to_ascii_lowercase().contains("\nbcc:"));
    }
}
