//! Email channel: pooled async SMTP via lettre. M0 targets Mailpit on port 1025, no TLS.

use std::time::Duration;

use lettre::message::header::{ContentType, HeaderName, HeaderValue};
use lettre::message::{Mailbox, Message};
use lettre::transport::smtp::response::Response;
use lettre::{AsyncSmtpTransport, AsyncTransport, Tokio1Executor};

use crate::channel::{BoxFuture, Channel, OutboundMessage, Receipt, SendError};

#[derive(Debug, Clone)]
pub struct SmtpConfig {
    pub host: String,
    pub port: u16,
    /// `From:` mailbox, e.g. `Beacon <beacon@example.test>`.
    pub from: String,
    /// Bound on connect + whole SMTP exchange.
    pub timeout: Duration,
}

impl Default for SmtpConfig {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 1025,
            from: "Beacon <beacon@example.test>".into(),
            timeout: Duration::from_secs(5),
        }
    }
}

pub struct EmailChannel {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: Mailbox,
    timeout: Duration,
}

impl EmailChannel {
    pub fn new(cfg: SmtpConfig) -> Result<Self, String> {
        let from: Mailbox = cfg
            .from
            .parse()
            .map_err(|e| format!("invalid smtp from address {:?}: {e}", cfg.from))?;
        // builder_dangerous = no TLS / STARTTLS (Mailpit on 1025). Pooling is on by default.
        let transport = AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(cfg.host)
            .port(cfg.port)
            .timeout(Some(cfg.timeout))
            .build();
        Ok(Self {
            transport,
            from,
            timeout: cfg.timeout,
        })
    }

    fn build_message(&self, msg: &OutboundMessage) -> Result<(Message, String), SendError> {
        let to: Mailbox = msg.address.parse().map_err(|e| {
            SendError::Permanent(format!("invalid recipient address {:?}: {e}", msg.address))
        })?;
        let message_id = format!("<{}@beacon>", msg.job_id);
        let mut email = Message::builder()
            .from(self.from.clone())
            .to(to)
            .subject(msg.subject.clone())
            .message_id(Some(message_id.clone()))
            .header(ContentType::TEXT_PLAIN)
            .body(msg.body.clone())
            .map_err(|e| SendError::Permanent(format!("cannot build message: {e}")))?;
        let headers = email.headers_mut();
        headers.insert_raw(HeaderValue::new(
            HeaderName::new_from_ascii_str("X-Beacon-Alert-Id"),
            msg.alert_id.to_string(),
        ));
        headers.insert_raw(HeaderValue::new(
            HeaderName::new_from_ascii_str("X-Beacon-Job-Id"),
            msg.job_id.to_string(),
        ));
        Ok((email, message_id))
    }
}

fn receipt_from(message_id: String, resp: &Response) -> Receipt {
    let message: Vec<&str> = resp.message().collect();
    Receipt {
        provider_message_id: message_id,
        detail: serde_json::json!({
            "smtp_code": resp.code().to_string().parse::<u16>().ok(),
            "smtp_message": message.join("\n"),
        }),
    }
}

impl Channel for EmailChannel {
    fn name(&self) -> &'static str {
        "email"
    }

    fn send<'a>(&'a self, msg: &'a OutboundMessage) -> BoxFuture<'a, Result<Receipt, SendError>> {
        Box::pin(async move {
            let (email, message_id) = self.build_message(msg)?;
            let outer = self.timeout + Duration::from_secs(1);
            match tokio::time::timeout(outer, self.transport.send(email)).await {
                Err(_elapsed) => Err(SendError::Ambiguous(format!(
                    "smtp send exceeded {}ms; server may have accepted the message",
                    outer.as_millis()
                ))),
                Ok(Ok(resp)) => Ok(receipt_from(message_id, &resp)),
                Ok(Err(e)) => {
                    let text = e.to_string();
                    if e.is_permanent() {
                        Err(SendError::Permanent(text))
                    } else if e.is_transient() {
                        Err(SendError::Transient(text))
                    } else if e.is_timeout() {
                        Err(SendError::Ambiguous(text))
                    } else {
                        Err(SendError::Transient(text))
                    }
                }
            }
        })
    }
}
