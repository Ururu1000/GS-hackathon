//! SMS and phone channels: HTTP clients of the fake gateway (`crate::gateway`), or of a real
//! provider adapter later. One struct, two channel names, so a real provider is a new impl.
//!
//! Wire contract (both directions owned by delivery):
//! `POST {base_url}/provider/{sms|phone}` with header `Idempotency-Key: <job_id>` and JSON
//! [`ProviderRequest`]. Responses:
//! - `200` [`ProviderResponse`] → `Ok(Receipt)` (also for a duplicate: same `receipt_id`).
//! - `429` / `503` → `SendError::RetryAfter` when a `Retry-After: <secs>` header is present,
//!   else `SendError::Transient`.
//! - `504`, connect/read timeout after the request was sent → `SendError::Ambiguous`.
//! - connection refused / DNS → `SendError::Transient`.
//! - other `4xx` → `SendError::Permanent`; other `5xx` → `SendError::Transient`.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::channel::{AlertView, BoxFuture, Channel, OutboundMessage, Receipt, SendError};
use crate::model::DeliveryJob;

/// Hard SMS length bound (single GSM segment).
pub const SMS_MAX_CHARS: usize = 160;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayKind {
    Sms,
    Phone,
}

impl GatewayKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sms => "sms",
            Self::Phone => "phone",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderRequest {
    pub job_id: Uuid,
    pub alert_id: Uuid,
    /// `user:<id>` (board #102).
    pub recipient: String,
    /// E.164 phone number.
    pub to: String,
    /// SMS text or TTS script.
    pub text: String,
    #[serde(default)]
    pub approval_challenge: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderResponse {
    pub receipt_id: Uuid,
    /// True when this job id had already been accepted.
    pub duplicate: bool,
}

#[derive(Debug, Clone)]
pub struct GatewayClientConfig {
    /// e.g. `http://localhost:8070`.
    pub base_url: String,
    /// Public UI base for SMS short links, e.g. `http://localhost:5173`.
    pub public_url: String,
    pub timeout: Duration,
}

impl Default for GatewayClientConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:8070".into(),
            public_url: "http://localhost:5173".into(),
            timeout: Duration::from_secs(5),
        }
    }
}

pub struct GatewayChannel {
    kind: GatewayKind,
    client: reqwest::Client,
    cfg: GatewayClientConfig,
}

impl GatewayChannel {
    pub fn new(kind: GatewayKind, cfg: GatewayClientConfig) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(cfg.timeout)
            .connect_timeout(cfg.timeout)
            .build()
            .map_err(|e| format!("cannot build gateway http client: {e}"))?;
        Ok(Self { kind, client, cfg })
    }

    /// Which provider path this channel posts to (`sms` | `phone`).
    pub fn kind(&self) -> GatewayKind {
        self.kind
    }
}

/// Severity abbreviation for the SMS prefix.
fn severity_abbr(severity: &str) -> String {
    match severity.to_ascii_lowercase().as_str() {
        "critical" => "CRIT".to_string(),
        "error" => "ERR".to_string(),
        "warning" => "WARN".to_string(),
        "info" => "INFO".to_string(),
        _ => severity.to_ascii_uppercase(),
    }
}

/// `[SEV] <title> <public_url>/a/<alert_id>`, title truncated on a char boundary (with `…`)
/// so the whole text is `<= SMS_MAX_CHARS` chars (counted in chars, not bytes).
fn render_sms(severity: &str, title: &str, alert_id: &Uuid, public_url: &str) -> String {
    let link = format!("{}/a/{}", public_url.trim_end_matches('/'), alert_id);
    let prefix = format!("[{}] ", severity_abbr(severity));
    // Chars taken by everything except the title.
    let fixed = prefix.chars().count() + 1 + link.chars().count();
    let budget = SMS_MAX_CHARS.saturating_sub(fixed);
    let title_out = if title.chars().count() <= budget {
        title.to_string()
    } else if budget <= 1 {
        // No room for the title at all; the link alone almost fills the segment.
        String::new()
    } else {
        format!("{}…", title.chars().take(budget - 1).collect::<String>())
    };
    let mut body = if title_out.is_empty() {
        format!("{prefix}{link}")
    } else {
        format!("{prefix}{title_out} {link}")
    };
    // Paranoia bound: never exceed the segment, even with a gigantic link.
    if body.chars().count() > SMS_MAX_CHARS {
        body = format!(
            "{}…",
            body.chars().take(SMS_MAX_CHARS - 1).collect::<String>()
        );
    }
    body
}

/// Short TTS script; no URL (a phone call cannot carry a link).
fn render_phone(severity: &str, title: &str) -> String {
    format!(
        "Beacon alert. Severity {}. {}. Details were sent to the Beacon app.",
        severity, title
    )
}

/// Map a `reqwest` transport error to a [`SendError`]: connection-level failures are transient,
/// timeouts after the request was sent are ambiguous (the provider may have accepted it).
fn classify_reqwest_error(e: &reqwest::Error) -> SendError {
    if e.is_connect() {
        SendError::Transient(format!("gateway connect failed: {e}"))
    } else if e.is_timeout() {
        SendError::Ambiguous(format!(
            "gateway request timed out after it was sent; it may have been accepted: {e}"
        ))
    } else {
        SendError::Transient(format!("gateway request failed: {e}"))
    }
}

/// Map a non-200 provider status to a [`SendError`]. `retry_after_secs` is the parsed
/// `Retry-After` header, if present. Pure function (no I/O), unit-tested below.
fn send_error_for_status(status: u16, retry_after_secs: Option<u64>, snippet: &str) -> SendError {
    let message = if snippet.is_empty() {
        format!("gateway returned HTTP {status}")
    } else {
        format!("gateway returned HTTP {status}: {snippet}")
    };
    match status {
        429 | 503 => match retry_after_secs {
            Some(secs) => SendError::RetryAfter(message, Duration::from_secs(secs)),
            None => SendError::Transient(message),
        },
        504 => SendError::Ambiguous(message),
        400..=499 => SendError::Permanent(message),
        500..=599 => SendError::Transient(message),
        _ => SendError::Transient(format!("unexpected {message}")),
    }
}

/// Parse a `Retry-After` header value in delay-seconds form.
fn parse_retry_after(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok()
}

impl Channel for GatewayChannel {
    fn name(&self) -> &'static str {
        self.kind.as_str()
    }

    /// SMS: `[SEV] <title> <public_url>/a/<alert_id>`, title truncated (with `…`) so the whole
    /// text is <= SMS_MAX_CHARS chars; SEV = CRIT|ERR|WARN|INFO. Phone: a short TTS script,
    /// e.g. "Beacon alert. Severity critical. <title>. Details were sent to the Beacon app."
    /// `subject` is empty for both.
    fn render(&self, alert: &AlertView, job: &DeliveryJob) -> OutboundMessage {
        let mut msg = OutboundMessage {
            job_id: job.id,
            alert_id: job.alert_id,
            recipient_id: job.recipient_id.clone(),
            address: job.address.clone(),
            severity: alert.severity.clone(),
            subject: String::new(),
            body: String::new(),
        };
        msg.body = match self.kind {
            GatewayKind::Sms => render_sms(
                &alert.severity,
                &alert.title,
                &job.alert_id,
                &self.cfg.public_url,
            ),
            GatewayKind::Phone => render_phone(&alert.severity, &alert.title),
        };
        msg
    }

    fn send<'a>(&'a self, msg: &'a OutboundMessage) -> BoxFuture<'a, Result<Receipt, SendError>> {
        Box::pin(async move {
            let url = format!(
                "{}/provider/{}",
                self.cfg.base_url.trim_end_matches('/'),
                self.kind.as_str()
            );
            let req_body = ProviderRequest {
                job_id: msg.job_id,
                alert_id: msg.alert_id,
                recipient: format!("user:{}", msg.recipient_id),
                to: msg.address.clone(),
                text: msg.body.clone(),
                approval_challenge: None,
            };
            let resp = match self
                .client
                .post(&url)
                .header("Idempotency-Key", msg.job_id.to_string())
                .json(&req_body)
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(e) => return Err(classify_reqwest_error(&e)),
            };
            let status = resp.status().as_u16();
            if status == 200 {
                let provider: ProviderResponse = resp
                    .json()
                    .await
                    .map_err(|e| SendError::Transient(format!("invalid provider response: {e}")))?;
                return Ok(Receipt {
                    provider_message_id: provider.receipt_id.to_string(),
                    detail: serde_json::json!({
                        "gateway": self.cfg.base_url,
                        "duplicate": provider.duplicate,
                        "http_status": 200,
                    }),
                });
            }
            let retry_after = resp
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(parse_retry_after);
            let snippet: String = resp
                .text()
                .await
                .unwrap_or_default()
                .chars()
                .take(300)
                .collect();
            Err(send_error_for_status(status, retry_after, &snippet))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(id: Uuid, alert_id: Uuid) -> DeliveryJob {
        let now = chrono::Utc::now();
        DeliveryJob {
            id,
            alert_id,
            notify_seq: 1,
            recipient_id: "anna".to_string(),
            channel: "sms".to_string(),
            address: "+15550001111".to_string(),
            route_info: serde_json::json!({}),
            status: "pending".to_string(),
            attempts: 0,
            max_attempts: 8,
            due_at: now,
            locked_by: None,
            locked_until: None,
            lease_token: None,
            last_error: None,
            last_error_class: None,
            provider_receipt: None,
            sent_at: None,
            fallback_of: None,
            escalation_step: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn sms_channel() -> GatewayChannel {
        GatewayChannel::new(GatewayKind::Sms, GatewayClientConfig::default())
            .expect("build sms channel")
    }

    #[test]
    fn sms_render_abbreviations_and_link() {
        let ch = sms_channel();
        let alert_id = Uuid::new_v4();
        for (severity, abbr) in [
            ("critical", "CRIT"),
            ("error", "ERR"),
            ("warning", "WARN"),
            ("info", "INFO"),
        ] {
            let alert = AlertView {
                id: alert_id,
                status: "open".to_string(),
                severity: severity.to_string(),
                title: "DB down".to_string(),
                body: "b".to_string(),
            };
            let msg = ch.render(&alert, &job(Uuid::new_v4(), alert_id));
            assert_eq!(msg.subject, "");
            assert_eq!(msg.address, "+15550001111");
            assert!(
                msg.body.starts_with(&format!("[{abbr}] DB down ")),
                "body: {}",
                msg.body
            );
            assert!(
                msg.body.ends_with(&format!("/a/{alert_id}")),
                "body: {}",
                msg.body
            );
        }
        // Unknown severity falls back to uppercase.
        let alert = AlertView {
            id: alert_id,
            status: "open".to_string(),
            severity: "major".to_string(),
            title: "X".to_string(),
            body: "b".to_string(),
        };
        let msg = ch.render(&alert, &job(Uuid::new_v4(), alert_id));
        assert!(msg.body.starts_with("[MAJOR] "), "body: {}", msg.body);
    }

    #[test]
    fn sms_render_length_bound_with_long_and_multibyte_title() {
        let ch = sms_channel();
        let alert_id = Uuid::new_v4();
        // 300-char title mixing ASCII and multibyte (€ is 3 bytes, 1 char).
        let title: String = "€".repeat(150) + &"x".repeat(150);
        assert_eq!(title.chars().count(), 300);
        let alert = AlertView {
            id: alert_id,
            status: "open".to_string(),
            severity: "critical".to_string(),
            title,
            body: "b".to_string(),
        };
        let msg = ch.render(&alert, &job(Uuid::new_v4(), alert_id));
        let len = msg.body.chars().count();
        assert!(
            len <= SMS_MAX_CHARS,
            "SMS body is {len} chars: {}",
            msg.body
        );
        assert!(msg.body.contains('…'), "expected ellipsis: {}", msg.body);
        assert!(
            msg.body.ends_with(&format!("/a/{alert_id}")),
            "link must survive truncation: {}",
            msg.body
        );
        // Short titles are untouched.
        let alert = AlertView {
            id: alert_id,
            status: "open".to_string(),
            severity: "warning".to_string(),
            title: "Latency high".to_string(),
            body: "b".to_string(),
        };
        let msg = ch.render(&alert, &job(Uuid::new_v4(), alert_id));
        assert!(msg.body.contains("Latency high"));
        assert!(!msg.body.contains('…'));
        assert!(msg.body.chars().count() <= SMS_MAX_CHARS);
    }

    #[test]
    fn phone_render_script_has_title_and_no_url() {
        let ch = GatewayChannel::new(GatewayKind::Phone, GatewayClientConfig::default())
            .expect("build phone channel");
        assert_eq!(ch.name(), "phone");
        assert_eq!(sms_channel().name(), "sms");
        let alert_id = Uuid::new_v4();
        let alert = AlertView {
            id: alert_id,
            status: "open".to_string(),
            severity: "critical".to_string(),
            title: "DB down in prod".to_string(),
            body: "b".to_string(),
        };
        let msg = ch.render(&alert, &job(Uuid::new_v4(), alert_id));
        assert_eq!(msg.subject, "");
        assert_eq!(msg.address, "+15550001111");
        assert!(msg.body.contains("DB down in prod"), "body: {}", msg.body);
        assert!(
            !msg.body.contains("http") && !msg.body.contains("/a/"),
            "phone script must not contain a URL: {}",
            msg.body
        );
    }

    #[test]
    fn status_mapping() {
        // 429/503 with Retry-After -> RetryAfter, else Transient.
        match send_error_for_status(503, Some(2), "busy") {
            SendError::RetryAfter(_, d) => assert_eq!(d, Duration::from_secs(2)),
            e => panic!("expected RetryAfter, got {e:?}"),
        }
        match send_error_for_status(429, Some(7), "") {
            SendError::RetryAfter(_, d) => assert_eq!(d, Duration::from_secs(7)),
            e => panic!("expected RetryAfter, got {e:?}"),
        }
        assert!(matches!(
            send_error_for_status(503, None, "busy"),
            SendError::Transient(_)
        ));
        // 504 -> Ambiguous.
        assert!(matches!(
            send_error_for_status(504, None, ""),
            SendError::Ambiguous(_)
        ));
        // Other 4xx -> Permanent.
        assert!(matches!(
            send_error_for_status(400, None, "bad"),
            SendError::Permanent(_)
        ));
        assert!(matches!(
            send_error_for_status(404, None, ""),
            SendError::Permanent(_)
        ));
        // Other 5xx -> Transient.
        assert!(matches!(
            send_error_for_status(500, None, ""),
            SendError::Transient(_)
        ));
        assert!(matches!(
            send_error_for_status(502, Some(3), ""),
            SendError::Transient(_)
        ));
        // Retry-After parsing.
        assert_eq!(parse_retry_after("2"), Some(2));
        assert_eq!(parse_retry_after("  10 "), Some(10));
        assert_eq!(parse_retry_after("soon"), None);
    }
}
