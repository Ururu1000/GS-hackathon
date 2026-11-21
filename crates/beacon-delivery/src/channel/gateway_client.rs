//! SMS and phone channels: HTTP clients of the fake gateway (`crate::gateway`), or of a real
//! provider adapter later. One struct, two channel names, so a real provider is a new impl.
//!
//! Wire contract (both directions owned by delivery):
//! `POST {base_url}/provider/{sms|phone}` with header `Idempotency-Key: <job_id>` and JSON
//! [`ProviderRequest`]. Responses:
//! - `200` [`ProviderResponse`] → `Ok(Receipt)` (also for a duplicate: same `receipt_id`).
//! - `429` / `503` → `SendError::RetryAfter` when a `Retry-After: <secs>` header is present
//!   (saturated and clamped to [`MAX_RETRY_AFTER`]), else `SendError::Transient`.
//! - `504`, connect/read timeout after the request was sent → `SendError::Ambiguous`.
//! - connection refused / DNS → `SendError::Transient`.
//! - `3xx` → `SendError::Permanent`: the client never follows redirects, so a 3xx means the
//!   configured provider URL is wrong (never a reason to retry, and never an excuse for the
//!   client to be pointed somewhere else).
//! - other `4xx` → `SendError::Permanent`; other `5xx` → `SendError::Transient`.
//!
//! Hardening (delivery review):
//! - redirects are disabled on the reqwest client (see above);
//! - error bodies are read with a bounded loop (at most 4 KiB) instead of buffered whole;
//! - success (200) bodies are read with a bounded loop too (at most 64 KiB) before JSON
//!   parsing; an oversize success body is `SendError::Ambiguous` (the provider may have
//!   accepted it, and buffering it whole would let a hostile provider eat memory);
//! - `Retry-After` is clamped to [`MAX_RETRY_AFTER`] so no provider answer can park a job
//!   for years;
//! - [`GatewayClientConfig::token`] makes the channel send `Authorization: Bearer <token>`
//!   when the gateway runs authenticated.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::channel::{AlertView, BoxFuture, Channel, OutboundMessage, Receipt, SendError};
use crate::model::DeliveryJob;

/// Hard SMS length bound (single GSM segment).
pub const SMS_MAX_CHARS: usize = 160;

/// Upper bound for a provider's `Retry-After` delay. `parse_retry_after` clamps to it, and
/// `worker::finalize_failed` clamps the final retry delay to it again (defence in depth:
/// other channels may produce `SendError::RetryAfter` too). Without it a hostile or broken
/// provider could park a job for decades.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(600);

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
    /// Bearer token for the gateway's provider routes. `Some` → every request carries
    /// `Authorization: Bearer <token>`; `None` (the default) → no auth header (dev gateway).
    pub token: Option<String>,
}

impl Default for GatewayClientConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:8070".into(),
            public_url: "http://localhost:5173".into(),
            timeout: Duration::from_secs(5),
            token: None,
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
            // Never follow redirects: a 3xx from the provider is a permanent misrouting
            // (the client must not POST credentials/payloads to wherever the provider
            // points), not something to retry against a different URL.
            .redirect(reqwest::redirect::Policy::none())
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

/// `[SEV] <title> <public_url>/#/alerts/<alert_id>`, title truncated on a char boundary (with `…`)
/// so the whole text is `<= SMS_MAX_CHARS` chars (counted in chars, not bytes).
fn render_sms(severity: &str, title: &str, alert_id: &Uuid, public_url: &str) -> String {
    let link = format!("{}/#/alerts/{}", public_url.trim_end_matches('/'), alert_id);
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

/// Short TTS script; no URL (a phone call cannot carry a link). The title is cut to 1000 chars
/// so the script stays under the gateway's 1600-char `text` limit.
fn render_phone(severity: &str, title: &str) -> String {
    format!(
        "Beacon alert. Severity {}. {}. Details were sent to the Beacon app.",
        severity,
        title.chars().take(1000).collect::<String>()
    )
}

/// The approval prompt an sms job with `route_info.approval` renders to, instead of the
/// normal link text:
/// `[SEV] Approve <action> for <title>? Reply "Y <code>" to approve, "N <code>" to reject.`
/// The title is truncated first (with `…`, on a char boundary) so the whole text stays
/// within [`SMS_MAX_CHARS`]; with no room for a title at all the question becomes a colon.
fn render_approval_sms(severity: &str, action: &str, title: &str, code: &str) -> String {
    let prefix = format!("[{}] Approve {} for ", severity_abbr(severity), action);
    let suffix = format!("? Reply \"Y {code}\" to approve, \"N {code}\" to reject.");
    let fixed = prefix.chars().count() + suffix.chars().count();
    let budget = SMS_MAX_CHARS.saturating_sub(fixed);
    if budget > 1 && title.chars().count() > budget {
        let title_out = format!("{}…", title.chars().take(budget - 1).collect::<String>());
        format!("{prefix}{title_out}{suffix}")
    } else if title.chars().count() <= budget {
        format!("{prefix}{title}{suffix}")
    } else {
        // No room for the title: keep the instruction, lead with a colon.
        let bare = format!(
            "[{}] Approve {}: Reply \"Y {code}\" to approve, \"N {code}\" to reject.",
            severity_abbr(severity),
            action
        );
        // Paranoia bound (a pathological action name): never exceed the segment.
        if bare.chars().count() > SMS_MAX_CHARS {
            format!(
                "{}…",
                bare.chars().take(SMS_MAX_CHARS - 1).collect::<String>()
            )
        } else {
            bare
        }
    }
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

/// Cap on error-body bytes read before truncation: a huge (or slow-dripping) error page must
/// not be buffered whole, and must not delay the worker's retry decision. Reading used to be
/// `resp.text().await`, which reads to EOF.
const ERROR_BODY_MAX_BYTES: usize = 4096;
/// Cap on success-body bytes read before JSON parsing: a huge 200 body must not be buffered
/// whole either. An oversize success body maps to [`SendError::Ambiguous`]: the provider
/// answered 200, so it may have accepted the message.
const SUCCESS_BODY_MAX_BYTES: usize = 64 * 1024;
/// Error snippets in `SendError` messages are kept short for logs and the job row.
const SNIPPET_MAX_CHARS: usize = 300;

/// Map a non-200 provider status to a [`SendError`]. `retry_after_secs` is the parsed and
/// already-clamped `Retry-After` header, if present. Pure function (no I/O), unit-tested below.
fn send_error_for_status(status: u16, retry_after_secs: Option<u64>, snippet: &str) -> SendError {
    let message = if snippet.is_empty() {
        format!("gateway returned HTTP {status}")
    } else {
        format!("gateway returned HTTP {status}: {snippet}")
    };
    match status {
        // 3xx: the client has redirects disabled, so this means the provider URL itself is
        // wrong. Permanent, never retried, never followed.
        300..=399 => SendError::Permanent(message),
        429 | 503 => match retry_after_secs {
            // Clamped again here as defence in depth: the caller should never be able to
            // hand us a value above MAX_RETRY_AFTER, but if one does, it stops here.
            Some(secs) => {
                SendError::RetryAfter(message, Duration::from_secs(secs).min(MAX_RETRY_AFTER))
            }
            None => SendError::Transient(message),
        },
        504 => SendError::Ambiguous(message),
        400..=499 => SendError::Permanent(message),
        500..=599 => SendError::Transient(message),
        _ => SendError::Transient(format!("unexpected {message}")),
    }
}

/// Parse a `Retry-After` header value in delay-seconds form. The number is accumulated
/// digit by digit with saturation (a 20-digit value must not fail to overflow-handling) and
/// clamped to [`MAX_RETRY_AFTER`]. The HTTP-date form (`Wed, 21 Oct 2015 07:28:00 GMT`) is
/// deliberately not supported: it contains non-digits, so it parses to `None` and the error
/// stays a plain `Transient`.
fn parse_retry_after(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut secs: u64 = 0;
    for b in trimmed.bytes() {
        if !b.is_ascii_digit() {
            return None;
        }
        secs = secs
            .checked_mul(10)
            .and_then(|s| s.checked_add(u64::from(b - b'0')))
            .unwrap_or(u64::MAX);
    }
    Some(secs.min(MAX_RETRY_AFTER.as_secs()))
}

/// Read at most [`SUCCESS_BODY_MAX_BYTES`] of a 200 body, then stop. Returns the bytes and
/// whether the body is longer than the cap: an oversize body is dropped with the response
/// (nothing past the cap is ever buffered or waited for).
async fn read_success_body(resp: &mut reqwest::Response) -> (Vec<u8>, bool) {
    let mut buf: Vec<u8> = Vec::new();
    let mut oversize = false;
    while buf.len() <= SUCCESS_BODY_MAX_BYTES {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                if buf.len() + chunk.len() > SUCCESS_BODY_MAX_BYTES {
                    oversize = true;
                    break;
                }
                buf.extend_from_slice(&chunk);
            }
            // End of body or transport error: keep whatever was read so far (a short or
            // broken body fails JSON parsing below, exactly as before).
            Ok(None) | Err(_) => break,
        }
    }
    (buf, oversize)
}

/// Read at most [`ERROR_BODY_MAX_BYTES`] of an error body, then stop: the body is only used
/// for a short snippet, so nothing beyond the cap is ever buffered or waited for.
async fn error_snippet(resp: &mut reqwest::Response) -> String {
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < ERROR_BODY_MAX_BYTES {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                let room = ERROR_BODY_MAX_BYTES - buf.len();
                if chunk.len() > room {
                    // Take what fits and stop reading: the rest of the body is dropped
                    // together with the response.
                    buf.extend_from_slice(&chunk[..room]);
                    break;
                }
                buf.extend_from_slice(&chunk);
            }
            // End of body or transport error: keep whatever was read so far.
            Ok(None) | Err(_) => break,
        }
    }
    String::from_utf8_lossy(&buf)
        .chars()
        .take(SNIPPET_MAX_CHARS)
        .collect()
}

impl Channel for GatewayChannel {
    fn name(&self) -> &'static str {
        self.kind.as_str()
    }

    /// SMS: `[SEV] <title> <public_url>/#/alerts/<alert_id>`, title truncated (with `…`) so the whole
    /// text is <= SMS_MAX_CHARS chars; SEV = CRIT|ERR|WARN|INFO. An sms job carrying
    /// `route_info.approval` (an approval prompt) instead renders
    /// `[SEV] Approve <action> for <title>? Reply "Y <code>" to approve, "N <code>" to reject.`
    /// and puts the provider's challenge (`Y <code>`) in `subject`, which is otherwise unused
    /// on this channel — `send` copies it into `ProviderRequest.approval_challenge` (the
    /// `Channel`/`OutboundMessage` contract has no dedicated field). Phone: a short TTS
    /// script, e.g. "Beacon alert. Severity critical. <title>. Details were sent to the
    /// Beacon app." Phone jobs never carry approvals. `subject` is otherwise empty.
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
        // An approval prompt rides only on sms jobs, and only with a code; phone jobs never
        // carry approvals.
        if self.kind == GatewayKind::Sms
            && let Some(approval) = job.route_info.get("approval").filter(|a| a.is_object())
            && let Some(code) = approval
                .get("code")
                .and_then(|c| c.as_str())
                .filter(|c| !c.is_empty())
        {
            let action = approval
                .get("action")
                .and_then(|v| v.as_str())
                .unwrap_or("action");
            msg.body = render_approval_sms(&alert.severity, action, &alert.title, code);
            // The provider challenge (`Y <code>`) travels in `subject`, otherwise unused on
            // this channel: `send` copies it into `ProviderRequest.approval_challenge`.
            msg.subject = format!("Y {code}");
        } else {
            msg.body = match self.kind {
                GatewayKind::Sms => render_sms(
                    &alert.severity,
                    &alert.title,
                    &job.alert_id,
                    &self.cfg.public_url,
                ),
                GatewayKind::Phone => render_phone(&alert.severity, &alert.title),
            };
        }
        msg
    }

    fn send<'a>(&'a self, msg: &'a OutboundMessage) -> BoxFuture<'a, Result<Receipt, SendError>> {
        Box::pin(async move {
            let url = format!(
                "{}/provider/{}",
                self.cfg.base_url.trim_end_matches('/'),
                self.kind.as_str()
            );
            // For approval prompts (sms only) `render` stashed the provider's challenge in
            // `subject`; phone jobs never carry one.
            let approval_challenge = match self.kind {
                GatewayKind::Sms if !msg.subject.is_empty() => Some(msg.subject.clone()),
                _ => None,
            };
            let req_body = ProviderRequest {
                job_id: msg.job_id,
                alert_id: msg.alert_id,
                recipient: format!("user:{}", msg.recipient_id),
                to: msg.address.clone(),
                text: msg.body.clone(),
                approval_challenge,
            };
            let mut request = self
                .client
                .post(&url)
                .header("Idempotency-Key", msg.job_id.to_string());
            if let Some(token) = self.cfg.token.as_deref() {
                request = request.bearer_auth(token);
            }
            let mut resp = match request.json(&req_body).send().await {
                Ok(resp) => resp,
                Err(e) => return Err(classify_reqwest_error(&e)),
            };
            let status = resp.status().as_u16();
            if status == 200 {
                let (body, oversize) = read_success_body(&mut resp).await;
                if oversize {
                    return Err(SendError::Ambiguous(format!(
                        "gateway returned an oversize success body (> {} bytes); \
                         it may have accepted the message",
                        SUCCESS_BODY_MAX_BYTES
                    )));
                }
                let provider: ProviderResponse = serde_json::from_slice(&body)
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
            // Bounded read (4 KiB max): never buffer a whole error page just to truncate it.
            let snippet = error_snippet(&mut resp).await;
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
                msg.body.ends_with(&format!("/#/alerts/{alert_id}")),
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
            msg.body.ends_with(&format!("/#/alerts/{alert_id}")),
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
            !msg.body.contains("http") && !msg.body.contains("#/alerts/"),
            "phone script must not contain a URL: {}",
            msg.body
        );
    }

    #[test]
    fn approval_sms_renders_prompt_and_carries_the_challenge() {
        let ch = sms_channel();
        let alert_id = Uuid::new_v4();
        let mut with_approval = job(Uuid::new_v4(), alert_id);
        with_approval.route_info = serde_json::json!({
            "target": "approval",
            "approval": { "id": Uuid::new_v4(), "code": "4821", "step_id": "s3",
                          "action": "escalate" },
        });
        let alert = AlertView {
            id: alert_id,
            status: "open".to_string(),
            severity: "critical".to_string(),
            title: "Settlement workflow wf-9 failed".to_string(),
            body: "b".to_string(),
        };
        let msg = ch.render(&alert, &with_approval);
        assert_eq!(
            msg.body,
            "[CRIT] Approve escalate for Settlement workflow wf-9 failed? \
             Reply \"Y 4821\" to approve, \"N 4821\" to reject."
        );
        // `subject` is the channel's out-of-band carriage for the provider challenge.
        assert_eq!(msg.subject, "Y 4821");
        assert!(msg.body.chars().count() <= SMS_MAX_CHARS);
        assert!(
            !msg.body.contains("#/alerts/"),
            "prompt replaces the link text"
        );

        // A long title is truncated first (with `…`), never the instruction.
        let alert = AlertView {
            id: alert_id,
            status: "open".to_string(),
            severity: "warning".to_string(),
            title: "x".repeat(120),
            body: "b".to_string(),
        };
        let msg = ch.render(&alert, &with_approval);
        assert!(msg.body.contains('…'), "title truncation: {}", msg.body);
        assert!(
            msg.body.contains("Approve escalate for "),
            "prefix survives: {}",
            msg.body
        );
        assert!(
            msg.body
                .contains("Reply \"Y 4821\" to approve, \"N 4821\" to reject."),
            "instruction must survive: {}",
            msg.body
        );
        assert!(msg.body.chars().count() <= SMS_MAX_CHARS);

        // A gigantic action name leaves no room for the title at all: the question becomes
        // a colon and the instruction still fits the segment.
        let mut huge_action = job(Uuid::new_v4(), alert_id);
        huge_action.route_info = serde_json::json!({
            "target": "approval",
            "approval": { "id": Uuid::new_v4(), "code": "4821", "step_id": "s3",
                          "action": "x".repeat(95) },
        });
        let msg = ch.render(&alert, &huge_action);
        assert!(
            msg.body
                .contains(": Reply \"Y 4821\" to approve, \"N 4821\" to reject."),
            "colon fallback: {}",
            msg.body
        );
        assert!(!msg.body.contains(" for "), "no title fit: {}", msg.body);
        assert!(msg.body.chars().count() <= SMS_MAX_CHARS);
        assert_eq!(msg.subject, "Y 4821");
    }

    #[test]
    fn approval_route_info_is_ignored_on_phone_and_codeless_sms() {
        let ch = GatewayChannel::new(GatewayKind::Phone, GatewayClientConfig::default())
            .expect("build phone channel");
        let alert_id = Uuid::new_v4();
        let mut with_approval = job(Uuid::new_v4(), alert_id);
        with_approval.route_info = serde_json::json!({
            "target": "approval",
            "approval": { "id": Uuid::new_v4(), "code": "4821", "step_id": "s3",
                          "action": "escalate" },
        });
        let alert = AlertView {
            id: alert_id,
            status: "open".to_string(),
            severity: "critical".to_string(),
            title: "DB down".to_string(),
            body: "b".to_string(),
        };
        let msg = ch.render(&alert, &with_approval);
        assert_eq!(msg.subject, "", "phone jobs never carry approvals");
        assert!(
            msg.body.starts_with("Beacon alert."),
            "TTS script: {}",
            msg.body
        );

        // An sms approval object without a code is not a prompt (defensive).
        let ch = sms_channel();
        let mut codeless = job(Uuid::new_v4(), alert_id);
        codeless.route_info = serde_json::json!({
            "target": "approval",
            "approval": { "id": Uuid::new_v4(), "step_id": "s3", "action": "escalate" },
        });
        let msg = ch.render(&alert, &codeless);
        assert_eq!(msg.subject, "");
        assert!(
            msg.body.ends_with(&format!("/#/alerts/{alert_id}")),
            "{}",
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
        // 3xx -> Permanent (the client never follows redirects).
        for status in [301u16, 302, 307, 308] {
            match send_error_for_status(status, None, "go away") {
                SendError::Permanent(m) => {
                    assert!(m.contains(&status.to_string()), "message: {m}");
                }
                e => panic!("HTTP {status} must be Permanent, got {e:?}"),
            }
        }
        // Retry-After parsing.
        assert_eq!(parse_retry_after("2"), Some(2));
        assert_eq!(parse_retry_after("  10 "), Some(10));
        assert_eq!(parse_retry_after("soon"), None);
        // The HTTP-date form stays unsupported: plain Transient, exactly as before.
        assert_eq!(parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"), None);
    }

    #[test]
    fn retry_after_is_saturated_and_clamped() {
        assert_eq!(MAX_RETRY_AFTER, Duration::from_secs(600));
        // A 20-digit value saturates instead of overflowing, then clamps to 600 s.
        assert_eq!(parse_retry_after("99999999999999999999"), Some(600));
        // 19 digits fit in u64 but still clamp.
        assert_eq!(parse_retry_after("9999999999999999999"), Some(600));
        // A decade is far above the cap.
        assert_eq!(parse_retry_after("315360000"), Some(600));
        // The cap itself passes unchanged, one second above it clamps.
        assert_eq!(parse_retry_after("600"), Some(600));
        assert_eq!(parse_retry_after("601"), Some(600));
        // Not a delay-seconds value at all.
        assert_eq!(parse_retry_after(""), None);
        assert_eq!(parse_retry_after("   "), None);
        assert_eq!(parse_retry_after("-5"), None);
        assert_eq!(parse_retry_after("1e9"), None);
        // Defence in depth: even a caller that smuggles a raw u64 in gets clamped.
        match send_error_for_status(503, Some(u64::MAX), "") {
            SendError::RetryAfter(_, d) => assert_eq!(d, MAX_RETRY_AFTER),
            e => panic!("expected RetryAfter, got {e:?}"),
        }
    }
}
