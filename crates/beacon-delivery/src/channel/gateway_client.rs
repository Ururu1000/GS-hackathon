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
    // agent 2: kind, shared reqwest::Client (with timeout), config.
}

impl GatewayChannel {
    pub fn new(kind: GatewayKind, cfg: GatewayClientConfig) -> Result<Self, String> {
        let _ = (kind, cfg);
        todo!("agent 2")
    }
}

impl Channel for GatewayChannel {
    fn name(&self) -> &'static str {
        todo!("agent 2: kind.as_str()")
    }

    /// SMS: `[SEV] <title> <public_url>/a/<alert_id>`, title truncated (with `…`) so the whole
    /// text is <= SMS_MAX_CHARS chars; SEV = CRIT|ERR|WARN|INFO. Phone: a short TTS script,
    /// e.g. "Beacon alert. Severity critical. <title>. Details were sent to the Beacon app."
    /// `subject` is empty for both.
    fn render(&self, alert: &AlertView, job: &DeliveryJob) -> OutboundMessage {
        let _ = (alert, job);
        todo!("agent 2")
    }

    fn send<'a>(&'a self, msg: &'a OutboundMessage) -> BoxFuture<'a, Result<Receipt, SendError>> {
        let _ = msg;
        todo!("agent 2")
    }
}
