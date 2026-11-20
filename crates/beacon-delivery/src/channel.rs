//! Channel adapters. A new provider is one `Channel` impl registered by name;
//! routing, leasing and retries stay in shared code (SPEC §1, delivery review).

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use uuid::Uuid;

use crate::model::{DeliveryJob, ErrorClass};

pub mod email;
pub mod gateway_client;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The alert fields a channel renders from, read by the worker right before each attempt.
#[derive(Debug, Clone)]
pub struct AlertView {
    pub id: Uuid,
    pub status: String,
    pub severity: String,
    pub title: String,
    pub body: String,
}

/// Everything a channel needs to send one job. Built by `Channel::render`.
#[derive(Debug, Clone)]
pub struct OutboundMessage {
    /// Stable across retries; email uses it as Message-ID `<{job_id}@beacon>`, the fake gateway
    /// as its idempotency key.
    pub job_id: Uuid,
    pub alert_id: Uuid,
    pub recipient_id: String,
    /// Frozen destination (email address, phone number, ...).
    pub address: String,
    pub severity: String,
    /// Email subject; empty for channels without one.
    pub subject: String,
    /// Email body, SMS text or phone TTS script.
    pub body: String,
}

/// Provider acceptance record, stored in `delivery_jobs.provider_receipt`.
#[derive(Debug, Clone)]
pub struct Receipt {
    pub provider_message_id: String,
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum SendError {
    #[error("transient: {0}")]
    Transient(String),
    /// Transient, and the provider asked us to wait at least this long (HTTP `Retry-After`).
    #[error("transient (retry after {1:?}): {0}")]
    RetryAfter(String, Duration),
    #[error("permanent: {0}")]
    Permanent(String),
    #[error("ambiguous: {0}")]
    Ambiguous(String),
}

impl SendError {
    pub fn class(&self) -> ErrorClass {
        match self {
            Self::Transient(_) | Self::RetryAfter(..) => ErrorClass::Transient,
            Self::Permanent(_) => ErrorClass::Permanent,
            Self::Ambiguous(_) => ErrorClass::Ambiguous,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::Transient(m)
            | Self::RetryAfter(m, _)
            | Self::Permanent(m)
            | Self::Ambiguous(m) => m,
        }
    }

    /// The provider's requested minimum delay, if any.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::RetryAfter(_, d) => Some(*d),
            _ => None,
        }
    }
}

pub trait Channel: Send + Sync {
    /// Matches `delivery_jobs.channel`, e.g. `"email"`, `"sms"`, `"phone"`.
    fn name(&self) -> &'static str;

    /// Render one job for this channel (SPEC §5.5). The default is the full email form:
    /// subject `[SEVERITY] title`, body + a footer with the alert and job ids.
    fn render(&self, alert: &AlertView, job: &DeliveryJob) -> OutboundMessage {
        OutboundMessage {
            job_id: job.id,
            alert_id: job.alert_id,
            recipient_id: job.recipient_id.clone(),
            address: job.address.clone(),
            severity: alert.severity.clone(),
            subject: format!("[{}] {}", alert.severity.to_uppercase(), alert.title),
            body: format!(
                "{}\n\n-- \nBeacon alert {} / job {}\n",
                alert.body, job.alert_id, job.id
            ),
        }
    }

    /// Send one message. Must not touch the database. Must be bounded by a timeout.
    fn send<'a>(&'a self, msg: &'a OutboundMessage) -> BoxFuture<'a, Result<Receipt, SendError>>;
}
