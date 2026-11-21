//! Channel adapters. A new provider is one `Channel` impl registered by name;
//! routing, leasing and retries stay in shared code (SPEC §1, delivery review).

use std::future::Future;
use std::pin::Pin;

use uuid::Uuid;

use crate::model::ErrorClass;

pub mod email;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Everything a channel needs to send one job. Rendered by the worker from the alert row.
#[derive(Debug, Clone)]
pub struct OutboundMessage {
    /// Stable across retries; email uses it as Message-ID `<{job_id}@beacon>`.
    pub job_id: Uuid,
    pub alert_id: Uuid,
    pub recipient_id: String,
    /// Frozen destination (email address, phone number, ...).
    pub address: String,
    pub severity: String,
    pub subject: String,
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
    #[error("permanent: {0}")]
    Permanent(String),
    #[error("ambiguous: {0}")]
    Ambiguous(String),
}

impl SendError {
    pub fn class(&self) -> ErrorClass {
        match self {
            Self::Transient(_) => ErrorClass::Transient,
            Self::Permanent(_) => ErrorClass::Permanent,
            Self::Ambiguous(_) => ErrorClass::Ambiguous,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::Transient(m) | Self::Permanent(m) | Self::Ambiguous(m) => m,
        }
    }
}

pub trait Channel: Send + Sync {
    /// Matches `delivery_jobs.channel`, e.g. `"email"`.
    fn name(&self) -> &'static str;

    /// Send one message. Must not touch the database. Must be bounded by a timeout.
    fn send<'a>(&'a self, msg: &'a OutboundMessage) -> BoxFuture<'a, Result<Receipt, SendError>>;
}
