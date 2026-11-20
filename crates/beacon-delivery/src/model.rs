//! Shared row and payload types. Column names match `migrations/*_delivery_create_delivery_jobs.sql`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `delivery_jobs.status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    InFlight,
    Sent,
    /// The alert was acked/resolved before the send.
    Cancelled,
    /// Retries exhausted, permanent error, or lease expired with no attempts left.
    Dead,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InFlight => "in_flight",
            Self::Sent => "sent",
            Self::Cancelled => "cancelled",
            Self::Dead => "dead",
        }
    }
}

/// `delivery_jobs.last_error_class`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    /// Safe to retry (connection refused, SMTP 4xx).
    Transient,
    /// Retrying will not help (SMTP 5xx, bad address).
    Permanent,
    /// The provider may or may not have accepted it (timeout after DATA, lease expired mid-send).
    Ambiguous,
}

impl ErrorClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::Permanent => "permanent",
            Self::Ambiguous => "ambiguous",
        }
    }
}

/// One `delivery_jobs` row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DeliveryJob {
    pub id: Uuid,
    pub alert_id: Uuid,
    pub notify_seq: i32,
    pub recipient_id: String,
    pub channel: String,
    pub address: String,
    pub route_info: serde_json::Value,
    pub status: String,
    pub attempts: i32,
    pub max_attempts: i32,
    pub due_at: DateTime<Utc>,
    pub locked_by: Option<String>,
    pub locked_until: Option<DateTime<Utc>>,
    pub lease_token: Option<Uuid>,
    pub last_error: Option<String>,
    pub last_error_class: Option<String>,
    pub provider_receipt: Option<serde_json::Value>,
    pub sent_at: Option<DateTime<Utc>>,
    pub fallback_of: Option<Uuid>,
    pub escalation_step: Option<i32>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// `outbox.payload` for `kind = 'notify'`, written by the aggregator (part (a)), board #27.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyPayload {
    pub alert_id: Uuid,
    pub notify_seq: i32,
    pub rule_id: String,
    pub rule_revision: i64,
    pub severity: String,
    /// `new | severity_up | renotify`.
    pub reason: String,
    /// The rule's `route:` as configured, unexpanded.
    pub route: Vec<RouteEntry>,
    pub created_at: DateTime<Utc>,
}

/// One entry of a rule's `route:` list (SPEC §4.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteEntry {
    /// `team:<id>`, `user:<id>` or `oncall:<schedule>`.
    pub to: String,
    /// Explicit channel restriction. When present it takes precedence (PR #1 review, finding 4).
    #[serde(default)]
    pub channels: Option<Vec<String>>,
}

/// Alert status as stored in `alerts.status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertStatus {
    Open,
    Acked,
    Resolved,
}

impl AlertStatus {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "open" => Some(Self::Open),
            "acked" => Some(Self::Acked),
            "resolved" => Some(Self::Resolved),
            _ => None,
        }
    }

    /// Only open alerts page people. Acked alerts keep counting without re-paging (SPEC §2.2).
    pub fn is_deliverable(self) -> bool {
        matches!(self, Self::Open)
    }
}
