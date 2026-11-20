//! Shared wire types between the parts:
//! - [`NotifyPayload`]: the outbox `kind='notify'` payload, ingest/engine (a) → delivery (b).
//! - [`FeedAlert`]: the full alert the UI receives (`alerts.updates`, REST, SSE).
//!
//! JSON is snake_case and timestamps are RFC 3339 UTC with milliseconds and `Z`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::rule::RouteSpec;

/// Outbox kinds written by the aggregator.
pub mod outbox_kind {
    /// People must be paged (new alert, severity increase, re-notify). Consumed by delivery.
    pub const NOTIFY: &str = "notify";
    /// Visible alert change (count bump, status). Relayed to `alerts.updates` by (a).
    pub const ALERT_UPDATE: &str = "alert_update";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotifyReason {
    New,
    SeverityUp,
    Renotify,
}

/// Payload of an outbox row with `kind = 'notify'` (board #27/#35, spec §13).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotifyPayload {
    pub alert_id: Uuid,
    pub notify_seq: i32,
    pub rule_id: String,
    pub rule_revision: i64,
    pub team: String,
    pub severity: String,
    pub reason: NotifyReason,
    /// The rule's route as configured, unexpanded (delivery expands it).
    pub route: Vec<RouteSpec>,
    #[serde(with = "rfc3339_ms")]
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuleRef {
    pub id: String,
    pub revision: i64,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlertField {
    pub path: String,
    pub label: String,
    pub value: Value,
}

/// The full alert as the UI sees it (ui-api-contract.md, `FeedAlert`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeedAlert {
    pub id: Uuid,
    pub team: String,
    pub version: i64,
    pub rule: RuleRef,
    pub status: String,
    pub severity: String,
    pub color: String,
    pub title: String,
    pub body: String,
    pub fields: Vec<AlertField>,
    pub count: i64,
    #[serde(with = "rfc3339_ms")]
    pub first_seen: DateTime<Utc>,
    #[serde(with = "rfc3339_ms")]
    pub last_seen: DateTime<Utc>,
    pub source: String,
    pub schema: String,
    pub acked_by: Option<String>,
    #[serde(with = "rfc3339_ms::option")]
    pub acked_at: Option<DateTime<Utc>>,
    pub resolved_by: Option<String>,
    #[serde(with = "rfc3339_ms::option")]
    pub resolved_at: Option<DateTime<Utc>>,
    pub resolved_reason: Option<String>,
    /// Id of the latest event folded into this alert (used to measure latency).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_event_id: Option<String>,
}

pub mod rfc3339_ms {
    use chrono::{DateTime, SecondsFormat, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(t: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&t.to_rfc3339_opts(SecondsFormat::Millis, true))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DateTime<Utc>, D::Error> {
        let s = String::deserialize(d)?;
        DateTime::parse_from_rfc3339(&s)
            .map(|t| t.with_timezone(&Utc))
            .map_err(serde::de::Error::custom)
    }

    pub mod option {
        use super::*;

        pub fn serialize<S: Serializer>(t: &Option<DateTime<Utc>>, s: S) -> Result<S::Ok, S::Error> {
            match t {
                Some(t) => super::serialize(t, s),
                None => s.serialize_none(),
            }
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<DateTime<Utc>>, D::Error> {
            let s = Option::<String>::deserialize(d)?;
            s.map(|s| {
                DateTime::parse_from_rfc3339(&s)
                    .map(|t| t.with_timezone(&Utc))
                    .map_err(serde::de::Error::custom)
            })
            .transpose()
        }
    }
}
