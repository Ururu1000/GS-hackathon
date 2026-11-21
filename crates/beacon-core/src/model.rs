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

/// Canonical facts about the event that opened (or escalated) an alert, for
/// delivery preferences such as "critical + payments-api" (board #100).
/// Absent values are omitted; delivery treats them as non-matching.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NotifyContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

/// The matched rule revision's delivery settings (board #132), so rules edited
/// through the API reach delivery without a restart. Delivery prefers these
/// over its startup copy of the rule files; an absent key falls back to it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NotifyPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalate_after_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throttle: Option<NotifyThrottle>,
    /// The rule's runbook id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runbook: Option<String>,
}

/// `throttle: {max, per}` normalised to milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotifyThrottle {
    pub max: u32,
    pub per_ms: u64,
}

impl NotifyThrottle {
    /// From a rule's `throttle: {max: 20, per: "1m" | <seconds>}`. `None` for
    /// a malformed value: delivery then keeps the rule file's throttle.
    pub fn from_rule_value(v: &Value) -> Option<Self> {
        let max = u32::try_from(v.get("max")?.as_u64()?).ok().filter(|&m| m >= 1)?;
        let per = v.get("per")?;
        let per_ms = if let Some(s) = per.as_str() {
            humantime_serde::re::humantime::parse_duration(s.trim()).ok()?.as_millis()
        } else if let Some(n) = per.as_u64() {
            u128::from(n) * 1000
        } else {
            let f = per.as_f64().filter(|f| f.is_finite() && *f > 0.0)?;
            (f * 1000.0) as u128
        };
        let per_ms = u64::try_from(per_ms).ok().filter(|&ms| ms > 0)?;
        Some(Self { max, per_ms })
    }
}

impl NotifyPolicy {
    pub fn from_rule(rule: &crate::rule::RuleSpec) -> Self {
        Self {
            escalate_after_ms: rule.escalate_after.and_then(|d| u64::try_from(d.as_millis()).ok()),
            throttle: rule.throttle.as_ref().and_then(NotifyThrottle::from_rule_value),
            runbook: rule.runbook.clone(),
        }
    }
}

/// The event that opened an alert, pinned for runbook templates such as
/// `{{workflowId}}` (board #172). `raw` is capped at [`MAX_NOTIFY_RAW`] bytes;
/// a larger payload becomes a truncated string preview.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotifyEvent {
    pub fields: serde_json::Map<String, Value>,
    pub raw: Value,
}

pub const MAX_NOTIFY_RAW: usize = 16 * 1024;

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
    #[serde(default)]
    pub context: NotifyContext,
    /// Opening event for explicit or team-matched runbooks, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<NotifyEvent>,
    /// `escalate_after_ms`, `throttle`, `runbook` of the matched rule revision,
    /// as top-level keys (absent when the rule has none).
    #[serde(flatten)]
    pub policy: NotifyPolicy,
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
