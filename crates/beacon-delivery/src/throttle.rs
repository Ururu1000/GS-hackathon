//! Per-(rule, recipient) throttle (SPEC §4.3, §5.4, §13), tables `throttle_buckets` and
//! `throttle_suppressed` (migration `20261120240100_delivery_throttles.sql`).
//!
//! - Token bucket: `capacity = max`, refill `max / per` tokens per second, starts full. The
//!   router debits ONE token per recipient per notification (however many channels), inside its
//!   transaction, holding the bucket row lock (`SELECT ... FOR UPDATE`), so concurrent routers
//!   serialize on a hot (rule, recipient) and never overshoot.
//! - No token: the planned jobs are stored in `throttle_suppressed` instead (nothing is dropped).
//! - Digest: `digest_once` finds (rule, recipient) pairs with undigested rows whose bucket has a
//!   token again, takes it, and files ONE job per channel of the latest suppressed row (address
//!   and `route_info` from that row) under that row's `alert_id`/`notify_seq` (its unique key is
//!   free, the notification created no job for this recipient). `route_info` gets
//!   `"target": "digest"` and `"digest": {"count", "rule_id", "alert_ids": [...]}` (all covered
//!   rows, oldest first); `fallback` is removed. Covered rows get `digested_at = now()` and
//!   `digest_alert`. All in one transaction per pair.
//! - Critical escalations bypass the throttle (the scheduler simply doesn't call `take`).
//! - Refill uses the database clock (`now()`) so all replicas agree.

use std::time::Duration;

use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ThrottleSpec {
    pub max: u32,
    pub per: Duration,
}

impl ThrottleSpec {
    /// From a rule file: `{ max: 20, per: 1m }` (`per` parsed with `rules::parse_duration`).
    /// `max >= 1`, `per > 0`.
    pub fn from_rule_value(v: &Value) -> Result<Self, String> {
        let _ = v;
        todo!("agent B")
    }

    /// From a notify payload: `{ "max": 20, "per_ms": 60000 }`.
    pub fn from_payload_value(v: &Value) -> Result<Self, String> {
        let _ = v;
        todo!("agent B")
    }

    pub fn refill_per_sec(&self) -> f64 {
        self.max as f64 / self.per.as_secs_f64()
    }
}

/// One planned job parked by the throttle (stored as JSON in `throttle_suppressed.jobs`).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PlannedJob {
    pub channel: String,
    pub address: String,
    pub route_info: Value,
}

/// Debit one token for `(rule_id, recipient_id)`; `false` if the bucket is empty. Creates the
/// bucket full on first use and updates `capacity`/`refill_per_sec` from `spec` (rule edits).
pub async fn take(
    tx: &mut Transaction<'_, Postgres>,
    rule_id: &str,
    recipient_id: &str,
    spec: &ThrottleSpec,
) -> Result<bool, sqlx::Error> {
    let _ = (tx, rule_id, recipient_id, spec);
    todo!("agent B")
}

/// Park the planned jobs of one recipient notification.
pub async fn suppress(
    tx: &mut Transaction<'_, Postgres>,
    rule_id: &str,
    recipient_id: &str,
    alert_id: Uuid,
    notify_seq: i32,
    jobs: &[PlannedJob],
) -> Result<(), sqlx::Error> {
    let _ = (tx, rule_id, recipient_id, alert_id, notify_seq, jobs);
    todo!("agent B")
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DigestStats {
    /// (rule, recipient) pairs that got a digest.
    pub digests: u64,
    pub jobs_inserted: u64,
    /// Suppressed rows covered.
    pub rows_covered: u64,
}

/// One pass over pairs with undigested rows (bounded by `limit` pairs, `FOR UPDATE SKIP LOCKED`
/// on the bucket rows so router replicas don't collide).
pub async fn digest_once(pool: &PgPool, limit: i64) -> Result<DigestStats, sqlx::Error> {
    let _ = (pool, limit);
    todo!("agent B")
}
