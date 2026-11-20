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
        let obj = v
            .as_object()
            .ok_or_else(|| format!("throttle must be an object, got {v}"))?;
        let max = parse_max(obj.get("max"))?;
        let per_raw = obj.get("per").ok_or_else(|| {
            "throttle.per is required (a duration string like \"1m\" or seconds)".to_string()
        })?;
        let per = parse_rule_per(per_raw)?;
        Ok(Self { max, per })
    }

    /// From a notify payload: `{ "max": 20, "per_ms": 60000 }`.
    pub fn from_payload_value(v: &Value) -> Result<Self, String> {
        let obj = v
            .as_object()
            .ok_or_else(|| format!("throttle must be an object, got {v}"))?;
        let max = parse_max(obj.get("max"))?;
        let per_ms_raw = obj.get("per_ms").ok_or_else(|| {
            "throttle.per_ms is required (a positive number of milliseconds)".to_string()
        })?;
        let per_ms = per_ms_raw.as_u64().ok_or_else(|| {
            format!("throttle.per_ms must be a positive integer of milliseconds, got {per_ms_raw}")
        })?;
        if per_ms == 0 {
            return Err("throttle.per_ms must be > 0".to_string());
        }
        Ok(Self {
            max,
            per: Duration::from_millis(per_ms),
        })
    }

    pub fn refill_per_sec(&self) -> f64 {
        self.max as f64 / self.per.as_secs_f64()
    }
}

fn parse_max(v: Option<&Value>) -> Result<u32, String> {
    let raw = v.ok_or_else(|| "throttle.max is required (an integer >= 1)".to_string())?;
    let n = raw
        .as_u64()
        .ok_or_else(|| format!("throttle.max must be an integer >= 1, got {raw}"))?;
    if n < 1 {
        return Err(format!("throttle.max must be an integer >= 1, got {raw}"));
    }
    u32::try_from(n).map_err(|_| format!("throttle.max must be an integer >= 1, got {raw}"))
}

fn parse_rule_per(v: &Value) -> Result<Duration, String> {
    if let Some(s) = v.as_str() {
        match crate::rules::parse_duration(s) {
            Some(d) if !d.is_zero() => Ok(d),
            _ => Err(format!(
                "throttle.per must be a positive duration like \"1m\" or seconds, got {v}"
            )),
        }
    } else if let Some(n) = v.as_u64() {
        if n == 0 {
            Err(format!(
                "throttle.per must be a positive duration like \"1m\" or seconds, got {v}"
            ))
        } else {
            Ok(Duration::from_secs(n))
        }
    } else if let Some(f) = v.as_f64() {
        if !f.is_finite() || f <= 0.0 {
            Err(format!(
                "throttle.per must be a positive duration like \"1m\" or seconds, got {v}"
            ))
        } else {
            Ok(Duration::from_secs_f64(f))
        }
    } else {
        Err(format!(
            "throttle.per must be a positive duration like \"1m\" or seconds, got {v}"
        ))
    }
}

/// One planned job parked by the throttle (stored as JSON in `throttle_suppressed.jobs`).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PlannedJob {
    pub channel: String,
    pub address: String,
    pub route_info: Value,
}

/// Refill math shared by `take` and `digest_once`: the bucket level after `elapsed_secs`
/// seconds, capped at `capacity`. Pure so tests can pin it.
fn refilled_tokens(tokens: f64, capacity: u32, refill_per_sec: f64, elapsed_secs: f64) -> f64 {
    let elapsed = elapsed_secs.max(0.0);
    (tokens + elapsed * refill_per_sec).min(f64::from(capacity))
}

/// Debit one token for `(rule_id, recipient_id)`; `false` if the bucket is empty. Creates the
/// bucket full on first use and updates `capacity`/`refill_per_sec` from `spec` (rule edits).
pub async fn take(
    tx: &mut Transaction<'_, Postgres>,
    rule_id: &str,
    recipient_id: &str,
    spec: &ThrottleSpec,
) -> Result<bool, sqlx::Error> {
    let capacity_i32 = i32::try_from(spec.max).unwrap_or(i32::MAX);
    let refill = spec.refill_per_sec();
    // Create the bucket full on first use. Tokens is double precision so bind it as float.
    sqlx::query(
        "INSERT INTO throttle_buckets \
            (rule_id, recipient_id, tokens, capacity, refill_per_sec, refilled_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, now(), now()) \
         ON CONFLICT DO NOTHING",
    )
    .bind(rule_id)
    .bind(recipient_id)
    .bind(f64::from(capacity_i32))
    .bind(capacity_i32)
    .bind(refill)
    .execute(&mut **tx)
    .await?;

    // Hold the row lock so concurrent routers serialize on a hot (rule, recipient).
    // Elapsed time comes from the DB clock so all replicas agree.
    let row: Option<(f64, f64)> = sqlx::query_as(
        "SELECT tokens, extract(epoch from now() - refilled_at)::float8 \
         FROM throttle_buckets WHERE rule_id = $1 AND recipient_id = $2 FOR UPDATE",
    )
    .bind(rule_id)
    .bind(recipient_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((tokens, elapsed_secs)) = row else {
        // Unreachable: the insert above guarantees the row.
        return Ok(false);
    };
    let refilled = refilled_tokens(tokens, spec.max, refill, elapsed_secs);
    if refilled >= 1.0 {
        sqlx::query(
            "UPDATE throttle_buckets \
             SET tokens = $3, capacity = $4, refill_per_sec = $5, \
                 refilled_at = now(), updated_at = now() \
             WHERE rule_id = $1 AND recipient_id = $2",
        )
        .bind(rule_id)
        .bind(recipient_id)
        .bind(refilled - 1.0)
        .bind(capacity_i32)
        .bind(refill)
        .execute(&mut **tx)
        .await?;
        Ok(true)
    } else {
        sqlx::query(
            "UPDATE throttle_buckets \
             SET tokens = $3, capacity = $4, refill_per_sec = $5, \
                 refilled_at = now(), updated_at = now() \
             WHERE rule_id = $1 AND recipient_id = $2",
        )
        .bind(rule_id)
        .bind(recipient_id)
        .bind(refilled)
        .bind(capacity_i32)
        .bind(refill)
        .execute(&mut **tx)
        .await?;
        Ok(false)
    }
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
    let jobs_json = serde_json::to_value(jobs).expect("PlannedJob serializes to JSON");
    sqlx::query(
        "INSERT INTO throttle_suppressed (rule_id, recipient_id, alert_id, notify_seq, jobs) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(rule_id)
    .bind(recipient_id)
    .bind(alert_id)
    .bind(notify_seq)
    .bind(jobs_json)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DigestStats {
    /// (rule, recipient) pairs that got a digest.
    pub digests: u64,
    pub jobs_inserted: u64,
    /// Suppressed rows covered.
    pub rows_covered: u64,
}

/// One suppressed row as read by the digest pass.
struct SuppressedRow {
    id: i64,
    alert_id: Uuid,
    notify_seq: i32,
    jobs: Value,
}

/// One pass over pairs with undigested rows (bounded by `limit` pairs, `FOR UPDATE SKIP LOCKED`
/// on the bucket rows so router replicas don't collide).
pub async fn digest_once(pool: &PgPool, limit: i64) -> Result<DigestStats, sqlx::Error> {
    let pairs: Vec<(String, String)> = sqlx::query_as(
        "SELECT rule_id, recipient_id FROM throttle_suppressed \
         WHERE digested_at IS NULL \
         GROUP BY rule_id, recipient_id ORDER BY rule_id, recipient_id LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let mut stats = DigestStats::default();
    for (rule_id, recipient_id) in &pairs {
        if let Some((jobs_inserted, rows_covered)) =
            digest_pair(pool, rule_id, recipient_id).await?
        {
            stats.digests += 1;
            stats.jobs_inserted += jobs_inserted;
            stats.rows_covered += rows_covered;
        }
    }
    Ok(stats)
}

/// Digest one (rule, recipient) pair. Returns `(jobs_inserted, rows_covered)` when a digest
/// was filed, `None` when there was nothing to do (no rows, bucket locked, or no token).
async fn digest_pair(
    pool: &PgPool,
    rule_id: &str,
    recipient_id: &str,
) -> Result<Option<(u64, u64)>, sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Lock the bucket row without blocking: a concurrent digest owns this pair.
    let bucket: Option<(f64, i32, f64, f64)> = sqlx::query_as(
        "SELECT tokens, capacity, refill_per_sec, \
                extract(epoch from now() - refilled_at)::float8 \
         FROM throttle_buckets WHERE rule_id = $1 AND recipient_id = $2 \
         FOR UPDATE SKIP LOCKED",
    )
    .bind(rule_id)
    .bind(recipient_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((tokens, capacity, refill_per_sec, elapsed_secs)) = bucket else {
        tx.rollback().await?;
        return Ok(None);
    };

    let rows: Vec<SuppressedRow> = sqlx::query_as(
        "SELECT id, alert_id, notify_seq, jobs FROM throttle_suppressed \
         WHERE rule_id = $1 AND recipient_id = $2 AND digested_at IS NULL \
         ORDER BY id FOR UPDATE",
    )
    .bind(rule_id)
    .bind(recipient_id)
    .fetch_all(&mut *tx)
    .await
    .map(|rows: Vec<(i64, Uuid, i32, Value)>| {
        rows.into_iter()
            .map(|(id, alert_id, notify_seq, jobs)| SuppressedRow {
                id,
                alert_id,
                notify_seq,
                jobs,
            })
            .collect()
    })?;
    if rows.is_empty() {
        tx.rollback().await?;
        return Ok(None);
    }

    let capacity_u32 = u32::try_from(capacity.max(1)).unwrap_or(u32::MAX);
    let refilled = refilled_tokens(tokens, capacity_u32, refill_per_sec, elapsed_secs);
    if refilled < 1.0 {
        // No token yet: persist the refill progress and leave the rows parked.
        sqlx::query(
            "UPDATE throttle_buckets SET tokens = $3, refilled_at = now(), updated_at = now() \
             WHERE rule_id = $1 AND recipient_id = $2",
        )
        .bind(rule_id)
        .bind(recipient_id)
        .bind(refilled)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        return Ok(None);
    }

    // Take the token.
    sqlx::query(
        "UPDATE throttle_buckets SET tokens = $3, refilled_at = now(), updated_at = now() \
         WHERE rule_id = $1 AND recipient_id = $2",
    )
    .bind(rule_id)
    .bind(recipient_id)
    .bind(refilled - 1.0)
    .execute(&mut *tx)
    .await?;

    let latest = rows.last().expect("rows non-empty");
    let planned: Vec<PlannedJob> = match serde_json::from_value(latest.jobs.clone()) {
        Ok(jobs) => jobs,
        Err(_) => {
            tx.rollback().await?;
            return Ok(None);
        }
    };
    if planned.is_empty() {
        tx.rollback().await?;
        return Ok(None);
    }

    // All covered rows, oldest first.
    let alert_ids: Vec<String> = rows.iter().map(|r| r.alert_id.to_string()).collect();
    let count = rows.len();
    let digest_value = serde_json::json!({
        "count": count,
        "rule_id": rule_id,
        "alert_ids": alert_ids,
    });
    let digest_jobs: Vec<PlannedJob> = planned
        .into_iter()
        .map(|mut job| {
            if let Some(obj) = job.route_info.as_object_mut() {
                obj.remove("fallback");
                obj.insert("target".to_string(), Value::String("digest".to_string()));
                obj.insert("digest".to_string(), digest_value.clone());
            }
            job
        })
        .collect();

    let inserted = crate::router::insert_jobs(
        &mut tx,
        latest.alert_id,
        latest.notify_seq,
        recipient_id,
        &digest_jobs,
        None,
    )
    .await?;

    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    sqlx::query(
        "UPDATE throttle_suppressed SET digested_at = now(), digest_alert = $3 \
         WHERE id = ANY($4)",
    )
    .bind(rule_id)
    .bind(recipient_id)
    .bind(latest.alert_id)
    .bind(&ids)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(Some((inserted, count as u64)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rule_spec_forms() {
        let s = ThrottleSpec::from_rule_value(&json!({"max": 20, "per": "1m"})).expect("rule form");
        assert_eq!(s.max, 20);
        assert_eq!(s.per, Duration::from_secs(60));
        assert!((s.refill_per_sec() - 20.0 / 60.0).abs() < 1e-9);

        // `per` as a plain number of seconds.
        let s = ThrottleSpec::from_rule_value(&json!({"max": 2, "per": 90})).expect("secs");
        assert_eq!(s.per, Duration::from_secs(90));

        // Compound durations go through parse_duration.
        let s =
            ThrottleSpec::from_rule_value(&json!({"max": 1, "per": "1h30m"})).expect("compound");
        assert_eq!(s.per, Duration::from_secs(5400));
    }

    #[test]
    fn rule_spec_errors() {
        for bad in [
            json!(null),
            json!([]),
            json!({}),
            json!({"max": 0, "per": "1m"}),
            json!({"max": -1, "per": "1m"}),
            json!({"max": "many", "per": "1m"}),
            json!({"max": 2}),
            json!({"max": 2, "per": "0s"}),
            json!({"max": 2, "per": "bogus"}),
            json!({"max": 2, "per": 0}),
            json!({"max": 2, "per": -5}),
            json!({"max": 2, "per": ""}),
        ] {
            assert!(
                ThrottleSpec::from_rule_value(&bad).is_err(),
                "rule form must reject {bad}"
            );
        }
    }

    #[test]
    fn payload_spec_forms() {
        let s = ThrottleSpec::from_payload_value(&json!({"max": 2, "per_ms": 60000}))
            .expect("payload form");
        assert_eq!(s.max, 2);
        assert_eq!(s.per, Duration::from_secs(60));
    }

    #[test]
    fn payload_spec_errors() {
        for bad in [
            json!(null),
            json!({}),
            json!({"max": 0, "per_ms": 1000}),
            json!({"max": 2}),
            json!({"max": 2, "per_ms": 0}),
            json!({"max": 2, "per_ms": "60s"}),
            json!({"max": 2, "per_ms": -1}),
        ] {
            assert!(
                ThrottleSpec::from_payload_value(&bad).is_err(),
                "payload form must reject {bad}"
            );
        }
    }

    #[test]
    fn refill_math_caps_and_accumulates() {
        // No elapsed: level unchanged.
        assert!((refilled_tokens(1.5, 2, 0.5, 0.0) - 1.5).abs() < 1e-9);
        // Accumulates linearly.
        assert!((refilled_tokens(0.0, 10, 0.5, 4.0) - 2.0).abs() < 1e-9);
        // Caps at capacity.
        assert!((refilled_tokens(9.0, 10, 1.0, 100.0) - 10.0).abs() < 1e-9);
        // Negative elapsed (clock skew) never drains.
        assert!((refilled_tokens(1.0, 2, 1.0, -30.0) - 1.0).abs() < 1e-9);
    }
}
