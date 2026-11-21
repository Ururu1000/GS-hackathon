//! Audit trail (SPEC §4.5 "transitions only"), table `audit_log`, projected into the contract's
//! `TimelineEntry {at, actor, kind, message}` for `AlertDetail.timeline` (beacon-api merges these
//! with its own entries). Never store raw event bodies, codes or secrets in `message`/`detail`.

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

/// Insert one row inside the caller's tx. `actor` = `user:<id>` | `system`; `via` = `ui` |
/// `sms` | `system`.
pub async fn record(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    via: &str,
    alert_id: Option<Uuid>,
    kind: &str,
    message: &str,
    detail: Value,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO audit_log (actor, via, alert_id, kind, message, detail) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(actor)
    .bind(via)
    .bind(alert_id)
    .bind(kind)
    .bind(message)
    .bind(detail)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// One timeline row before its JSON projection.
struct Entry {
    at: DateTime<Utc>,
    actor: String,
    kind: String,
    message: String,
}

impl Entry {
    fn to_json(&self) -> Value {
        json!({
            "at": self.at.to_rfc3339_opts(SecondsFormat::Millis, true),
            "actor": self.actor,
            "kind": self.kind,
            "message": self.message,
        })
    }
}

/// Timeline entries for one alert, oldest first, plus delivery's own derived entries:
/// escalation steps fired (`kind: "escalated"`, from delivery_jobs with `route_info.target =
/// "escalation"` or starting with `"escalate:"`) and throttle digests. `at` is RFC 3339 UTC
/// with ms and `Z`.
pub async fn timeline_entries(pool: &PgPool, alert_id: Uuid) -> Result<Vec<Value>, sqlx::Error> {
    let mut entries: Vec<Entry> = Vec::new();

    // The audit trail itself, oldest first (id breaks `at` ties deterministically).
    let rows: Vec<(DateTime<Utc>, String, String, String)> = sqlx::query_as(
        "SELECT at, actor, kind, message FROM audit_log \
         WHERE alert_id = $1 ORDER BY at, id",
    )
    .bind(alert_id)
    .fetch_all(pool)
    .await?;
    entries.extend(rows.into_iter().map(|(at, actor, kind, message)| Entry {
        at,
        actor,
        kind,
        message,
    }));

    // Derived: escalation steps fired. One entry per (recipient, moment): the channels of one
    // step (phone + sms, say) are inserted in one transaction, so they share `created_at` and
    // collapse into a single timeline row.
    let escalations: Vec<(String, Value, DateTime<Utc>)> = sqlx::query_as(
        "SELECT DISTINCT ON (recipient_id, created_at, \
                                 coalesce(route_info->>'via', route_info->>'target')) \
                recipient_id, route_info, created_at \
         FROM delivery_jobs \
         WHERE alert_id = $1 \
           AND (route_info->>'target' = 'escalation' \
                OR route_info->>'target' LIKE 'escalate:%') \
         ORDER BY recipient_id, created_at, \
                  coalesce(route_info->>'via', route_info->>'target')",
    )
    .bind(alert_id)
    .fetch_all(pool)
    .await?;
    for (recipient, route_info, at) in escalations {
        // The escalation chain says who via `via` ("oncall:<schedule>" / "lead:<team>"); the
        // `escalate` action says which team in its target.
        let message = match route_info.get("via").and_then(Value::as_str) {
            Some(via) => format!("escalated to {recipient} via {via}"),
            None => {
                let target = route_info
                    .get("target")
                    .and_then(Value::as_str)
                    .unwrap_or("escalation");
                format!("escalated to {recipient} ({target})")
            }
        };
        entries.push(Entry {
            at,
            actor: "system".to_string(),
            kind: "escalated".to_string(),
            message,
        });
    }

    // Derived: throttle digests. The digest job is filed under the alert it summarizes (the
    // latest covered one), with `route_info.digest = {count, rule_id, alert_ids}`.
    let digests: Vec<(String, Value, DateTime<Utc>)> = sqlx::query_as(
        "SELECT recipient_id, route_info, created_at FROM delivery_jobs \
         WHERE alert_id = $1 AND route_info->>'target' = 'digest'",
    )
    .bind(alert_id)
    .fetch_all(pool)
    .await?;
    for (recipient, route_info, at) in digests {
        let digest = route_info.get("digest").cloned().unwrap_or(Value::Null);
        let count = digest.get("count").and_then(Value::as_u64).unwrap_or(0);
        let rule = digest
            .get("rule_id")
            .and_then(Value::as_str)
            .unwrap_or("<unknown rule>");
        entries.push(Entry {
            at,
            actor: "system".to_string(),
            kind: "digest".to_string(),
            message: format!(
                "digest delivered to {recipient}: {count} suppressed notifications (rule {rule})"
            ),
        });
    }

    // Derived: off-hours holds (`crate::offhours`): held outside someone's working hours,
    // then sent in their digest email when the hours started.
    type HeldRow = (String, DateTime<Utc>, DateTime<Utc>, Option<DateTime<Utc>>);
    let held: Vec<HeldRow> = sqlx::query_as(
        "SELECT recipient_id, created_at, release_at, released_at FROM offhours_held \
         WHERE alert_id = $1 ORDER BY id",
    )
    .bind(alert_id)
    .fetch_all(pool)
    .await?;
    for (recipient, at, release_at, released_at) in held {
        entries.push(Entry {
            at,
            actor: "system".to_string(),
            kind: "held".to_string(),
            message: format!(
                "held for {recipient}'s off-hours digest until {}",
                release_at.to_rfc3339_opts(SecondsFormat::Secs, true)
            ),
        });
        if let Some(released) = released_at {
            entries.push(Entry {
                at: released,
                actor: "system".to_string(),
                kind: "digest".to_string(),
                message: format!("sent to {recipient} in their off-hours digest"),
            });
        }
    }

    entries.sort_by_key(|e| e.at);
    Ok(entries.iter().map(Entry::to_json).collect())
}
