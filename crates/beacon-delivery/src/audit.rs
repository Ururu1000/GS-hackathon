//! Audit trail (SPEC §4.5 "transitions only"), table `audit_log`, projected into the contract's
//! `TimelineEntry {at, actor, kind, message}` for `AlertDetail.timeline` (beacon-api merges these
//! with its own entries). Never store raw event bodies, codes or secrets in `message`/`detail`.

use serde_json::Value;
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
    let _ = (tx, actor, via, alert_id, kind, message, detail);
    todo!("agent C")
}

/// Timeline entries for one alert, oldest first, plus delivery's own derived entries:
/// escalation steps fired (`kind: "escalated"`, from delivery_jobs with `route_info.target =
/// "escalation"`) and throttle digests. `at` is RFC 3339 UTC with ms and `Z`.
pub async fn timeline_entries(pool: &PgPool, alert_id: Uuid) -> Result<Vec<Value>, sqlx::Error> {
    let _ = (pool, alert_id);
    todo!("agent C")
}
