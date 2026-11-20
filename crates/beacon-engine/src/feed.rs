//! Reading alerts as the UI sees them ([`FeedAlert`]). Used by the relay, and
//! available to the API layer for REST pages and SSE snapshots.

use anyhow::Result;
use beacon_core::FeedAlert;
use beacon_core::model::{AlertField, RuleRef};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use uuid::Uuid;

pub const FEED_COLUMNS: &str = "id, team, version, rule_id, rule_revision, rule_name, status, severity, color,
    title, body, fields, count, first_seen, last_seen, source, schema, acked_by, acked_at,
    resolved_by, resolved_at, resolved_reason, last_event_id";

pub fn row_to_feed(r: &PgRow) -> Result<FeedAlert> {
    let fields: serde_json::Value = r.try_get("fields")?;
    let fields: Vec<AlertField> = serde_json::from_value(fields).unwrap_or_default();
    Ok(FeedAlert {
        id: r.try_get("id")?,
        team: r.try_get("team")?,
        version: r.try_get("version")?,
        rule: RuleRef {
            id: r.try_get("rule_id")?,
            revision: r.try_get("rule_revision")?,
            name: r.try_get("rule_name")?,
        },
        status: r.try_get("status")?,
        severity: r.try_get("severity")?,
        color: r.try_get("color")?,
        title: r.try_get("title")?,
        body: r.try_get("body")?,
        fields,
        count: r.try_get("count")?,
        first_seen: r.try_get("first_seen")?,
        last_seen: r.try_get("last_seen")?,
        source: r.try_get("source")?,
        schema: r.try_get("schema")?,
        acked_by: r.try_get("acked_by")?,
        acked_at: r.try_get("acked_at")?,
        resolved_by: r.try_get("resolved_by")?,
        resolved_at: r.try_get("resolved_at")?,
        resolved_reason: r.try_get("resolved_reason")?,
        last_event_id: r.try_get("last_event_id")?,
    })
}

pub async fn load_feed_alerts(pool: &PgPool, ids: &[Uuid]) -> Result<Vec<FeedAlert>> {
    let rows = sqlx::query(&format!("SELECT {FEED_COLUMNS} FROM alerts WHERE id = ANY($1)"))
        .bind(ids)
        .fetch_all(pool)
        .await?;
    rows.iter().map(row_to_feed).collect()
}
