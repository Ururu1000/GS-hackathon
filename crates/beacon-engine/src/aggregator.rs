//! Aggregator role: `alerts.matches` → dedup → Postgres `alerts` + `outbox`.
//!
//! Matches are keyed by fingerprint, so one consumer owns each incident's
//! partition. A batch of matches is planned in memory ([`plan`], pure and unit
//! tested) and written in **one transaction** with a constant number of
//! statements, however many events it folds: duplicates cost an in-memory
//! increment, not a database write.
//!
//! M0 dedup (spec §13): a match within `window` of the active alert's
//! `last_seen` bumps `count`. Otherwise the old alert is retired
//! (`dedup_active = false`, it stays open) and a new alert is inserted, which
//! writes a `notify` outbox row for delivery.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use beacon_core::MatchKind;
use beacon_core::MatchRecord;
use beacon_core::model::{NotifyPayload, NotifyReason, outbox_kind};
use beacon_core::severity::Severity;
use beacon_ingest::kafka::{self, ALERTS_MATCHES};
use chrono::{DateTime, Utc};
use rdkafka::Message;
use rdkafka::consumer::{CommitMode, Consumer};
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use tokio::sync::watch;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::OUTBOX_CHANNEL;

pub const GROUP: &str = "beacon-aggregator";
const MAX_BATCH: usize = 5000;

/// How long to keep collecting after the first match of a batch. Longer means
/// fewer, larger transactions and more duplicates coalesced per write.
fn linger() -> Duration {
    let ms = std::env::var("AGGREGATOR_LINGER_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(20);
    Duration::from_millis(ms)
}

/// In-memory state of one alert row while a batch is planned.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AlertState {
    pub id: Uuid,
    pub rule_id: String,
    pub rule_revision: i64,
    pub rule_name: String,
    pub team: String,
    pub fingerprint: String,
    pub generation: i32,
    pub dedup_active: bool,
    pub dedup_window_ms: i64,
    pub status: String,
    pub severity: String,
    pub color: String,
    pub title: String,
    pub body: String,
    pub fields: Value,
    pub count: i64,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub version: i64,
    pub status_version: i64,
    pub notify_seq: i32,
    pub source: String,
    pub schema: String,
    pub last_event_id: Option<String>,
    pub resolved_by: Option<String>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub resolved_reason: Option<String>,
    #[serde(skip)]
    pub is_new: bool,
    #[serde(skip)]
    pub touched: bool,
    #[serde(skip)]
    pub status_changed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutboxRow {
    pub kind: &'static str,
    pub alert_id: Uuid,
    pub payload: Value,
}

#[derive(Debug, Default)]
pub struct Plan {
    pub inserts: Vec<AlertState>,
    pub updates: Vec<AlertState>,
    pub outbox: Vec<OutboxRow>,
    /// Matches folded into an existing or new alert (for metrics).
    pub folded: usize,
    /// Fires older than a recovery already seen for their incident key.
    pub suppressed: usize,
    /// Latest recovery time per (rule_id, fingerprint), to upsert.
    pub recoveries: Vec<(String, String, DateTime<Utc>)>,
}

/// Latest recovery seen per (rule_id, fingerprint). Fire and recovery events
/// have different event ids, so they can reach the aggregator out of order.
pub type Recoveries = HashMap<(String, String), DateTime<Utc>>;

fn rank(s: &str) -> u8 {
    Severity::normalize(s).map(Severity::rank).unwrap_or(0)
}

/// Plan the effect of `batch` given the currently active alerts and the latest
/// recoveries for its fingerprints. Pure: no I/O, deterministic except for new
/// alert ids.
pub fn plan(batch: &[MatchRecord], existing: Vec<AlertState>, mut recovered: Recoveries, now: DateTime<Utc>) -> Plan {
    let mut states: Vec<AlertState> = existing;
    // (rule_id, fingerprint) → index of the active alert, if any.
    let mut active: HashMap<(String, String), usize> = states
        .iter()
        .enumerate()
        .map(|(i, s)| ((s.rule_id.clone(), s.fingerprint.clone()), i))
        .collect();
    let mut notifies: Vec<(usize, NotifyReason, i32, &MatchRecord)> = Vec::new();
    let mut plan = Plan::default();
    let mut recovered_dirty = std::collections::HashSet::new();

    for m in batch {
        let key = (m.rule_id.clone(), m.fingerprint.clone());
        let current = active.get(&key).copied();
        match m.kind {
            MatchKind::Fire if recovered.get(&key).is_some_and(|&at| m.ingested_at <= at) => {
                // The incident already recovered after this fire happened.
                plan.suppressed += 1;
            }
            MatchKind::Fire => {
                if let Some(i) = current.filter(|&i| within(&states[i], m)) {
                    let s = &mut states[i];
                    s.count += 1;
                    if m.ingested_at > s.last_seen {
                        s.last_seen = m.ingested_at;
                    }
                    s.last_event_id = Some(m.event_id.clone());
                    s.touched = true;
                    if rank(&m.severity) > rank(&s.severity) {
                        s.severity = m.severity.clone();
                        s.notify_seq += 1;
                        notifies.push((i, NotifyReason::SeverityUp, s.notify_seq, m));
                    }
                    plan.folded += 1;
                } else {
                    let generation = match current {
                        Some(i) => {
                            let old = &mut states[i];
                            old.dedup_active = false;
                            old.touched = true;
                            old.generation + 1
                        }
                        None => 1,
                    };
                    states.push(new_alert(m, generation));
                    let i = states.len() - 1;
                    active.insert(key, i);
                    notifies.push((i, NotifyReason::New, 1, m));
                }
            }
            MatchKind::Recover => {
                let latest = recovered.entry(key.clone()).or_insert(m.ingested_at);
                if m.ingested_at > *latest {
                    *latest = m.ingested_at;
                }
                recovered_dirty.insert(key.clone());
                // A recovery older than the alert's latest fire does not close it.
                if let Some(i) = current.filter(|&i| m.ingested_at >= states[i].last_seen) {
                    let s = &mut states[i];
                    s.status = "resolved".into();
                    s.resolved_at = Some(m.ingested_at);
                    s.resolved_by = Some("system".into());
                    s.resolved_reason = Some("recovered".into());
                    s.dedup_active = false;
                    s.status_changed = true;
                    s.touched = true;
                    s.last_event_id = Some(m.event_id.clone());
                    active.remove(&key);
                }
            }
        }
    }

    // One version bump per alert per batch; status_version follows status changes.
    for s in states.iter_mut().filter(|s| s.touched && !s.is_new) {
        s.version += 1;
        if s.status_changed {
            s.status_version = s.version;
        }
    }

    // One notify per opened alert (and per severity escalation, §5.3); delivery
    // re-checks eligibility before sending, so an alert resolved in the same
    // batch is still recorded.
    for (i, reason, seq, m) in notifies {
        let s = &states[i];
        let payload = NotifyPayload {
            alert_id: s.id,
            notify_seq: seq,
            rule_id: s.rule_id.clone(),
            rule_revision: s.rule_revision,
            team: s.team.clone(),
            severity: if reason == NotifyReason::New { m.severity.clone() } else { s.severity.clone() },
            reason,
            route: m.route.clone(),
            created_at: now,
        };
        plan.outbox.push(OutboxRow {
            kind: outbox_kind::NOTIFY,
            alert_id: s.id,
            payload: serde_json::to_value(payload).expect("notify payload serializes"),
        });
    }
    for key in recovered_dirty {
        let at = recovered[&key];
        plan.recoveries.push((key.0, key.1, at));
    }
    for s in states.into_iter().filter(|s| s.touched) {
        plan.outbox.push(OutboxRow {
            kind: outbox_kind::ALERT_UPDATE,
            alert_id: s.id,
            payload: json!({"version": s.version}),
        });
        if s.is_new {
            plan.inserts.push(s);
        } else {
            plan.updates.push(s);
        }
    }
    plan
}

fn within(s: &AlertState, m: &MatchRecord) -> bool {
    let gap = m.ingested_at.signed_duration_since(s.last_seen).num_milliseconds();
    gap <= m.dedup_window_ms as i64
}

fn new_alert(m: &MatchRecord, generation: i32) -> AlertState {
    AlertState {
        id: Uuid::now_v7(),
        rule_id: m.rule_id.clone(),
        rule_revision: m.rule_revision,
        rule_name: m.rule_name.clone(),
        team: m.team.clone(),
        fingerprint: m.fingerprint.clone(),
        generation,
        dedup_active: true,
        dedup_window_ms: m.dedup_window_ms as i64,
        status: "open".into(),
        severity: m.severity.clone(),
        color: m.color.clone(),
        title: m.title.clone(),
        body: m.body.clone(),
        fields: serde_json::to_value(&m.fields).unwrap_or(Value::Array(vec![])),
        count: 1,
        first_seen: m.ingested_at,
        last_seen: m.ingested_at,
        version: 1,
        status_version: 1,
        notify_seq: 1,
        source: m.source.clone(),
        schema: m.schema.clone(),
        last_event_id: Some(m.event_id.clone()),
        resolved_by: None,
        resolved_at: None,
        resolved_reason: None,
        is_new: true,
        touched: true,
        status_changed: false,
    }
}

/// Load the active alerts for the batch's fingerprints, locking them.
async fn load_active(tx: &mut sqlx::PgConnection, keys: &[(String, String)]) -> Result<Vec<AlertState>> {
    let (rules, fps): (Vec<String>, Vec<String>) = keys.iter().cloned().unzip();
    let rows = sqlx::query(
        "SELECT id, rule_id, rule_revision, rule_name, team, fingerprint, generation, dedup_active,
                dedup_window_ms, status, severity, color, title, body, fields, count, first_seen,
                last_seen, version, status_version, notify_seq, source, schema, last_event_id,
                resolved_by, resolved_at, resolved_reason
         FROM alerts
         WHERE (rule_id, fingerprint) IN (SELECT * FROM unnest($1::text[], $2::text[]))
           AND dedup_active AND status IN ('open', 'acked')
         FOR UPDATE",
    )
    .bind(&rules)
    .bind(&fps)
    .fetch_all(&mut *tx)
    .await
    .context("loading active alerts")?;
    rows.into_iter()
        .map(|r| {
            Ok(AlertState {
                id: r.try_get("id")?,
                rule_id: r.try_get("rule_id")?,
                rule_revision: r.try_get("rule_revision")?,
                rule_name: r.try_get("rule_name")?,
                team: r.try_get("team")?,
                fingerprint: r.try_get("fingerprint")?,
                generation: r.try_get("generation")?,
                dedup_active: r.try_get("dedup_active")?,
                dedup_window_ms: r.try_get("dedup_window_ms")?,
                status: r.try_get("status")?,
                severity: r.try_get("severity")?,
                color: r.try_get("color")?,
                title: r.try_get("title")?,
                body: r.try_get("body")?,
                fields: r.try_get("fields")?,
                count: r.try_get("count")?,
                first_seen: r.try_get("first_seen")?,
                last_seen: r.try_get("last_seen")?,
                version: r.try_get("version")?,
                status_version: r.try_get("status_version")?,
                notify_seq: r.try_get("notify_seq")?,
                source: r.try_get("source")?,
                schema: r.try_get("schema")?,
                last_event_id: r.try_get("last_event_id")?,
                resolved_by: r.try_get("resolved_by")?,
                resolved_at: r.try_get("resolved_at")?,
                resolved_reason: r.try_get("resolved_reason")?,
                is_new: false,
                touched: false,
                status_changed: false,
            })
        })
        .collect()
}

async fn load_recoveries(tx: &mut sqlx::PgConnection, keys: &[(String, String)]) -> Result<Recoveries> {
    let (rules, fps): (Vec<String>, Vec<String>) = keys.iter().cloned().unzip();
    let rows = sqlx::query(
        "SELECT rule_id, fingerprint, recovered_at FROM recoveries
         WHERE (rule_id, fingerprint) IN (SELECT * FROM unnest($1::text[], $2::text[]))",
    )
    .bind(&rules)
    .bind(&fps)
    .fetch_all(&mut *tx)
    .await
    .context("loading recoveries")?;
    rows.into_iter()
        .map(|r| Ok(((r.try_get("rule_id")?, r.try_get("fingerprint")?), r.try_get("recovered_at")?)))
        .collect()
}

/// Apply one batch in a single transaction. Returns the plan that was written.
pub async fn apply_batch(pool: &PgPool, batch: &[MatchRecord]) -> Result<Plan> {
    // Lookup by (rule_id, fingerprint) so the partial unique index is used.
    let mut keys: Vec<(String, String)> = batch.iter().map(|m| (m.rule_id.clone(), m.fingerprint.clone())).collect();
    keys.sort();
    keys.dedup();
    let mut tx = pool.begin().await?;
    let existing = load_active(&mut tx, &keys).await?;
    let recovered = load_recoveries(&mut tx, &keys).await?;
    let plan = plan(batch, existing, recovered, Utc::now());

    if !plan.updates.is_empty() {
        sqlx::query(
            "UPDATE alerts a SET
                count = u.count, last_seen = u.last_seen, version = u.version, status = u.status,
                status_version = u.status_version, severity = u.severity, dedup_active = u.dedup_active,
                notify_seq = u.notify_seq, last_event_id = u.last_event_id, resolved_at = u.resolved_at,
                resolved_by = u.resolved_by, resolved_reason = u.resolved_reason
             FROM jsonb_populate_recordset(NULL::alerts, $1) AS u
             WHERE a.id = u.id",
        )
        .bind(serde_json::to_value(&plan.updates)?)
        .execute(&mut *tx)
        .await
        .context("updating alerts")?;
    }
    if !plan.inserts.is_empty() {
        sqlx::query(
            "INSERT INTO alerts (id, rule_id, rule_revision, rule_name, team, fingerprint, generation,
                dedup_active, dedup_window_ms, status, severity, color, title, body, fields, count,
                first_seen, last_seen, version, status_version, notify_seq, source, schema,
                last_event_id, resolved_by, resolved_at, resolved_reason)
             SELECT id, rule_id, rule_revision, rule_name, team, fingerprint, generation,
                dedup_active, dedup_window_ms, status, severity, color, title, body, fields, count,
                first_seen, last_seen, version, status_version, notify_seq, source, schema,
                last_event_id, resolved_by, resolved_at, resolved_reason
             FROM jsonb_populate_recordset(NULL::alerts, $1)",
        )
        .bind(serde_json::to_value(&plan.inserts)?)
        .execute(&mut *tx)
        .await
        .context("inserting alerts")?;
    }
    if !plan.recoveries.is_empty() {
        let (rules, rest): (Vec<String>, Vec<(String, DateTime<Utc>)>) =
            plan.recoveries.iter().map(|(r, f, at)| (r.clone(), (f.clone(), *at))).unzip();
        let (fps, ats): (Vec<String>, Vec<DateTime<Utc>>) = rest.into_iter().unzip();
        sqlx::query(
            "INSERT INTO recoveries (rule_id, fingerprint, recovered_at)
             SELECT * FROM unnest($1::text[], $2::text[], $3::timestamptz[])
             ON CONFLICT (rule_id, fingerprint)
             DO UPDATE SET recovered_at = GREATEST(recoveries.recovered_at, EXCLUDED.recovered_at)",
        )
        .bind(&rules)
        .bind(&fps)
        .bind(&ats)
        .execute(&mut *tx)
        .await
        .context("recording recoveries")?;
    }
    if !plan.outbox.is_empty() {
        let rows: Vec<Value> = plan
            .outbox
            .iter()
            .map(|o| json!({"kind": o.kind, "alert_id": o.alert_id, "payload": o.payload}))
            .collect();
        sqlx::query(
            "INSERT INTO outbox (kind, alert_id, payload)
             SELECT kind, alert_id, payload FROM jsonb_to_recordset($1) AS x(kind text, alert_id uuid, payload jsonb)",
        )
        .bind(Value::Array(rows))
        .execute(&mut *tx)
        .await
        .context("inserting outbox rows")?;
        sqlx::query("SELECT pg_notify($1, '')")
            .bind(OUTBOX_CHANNEL)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(plan)
}

pub async fn run_aggregator(bootstrap: String, pool: PgPool, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let consumer = kafka::consumer(&bootstrap, GROUP, &[ALERTS_MATCHES], true)?;
    let linger = linger();
    info!(linger_ms = linger.as_millis() as u64, "aggregator started");
    loop {
        // Block for the first message, then linger briefly to build a batch.
        let first = tokio::select! {
            _ = shutdown.changed() => break,
            m = consumer.recv() => m,
        };
        let mut msgs = Vec::with_capacity(MAX_BATCH);
        match first {
            Ok(m) => msgs.push(m),
            Err(e) => {
                warn!(error = %e, "aggregator receive error");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        }
        let deadline = tokio::time::Instant::now() + linger;
        while msgs.len() < MAX_BATCH {
            match tokio::time::timeout_at(deadline, consumer.recv()).await {
                Ok(Ok(m)) => msgs.push(m),
                Ok(Err(e)) => {
                    warn!(error = %e, "aggregator receive error");
                    break;
                }
                Err(_) => break,
            }
        }
        let mut batch = Vec::with_capacity(msgs.len());
        for m in &msgs {
            match m.payload().map(serde_json::from_slice::<MatchRecord>) {
                Some(Ok(rec)) => batch.push(rec),
                Some(Err(e)) => error!(partition = m.partition(), offset = m.offset(), error = %e, "undecodable match; skipped"),
                None => {}
            }
        }
        let mut backoff = Duration::from_millis(50);
        loop {
            match apply_batch(&pool, &batch).await {
                Ok(plan) => {
                    tracing::debug!(
                        matches = batch.len(),
                        inserts = plan.inserts.len(),
                        updates = plan.updates.len(),
                        "batch applied"
                    );
                    break;
                }
                Err(e) => {
                    error!(error = format!("{e:#}"), "applying batch failed; retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                    if *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
        for m in &msgs {
            if let Err(e) = consumer.store_offset_from_message(m) {
                warn!(error = %e, "store offset failed");
            }
        }
    }
    let _ = consumer.commit_consumer_state(CommitMode::Sync);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use beacon_core::rule::RouteSpec;
    use chrono::TimeZone;

    fn t(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000 + secs, 0).unwrap()
    }

    fn m(event: &str, fp: &str, at: i64, kind: MatchKind, sev: &str) -> MatchRecord {
        MatchRecord {
            match_id: format!("{event}:r"),
            kind,
            rule_id: "r".into(),
            rule_revision: 1,
            rule_name: "R".into(),
            team: "t".into(),
            fingerprint: fp.into(),
            key: vec![],
            dedup_window_ms: 60_000,
            severity: sev.into(),
            color: "red".into(),
            title: "title".into(),
            body: String::new(),
            fields: vec![],
            route: vec![RouteSpec { to: "team:t".into(), channels: None }],
            event_id: event.into(),
            source: "http:x".into(),
            schema: "s".into(),
            ingested_at: t(at),
            diagnostics: vec![],
        }
    }

    fn fire(e: &str, fp: &str, at: i64) -> MatchRecord {
        m(e, fp, at, MatchKind::Fire, "error")
    }

    fn kinds(p: &Plan) -> Vec<&str> {
        p.outbox.iter().map(|o| o.kind).collect()
    }

    #[test]
    fn five_hundred_dupes_make_one_alert() {
        let batch: Vec<_> = (0..500).map(|i| fire(&format!("e{i}"), "fp", i / 10)).collect();
        let p = plan(&batch, vec![], Recoveries::new(), t(100));
        assert_eq!(p.inserts.len(), 1);
        assert_eq!(p.inserts[0].count, 500);
        assert_eq!(p.inserts[0].version, 1);
        assert_eq!(p.inserts[0].last_seen, t(49));
        assert_eq!(p.inserts[0].last_event_id.as_deref(), Some("e499"));
        assert_eq!(kinds(&p), vec!["notify", "alert_update"]);
        let n: NotifyPayload = serde_json::from_value(p.outbox[0].payload.clone()).unwrap();
        assert_eq!(n.notify_seq, 1);
        assert_eq!(n.reason, NotifyReason::New);
        assert_eq!(n.route[0].to, "team:t");
    }

    #[test]
    fn distinct_fingerprints_make_distinct_alerts() {
        let p = plan(&[fire("a", "fp1", 0), fire("b", "fp2", 0), fire("c", "fp1", 1)], vec![], Recoveries::new(), t(5));
        assert_eq!(p.inserts.len(), 2);
        let counts: Vec<i64> = p.inserts.iter().map(|s| s.count).collect();
        assert_eq!(counts, vec![2, 1]);
    }

    #[test]
    fn bump_existing_alert_without_repaging() {
        let existing = plan(&[fire("a", "fp", 0)], vec![], Recoveries::new(), t(0)).inserts;
        let mut ex = existing[0].clone();
        ex.is_new = false;
        ex.touched = false;
        let p = plan(&[fire("b", "fp", 30), fire("c", "fp", 40)], vec![ex], Recoveries::new(), t(41));
        assert!(p.inserts.is_empty());
        assert_eq!(p.updates.len(), 1);
        assert_eq!(p.updates[0].count, 3);
        assert_eq!(p.updates[0].version, 2, "one version bump per batch");
        assert_eq!(kinds(&p), vec!["alert_update"], "count bumps never page");
    }

    #[test]
    fn window_expiry_retires_and_opens_new_generation() {
        let mut ex = plan(&[fire("a", "fp", 0)], vec![], Recoveries::new(), t(0)).inserts.remove(0);
        ex.is_new = false;
        ex.touched = false;
        let p = plan(&[fire("b", "fp", 61)], vec![ex.clone()], Recoveries::new(), t(61));
        assert_eq!(p.updates.len(), 1);
        assert!(!p.updates[0].dedup_active);
        assert_eq!(p.updates[0].status, "open", "retired alert stays open");
        assert_eq!(p.inserts.len(), 1);
        assert_eq!(p.inserts[0].generation, 2);
        assert_ne!(p.inserts[0].id, ex.id);
        assert_eq!(kinds(&p).iter().filter(|k| **k == "notify").count(), 1);
    }

    #[test]
    fn severity_increase_pages_again() {
        let mut ex = plan(&[fire("a", "fp", 0)], vec![], Recoveries::new(), t(0)).inserts.remove(0);
        ex.is_new = false;
        ex.touched = false;
        let p = plan(&[m("b", "fp", 5, MatchKind::Fire, "critical")], vec![ex], Recoveries::new(), t(5));
        assert_eq!(p.updates[0].severity, "critical");
        assert_eq!(p.updates[0].notify_seq, 2);
        let n: NotifyPayload = serde_json::from_value(p.outbox[0].payload.clone()).unwrap();
        assert_eq!(n.reason, NotifyReason::SeverityUp);
        assert_eq!(n.notify_seq, 2);
    }

    #[test]
    fn recovery_resolves_active_alert() {
        let mut ex = plan(&[fire("a", "fp", 0)], vec![], Recoveries::new(), t(0)).inserts.remove(0);
        ex.is_new = false;
        ex.touched = false;
        let p = plan(&[m("r", "fp", 10, MatchKind::Recover, "error")], vec![ex], Recoveries::new(), t(10));
        let s = &p.updates[0];
        assert_eq!(s.status, "resolved");
        assert_eq!(s.resolved_reason.as_deref(), Some("recovered"));
        assert!(!s.dedup_active);
        assert_eq!(s.status_version, s.version);
        assert_eq!(kinds(&p), vec!["alert_update"]);
    }

    #[test]
    fn fire_after_recovery_opens_new_and_both_are_notified() {
        let p = plan(
            &[fire("a", "fp", 0), m("r", "fp", 1, MatchKind::Recover, "error"), fire("b", "fp", 2)],
            vec![],
            Recoveries::new(),
            t(2),
        );
        assert_eq!(p.inserts.len(), 2);
        assert_eq!(p.inserts[0].status, "resolved");
        assert_eq!(p.inserts[1].status, "open");
        // One notify per opened alert; delivery re-checks eligibility.
        assert_eq!(kinds(&p).iter().filter(|k| **k == "notify").count(), 2);
    }

    #[test]
    fn recovery_overtaking_its_fire_suppresses_the_fire() {
        // The recovery (t=10) reaches the aggregator before the fire it ends (t=5).
        let p = plan(&[m("r", "fp", 10, MatchKind::Recover, "error")], vec![], Recoveries::new(), t(10));
        assert!(p.inserts.is_empty() && p.outbox.is_empty());
        assert_eq!(p.recoveries, vec![("r".to_string(), "fp".to_string(), t(10))]);

        let recovered: Recoveries = [(("r".to_string(), "fp".to_string()), t(10))].into();
        let p = plan(&[fire("late", "fp", 5)], vec![], recovered.clone(), t(11));
        assert!(p.inserts.is_empty(), "an incident that already recovered is not reopened");
        assert_eq!(p.suppressed, 1);

        let p = plan(&[fire("new", "fp", 12)], vec![], recovered, t(12));
        assert_eq!(p.inserts.len(), 1, "a fire after the recovery opens a new alert");
    }

    #[test]
    fn stale_recovery_does_not_close_a_newer_fire() {
        let mut ex = plan(&[fire("a", "fp", 0), fire("b", "fp", 8)], vec![], Recoveries::new(), t(8)).inserts.remove(0);
        ex.is_new = false;
        ex.touched = false;
        let p = plan(&[m("r", "fp", 5, MatchKind::Recover, "error")], vec![ex], Recoveries::new(), t(9));
        assert!(p.updates.is_empty(), "recovery at t=5 is older than the fire at t=8");
    }
}
