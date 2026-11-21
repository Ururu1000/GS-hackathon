//! Aggregator against a real Postgres (needs DATABASE_URL; sqlx::test creates
//! a fresh database per test and applies ../../migrations).

use beacon_core::MatchKind;
use beacon_core::MatchRecord;
use beacon_core::model::NotifyPayload;
use beacon_core::rule::RouteSpec;
use beacon_engine::aggregator::apply_batch;
use beacon_engine::feed::load_feed_alerts;
use chrono::{Duration, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;

fn rec(event: &str, fp: &str, kind: MatchKind, at: chrono::DateTime<Utc>) -> MatchRecord {
    MatchRecord {
        match_id: format!("{event}:rates-rejected-trades"),
        kind,
        rule_id: "rates-rejected-trades".into(),
        rule_revision: 1,
        rule_name: "Large rejected rates trades".into(),
        team: "rates".into(),
        fingerprint: fp.into(),
        key: vec![serde_json::json!("T-1")],
        dedup_window_ms: 600_000,
        severity: "warning".into(),
        color: "amber".into(),
        title: "Rejected EUR 25,000,000 trade T-1".into(),
        body: "Reason: LIMIT_BREACH".into(),
        fields: vec![beacon_core::model::AlertField {
            path: "raw.payload.desk".into(),
            label: "Desk".into(),
            value: serde_json::json!("rates"),
        }],
        route: vec![RouteSpec { to: "team:rates".into(), channels: Some(vec!["email".into(), "ui".into()]) }],
        event_id: event.into(),
        source: "kafka:trades".into(),
        schema: "envelope.meta-payload".into(),
        ingested_at: at,
        diagnostics: vec![],
        context: beacon_core::model::NotifyContext {
            service: Some("trade-gateway".into()),
            ..Default::default()
        },
        event: Some(beacon_core::model::NotifyEvent {
            fields: serde_json::Map::new(),
            raw: serde_json::json!({"payload": {"tradeId": "T-1"}}),
        }),
        quiet_for_ms: None,
        group_key: None,
    }
}

async fn alert_rows(pool: &PgPool) -> Vec<(Uuid, i64, String, bool, i64)> {
    sqlx::query("SELECT id, count, status, dedup_active, version FROM alerts ORDER BY first_seen, id")
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
        .collect()
}

#[sqlx::test(migrations = "../../migrations")]
async fn dedup_across_batches(pool: PgPool) {
    let now = Utc::now();
    let first: Vec<_> = (0..300).map(|i| rec(&format!("a{i}"), "fp1", MatchKind::Fire, now)).collect();
    apply_batch(&pool, &first).await.unwrap();
    let second: Vec<_> = (0..200).map(|i| rec(&format!("b{i}"), "fp1", MatchKind::Fire, now + Duration::seconds(1))).collect();
    apply_batch(&pool, &second).await.unwrap();

    let rows = alert_rows(&pool).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, 500, "500 dupes -> 1 alert with exact count");
    assert_eq!(rows[0].4, 2, "one version bump per batch");

    let notify: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT payload FROM outbox WHERE kind = 'notify'").fetch_all(&pool).await.unwrap();
    assert_eq!(notify.len(), 1, "count bumps never page again");
    let n: NotifyPayload = serde_json::from_value(notify[0].clone()).unwrap();
    assert_eq!(n.alert_id, rows[0].0);
    assert_eq!(n.notify_seq, 1);
    assert_eq!(n.route[0].channels.as_deref(), Some(&["email".to_string(), "ui".to_string()][..]));
    assert_eq!(n.context.service.as_deref(), Some("trade-gateway"));
    assert!(notify[0]["context"].get("environment").is_none(), "absent keys are omitted");
    assert_eq!(n.event.as_ref().unwrap().raw["payload"]["tradeId"], serde_json::json!("T-1"));

    let notify_seq: i32 = sqlx::query_scalar("SELECT notify_seq FROM alerts").fetch_one(&pool).await.unwrap();
    assert_eq!(notify_seq, 1);
    let updates: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox WHERE kind = 'alert_update'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(updates, 2, "one alert_update per alert per batch");
}

#[sqlx::test(migrations = "../../migrations")]
async fn window_expiry_and_recovery(pool: PgPool) {
    let t0 = Utc::now();
    apply_batch(&pool, &[rec("a", "fp", MatchKind::Fire, t0)]).await.unwrap();
    // 11 minutes later: outside the 10-minute window -> new generation.
    apply_batch(&pool, &[rec("b", "fp", MatchKind::Fire, t0 + Duration::minutes(11))]).await.unwrap();
    let rows = alert_rows(&pool).await;
    assert_eq!(rows.len(), 2);
    assert_eq!((rows[0].2.as_str(), rows[0].3), ("open", false), "retired alert stays open");
    assert_eq!((rows[1].2.as_str(), rows[1].3), ("open", true));

    apply_batch(&pool, &[rec("r", "fp", MatchKind::Recover, t0 + Duration::minutes(12))]).await.unwrap();
    let rows = alert_rows(&pool).await;
    assert_eq!((rows[1].2.as_str(), rows[1].3), ("resolved", false));

    let feed = load_feed_alerts(&pool, &[rows[1].0]).await.unwrap();
    assert_eq!(feed[0].resolved_reason.as_deref(), Some("recovered"));
    assert_eq!(feed[0].fields[0].label, "Desk");
    assert_eq!(feed[0].rule.name, "Large rejected rates trades");
}

#[sqlx::test(migrations = "../../migrations")]
async fn acked_alerts_keep_counting(pool: PgPool) {
    let t0 = Utc::now();
    apply_batch(&pool, &[rec("a", "fp", MatchKind::Fire, t0)]).await.unwrap();
    sqlx::query("UPDATE alerts SET status = 'acked', acked_by = 'bob', acked_at = now()").execute(&pool).await.unwrap();
    apply_batch(&pool, &[rec("b", "fp", MatchKind::Fire, t0 + Duration::seconds(5))]).await.unwrap();
    let rows = alert_rows(&pool).await;
    assert_eq!(rows.len(), 1);
    assert_eq!((rows[0].1, rows[0].2.as_str()), (2, "acked"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn nul_bytes_in_match_do_not_poison_batch(pool: PgPool) {
    let t0 = Utc::now();
    let mut poisoned = rec("nul-1", "fp-nul", MatchKind::Fire, t0);
    poisoned.title = "Bad\0Title".into();
    poisoned.body = "Body\0with\0nuls".into();
    poisoned.fields[0].value = serde_json::json!({"k\0": "v\0"});
    apply_batch(&pool, &[poisoned]).await.unwrap();

    let rows = alert_rows(&pool).await;
    assert_eq!(rows.len(), 1);
    let feed = load_feed_alerts(&pool, &[rows[0].0]).await.unwrap();
    assert_eq!(feed[0].title, "BadTitle");
    assert_eq!(feed[0].body, "Bodywithnuls");
}

#[sqlx::test(migrations = "../../migrations")]
async fn shared_transitions_for_delivery(pool: PgPool) {
    use beacon_engine::alerts::{Transition, TransitionError, touch, transition};
    apply_batch(&pool, &[rec("a", "fp", MatchKind::Fire, Utc::now())]).await.unwrap();
    let id: Uuid = sqlx::query_scalar("SELECT id FROM alerts").fetch_one(&pool).await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    assert!(touch(&mut tx, id).await.unwrap());
    // A system actor (e.g. SMS "ACK") skips the version check.
    let a = transition(&mut tx, id, Transition::Ack, "user:bob", None).await.unwrap().unwrap();
    assert_eq!((a.status.as_str(), a.version), ("acked", 3));
    let r = transition(&mut tx, id, Transition::Resolve { reason: "runbook".into() }, "system", None).await.unwrap().unwrap();
    assert_eq!(r.resolved_reason.as_deref(), Some("runbook"));
    assert!(matches!(
        transition(&mut tx, id, Transition::Ack, "user:bob", None).await.unwrap(),
        Err(TransitionError::InvalidTransition(_))
    ));
    assert!(!touch(&mut tx, Uuid::now_v7()).await.unwrap());
    tx.commit().await.unwrap();
    let updates: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox WHERE kind = 'alert_update'").fetch_one(&pool).await.unwrap();
    assert_eq!(updates, 1 + 3, "aggregator + touch + ack + resolve");
}

#[sqlx::test(migrations = "../../migrations")]
async fn rules_reload_live(pool: PgPool) {
    use beacon_engine::config_store::{create, seed_from_yaml, watch_config};
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config");
    seed_from_yaml(&pool, &dir).await.unwrap();
    let mut rx = watch_config(pool.clone(), dir).await.unwrap();
    assert_eq!(rx.borrow().rules.len(), 4);
    let rule = serde_json::json!({"id": "disk-full", "team": "infra", "when": {"path": "raw.used_pct", "op": "gte", "value": 95},
        "route": [{"to": "team:infra"}], "present": {"title": "Disk full"}});
    create(&pool, &rule, "user:test").await.unwrap().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), rx.wait_for(|s| s.rules.len() == 5)).await.unwrap().unwrap();
    let ev = beacon_core::Event::new("e".into(), "http:x".into(), serde_json::json!({"used_pct": 97}));
    let (_, m, _) = rx.borrow().process(ev);
    assert_eq!(m[0].rule_id, "disk-full", "the new rule matches without a restart");
}

#[sqlx::test(migrations = "../../migrations")]
async fn fenced_checkpoints(pool: PgPool) {
    use beacon_engine::aggregator::{Fence, Fenced, apply_batch_fenced, claim_partition};
    let now = Utc::now();
    let (e1, next) = claim_partition(&pool, 3).await.unwrap();
    assert_eq!((e1, next), (1, -1));
    let b1: Vec<_> = (0..10).map(|i| rec(&format!("a{i}"), "fp", MatchKind::Fire, now)).collect();
    apply_batch_fenced(&pool, &b1, &[Fence { partition: 3, epoch: e1, next_offset: 10 }]).await.unwrap().unwrap();

    // A new owner claims the partition (e.g. after a rebalance or a crash).
    let (e2, next) = claim_partition(&pool, 3).await.unwrap();
    assert_eq!((e2, next), (2, 10), "the new owner resumes after what was applied");

    // The stale owner's next write is rejected and leaves no trace.
    let b2: Vec<_> = (0..5).map(|i| rec(&format!("b{i}"), "fp", MatchKind::Fire, now)).collect();
    let r = apply_batch_fenced(&pool, &b2, &[Fence { partition: 3, epoch: e1, next_offset: 15 }]).await.unwrap();
    assert!(matches!(r, Err(Fenced)));
    let count: i64 = sqlx::query_scalar("SELECT count FROM alerts").fetch_one(&pool).await.unwrap();
    assert_eq!(count, 10, "a fenced batch changes nothing");

    apply_batch_fenced(&pool, &b2, &[Fence { partition: 3, epoch: e2, next_offset: 15 }]).await.unwrap().unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count FROM alerts").fetch_one(&pool).await.unwrap();
    assert_eq!(count, 15);
    let next: i64 = sqlx::query_scalar("SELECT next_offset FROM checkpoints WHERE partition = 3").fetch_one(&pool).await.unwrap();
    assert_eq!(next, 15);
    // A retry of the same batch after a lost commit acknowledgement is a no-op.
    let again = apply_batch_fenced(&pool, &b2, &[Fence { partition: 3, epoch: e2, next_offset: 15 }]).await.unwrap();
    assert!(again.is_ok(), "already applied is not a fence error");
    let count: i64 = sqlx::query_scalar("SELECT count FROM alerts").fetch_one(&pool).await.unwrap();
    assert_eq!(count, 15, "the retried batch is not applied twice");
}

#[sqlx::test(migrations = "../../migrations")]
async fn quiet_for_resolves_idle_alerts(pool: PgPool) {
    use beacon_engine::aggregator::sweep_quiet;
    let mut quiet = rec("q", "fp-quiet", MatchKind::Fire, Utc::now() - Duration::minutes(20));
    quiet.quiet_for_ms = Some(10 * 60 * 1000);
    let mut busy = rec("b", "fp-busy", MatchKind::Fire, Utc::now());
    busy.quiet_for_ms = Some(10 * 60 * 1000);
    let plain = rec("p", "fp-plain", MatchKind::Fire, Utc::now() - Duration::minutes(20));
    apply_batch(&pool, &[quiet, busy, plain]).await.unwrap();
    assert_eq!(sweep_quiet(&pool).await.unwrap(), 1);
    let rows: Vec<(String, String, Option<String>, bool)> =
        sqlx::query_as("SELECT fingerprint, status, resolved_reason, dedup_active FROM alerts ORDER BY fingerprint")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(rows[0], ("fp-busy".into(), "open".into(), None, true));
    assert_eq!(rows[1], ("fp-plain".into(), "open".into(), None, true), "no quiet_for, never auto-resolved");
    assert_eq!(rows[2], ("fp-quiet".into(), "resolved".into(), Some("quiet_for".into()), false));
    assert_eq!(sweep_quiet(&pool).await.unwrap(), 0, "idempotent");
    let updates: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox WHERE kind = 'alert_update'").fetch_one(&pool).await.unwrap();
    assert_eq!(updates, 3 + 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn grouped_recovery_closes_every_window(pool: PgPool) {
    let rule: beacon_core::rule::RuleSpec = serde_json::from_value(serde_json::json!({
        "id": "lat", "team": "pricing",
        "when": {"all": [{"path": "raw.kind", "op": "eq", "value": "latency"}, {"path": "raw.state", "op": "eq", "value": "high"}]},
        "route": [{"to": "team:pricing"}], "present": {"title": "Latency {{ raw.instrument }}"},
        "group": {"by": ["raw.instrument"], "window": "5m"},
        "auto_resolve": {"recover_when": {"all": [{"path": "raw.kind", "op": "eq", "value": "latency"}, {"path": "raw.state", "op": "eq", "value": "ok"}]}}
    }))
    .unwrap();
    let cfg = beacon_core::ConfigSnapshot::build(1, vec![], vec![rule]).unwrap();
    let ev = |id: &str, state: &str, at: chrono::DateTime<Utc>| {
        let mut e = beacon_core::Event::new(id.into(), "http:x".into(), serde_json::json!({"kind": "latency", "state": state, "instrument": "EURUSD"}));
        e.ingested_at = at;
        cfg.process(e).1.remove(0)
    };
    let t0 = Utc::now() - Duration::minutes(12);
    // Two digests in different 5-minute windows, then a recovery in a third.
    apply_batch(&pool, &[ev("a", "high", t0)]).await.unwrap();
    apply_batch(&pool, &[ev("b", "high", t0 + Duration::minutes(6))]).await.unwrap();
    let open: i64 = sqlx::query_scalar("SELECT count(*) FROM alerts WHERE status = 'open'").fetch_one(&pool).await.unwrap();
    assert_eq!(open, 2);
    let rec = ev("r", "ok", t0 + Duration::minutes(11));
    assert_eq!(rec.kind, MatchKind::Recover);
    apply_batch(&pool, &[rec]).await.unwrap();
    let rows: Vec<(String, Option<String>)> = sqlx::query_as("SELECT status, resolved_reason FROM alerts").fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|(s, r)| s == "resolved" && r.as_deref() == Some("recovered")), "{rows:?}");
}

#[sqlx::test(migrations = "../../migrations")]
async fn replayed_partition_next_to_new_work_is_not_fenced(pool: PgPool) {
    use beacon_engine::aggregator::{Fence, active_fences, apply_batch_fenced, claim_partition};
    use std::collections::HashMap;
    let now = Utc::now();
    let (ea, _) = claim_partition(&pool, 1).await.unwrap();
    let (eb, _) = claim_partition(&pool, 2).await.unwrap();
    apply_batch_fenced(&pool, &[rec("a", "fa", MatchKind::Fire, now)], &[Fence { partition: 1, epoch: ea, next_offset: 10 }]).await.unwrap().unwrap();
    // Both partitions appear in the batch: partition 1 is fully replayed (next_offset == owned 10),
    // partition 2 advances to 5. Production active_fences must drop partition 1's non-advancing fence.
    let owned = HashMap::from([(1, (ea, 10)), (2, (eb, 0))]);
    let raw = HashMap::from([
        (1, Fence { partition: 1, epoch: ea, next_offset: 10 }),
        (2, Fence { partition: 2, epoch: eb, next_offset: 5 }),
    ]);
    let fences = active_fences(raw, &owned);
    assert_eq!(fences.len(), 1);
    assert_eq!(fences[0].partition, 2);
    let r = apply_batch_fenced(&pool, &[rec("b", "fb", MatchKind::Fire, now)], &fences).await.unwrap();
    assert!(r.is_ok());
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM alerts").fetch_one(&pool).await.unwrap();
    assert_eq!(n, 2);
}
