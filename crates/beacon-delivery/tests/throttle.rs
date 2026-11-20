//! M5a throttle + digest integration suite (SPEC §4.3, §5.4, §13).

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use beacon_delivery::RouterConfig;
use beacon_delivery::channel::{BoxFuture, Channel, OutboundMessage, Receipt};
use beacon_delivery::config::{Contact, Directory, Person, Team};
use beacon_delivery::router::route_once;
use beacon_delivery::throttle::{self, PlannedJob, ThrottleSpec};
use beacon_delivery::worker::work_once;
use common::{insert_alert, insert_outbox_raw};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

fn person(id: &str) -> Person {
    Person {
        id: id.to_string(),
        name: None,
        contact: Contact {
            email: Some(format!("{id}@example.test")),
            phone: Some("+44 7700 900123".to_string()),
        },
        preferences: None,
        rules: Vec::new(),
    }
}

fn directory() -> Directory {
    Directory::from_parts(
        vec![person("anna"), person("bob")],
        vec![Team {
            id: "rates".into(),
            members: vec!["anna".into(), "bob".into()],
            lead: Some("bob".into()),
            oncall: Default::default(),
            default_channels: Some(vec!["email".into()]),
        }],
    )
    .expect("directory")
}

fn notify_payload(
    alert_id: Uuid,
    notify_seq: i32,
    rule_id: &str,
    route: Value,
    throttle: Value,
) -> Value {
    let mut p = json!({
        "alert_id": alert_id,
        "notify_seq": notify_seq,
        "rule_id": rule_id,
        "rule_revision": 1,
        "team": "rates",
        "severity": "warning",
        "reason": "new",
        "route": route,
        "created_at": chrono::Utc::now(),
    });
    p["throttle"] = throttle;
    p
}

async fn big_pool(pool: &PgPool, max: u32) -> PgPool {
    PgPoolOptions::new()
        .max_connections(max)
        .acquire_timeout(Duration::from_secs(60))
        .connect_with((*pool.connect_options()).clone())
        .await
        .expect("big pool")
}

async fn suppressed_pending(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM throttle_suppressed WHERE digested_at IS NULL")
        .fetch_one(pool)
        .await
        .expect("count suppressed")
}

#[sqlx::test(migrations = "../../migrations")]
async fn take_capacity_refill_and_spec_update(pool: PgPool) {
    let spec = ThrottleSpec {
        max: 2,
        per: Duration::from_secs(60),
    };
    let mut tx = pool.begin().await.expect("begin");
    assert!(
        throttle::take(&mut tx, "rule-t", "anna", &spec)
            .await
            .expect("take")
    );
    assert!(
        throttle::take(&mut tx, "rule-t", "anna", &spec)
            .await
            .expect("take")
    );
    assert!(
        !throttle::take(&mut tx, "rule-t", "anna", &spec)
            .await
            .expect("take")
    );
    tx.commit().await.expect("commit");

    // Move the refill clock back by a full period: one more token is available.
    sqlx::query(
        "UPDATE throttle_buckets SET refilled_at = now() - interval '60 seconds' \
         WHERE rule_id = 'rule-t' AND recipient_id = 'anna'",
    )
    .execute(&pool)
    .await
    .expect("rewind");
    let mut tx = pool.begin().await.expect("begin");
    assert!(
        throttle::take(&mut tx, "rule-t", "anna", &spec)
            .await
            .expect("take")
    );
    tx.commit().await.expect("commit");

    // A rule edit updates the stored capacity/refill.
    let wider = ThrottleSpec {
        max: 5,
        per: Duration::from_secs(60),
    };
    let mut tx = pool.begin().await.expect("begin");
    assert!(
        throttle::take(&mut tx, "rule-t", "anna", &wider)
            .await
            .expect("take")
    );
    tx.commit().await.expect("commit");
    let (capacity, refill): (i32, f64) = sqlx::query_as(
        "SELECT capacity, refill_per_sec FROM throttle_buckets \
         WHERE rule_id = 'rule-t' AND recipient_id = 'anna'",
    )
    .fetch_one(&pool)
    .await
    .expect("bucket");
    assert_eq!(capacity, 5);
    assert!((refill - wider.refill_per_sec()).abs() < 1e-9, "{refill}");
}

#[sqlx::test(migrations = "../../migrations")]
async fn router_parks_excess_and_counts_suppressed(pool: PgPool) {
    let dir = directory();
    let cfg = RouterConfig::default();
    let rule_id = "throttle-test-rule";
    let route = json!([{"to": "user:anna", "channels": ["email"]}]);
    let throttle = json!({"max": 2, "per_ms": 60000});

    let mut alerts = Vec::new();
    for _ in 0..5 {
        let alert = insert_alert(&pool, "open", "throttle park").await;
        insert_outbox_raw(
            &pool,
            "notify",
            alert,
            notify_payload(alert, 1, rule_id, route.clone(), throttle.clone()),
        )
        .await;
        alerts.push(alert);
    }

    let stats = route_once(&pool, &dir, &cfg).await.expect("route_once");
    assert_eq!(stats.outbox_rows, 5, "{stats:?}");
    assert_eq!(stats.jobs_inserted, 2, "{stats:?}");
    assert_eq!(stats.suppressed, 3, "{stats:?}");
    assert_eq!(suppressed_pending(&pool).await, 3);

    // The first two notifications created jobs; the parked three created none.
    for (i, alert) in alerts.iter().enumerate() {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM delivery_jobs WHERE alert_id = $1")
            .bind(*alert)
            .fetch_one(&pool)
            .await
            .expect("count jobs");
        if i < 2 {
            assert_eq!(n, 1, "alert {i} should have a job");
        } else {
            assert_eq!(n, 0, "parked alert {i} must have no jobs");
        }
    }
    // Suppressed rows carry the planned jobs.
    let jobs: Value =
        sqlx::query_scalar("SELECT jobs FROM throttle_suppressed WHERE alert_id = $1")
            .bind(alerts[2])
            .fetch_one(&pool)
            .await
            .expect("suppressed jobs");
    let planned: Vec<PlannedJob> = serde_json::from_value(jobs).expect("planned jobs decode");
    assert_eq!(planned.len(), 1);
    assert_eq!(planned[0].channel, "email");
}

#[sqlx::test(migrations = "../../migrations")]
async fn router_debits_one_token_per_recipient(pool: PgPool) {
    let dir = directory();
    let cfg = RouterConfig::default();
    let rule_id = "throttle-test-chan";
    // One notification, two channels: still a single token.
    let route = json!([{"to": "user:anna", "channels": ["email", "sms"]}]);
    let throttle = json!({"max": 2, "per_ms": 60000});
    let alert = insert_alert(&pool, "open", "throttle one token").await;
    insert_outbox_raw(
        &pool,
        "notify",
        alert,
        notify_payload(alert, 1, rule_id, route, throttle.clone()),
    )
    .await;

    let stats = route_once(&pool, &dir, &cfg).await.expect("route_once");
    assert_eq!(stats.jobs_inserted, 2, "{stats:?}");
    assert_eq!(stats.suppressed, 0, "{stats:?}");
    let tokens: f64 = sqlx::query_scalar(
        "SELECT tokens FROM throttle_buckets WHERE rule_id = $1 AND recipient_id = 'anna'",
    )
    .bind(rule_id)
    .fetch_one(&pool)
    .await
    .expect("tokens");
    assert!(
        (tokens - 1.0).abs() < 0.05,
        "two channels must cost one token, got {tokens}"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn digest_needs_token_and_covers_all_rows(pool: PgPool) {
    let dir = directory();
    let cfg = RouterConfig::default();
    let rule_id = "throttle-test-digest";
    let route = json!([{"to": "user:anna", "channels": ["email"]}]);
    let throttle = json!({"max": 2, "per_ms": 60000});

    let mut alerts = Vec::new();
    for _ in 0..5 {
        let alert = insert_alert(&pool, "open", "throttle digest").await;
        insert_outbox_raw(
            &pool,
            "notify",
            alert,
            notify_payload(alert, 1, rule_id, route.clone(), throttle.clone()),
        )
        .await;
        alerts.push(alert);
    }
    let stats = route_once(&pool, &dir, &cfg).await.expect("route_once");
    assert_eq!(stats.suppressed, 3, "{stats:?}");

    // Bucket is empty: no digest.
    let empty = throttle::digest_once(&pool, 100).await.expect("digest");
    assert_eq!(empty.digests, 0, "{empty:?}");
    assert_eq!(suppressed_pending(&pool).await, 3);

    // Refill, then exactly one digest job for the single channel.
    sqlx::query(
        "UPDATE throttle_buckets SET refilled_at = now() - interval '60 seconds' \
         WHERE rule_id = $1 AND recipient_id = 'anna'",
    )
    .bind(rule_id)
    .execute(&pool)
    .await
    .expect("rewind");
    let d = throttle::digest_once(&pool, 100).await.expect("digest");
    assert_eq!(d.digests, 1, "{d:?}");
    assert_eq!(d.jobs_inserted, 1, "{d:?}");
    assert_eq!(d.rows_covered, 3, "{d:?}");
    assert_eq!(suppressed_pending(&pool).await, 0);

    // The digest job is filed under the latest suppressed notification.
    let parked = &alerts[2..];
    let digest_jobs: Vec<(Uuid, i32, Value)> = sqlx::query_as(
        "SELECT alert_id, notify_seq, route_info FROM delivery_jobs \
         WHERE route_info->>'target' = 'digest'",
    )
    .fetch_all(&pool)
    .await
    .expect("digest jobs");
    assert_eq!(digest_jobs.len(), 1);
    assert_eq!(digest_jobs[0].0, parked[2], "filed under the latest row");
    let digest = &digest_jobs[0].2["digest"];
    assert_eq!(digest["count"], 3);
    assert_eq!(digest["rule_id"], rule_id);
    let ids: Vec<String> =
        serde_json::from_value(digest["alert_ids"].clone()).expect("alert_ids decode");
    assert_eq!(
        ids,
        parked.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
        "oldest first"
    );
    // Fallback is stripped, target is digest.
    assert!(digest_jobs[0].2.get("fallback").is_none());

    // Covered rows are marked with the digest alert.
    let marked: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM throttle_suppressed \
         WHERE rule_id = $1 AND digested_at IS NOT NULL AND digest_alert = $2",
    )
    .bind(rule_id)
    .bind(parked[2])
    .fetch_one(&pool)
    .await
    .expect("marked");
    assert_eq!(marked, 3);

    // A second pass does nothing.
    let again = throttle::digest_once(&pool, 100).await.expect("digest");
    assert_eq!(
        again,
        beacon_delivery::throttle::DigestStats::default(),
        "{again:?}"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn concurrent_takes_grant_exactly_one_token(pool: PgPool) {
    let pool = big_pool(&pool, 4).await;
    let spec = ThrottleSpec {
        max: 1,
        per: Duration::from_secs(60),
    };
    let take_one = |pool: PgPool, spec: ThrottleSpec| async move {
        let mut tx = pool.begin().await.expect("begin");
        let got = throttle::take(&mut tx, "rule-race", "anna", &spec)
            .await
            .expect("take");
        tx.commit().await.expect("commit");
        got
    };
    let (a, b) = tokio::join!(take_one(pool.clone(), spec), take_one(pool.clone(), spec));
    assert!(a != b, "exactly one take wins, got {a},{b}");
}

#[sqlx::test(migrations = "../../migrations")]
async fn concurrent_digests_file_one_digest(pool: PgPool) {
    let dir = directory();
    let cfg = RouterConfig::default();
    let rule_id = "throttle-test-race";
    let route = json!([{"to": "user:anna", "channels": ["email"]}]);
    let throttle = json!({"max": 1, "per_ms": 60000});
    for _ in 0..2 {
        let alert = insert_alert(&pool, "open", "throttle digest race").await;
        insert_outbox_raw(
            &pool,
            "notify",
            alert,
            notify_payload(alert, 1, rule_id, route.clone(), throttle.clone()),
        )
        .await;
    }
    let stats = route_once(&pool, &dir, &cfg).await.expect("route");
    assert_eq!(stats.suppressed, 1, "{stats:?}");
    sqlx::query(
        "UPDATE throttle_buckets SET refilled_at = now() - interval '60 seconds' \
         WHERE rule_id = $1 AND recipient_id = 'anna'",
    )
    .bind(rule_id)
    .execute(&pool)
    .await
    .expect("rewind");

    let pool = big_pool(&pool, 4).await;
    let (a, b) = tokio::join!(
        throttle::digest_once(&pool, 100),
        throttle::digest_once(&pool, 100)
    );
    let (a, b) = (a.expect("digest a"), b.expect("digest b"));
    assert_eq!(a.digests + b.digests, 1, "{a:?} {b:?}");
    assert_eq!(a.jobs_inserted + b.jobs_inserted, 1, "{a:?} {b:?}");
    assert_eq!(a.rows_covered + b.rows_covered, 1, "{a:?} {b:?}");
}

/// Records rendered messages; named `email` so the worker claims the jobs.
struct RecordingChannel {
    sent: Mutex<Vec<OutboundMessage>>,
}

impl RecordingChannel {
    fn new() -> Self {
        Self {
            sent: Mutex::new(Vec::new()),
        }
    }
}

impl Channel for RecordingChannel {
    fn name(&self) -> &'static str {
        "email"
    }

    fn send<'a>(
        &'a self,
        msg: &'a OutboundMessage,
    ) -> BoxFuture<'a, Result<Receipt, beacon_delivery::channel::SendError>> {
        Box::pin(async move {
            self.sent.lock().expect("sent lock").push(msg.clone());
            Ok(Receipt {
                provider_message_id: format!("<{}@fake>", msg.job_id),
                detail: json!({"fake": true}),
            })
        })
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn worker_sends_digest_even_when_alert_acked(pool: PgPool) {
    let digest_alert = insert_alert(&pool, "open", "throttle worker digest").await;
    let covered: Vec<Uuid> = vec![Uuid::new_v4(), Uuid::new_v4()];
    let digest_info = json!({
        "target": "digest",
        "rule_id": "my-rule",
        "reason": "new",
        "digest": {
            "count": 2,
            "rule_id": "my-rule",
            "alert_ids": [covered[0].to_string(), covered[1].to_string()],
        },
    });
    let digest_job: Uuid = sqlx::query_scalar(
        "INSERT INTO delivery_jobs (alert_id, notify_seq, recipient_id, channel, address, route_info) \
         VALUES ($1, 1, 'anna', 'email', 'anna@example.test', $2) RETURNING id",
    )
    .bind(digest_alert)
    .bind(digest_info)
    .fetch_one(&pool)
    .await
    .expect("insert digest job");
    // A normal page for the same alert must be cancelled once acked.
    let normal_job: Uuid = sqlx::query_scalar(
        "INSERT INTO delivery_jobs (alert_id, notify_seq, recipient_id, channel, address, route_info) \
         VALUES ($1, 2, 'anna', 'email', 'anna@example.test', '{\"target\":\"team:rates\"}') \
         RETURNING id",
    )
    .bind(digest_alert)
    .fetch_one(&pool)
    .await
    .expect("insert normal job");
    common::update_alert_status(&pool, digest_alert, "acked").await;

    let fake = Arc::new(RecordingChannel::new());
    let channel: Arc<dyn Channel> = fake.clone();
    let cfg = common::worker_cfg("w-digest");
    let stats = work_once(&pool, &channel, &cfg).await.expect("work_once");
    assert_eq!(stats.sent, 1, "{stats:?}");
    assert_eq!(stats.cancelled, 1, "{stats:?}");

    let sent = fake.sent.lock().expect("sent lock").clone();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].job_id, digest_job, "the digest job is the one sent");
    let title = "2 more alert(s) for rule my-rule were held back by throttling";
    assert!(
        sent[0].subject.contains(title),
        "subject carries the digest title: {}",
        sent[0].subject
    );
    for id in &covered {
        assert!(
            sent[0].body.contains(&id.to_string()),
            "body lists covered alert {id}: {}",
            sent[0].body
        );
    }
    assert!(
        sent[0].body.contains("Open the Beacon feed for details."),
        "body has the footer: {}",
        sent[0].body
    );

    let statuses: Vec<(Uuid, String)> =
        sqlx::query_as("SELECT id, status FROM delivery_jobs WHERE id = ANY($1)")
            .bind(vec![digest_job, normal_job])
            .fetch_all(&pool)
            .await
            .expect("statuses");
    for (id, status) in statuses {
        if id == digest_job {
            assert_eq!(status, "sent");
        } else {
            assert_eq!(status, "cancelled");
        }
    }
}
