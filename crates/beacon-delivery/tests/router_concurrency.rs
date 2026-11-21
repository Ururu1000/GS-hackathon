//! Router concurrency suite (M7): the one-statement `insert_jobs` batch, and four concurrent
//! `route_once` loops draining one shared outbox.
//!
//! Needs Postgres (`DATABASE_URL`; sqlx creates a throwaway database per test).
//!
//! Run: `cargo test -p beacon-delivery --test router_concurrency -- --nocapture`

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use beacon_delivery::RouterConfig;
use beacon_delivery::config::{Contact, Directory, Person, Team};
use beacon_delivery::router::{insert_jobs, route_once};
use beacon_delivery::throttle::PlannedJob;
use common::{insert_alert, insert_outbox_raw};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

/// Alerts routed by the four routers in `four_routers_route_each_alert_once`.
const ALERTS: usize = 2000;
/// Alerts routed at one throttled recipient in `four_routers_throttle_never_overspends`.
const THROTTLE_ALERTS: usize = 200;
/// Concurrent `route_once` loops.
const ROUTERS: usize = 4;
/// Guard so a stuck router fails the test instead of hanging it.
const DRAIN_LIMIT: Duration = Duration::from_secs(120);

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

/// anna and bob, both members of team `rates`, whose default channel is email.
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

/// `n` alerts, each with one `notify` outbox row carrying `route` and `throttle`.
async fn seed_notifications(
    pool: &PgPool,
    rule_id: &str,
    route: &Value,
    throttle: &Value,
    n: usize,
) {
    for _ in 0..n {
        let alert = insert_alert(pool, "open", rule_id).await;
        insert_outbox_raw(
            pool,
            "notify",
            alert,
            notify_payload(alert, 1, rule_id, route.clone(), throttle.clone()),
        )
        .await;
    }
}

// ---------------------------------------------------------------------------------------------
// insert_jobs: one statement per call
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn insert_jobs_batch_matches_per_row(pool: PgPool) {
    let alert = insert_alert(&pool, "open", "insert_jobs batch").await;
    let jobs = vec![
        PlannedJob {
            channel: "email".into(),
            address: "anna@example.test".into(),
            route_info: json!({ "target": "user:anna", "slot": 0 }),
        },
        PlannedJob {
            channel: "sms".into(),
            address: "+44 7700 900123".into(),
            route_info: json!({ "target": "user:anna", "slot": 1 }),
        },
        PlannedJob {
            channel: "phone".into(),
            address: "+44 7700 900123".into(),
            route_info: json!({ "target": "user:anna", "slot": 2 }),
        },
    ];

    let mut tx = pool.begin().await.expect("begin");
    let n = insert_jobs(&mut tx, alert, 1, "anna", &jobs, None)
        .await
        .expect("batched insert");
    assert_eq!(n, 3, "three distinct channels insert three rows");
    // An empty call is a no-op.
    assert_eq!(
        insert_jobs(&mut tx, alert, 1, "anna", &[], None)
            .await
            .expect("empty insert"),
        0
    );
    tx.commit().await.expect("commit");

    let rows: Vec<(String, String, Value, i32)> = sqlx::query_as(
        "SELECT channel, address, route_info, escalation_step \
         FROM delivery_jobs WHERE alert_id = $1 ORDER BY channel",
    )
    .bind(alert)
    .fetch_all(&pool)
    .await
    .expect("read jobs");
    // Ordered by channel: email, phone, sms.
    let expected = [
        ("email", "anna@example.test", 0),
        ("phone", "+44 7700 900123", 2),
        ("sms", "+44 7700 900123", 1),
    ];
    assert_eq!(rows.len(), expected.len());
    for ((channel, address, info, step), (want_channel, want_address, want_slot)) in
        rows.iter().zip(expected)
    {
        assert_eq!(channel, want_channel);
        assert_eq!(address, want_address);
        assert_eq!(*step, 0, "a plain notification carries escalation_step 0");
        assert_eq!(
            info["slot"].as_i64(),
            Some(want_slot as i64),
            "route_info frozen per job: {info:?}"
        );
        assert_eq!(info["target"], "user:anna");
    }

    // The same jobs again: every key conflicts, so nothing new is inserted.
    let mut tx = pool.begin().await.expect("begin");
    let again = insert_jobs(&mut tx, alert, 1, "anna", &jobs, None)
        .await
        .expect("conflicting insert");
    tx.commit().await.expect("commit");
    assert_eq!(again, 0, "conflicts are excluded from the count");
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM delivery_jobs WHERE alert_id = $1")
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("count jobs");
    assert_eq!(count, 3);

    // Two jobs sharing a channel in ONE call: the second is silently skipped. notify_seq 2
    // isolates the pair from the rows above, so only the two jobs conflict with each other.
    let dup = vec![
        PlannedJob {
            channel: "email".into(),
            address: "anna@example.test".into(),
            route_info: json!({ "target": "dup", "first": true }),
        },
        PlannedJob {
            channel: "email".into(),
            address: "anna@example.test".into(),
            route_info: json!({ "target": "dup", "first": false }),
        },
    ];
    let mut tx = pool.begin().await.expect("begin");
    let one = insert_jobs(&mut tx, alert, 2, "anna", &dup, None)
        .await
        .expect("duplicate insert");
    tx.commit().await.expect("commit");
    assert_eq!(one, 1, "a duplicate channel within one call inserts once");
    let first: Value = sqlx::query_scalar(
        "SELECT route_info FROM delivery_jobs WHERE alert_id = $1 AND notify_seq = 2",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .expect("duplicate row");
    assert_eq!(
        first["first"], true,
        "the first job in input order wins: {first:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// Four routers, one outbox
// ---------------------------------------------------------------------------------------------

/// One router loop: `route_once` until it claims nothing three calls in a row, retrying
/// after 10 ms on error. A losing transaction rolls back whole and its rows return to the
/// queue, so retrying is safe. Returns (jobs inserted, errors seen).
async fn router_loop(pool: PgPool, dir: Arc<Directory>) -> (u64, u64) {
    let cfg = RouterConfig::default();
    let mut empty_streak = 0;
    let (mut inserted, mut errors) = (0u64, 0u64);
    while empty_streak < 3 {
        match route_once(&pool, &dir, &cfg).await {
            Ok(stats) => {
                empty_streak = if stats.outbox_rows == 0 {
                    empty_streak + 1
                } else {
                    0
                };
                inserted += stats.jobs_inserted;
            }
            Err(err) => {
                errors += 1;
                eprintln!("route_once failed, retrying in 10 ms: {err}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    (inserted, errors)
}

/// Spawn `routers` concurrent router loops over one pool and join them, bounded by
/// [`DRAIN_LIMIT`]. Returns each task's (jobs inserted, errors seen), in task order.
async fn run_routers(pool: &PgPool, dir: Directory, routers: usize) -> Vec<(u64, u64)> {
    let dir = Arc::new(dir);
    let mut handles = Vec::new();
    for _ in 0..routers {
        let (pool, dir) = (pool.clone(), dir.clone());
        handles.push(tokio::spawn(router_loop(pool, dir)));
    }
    tokio::time::timeout(DRAIN_LIMIT, async {
        let mut out = Vec::new();
        for h in handles {
            out.push(h.await.expect("router task panicked"));
        }
        out
    })
    .await
    .unwrap_or_else(|_| panic!("routers did not drain the outbox within {DRAIN_LIMIT:?}"))
}

#[sqlx::test(migrations = "../../migrations")]
async fn four_routers_route_each_alert_once(pool: PgPool) {
    let pool = big_pool(&pool, 12).await;
    let route = json!([{ "to": "team:rates" }]);
    let t0 = Instant::now();
    seed_notifications(&pool, "four-routers-rule", &route, &Value::Null, ALERTS).await;
    println!("seeded {ALERTS} alerts in {:?}", t0.elapsed());

    let t0 = Instant::now();
    let results = run_routers(&pool, directory(), ROUTERS).await;
    println!(
        "routed {ALERTS} alerts with {ROUTERS} routers in {:?}",
        t0.elapsed()
    );

    // Every outbox row was marked processed.
    let unprocessed: i64 =
        sqlx::query_scalar("SELECT count(*) FROM outbox WHERE processed_at IS NULL")
            .fetch_one(&pool)
            .await
            .expect("count unprocessed");
    assert_eq!(unprocessed, 0, "every outbox row processed");

    // Exactly two jobs per alert: anna and bob on the team's default email.
    let jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM delivery_jobs")
        .fetch_one(&pool)
        .await
        .expect("count jobs");
    assert_eq!(jobs, (ALERTS * 2) as i64, "2000 alerts x 2 members");

    // No (alert, recipient, channel, notify_seq) key was written twice.
    let dupes: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ( \
             SELECT alert_id, recipient_id, channel, notify_seq FROM delivery_jobs \
             GROUP BY 1, 2, 3, 4 HAVING count(*) > 1 \
         ) d",
    )
    .fetch_one(&pool)
    .await
    .expect("count duplicate keys");
    assert_eq!(
        dupes, 0,
        "no duplicate (alert, recipient, channel, notify_seq)"
    );

    let inserted: u64 = results.iter().map(|r| r.0).sum();
    let errors: u64 = results.iter().map(|r| r.1).sum();
    assert_eq!(
        inserted,
        (ALERTS * 2) as u64,
        "sum of jobs_inserted over all {ROUTERS} router tasks"
    );
    assert_eq!(errors, 0, "routing shared rows without any db error");
}

#[sqlx::test(migrations = "../../migrations")]
async fn four_routers_throttle_never_overspends(pool: PgPool) {
    let pool = big_pool(&pool, 12).await;
    let rule_id = "four-routers-throttle-rule";
    let route = json!([{ "to": "user:anna", "channels": ["email"] }]);
    // 5 per hour: nothing refills during the run, so exactly 5 jobs may exist.
    let throttle = json!({ "max": 5, "per_ms": 3600000 });
    seed_notifications(&pool, rule_id, &route, &throttle, THROTTLE_ALERTS).await;

    let results = run_routers(&pool, directory(), ROUTERS).await;
    let errors: u64 = results.iter().map(|r| r.1).sum();
    // Errors are tolerated here: transactions racing for one hot bucket row can deadlock
    // (SQLSTATE 40P01); the loser rolls back whole and the retry reclaims its rows.
    println!("throttle race: {THROTTLE_ALERTS} alerts, {errors} retried errors");

    let anna: i64 =
        sqlx::query_scalar("SELECT count(*) FROM delivery_jobs WHERE recipient_id = 'anna'")
            .fetch_one(&pool)
            .await
            .expect("count anna jobs");
    assert_eq!(anna, 5, "max 5 tokens, none refilled during the run");

    let suppressed: i64 = sqlx::query_scalar("SELECT count(*) FROM throttle_suppressed")
        .fetch_one(&pool)
        .await
        .expect("count suppressed");
    assert_eq!(suppressed, (THROTTLE_ALERTS - 5) as i64, "the rest parked");

    let unprocessed: i64 =
        sqlx::query_scalar("SELECT count(*) FROM outbox WHERE processed_at IS NULL")
            .fetch_one(&pool)
            .await
            .expect("count unprocessed");
    assert_eq!(unprocessed, 0, "every outbox row processed");

    let tokens: f64 = sqlx::query_scalar(
        "SELECT tokens FROM throttle_buckets WHERE rule_id = $1 AND recipient_id = 'anna'",
    )
    .bind(rule_id)
    .fetch_one(&pool)
    .await
    .expect("bucket row");
    assert!(tokens >= 0.0, "the bucket never overspent: {tokens}");
}
