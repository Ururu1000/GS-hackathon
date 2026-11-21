//! LISTEN/NOTIFY wake-ups (SPEC §9, M6 latency): with a 30 s poll interval, new work must
//! still be picked up within a second, so the wake-up and not the poll did it.
//!
//! Needs Postgres (`DATABASE_URL`), like the rest of the suite.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use beacon_delivery::config::{Contact, Directory, Person, Team};
use beacon_delivery::wake::{JOBS_CHANNEL, OUTBOX_CHANNEL, Wake};
use beacon_delivery::{RouterConfig, WorkerConfig, router, worker};
use common::*;
use serde_json::json;
use sqlx::PgPool;
use tokio::sync::watch;

const LONG_POLL: Duration = Duration::from_secs(30);
const WITHIN: Duration = Duration::from_secs(3);

fn directory() -> Directory {
    let person = |id: &str| Person {
        id: id.to_string(),
        name: None,
        contact: Contact {
            email: Some(format!("{id}@example.test")),
            phone: None,
        },
        preferences: None,
        rules: Vec::new(),
    };
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

async fn eventually<F, Fut>(what: &str, mut check: F) -> Duration
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let t0 = Instant::now();
    while t0.elapsed() < WITHIN {
        if check().await {
            return t0.elapsed();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{what} not within {WITHIN:?} (poll interval is {LONG_POLL:?})");
}

/// Let the first (empty) pass run and the listener subscribe.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(500)).await;
}

#[sqlx::test(migrations = "../../migrations")]
async fn inserted_job_wakes_an_idle_worker(pool: PgPool) {
    let (stop_tx, stop_rx) = watch::channel(false);
    let (fake, channel) = FakeChannel::new().into_channel();
    let cfg = WorkerConfig {
        poll_interval: LONG_POLL,
        wake: Wake::listen(&pool, JOBS_CHANNEL, stop_rx.clone()),
        ..worker_cfg("w-wake")
    };
    let task = tokio::spawn(worker::run_worker(pool.clone(), channel, cfg, stop_rx));
    settle().await;

    // Two inserts, apart: each must wake the worker, not just the first.
    for i in 0..2 {
        let alert = insert_alert(&pool, "open", &format!("wake job {i} {}", unique_tag())).await;
        let job = insert_job(&pool, alert, "anna", "anna@example.test").await;
        let took = eventually("job sent", || {
            let fake = fake.clone();
            async move { fake.sent().contains(&job) }
        })
        .await;
        println!("job {i} sent {took:?} after insert");
    }

    stop_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("worker stops")
        .unwrap();
}

#[sqlx::test(migrations = "../../migrations")]
async fn outbox_notify_wakes_an_idle_router(pool: PgPool) {
    let (stop_tx, stop_rx) = watch::channel(false);
    let cfg = RouterConfig {
        poll_interval: LONG_POLL,
        wake: Wake::listen(&pool, OUTBOX_CHANNEL, stop_rx.clone()),
        ..RouterConfig::default()
    };
    let task = tokio::spawn(router::run_router(
        pool.clone(),
        Arc::new(directory()),
        cfg,
        stop_rx,
    ));
    settle().await;

    // As the aggregator does: the outbox row and the NOTIFY in one transaction.
    let alert = insert_alert(&pool, "open", &format!("wake router {}", unique_tag())).await;
    let mut tx = pool.begin().await.unwrap();
    let outbox: i64 = sqlx::query_scalar(
        "INSERT INTO outbox (kind, alert_id, payload) VALUES ('notify', $1, $2) RETURNING id",
    )
    .bind(alert)
    .bind(json!({
        "alert_id": alert,
        "notify_seq": 1,
        "rule_id": "rates-rejected-trades",
        "rule_revision": 1,
        "team": "rates",
        "severity": "warning",
        "reason": "new",
        "route": [{"to": "team:rates", "channels": ["email"]}],
        "created_at": chrono::Utc::now(),
    }))
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    sqlx::query("SELECT pg_notify($1, '')")
        .bind(OUTBOX_CHANNEL)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let took = eventually("outbox routed", || {
        let pool = pool.clone();
        async move { outbox_processed(&pool, outbox).await }
    })
    .await;
    println!("outbox row routed {took:?} after commit");
    assert_eq!(job_rows(&pool, alert).await.len(), 2, "one job per member");

    stop_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("router stops")
        .unwrap();
}
