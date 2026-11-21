//! Delivery-only latency under load: outbox `notify` row (created_at) → job `sent_at`,
//! with 4 router loops and one worker, as `beacon --role router,delivery` runs them.
//! Compares polling with LISTEN/NOTIFY wake-ups without the rest of the stack.
//!
//! Opt-in: `WAKE_LOAD=1 cargo test --release -p beacon-delivery --test wake_load -- --nocapture`
//! (`WAKE_LOAD_EVENTS`, `WAKE_LOAD_RATE` default 500 events at 50/s).

mod common;

use std::sync::Arc;
use std::time::Duration;

use beacon_delivery::config::{Contact, Directory, Person, Team};
use beacon_delivery::wake::{JOBS_CHANNEL, OUTBOX_CHANNEL, OUTBOX_KEY, Wake, Wakes};
use beacon_delivery::{RouterConfig, WorkerConfig, router, worker};
use common::*;
use serde_json::json;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::watch;

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

fn env_num(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    let rank = ((p / 100.0) * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1]
}

async fn commits(pool: &PgPool) -> i64 {
    // Backend stats are flushed about once a second.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    sqlx::query_scalar(
        "SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()",
    )
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Latencies (sorted, ms) and database commits during the run.
async fn run(pool: &PgPool, wake: bool, events: u64, rate: u64) -> (Vec<f64>, i64) {
    let (stop_tx, stop_rx) = watch::channel(false);
    let dir = Arc::new(directory());
    let (outbox_wake, jobs_wake) = if wake {
        (
            Wakes::listen(pool, OUTBOX_CHANNEL, stop_rx.clone()).get(OUTBOX_KEY),
            Wakes::listen(pool, JOBS_CHANNEL, stop_rx.clone()).get("email"),
        )
    } else {
        (Wake::never(), Wake::never())
    };
    let mut tasks = Vec::new();
    for i in 0..4 {
        tasks.push(tokio::spawn(router::run_router(
            pool.clone(),
            dir.clone(),
            RouterConfig {
                digest: i == 0,
                wake: outbox_wake.clone(),
                ..RouterConfig::default()
            },
            stop_rx.clone(),
        )));
    }
    // A gateway-like send: a few ms per message.
    let (_fake, channel) = FakeChannel::new()
        .with_delay(Duration::from_millis(5))
        .into_channel();
    tasks.push(tokio::spawn(worker::run_worker(
        pool.clone(),
        channel,
        WorkerConfig {
            wake: jobs_wake,
            ..worker_cfg("w-load")
        },
        stop_rx.clone(),
    )));
    let commits_before = commits(pool).await;

    // Open loop, like the aggregator: each event's outbox row + NOTIFY in one transaction.
    let tag = unique_tag();
    let start = tokio::time::Instant::now();
    let mut inserts = Vec::new();
    for i in 0..events {
        tokio::time::sleep_until(start + Duration::from_micros(i * 1_000_000 / rate)).await;
        let pool = pool.clone();
        let tag = tag.clone();
        inserts.push(tokio::spawn(async move {
            let alert = insert_alert(&pool, "open", &format!("load {tag} {i}")).await;
            let mut tx = pool.begin().await.unwrap();
            sqlx::query("INSERT INTO outbox (kind, alert_id, payload) VALUES ('notify', $1, $2)")
                .bind(alert)
                .bind(json!({
                    "alert_id": alert, "notify_seq": 1, "rule_id": "rates-rejected-trades",
                    "rule_revision": 1, "team": "rates", "severity": "warning", "reason": "new",
                    "route": [{"to": "team:rates", "channels": ["email"]}],
                    "created_at": chrono::Utc::now(),
                }))
                .execute(&mut *tx)
                .await
                .unwrap();
            sqlx::query("SELECT pg_notify($1, '')")
                .bind(OUTBOX_CHANNEL)
                .execute(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }));
    }
    for h in inserts {
        h.await.unwrap();
    }
    let expected = (events * 2) as i64;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let sent: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM delivery_jobs j JOIN alerts a ON a.id = j.alert_id \
             WHERE a.title LIKE $1 AND j.status = 'sent'",
        )
        .bind(format!("load {tag} %"))
        .fetch_one(pool)
        .await
        .unwrap();
        if sent >= expected {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {sent}/{expected} sent"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let commits_during = commits(pool).await - commits_before;
    stop_tx.send(true).unwrap();
    for t in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(5), t).await;
    }

    let mut ms: Vec<f64> = sqlx::query_scalar(
        "SELECT (EXTRACT(EPOCH FROM (j.sent_at - o.created_at)) * 1000)::float8 \
         FROM delivery_jobs j JOIN outbox o ON o.alert_id = j.alert_id AND o.kind = 'notify' \
         JOIN alerts a ON a.id = j.alert_id WHERE a.title LIKE $1",
    )
    .bind(format!("load {tag} %"))
    .fetch_all(pool)
    .await
    .unwrap();
    ms.sort_by(f64::total_cmp);
    // Where the time goes: outbox → job row (router) and job row → sent (worker).
    let (router_p50, worker_p50): (f64, f64) = sqlx::query_as(
        "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (j.created_at - o.created_at)) * 1000)::float8, \
                percentile_disc(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (j.sent_at - j.created_at)) * 1000)::float8 \
         FROM delivery_jobs j JOIN outbox o ON o.alert_id = j.alert_id AND o.kind = 'notify' \
         JOIN alerts a ON a.id = j.alert_id WHERE a.title LIKE $1",
    )
    .bind(format!("load {tag} %"))
    .fetch_one(pool)
    .await
    .unwrap();
    println!("  split p50: outbox→job {router_p50:.0} ms, job→sent {worker_p50:.0} ms");
    (ms, commits_during)
}

#[sqlx::test(migrations = "../../migrations")]
async fn outbox_to_sent_under_load(pool: PgPool) {
    if std::env::var("WAKE_LOAD").is_err() {
        eprintln!("skipped: set WAKE_LOAD=1");
        return;
    }
    let pool = PgPoolOptions::new()
        .max_connections(40)
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    let events = env_num("WAKE_LOAD_EVENTS", 500);
    let rate = env_num("WAKE_LOAD_RATE", 50);
    for wake in [false, true] {
        let (ms, commits) = run(&pool, wake, events, rate).await;
        println!(
            "wake={wake:<5} events={events} rate={rate}/s jobs={} commits={commits} | p50 {:.0} p90 {:.0} p99 {:.0} max {:.0} ms",
            ms.len(),
            pct(&ms, 50.0),
            pct(&ms, 90.0),
            pct(&ms, 99.0),
            ms[ms.len() - 1]
        );
    }
}
