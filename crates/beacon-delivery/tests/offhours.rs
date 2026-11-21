//! Off-hours digest (`beacon_delivery::offhours`): hold outside working hours, release once.

mod common;

use std::sync::{Arc, Mutex};

use beacon_delivery::RouterConfig;
use beacon_delivery::channel::{BoxFuture, Channel, OutboundMessage, Receipt, SendError};
use beacon_delivery::clock::FixedClock;
use beacon_delivery::config::{Contact, Directory, Person, Team};
use beacon_delivery::offhours;
use beacon_delivery::router::route_once;
use beacon_delivery::worker::work_once;
use chrono::{DateTime, Utc};
use common::{insert_alert, insert_outbox_raw, update_alert_status};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

fn utc(s: &str) -> DateTime<Utc> {
    s.parse().expect("RFC 3339")
}

/// anna: warnings by email 08:00-20:00 London, otherwise held for the digest.
/// frank: no preferences, the team default (email) at any hour.
fn directory() -> Directory {
    let person = |id: &str, prefs: Option<Value>| Person {
        id: id.into(),
        name: None,
        contact: Contact {
            email: Some(format!("{id}@example.test")),
            phone: Some("+44 7700 900123".into()),
        },
        preferences: prefs,
        rules: Vec::new(),
    };
    Directory::from_parts(
        vec![
            person(
                "anna",
                Some(json!([
                    { "match": { "severity": "warning" }, "channels": ["email"],
                      "when": { "between": "08:00-20:00", "tz": "Europe/London" },
                      "otherwise": ["digest"] },
                    { "match": {}, "channels": ["email"],
                      "when": { "between": "08:00-20:00", "tz": "Europe/London" },
                      "otherwise": ["digest"] },
                ])),
            ),
            person("frank", None),
        ],
        vec![Team {
            id: "payments".into(),
            members: vec!["anna".into(), "frank".into()],
            lead: Some("frank".into()),
            oncall: Default::default(),
            default_channels: Some(vec!["email".into()]),
        }],
    )
    .expect("directory")
}

fn router_cfg(clock: Arc<FixedClock>) -> RouterConfig {
    RouterConfig {
        clock,
        ..RouterConfig::default()
    }
}

async fn notify(pool: &PgPool, alert_id: Uuid, severity: &str) {
    let payload = json!({
        "alert_id": alert_id,
        "notify_seq": 1,
        "rule_id": "payments-api-warnings",
        "rule_revision": 1,
        "team": "payments",
        "severity": severity,
        "reason": "new",
        "route": [{ "to": "team:payments" }],
        "created_at": Utc::now(),
    });
    insert_outbox_raw(pool, "notify", alert_id, payload).await;
}

async fn jobs_for(pool: &PgPool, recipient: &str) -> Vec<(Uuid, String, i32, Value)> {
    sqlx::query_as(
        "SELECT alert_id, channel, escalation_step, route_info FROM delivery_jobs \
         WHERE recipient_id = $1 ORDER BY created_at, id",
    )
    .bind(recipient)
    .fetch_all(pool)
    .await
    .expect("jobs")
}

async fn held_pending(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM offhours_held WHERE released_at IS NULL")
        .fetch_one(pool)
        .await
        .expect("count held")
}

#[derive(Default)]
struct Recording {
    sent: Mutex<Vec<OutboundMessage>>,
}

impl Channel for Recording {
    fn name(&self) -> &'static str {
        "email"
    }

    fn send<'a>(&'a self, msg: &'a OutboundMessage) -> BoxFuture<'a, Result<Receipt, SendError>> {
        Box::pin(async move {
            self.sent.lock().expect("lock").push(msg.clone());
            Ok(Receipt {
                provider_message_id: format!("<{}@fake>", msg.job_id),
                detail: json!({ "fake": true }),
            })
        })
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn night_warnings_are_held_and_sent_as_one_morning_email(pool: PgPool) {
    let dir = directory();
    // 03:00 London (GMT in January).
    let clock = Arc::new(FixedClock::new(utc("2026-01-15T03:00:00Z")));
    let cfg = router_cfg(clock.clone());

    let a1 = insert_alert(&pool, "open", "HighLatency on payments-api").await;
    let a2 = insert_alert(&pool, "open", "QueueLag on payments-api").await;
    let a3 = insert_alert(&pool, "open", "DiskFull on payments-db").await;
    for a in [a1, a2, a3] {
        notify(&pool, a, "warning").await;
    }
    let stats = route_once(&pool, &dir, &cfg).await.expect("route");
    assert_eq!(stats.held, 3, "{stats:?}");

    // Anna: nothing sent at 03:00. Frank (no window) still gets his three emails.
    assert!(jobs_for(&pool, "anna").await.is_empty());
    assert_eq!(jobs_for(&pool, "frank").await.len(), 3);
    assert_eq!(held_pending(&pool).await, 3);

    // One resolves by itself overnight.
    update_alert_status(&pool, a2, "resolved").await;

    // 07:59: not yet.
    let r = offhours::release_once(&pool, utc("2026-01-15T07:59:59Z"), 100)
        .await
        .expect("release");
    assert_eq!(r, offhours::ReleaseStats::default());

    // 08:00: one digest job for anna covering all three alerts.
    let r = offhours::release_once(&pool, utc("2026-01-15T08:00:00Z"), 100)
        .await
        .expect("release");
    assert_eq!(r.digests, 1);
    assert_eq!(r.rows_released, 3);
    assert_eq!(held_pending(&pool).await, 0);
    let jobs = jobs_for(&pool, "anna").await;
    assert_eq!(jobs.len(), 1, "{jobs:?}");
    let (alert_id, channel, step, info) = &jobs[0];
    assert_eq!(
        (alert_id, channel.as_str(), *step),
        (&a3, "email", offhours::STEP)
    );
    assert_eq!(info["target"], json!("offhours"));
    assert_eq!(info["offhours"]["count"], json!(3));
    assert_eq!(
        info["offhours"]["alert_ids"],
        json!([a1.to_string(), a2.to_string(), a3.to_string()])
    );

    // A second pass finds nothing (no double send).
    let r = offhours::release_once(&pool, utc("2026-01-15T09:00:00Z"), 100)
        .await
        .expect("release");
    assert_eq!(r.digests, 0);

    // The worker sends it with the alerts' CURRENT state, even though one is resolved.
    let rec = Arc::new(Recording::default());
    let channel: Arc<dyn Channel> = rec.clone();
    // Frank's three plain emails go out too; drain everything.
    for _ in 0..5 {
        work_once(&pool, &channel, &common::worker_cfg("w1"))
            .await
            .expect("work");
    }
    let sent = rec.sent.lock().unwrap().clone();
    let digest = sent
        .iter()
        .find(|m| m.recipient_id == "anna")
        .expect("anna's digest was sent");
    assert!(
        digest
            .subject
            .contains("While you were off: 3 alerts, 1 resolved since"),
        "{}",
        digest.subject
    );
    assert!(
        digest
            .body
            .contains("- [resolved] warning: QueueLag on payments-api"),
        "{}",
        digest.body
    );
    assert!(
        digest
            .body
            .contains("- [open] warning: HighLatency on payments-api"),
        "{}",
        digest.body
    );

    // The timeline of a covered alert says it was held, then digested.
    let timeline = beacon_delivery::audit::timeline_entries(&pool, a1)
        .await
        .expect("timeline");
    let kinds: Vec<&str> = timeline.iter().filter_map(|e| e["kind"].as_str()).collect();
    assert!(
        kinds.contains(&"held") && kinds.contains(&"digest"),
        "{timeline:?}"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn critical_is_never_held_and_daytime_is_sent_now(pool: PgPool) {
    let dir = directory();
    let clock = Arc::new(FixedClock::new(utc("2026-01-15T03:00:00Z")));
    let cfg = router_cfg(clock.clone());

    // Critical at 03:00: anna's second line holds it, but critical goes out as email now.
    let crit = insert_alert(&pool, "open", "payments-api down").await;
    notify(&pool, crit, "critical").await;
    let stats = route_once(&pool, &dir, &cfg).await.expect("route");
    assert_eq!(stats.held, 0, "{stats:?}");
    let jobs = jobs_for(&pool, "anna").await;
    assert_eq!(jobs.len(), 1);
    assert_eq!((jobs[0].1.as_str(), jobs[0].2), ("email", 0));

    // A warning at 10:00 is inside the window: plain email, nothing held.
    clock.set(utc("2026-01-15T10:00:00Z"));
    let warn = insert_alert(&pool, "open", "HighLatency on payments-api").await;
    notify(&pool, warn, "warning").await;
    route_once(&pool, &dir, &cfg).await.expect("route");
    assert_eq!(held_pending(&pool).await, 0);
    assert_eq!(jobs_for(&pool, "anna").await.len(), 2);
}

#[sqlx::test(migrations = "../../migrations")]
async fn evening_hold_waits_for_the_next_morning(pool: PgPool) {
    let dir = directory();
    // 21:00 London: released at 08:00 the next day.
    let clock = Arc::new(FixedClock::new(utc("2026-01-15T21:00:00Z")));
    let cfg = router_cfg(clock);
    let a = insert_alert(&pool, "open", "HighLatency on payments-api").await;
    notify(&pool, a, "warning").await;
    route_once(&pool, &dir, &cfg).await.expect("route");

    let release_at: DateTime<Utc> =
        sqlx::query_scalar("SELECT release_at FROM offhours_held WHERE alert_id = $1")
            .bind(a)
            .fetch_one(&pool)
            .await
            .expect("held row");
    assert_eq!(release_at, utc("2026-01-16T08:00:00Z"));

    // Routing the same outbox row again (it is already processed) holds nothing new.
    route_once(
        &pool,
        &dir,
        &router_cfg(Arc::new(FixedClock::new(utc("2026-01-15T22:00:00Z")))),
    )
    .await
    .expect("route");
    assert_eq!(held_pending(&pool).await, 1);
}
