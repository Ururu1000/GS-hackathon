//! M5a escalation integration suite (SPEC §4.4, §13): timers scheduled by the router,
//! chain fires, cancels, leases, preference resolution at fire time, the throttle bypass
//! for critical escalations, and the scheduler loop.
//!
//! Needs Postgres (`DATABASE_URL`; sqlx creates a throwaway database per test). Time is
//! driven by pushing `due_at` into the past in SQL; nothing sleeps for an interval.
//!
//! Run: `cargo test -p beacon-delivery --test escalation -- --nocapture`

mod common;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use beacon_delivery::RouterConfig;
use beacon_delivery::config::{Contact, Directory, Person, Team};
use beacon_delivery::escalation::{
    self, ChainStep, ClaimedTimer, EscalationStats, FireOutcome, SchedulerConfig,
};
use beacon_delivery::prefs::RoutingContext;
use beacon_delivery::router::route_once;
use beacon_delivery::rules::RulePolicy;
use beacon_delivery::throttle::{self, ThrottleSpec};
use common::job_rows;
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

fn seed_dir() -> Directory {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");
    Directory::load(&root).expect("seed config directory loads")
}

fn router_cfg(escalate_after_ms: u64) -> RouterConfig {
    RouterConfig {
        escalate_after_override: Some(Duration::from_millis(escalate_after_ms)),
        ..RouterConfig::default()
    }
}

fn sched_cfg(worker_id: &str) -> SchedulerConfig {
    SchedulerConfig {
        worker_id: worker_id.to_string(),
        ..SchedulerConfig::default()
    }
}

fn person(
    id: &str,
    email: Option<&str>,
    phone: Option<&str>,
    preferences: Option<Value>,
) -> Person {
    Person {
        id: id.to_string(),
        name: None,
        contact: Contact {
            email: email.map(str::to_string),
            phone: phone.map(str::to_string),
        },
        preferences,
        rules: Vec::new(),
    }
}

fn team(id: &str, members: &[&str], lead: Option<&str>, defaults: Option<&[&str]>) -> Team {
    Team {
        id: id.to_string(),
        members: members.iter().map(|s| s.to_string()).collect(),
        lead: lead.map(str::to_string),
        oncall: HashMap::new(),
        default_channels: defaults.map(|c| c.iter().map(|s| s.to_string()).collect()),
    }
}

/// Insert an alert with full control over severity / team / rule (`common::insert_alert`
/// fixes all three).
async fn insert_alert_as(
    pool: &PgPool,
    status: &str,
    severity: &str,
    team_id: &str,
    rule_id: &str,
) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO alerts (id, rule_id, rule_revision, rule_name, team, fingerprint, \
                             dedup_window_ms, status, severity, color, title, body, \
                             first_seen, last_seen, source, schema, notify_seq) \
         VALUES ($1, $2, 1, $3, $4, $5, 600000, $6, $7, 'amber', 'Escalation test alert', \
                 '', now(), now(), 'test', 'escalation.test.v1', 1)",
    )
    .bind(id)
    .bind(rule_id)
    .bind(rule_id)
    .bind(team_id)
    .bind(format!("fp-{id}"))
    .bind(status)
    .bind(severity)
    .execute(pool)
    .await
    .expect("insert alert");
    id
}

/// What varies between the notify rows the tests insert.
struct Notify<'a> {
    rule_id: &'a str,
    team_id: &'a str,
    severity: &'a str,
    reason: &'a str,
    route: Value,
}

/// Insert an outbox notify row with full control over rule / team / severity / reason.
async fn insert_notify(pool: &PgPool, alert_id: Uuid, notify_seq: i32, spec: Notify<'_>) {
    let payload = json!({
        "alert_id": alert_id,
        "notify_seq": notify_seq,
        "rule_id": spec.rule_id,
        "rule_revision": 1,
        "team": spec.team_id,
        "severity": spec.severity,
        "reason": spec.reason,
        "route": spec.route,
        "created_at": chrono::Utc::now(),
    });
    sqlx::query("INSERT INTO outbox (kind, alert_id, payload) VALUES ('notify', $1, $2)")
        .bind(alert_id)
        .bind(payload)
        .execute(pool)
        .await
        .expect("insert notify row");
}

/// The seed settlements scenario: bob (on call) is routed phone+sms, the escalation chain
/// is `[carol via oncall:settlements]`. Returns the alert id; asserts one timer exists.
async fn route_settlements_alert(pool: &PgPool, dir: &Directory, severity: &str, ms: u64) -> Uuid {
    let alert = insert_alert_as(
        pool,
        "open",
        severity,
        "settlements",
        "settlements-workflow-failed",
    )
    .await;
    insert_notify(
        pool,
        alert,
        1,
        Notify {
            rule_id: "settlements-workflow-failed",
            team_id: "settlements",
            severity,
            reason: "new",
            route: json!([{ "to": "oncall:settlements", "channels": ["phone", "sms"] }]),
        },
    )
    .await;
    let stats = route_once(pool, dir, &router_cfg(ms))
        .await
        .expect("route_once");
    assert_eq!(stats.timers, 1, "the router must start exactly one timer");
    assert_eq!(stats.jobs_inserted, 2, "bob gets phone + sms");
    alert
}

async fn make_due(pool: &PgPool, alert_id: Uuid) {
    sqlx::query(
        "UPDATE escalation_timers SET due_at = now() - interval '1 second', updated_at = now() \
         WHERE alert_id = $1",
    )
    .bind(alert_id)
    .execute(pool)
    .await
    .expect("make timer due");
}

/// A routing context naming only the rule (`schedule` reads `rule_id`, `service`,
/// `environment` from it).
fn rule_ctx(rule_id: &str) -> RoutingContext {
    RoutingContext {
        severity: "warning".into(),
        team: "t".into(),
        rule_id: rule_id.into(),
        service: None,
        environment: None,
    }
}

/// Start a timer directly through `escalation::schedule` (no router involved).
async fn schedule_direct(pool: &PgPool, alert_id: Uuid, rule_id: &str, chain: Vec<ChainStep>) {
    let mut tx = pool.begin().await.expect("tx");
    let inserted = escalation::schedule(
        &mut tx,
        alert_id,
        1,
        &rule_ctx(rule_id),
        &chain,
        Duration::from_secs(600),
    )
    .await
    .expect("schedule");
    tx.commit().await.expect("commit");
    assert!(inserted);
}

async fn carol_job_count(pool: &PgPool, alert_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM delivery_jobs WHERE alert_id = $1 AND recipient_id = 'carol'",
    )
    .bind(alert_id)
    .fetch_one(pool)
    .await
    .expect("count carol jobs")
}

/// A larger pool on the same per-test database, for the concurrency tests.
async fn big_pool(pool: &PgPool, max: u32) -> PgPool {
    PgPoolOptions::new()
        .max_connections(max)
        .acquire_timeout(Duration::from_secs(60))
        .connect_with((*pool.connect_options()).clone())
        .await
        .expect("big pool")
}

// ---------------------------------------------------------------------------------------------
// Router -> timer
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn router_schedules_one_timer_and_renotify_does_not_restart_it(pool: PgPool) {
    let dir = seed_dir();
    let alert = insert_alert_as(
        &pool,
        "open",
        "critical",
        "settlements",
        "settlements-workflow-failed",
    )
    .await;
    insert_notify(
        &pool,
        alert,
        1,
        Notify {
            rule_id: "settlements-workflow-failed",
            team_id: "settlements",
            severity: "critical",
            reason: "new",
            route: json!([{ "to": "oncall:settlements", "channels": ["phone", "sms"] }]),
        },
    )
    .await;
    let stats = route_once(&pool, &dir, &router_cfg(1234))
        .await
        .expect("route_once");
    assert_eq!(stats.timers, 1);

    let (chain, step, interval_ms, state, notify_seq, due_soon): (
        Value,
        i32,
        i64,
        String,
        i32,
        bool,
    ) = sqlx::query_as(
        "SELECT chain, step, interval_ms, state, notify_seq, \
                due_at > now() AND due_at < now() + interval '3 seconds' \
         FROM escalation_timers WHERE alert_id = $1",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .expect("timer row");
    assert_eq!(
        chain,
        json!([{ "recipient_id": "carol", "via": "oncall:settlements",
                 "channels": ["phone", "sms"] }])
    );
    assert_eq!(step, 0);
    assert_eq!(interval_ms, 1234);
    assert_eq!(state, "pending");
    assert_eq!(notify_seq, 1);
    assert!(due_soon, "due_at must be ~now + 1234 ms");

    // A renotify for the same alert plans bob's jobs but must not touch the clock.
    insert_notify(
        &pool,
        alert,
        2,
        Notify {
            rule_id: "settlements-workflow-failed",
            team_id: "settlements",
            severity: "critical",
            reason: "renotify",
            route: json!([{ "to": "oncall:settlements", "channels": ["phone", "sms"] }]),
        },
    )
    .await;
    let stats = route_once(&pool, &dir, &router_cfg(1234))
        .await
        .expect("route renotify");
    assert_eq!(stats.timers, 0);

    // Even a duplicate `new` inserts nothing: one timer per alert, DO NOTHING.
    insert_notify(
        &pool,
        alert,
        3,
        Notify {
            rule_id: "settlements-workflow-failed",
            team_id: "settlements",
            severity: "critical",
            reason: "new",
            route: json!([{ "to": "oncall:settlements", "channels": ["phone", "sms"] }]),
        },
    )
    .await;
    let stats = route_once(&pool, &dir, &router_cfg(999_999))
        .await
        .expect("route duplicate new");
    assert_eq!(stats.timers, 0);
    let timers: i64 =
        sqlx::query_scalar("SELECT count(*) FROM escalation_timers WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("count timers");
    assert_eq!(timers, 1);
    // The original schedule stands: the duplicate's much later interval did not move it.
    let (interval_ms, notify_seq): (i64, i32) =
        sqlx::query_as("SELECT interval_ms, notify_seq FROM escalation_timers WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("timer");
    assert_eq!((interval_ms, notify_seq), (1234, 1));

    // An empty chain never starts a timer (module doc).
    let bare = insert_alert_as(
        &pool,
        "open",
        "warning",
        "settlements",
        "settlements-workflow-failed",
    )
    .await;
    let mut tx = pool.begin().await.expect("tx");
    let inserted = escalation::schedule(
        &mut tx,
        bare,
        1,
        &rule_ctx("settlements-workflow-failed"),
        &[],
        Duration::from_secs(600),
    )
    .await
    .expect("schedule empty chain");
    tx.commit().await.expect("commit");
    assert!(!inserted, "an empty chain starts no timer");
    let timers: i64 =
        sqlx::query_scalar("SELECT count(*) FROM escalation_timers WHERE alert_id = $1")
            .bind(bare)
            .fetch_one(&pool)
            .await
            .expect("count timers");
    assert_eq!(timers, 0);
}

// ---------------------------------------------------------------------------------------------
// Firing
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn due_timer_fires_carol_with_explicit_channels(pool: PgPool) {
    let dir = seed_dir();
    let alert = route_settlements_alert(&pool, &dir, "critical", 600_000).await;
    make_due(&pool, alert).await;

    let stats = escalation::escalate_once(&pool, &dir, &sched_cfg("s1"))
        .await
        .expect("escalate_once");
    assert_eq!(
        stats,
        EscalationStats {
            claimed: 1,
            fired: 1,
            jobs_inserted: 2,
            suppressed: 0,
            cancelled: 0,
            finished: 1,
            skipped: 0,
            lost_lease: 0,
        }
    );

    let jobs: Vec<_> = job_rows(&pool, alert)
        .await
        .into_iter()
        .filter(|j| j.recipient_id == "carol")
        .collect();
    assert_eq!(jobs.len(), 2);
    let channels: Vec<_> = jobs.iter().map(|j| j.channel.as_str()).collect();
    assert_eq!(channels, ["phone", "sms"]);
    for job in &jobs {
        assert_eq!(job.escalation_step, Some(1));
        assert_eq!(job.notify_seq, 1);
        assert_eq!(job.status, "pending");
        assert_eq!(job.address, "+44 7700 900125");
        assert_eq!(job.route_info["target"], "escalation");
        assert_eq!(job.route_info["via"], "oncall:settlements");
        assert_eq!(job.route_info["step"], json!(0));
        assert_eq!(job.route_info["rule_id"], "settlements-workflow-failed");
        assert_eq!(job.route_info["decision"], json!({"source": "explicit"}));
        assert!(job.route_info["decided_at"].is_string());
    }
    // One-hop fallbacks frozen like the router does (phone -> sms -> email).
    let phone = jobs
        .iter()
        .find(|j| j.channel == "phone")
        .expect("phone job");
    assert_eq!(
        phone.route_info["fallback"],
        json!({ "channel": "sms", "address": "+44 7700 900125" })
    );
    let sms = jobs.iter().find(|j| j.channel == "sms").expect("sms job");
    assert_eq!(
        sms.route_info["fallback"],
        json!({ "channel": "email", "address": "carol@example.test" })
    );

    // Single-step chain: the timer is done, lease cleared, no note.
    let (state, step, lease_token, locked_by, last_note): (
        String,
        i32,
        Option<Uuid>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT state, step, lease_token, locked_by, last_note \
         FROM escalation_timers WHERE alert_id = $1",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .expect("timer row");
    assert_eq!(state, "done");
    assert_eq!(step, 1);
    assert_eq!(lease_token, None);
    assert_eq!(locked_by, None);
    assert_eq!(last_note, None);

    // Nothing left to claim.
    let stats = escalation::escalate_once(&pool, &dir, &sched_cfg("s1"))
        .await
        .expect("second pass");
    assert_eq!(stats.claimed, 0);
}

#[sqlx::test(migrations = "../../migrations")]
async fn two_step_chain_advances_then_finishes(pool: PgPool) {
    let dir = Directory::from_parts(
        vec![
            person("p1", Some("p1@example.test"), Some("+44 7700 900201"), None),
            person("p2", Some("p2@example.test"), Some("+44 7700 900202"), None),
            person("p3", Some("p3@example.test"), Some("+44 7700 900203"), None),
        ],
        vec![Team {
            id: "t".into(),
            members: vec!["p1".into(), "p2".into(), "p3".into()],
            lead: Some("p3".into()),
            oncall: HashMap::from([(
                "sched".to_string(),
                vec!["p1".to_string(), "p2".to_string()],
            )]),
            default_channels: Some(vec!["email".into()]),
        }],
    )
    .expect("directory");

    let alert = insert_alert_as(&pool, "open", "warning", "t", "two-step-rule").await;
    insert_notify(
        &pool,
        alert,
        1,
        Notify {
            rule_id: "two-step-rule",
            team_id: "t",
            severity: "warning",
            reason: "new",
            route: json!([{ "to": "oncall:sched" }]),
        },
    )
    .await;
    let stats = route_once(&pool, &dir, &router_cfg(60_000))
        .await
        .expect("route_once");
    assert_eq!(stats.timers, 1);

    // Chain: the rest of the rotation after p1 (on call), then the lead p3.
    let chain: Value =
        sqlx::query_scalar("SELECT chain FROM escalation_timers WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("chain");
    assert_eq!(
        chain,
        json!([
            { "recipient_id": "p2", "via": "oncall:sched" },
            { "recipient_id": "p3", "via": "lead:t" },
        ])
    );

    make_due(&pool, alert).await;
    let s = escalation::escalate_once(&pool, &dir, &sched_cfg("s1"))
        .await
        .expect("fire 1");
    assert_eq!(
        (s.claimed, s.fired, s.jobs_inserted, s.finished),
        (1, 1, 1, 0)
    );
    // p2 was paged (email via the team default); the timer advanced and is due again.
    let p2 = job_rows(&pool, alert)
        .await
        .into_iter()
        .find(|j| j.recipient_id == "p2")
        .expect("p2 job");
    assert_eq!(p2.channel, "email");
    assert_eq!(p2.escalation_step, Some(1));
    assert_eq!(p2.route_info["via"], "oncall:sched");
    assert_eq!(
        p2.route_info["decision"],
        json!({"source": "team_default", "team": "t"})
    );
    let (state, step, due_later): (String, i32, bool) = sqlx::query_as(
        "SELECT state, step, due_at > now() + interval '50 seconds' \
         FROM escalation_timers WHERE alert_id = $1",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .expect("timer");
    assert_eq!((state.as_str(), step), ("pending", 1));
    assert!(due_later, "the next step must be ~one interval away");

    make_due(&pool, alert).await;
    let s = escalation::escalate_once(&pool, &dir, &sched_cfg("s1"))
        .await
        .expect("fire 2");
    assert_eq!(
        (s.claimed, s.fired, s.jobs_inserted, s.finished),
        (1, 1, 1, 1)
    );
    let p3 = job_rows(&pool, alert)
        .await
        .into_iter()
        .find(|j| j.recipient_id == "p3")
        .expect("p3 job");
    assert_eq!(p3.channel, "email");
    assert_eq!(p3.escalation_step, Some(2));
    assert_eq!(p3.route_info["via"], "lead:t");
    let (state, step): (String, i32) =
        sqlx::query_as("SELECT state, step FROM escalation_timers WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("timer");
    assert_eq!((state.as_str(), step), ("done", 2));
}

#[sqlx::test(migrations = "../../migrations")]
async fn acked_and_resolved_alerts_cancel_their_timers(pool: PgPool) {
    let dir = seed_dir();
    let acked = route_settlements_alert(&pool, &dir, "critical", 600_000).await;
    let resolved = route_settlements_alert(&pool, &dir, "critical", 600_000).await;
    common::update_alert_status(&pool, acked, "acked").await;
    common::update_alert_status(&pool, resolved, "resolved").await;
    make_due(&pool, acked).await;
    make_due(&pool, resolved).await;

    let stats = escalation::escalate_once(&pool, &dir, &sched_cfg("s1"))
        .await
        .expect("escalate_once");
    assert_eq!(
        stats,
        EscalationStats {
            claimed: 2,
            fired: 0,
            jobs_inserted: 0,
            suppressed: 0,
            cancelled: 2,
            finished: 0,
            skipped: 0,
            lost_lease: 0,
        }
    );

    for (alert, note) in [(acked, "alert acked"), (resolved, "alert resolved")] {
        let (state, last_note, lease_token, carol_jobs): (
            String,
            Option<String>,
            Option<Uuid>,
            i64,
        ) = sqlx::query_as(
            "SELECT state, last_note, lease_token, \
                    (SELECT count(*) FROM delivery_jobs \
                     WHERE alert_id = t.alert_id AND recipient_id = 'carol') \
             FROM escalation_timers t WHERE alert_id = $1",
        )
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("timer row");
        assert_eq!(state, "cancelled", "{alert}");
        assert_eq!(last_note.as_deref(), Some(note), "{alert}");
        assert_eq!(lease_token, None, "{alert}");
        assert_eq!(carol_jobs, 0, "no escalation jobs for {alert}");
    }
}

// ---------------------------------------------------------------------------------------------
// Concurrency and leases
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn two_schedulers_fire_a_due_timer_exactly_once(pool: PgPool) {
    let dir = seed_dir();
    let alert = route_settlements_alert(&pool, &dir, "critical", 600_000).await;
    make_due(&pool, alert).await;

    let big = big_pool(&pool, 4).await;
    let cfg_a = sched_cfg("sched-a");
    let cfg_b = sched_cfg("sched-b");
    let (a, b) = tokio::join!(
        escalation::escalate_once(&big, &dir, &cfg_a),
        escalation::escalate_once(&big, &dir, &cfg_b),
    );
    let (a, b) = (a.expect("escalate a"), b.expect("escalate b"));
    assert_eq!(
        a.claimed + b.claimed,
        1,
        "SKIP LOCKED must give the timer to exactly one scheduler"
    );
    assert_eq!(a.fired + b.fired, 1);
    assert_eq!(a.jobs_inserted + b.jobs_inserted, 2);
    assert_eq!(a.lost_lease + b.lost_lease, 0);

    // Carol was paged exactly once (phone + sms), and the timer is finished.
    assert_eq!(carol_job_count(&pool, alert).await, 2);
    let (state,): (String,) =
        sqlx::query_as("SELECT state FROM escalation_timers WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("timer state");
    assert_eq!(state, "done");
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_expired_lease_is_reclaimed(pool: PgPool) {
    let dir = seed_dir();
    let alert = route_settlements_alert(&pool, &dir, "critical", 600_000).await;
    make_due(&pool, alert).await;
    // A scheduler died holding the lease a while ago.
    sqlx::query(
        "UPDATE escalation_timers SET lease_token = $2, locked_by = 'dead-worker', \
                locked_until = now() - interval '5 seconds', updated_at = now() \
         WHERE alert_id = $1",
    )
    .bind(alert)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .expect("plant expired lease");

    let stats = escalation::escalate_once(&pool, &dir, &sched_cfg("s1"))
        .await
        .expect("escalate_once");
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.fired, 1);
    assert_eq!(stats.jobs_inserted, 2);
    assert_eq!(carol_job_count(&pool, alert).await, 2);
    let (state,): (String,) =
        sqlx::query_as("SELECT state FROM escalation_timers WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("timer state");
    assert_eq!(state, "done");
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_stale_leases_finalize_writes_nothing(pool: PgPool) {
    let dir = seed_dir();
    let alert = route_settlements_alert(&pool, &dir, "critical", 600_000).await;
    make_due(&pool, alert).await;

    // Another worker owns the timer now (it reclaimed our expired lease).
    let theirs = Uuid::new_v4();
    sqlx::query(
        "UPDATE escalation_timers SET lease_token = $2, locked_by = 'them', \
                locked_until = now() + interval '1 hour', updated_at = now() \
         WHERE alert_id = $1",
    )
    .bind(alert)
    .bind(theirs)
    .execute(&pool)
    .await
    .expect("plant live lease");

    // A live foreign lease is not claimable.
    let stats = escalation::escalate_once(&pool, &dir, &sched_cfg("s1"))
        .await
        .expect("escalate_once");
    assert_eq!(stats.claimed, 0);

    // Our (stale) fire inserts carol's jobs and finalizes against OUR token: the fence
    // rejects the UPDATE, the transaction is dropped, and nothing was written.
    let stale = Uuid::new_v4();
    let claimed = ClaimedTimer {
        alert_id: alert,
        notify_seq: 1,
        rule_id: "settlements-workflow-failed".into(),
        service: None,
        environment: None,
        chain: vec![ChainStep {
            recipient_id: "carol".into(),
            via: "oncall:settlements".into(),
            channels: Some(vec!["phone".into(), "sms".into()]),
        }],
        step: 0,
        interval: Duration::from_secs(600),
        lease_token: stale,
    };
    let outcome = escalation::fire_claimed(&pool, &dir, &claimed)
        .await
        .expect("fire_claimed");
    assert_eq!(outcome, FireOutcome::LostLease);

    assert_eq!(
        carol_job_count(&pool, alert).await,
        0,
        "no jobs may survive the rollback"
    );
    let (state, step, lease_token, still_due): (String, i32, Option<Uuid>, bool) = sqlx::query_as(
        "SELECT state, step, lease_token, due_at <= now() \
         FROM escalation_timers WHERE alert_id = $1",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .expect("timer row");
    assert_eq!((state.as_str(), step), ("pending", 0));
    assert_eq!(lease_token, Some(theirs));
    assert!(
        still_due,
        "the timer is exactly as make_due left it: not advanced"
    );
}

// ---------------------------------------------------------------------------------------------
// Preference resolution at fire time
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn fire_resolves_preferences_and_falls_back_to_existing_job_channels(pool: PgPool) {
    let dir = Directory::from_parts(
        vec![
            // y1: warning -> email (the initial notify's recipient in team t).
            person(
                "y1",
                Some("y1@example.test"),
                Some("+44 7700 900301"),
                Some(json!([{ "match": { "severity": "warning" }, "channels": ["email"] }])),
            ),
            // x1 (lead of t): warning -> sms at fire time.
            person(
                "x1",
                Some("x1@example.test"),
                Some("+44 7700 900302"),
                Some(json!([{ "match": { "severity": "warning" }, "channels": ["sms"] }])),
            ),
            // y2: warning -> email (the initial notify's recipient in team u).
            person(
                "y2",
                Some("y2@example.test"),
                Some("+44 7700 900303"),
                Some(json!([{ "match": { "severity": "warning" }, "channels": ["email"] }])),
            ),
            // z1 (lead of u): no preferences, no team default -> UI only at fire time.
            person("z1", Some("z1@example.test"), Some("+44 7700 900304"), None),
        ],
        vec![
            team("t", &["x1", "y1"], Some("x1"), None),
            team("u", &["z1", "y2"], Some("z1"), None),
        ],
    )
    .expect("directory");

    // (a) The chain step x1 resolves her own preference: sms.
    let alert_a = insert_alert_as(&pool, "open", "warning", "t", "pref-rule").await;
    insert_notify(
        &pool,
        alert_a,
        1,
        Notify {
            rule_id: "pref-rule",
            team_id: "t",
            severity: "warning",
            reason: "new",
            route: json!([{ "to": "user:y1" }]),
        },
    )
    .await;
    let stats = route_once(&pool, &dir, &router_cfg(60_000))
        .await
        .expect("route a");
    assert_eq!(stats.timers, 1);

    // (b) z1 resolves to UI only: the step falls back to the channels the alert's
    // existing jobs already used (y2's email).
    let alert_b = insert_alert_as(&pool, "open", "warning", "u", "pref-rule").await;
    insert_notify(
        &pool,
        alert_b,
        1,
        Notify {
            rule_id: "pref-rule",
            team_id: "u",
            severity: "warning",
            reason: "new",
            route: json!([{ "to": "user:y2" }]),
        },
    )
    .await;
    let stats = route_once(&pool, &dir, &router_cfg(60_000))
        .await
        .expect("route b");
    assert_eq!(stats.timers, 1);

    make_due(&pool, alert_a).await;
    make_due(&pool, alert_b).await;
    let s = escalation::escalate_once(&pool, &dir, &sched_cfg("s1"))
        .await
        .expect("escalate_once");
    assert_eq!(
        (s.claimed, s.fired, s.jobs_inserted, s.skipped),
        (2, 2, 2, 0)
    );

    let x1 = job_rows(&pool, alert_a)
        .await
        .into_iter()
        .find(|j| j.recipient_id == "x1")
        .expect("x1 job");
    assert_eq!(x1.channel, "sms");
    assert_eq!(x1.escalation_step, Some(1));
    assert_eq!(x1.route_info["via"], "lead:t");
    assert_eq!(
        x1.route_info["decision"],
        json!({"source": "preference", "index": 0, "window": "none"})
    );
    assert_eq!(
        x1.route_info["fallback"],
        json!({ "channel": "email", "address": "x1@example.test" })
    );

    let z1 = job_rows(&pool, alert_b)
        .await
        .into_iter()
        .find(|j| j.recipient_id == "z1")
        .expect("z1 job");
    assert_eq!(z1.channel, "email");
    assert_eq!(z1.escalation_step, Some(1));
    assert_eq!(z1.route_info["via"], "lead:u");
    assert_eq!(z1.route_info["decision"], json!({"source": "ui_default"}));
    // Both single-step chains finished.
    for alert in [alert_a, alert_b] {
        let (state,): (String,) =
            sqlx::query_as("SELECT state FROM escalation_timers WHERE alert_id = $1")
                .bind(alert)
                .fetch_one(&pool)
                .await
                .expect("timer state");
        assert_eq!(state, "done");
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn fire_matches_preferences_on_the_first_notifications_service(pool: PgPool) {
    let dir = Directory::from_parts(
        vec![
            person("y1", Some("y1@example.test"), None, None),
            // x1 (lead of t): payments-api -> sms, anything else -> email.
            person(
                "x1",
                Some("x1@example.test"),
                Some("+44 7700 900302"),
                Some(json!([
                    { "match": { "service": "payments-api", "environment": "prod" }, "channels": ["sms"] },
                    { "default": ["email"] },
                ])),
            ),
        ],
        vec![team("t", &["x1", "y1"], Some("x1"), None)],
    )
    .expect("directory");

    let alert = insert_alert_as(&pool, "open", "warning", "t", "pref-rule").await;
    let payload = json!({
        "alert_id": alert, "notify_seq": 1, "rule_id": "pref-rule", "rule_revision": 1,
        "team": "t", "severity": "warning", "reason": "new",
        "route": [{ "to": "user:y1" }],
        "context": { "service": "payments-api", "environment": "prod" },
        "created_at": chrono::Utc::now(),
    });
    sqlx::query("INSERT INTO outbox (kind, alert_id, payload) VALUES ('notify', $1, $2)")
        .bind(alert)
        .bind(payload)
        .execute(&pool)
        .await
        .expect("insert notify row");
    let stats = route_once(&pool, &dir, &router_cfg(60_000))
        .await
        .expect("route");
    assert_eq!(stats.timers, 1);

    make_due(&pool, alert).await;
    escalation::escalate_once(&pool, &dir, &sched_cfg("s1"))
        .await
        .expect("escalate_once");
    let x1 = job_rows(&pool, alert)
        .await
        .into_iter()
        .find(|j| j.recipient_id == "x1")
        .expect("x1 job");
    assert_eq!(
        x1.channel, "sms",
        "the service preference must match at fire time"
    );
}

// ---------------------------------------------------------------------------------------------
// Skipped steps
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn an_unknown_chain_person_skips_the_step_with_a_note(pool: PgPool) {
    // The directory no longer knows the person the chain froze.
    let dir = Directory::from_parts(
        vec![person("y1", Some("y1@example.test"), None, None)],
        vec![team("t", &["y1"], Some("y1"), Some(&["email"]))],
    )
    .expect("directory");
    let alert = insert_alert_as(&pool, "open", "warning", "t", "ghost-rule").await;
    schedule_direct(
        &pool,
        alert,
        "ghost-rule",
        vec![ChainStep {
            recipient_id: "ghost".into(),
            via: "lead:t".into(),
            channels: None,
        }],
    )
    .await;
    make_due(&pool, alert).await;

    let s = escalation::escalate_once(&pool, &dir, &sched_cfg("s1"))
        .await
        .expect("escalate_once");
    assert_eq!((s.claimed, s.fired, s.skipped, s.finished), (1, 0, 1, 1));
    let (state, step, last_note, jobs): (String, i32, Option<String>, i64) = sqlx::query_as(
        "SELECT state, step, last_note, \
                (SELECT count(*) FROM delivery_jobs WHERE alert_id = $1) \
         FROM escalation_timers WHERE alert_id = $1",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .expect("timer row");
    assert_eq!(state, "done");
    assert_eq!(step, 1);
    assert_eq!(last_note.as_deref(), Some("unknown person ghost"));
    assert_eq!(jobs, 0);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_step_without_any_address_is_skipped_with_a_note(pool: PgPool) {
    // n1 prefers sms but has no phone number.
    let dir = Directory::from_parts(
        vec![person(
            "n1",
            Some("n1@example.test"),
            None,
            Some(json!([{ "match": { "severity": "warning" }, "channels": ["sms"] }])),
        )],
        vec![team("t", &["n1"], Some("n1"), None)],
    )
    .expect("directory");
    let alert = insert_alert_as(&pool, "open", "warning", "t", "addr-rule").await;
    schedule_direct(
        &pool,
        alert,
        "addr-rule",
        vec![ChainStep {
            recipient_id: "n1".into(),
            via: "lead:t".into(),
            channels: None,
        }],
    )
    .await;
    make_due(&pool, alert).await;

    let s = escalation::escalate_once(&pool, &dir, &sched_cfg("s1"))
        .await
        .expect("escalate_once");
    assert_eq!((s.claimed, s.fired, s.skipped, s.finished), (1, 0, 1, 1));
    let (state, last_note): (String, Option<String>) =
        sqlx::query_as("SELECT state, last_note FROM escalation_timers WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("timer row");
    assert_eq!(state, "done");
    assert_eq!(last_note.as_deref(), Some("no address for n1"));
}

// ---------------------------------------------------------------------------------------------
// Throttle interaction (agent B's tables; critical escalations bypass the throttle)
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn non_critical_steps_respect_the_throttle_and_critical_ones_bypass_it(pool: PgPool) {
    // One token a day: once the bucket is drained it stays empty for the whole test.
    let spec = ThrottleSpec {
        max: 1,
        per: Duration::from_secs(86_400),
    };
    let mut dir = Directory::from_parts(
        vec![
            person("x1", Some("x1@example.test"), None, None),
            person("y1", Some("y1@example.test"), None, None),
        ],
        vec![team("t", &["x1", "y1"], Some("x1"), Some(&["email"]))],
    )
    .expect("directory");
    dir.rules.by_rule.insert(
        "throttled-rule".to_string(),
        RulePolicy {
            escalate_after: None,
            throttle: Some(spec),
            runbook: None,
        },
    );

    // `take` creates the bucket full on first use; drain x1's so the escalation step
    // finds no token.
    for _ in 0..50 {
        let mut tx = pool.begin().await.expect("tx");
        let ok = throttle::take(&mut tx, "throttled-rule", "x1", &spec)
            .await
            .expect("take");
        tx.commit().await.expect("commit");
        if !ok {
            break;
        }
    }

    let chain = || {
        vec![ChainStep {
            recipient_id: "x1".into(),
            via: "lead:t".into(),
            channels: None,
        }]
    };
    let warning = insert_alert_as(&pool, "open", "warning", "t", "throttled-rule").await;
    schedule_direct(&pool, warning, "throttled-rule", chain()).await;
    let critical = insert_alert_as(&pool, "open", "critical", "t", "throttled-rule").await;
    schedule_direct(&pool, critical, "throttled-rule", chain()).await;
    make_due(&pool, warning).await;
    make_due(&pool, critical).await;

    let s = escalation::escalate_once(&pool, &dir, &sched_cfg("s1"))
        .await
        .expect("escalate_once");
    assert_eq!(
        (
            s.claimed,
            s.fired,
            s.suppressed,
            s.jobs_inserted,
            s.finished
        ),
        (2, 2, 1, 1, 2)
    );

    // The warning's step was parked, not dropped; x1 has no job row for it.
    let parked: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM throttle_suppressed \
         WHERE rule_id = 'throttled-rule' AND recipient_id = 'x1' AND alert_id = $1",
    )
    .bind(warning)
    .fetch_one(&pool)
    .await
    .expect("count suppressed");
    assert_eq!(parked, 1);
    let job0: Value = sqlx::query_scalar(
        "SELECT jobs->0 FROM throttle_suppressed \
         WHERE rule_id = 'throttled-rule' AND recipient_id = 'x1' AND alert_id = $1",
    )
    .bind(warning)
    .fetch_one(&pool)
    .await
    .expect("parked job");
    assert_eq!(job0["channel"], "email");
    assert_eq!(job0["address"], "x1@example.test");
    assert_eq!(job0["route_info"]["target"], "escalation");
    let warning_jobs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM delivery_jobs WHERE alert_id = $1 AND recipient_id = 'x1'",
    )
    .bind(warning)
    .fetch_one(&pool)
    .await
    .expect("count x1 jobs");
    assert_eq!(warning_jobs, 0, "the throttled step must not have paged x1");

    // The critical alert bypasses the empty bucket: x1 is paged directly.
    let critical_job = job_rows(&pool, critical)
        .await
        .into_iter()
        .find(|j| j.recipient_id == "x1")
        .expect("critical bypass job");
    assert_eq!(critical_job.channel, "email");
    assert_eq!(critical_job.escalation_step, Some(1));
    let critical_parked: i64 =
        sqlx::query_scalar("SELECT count(*) FROM throttle_suppressed WHERE alert_id = $1")
            .bind(critical)
            .fetch_one(&pool)
            .await
            .expect("count suppressed");
    assert_eq!(critical_parked, 0);
}

// ---------------------------------------------------------------------------------------------
// The scheduler loop
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn run_scheduler_fires_due_timers_and_stops_on_shutdown(pool: PgPool) {
    let dir = seed_dir();
    let alert = route_settlements_alert(&pool, &dir, "critical", 600_000).await;
    make_due(&pool, alert).await;

    let (tx, rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(escalation::run_scheduler(
        pool.clone(),
        Arc::new(dir),
        SchedulerConfig {
            poll_interval: Duration::from_millis(50),
            ..SchedulerConfig::default()
        },
        rx,
    ));

    // The scheduler picks the due timer up within its poll interval.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if carol_job_count(&pool, alert).await == 2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "run_scheduler never paged carol"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    tx.send(true).expect("signal shutdown");
    tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("run_scheduler must stop on shutdown")
        .expect("scheduler task must not panic");

    let (state,): (String,) =
        sqlx::query_as("SELECT state FROM escalation_timers WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("timer state");
    assert_eq!(state, "done");
}

#[sqlx::test(migrations = "../../migrations")]
async fn run_scheduler_stops_when_the_shutdown_sender_is_dropped(pool: PgPool) {
    let (tx, rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(escalation::run_scheduler(
        pool.clone(),
        Arc::new(seed_dir()),
        SchedulerConfig {
            poll_interval: Duration::from_millis(50),
            ..SchedulerConfig::default()
        },
        rx,
    ));
    drop(tx);
    tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("run_scheduler must stop when the sender is dropped")
        .expect("scheduler task must not panic");
}
