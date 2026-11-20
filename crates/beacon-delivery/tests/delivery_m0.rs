//! M0 delivery integration suite: router, worker, retries, leases and load.
//!
//! Needs Postgres (`DATABASE_URL`, sqlx creates a throwaway database per test) and, for the
//! tests that touch email, Mailpit (SMTP `SMTP_HOST:SMTP_PORT`, default localhost:1025, and
//! API `MAILPIT_URL`, default http://localhost:8025). Infra: `docker compose -f deploy/docker-compose.yml up -d postgres mailpit`.
//!
//! Run: `cargo test -p beacon-delivery --test delivery_m0 -- --nocapture`
//!
//! Mailpit is never wiped: every test puts a unique tag in its alert title and searches by it.

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beacon_delivery::channel::{Channel, Receipt, SendError};
use beacon_delivery::config::{Contact, Directory, Person, Team};
use beacon_delivery::model::JobStatus;
use beacon_delivery::router::route_once;
use beacon_delivery::worker::{claim_batch, finalize_cancelled, finalize_failed, finalize_sent};
use beacon_delivery::{BackoffPolicy, RouterConfig};
use common::*;
use serde_json::json;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

// ---------------------------------------------------------------------------------------------
// Fixtures built in code
// ---------------------------------------------------------------------------------------------

fn person(id: &str, email: Option<&str>) -> Person {
    Person {
        id: id.to_string(),
        name: None,
        contact: Contact {
            email: email.map(str::to_string),
            phone: None,
        },
        preferences: None,
    }
}

/// anna, bob, carol in team `rates`; dave has no email.
fn directory() -> Directory {
    Directory::from_parts(
        vec![
            person("anna", Some("anna@example.test")),
            person("bob", Some("bob@example.test")),
            person("carol", Some("carol@example.test")),
            person("dave", None),
        ],
        vec![Team {
            id: "rates".into(),
            members: vec!["anna".into(), "bob".into(), "carol".into()],
            lead: Some("bob".into()),
            oncall: Default::default(),
        }],
    )
    .expect("directory")
}

fn alert_title(tag: &str) -> String {
    format!("Rejected EUR 25,000,000 trade T-99877 {tag}")
}

fn receipt() -> Receipt {
    Receipt {
        provider_message_id: "test".into(),
        detail: json!({"test": true}),
    }
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

async fn db_now_gt(pool: &PgPool, job_id: Uuid, column: &str) -> bool {
    sqlx::query_scalar(&format!(
        "SELECT {column} > now() FROM delivery_jobs WHERE id = $1"
    ))
    .bind(job_id)
    .fetch_one(pool)
    .await
    .expect("compare to now()")
}

// ---------------------------------------------------------------------------------------------
// 1. Router
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn router_expands_team_and_is_idempotent(pool: PgPool) {
    let dir = directory();
    let cfg = RouterConfig::default();
    let alert = insert_alert(&pool, "open", &alert_title(&unique_tag())).await;
    let outbox1 = insert_notify(
        &pool,
        alert,
        1,
        json!([{"to": "team:rates", "channels": ["email", "ui"]}]),
    )
    .await;

    let stats = route_once(&pool, &dir, &cfg).await.expect("route_once");
    assert_eq!(stats.outbox_rows, 1, "{stats:?}");
    assert_eq!(
        stats.jobs_inserted, 3,
        "one job per member, `ui` makes none: {stats:?}"
    );
    assert_eq!(stats.malformed, 0);

    let jobs = job_rows(&pool, alert).await;
    let recipients: Vec<&str> = jobs.iter().map(|j| j.recipient_id.as_str()).collect();
    assert_eq!(recipients, ["anna", "bob", "carol"]);
    for j in &jobs {
        assert_eq!(j.channel, "email");
        assert_eq!(j.status, "pending");
        assert_eq!(j.notify_seq, 1);
        assert_eq!(j.address, format!("{}@example.test", j.recipient_id));
        assert_eq!(j.attempts, 0);
    }
    assert!(
        outbox_processed(&pool, outbox1).await,
        "outbox row must be processed"
    );

    // Running again: nothing left to claim, nothing inserted.
    let again = route_once(&pool, &dir, &cfg)
        .await
        .expect("route_once again");
    assert_eq!(again.outbox_rows, 0, "{again:?}");
    assert_eq!(again.jobs_inserted, 0, "{again:?}");

    // A duplicate outbox row with the same notify_seq is consumed but inserts no new jobs.
    let outbox2 = insert_notify(
        &pool,
        alert,
        1,
        json!([{"to": "team:rates", "channels": ["email", "ui"]}]),
    )
    .await;
    let dup = route_once(&pool, &dir, &cfg)
        .await
        .expect("route_once duplicate");
    assert_eq!(dup.outbox_rows, 1, "{dup:?}");
    assert_eq!(dup.jobs_inserted, 0, "{dup:?}");
    assert!(outbox_processed(&pool, outbox2).await);
    assert_eq!(job_rows(&pool, alert).await.len(), 3);

    // A new notify_seq (renotify) does produce a fresh set of jobs.
    let outbox3 = insert_notify(
        &pool,
        alert,
        2,
        json!([{"to": "team:rates", "channels": ["email"]}]),
    )
    .await;
    let renotify = route_once(&pool, &dir, &cfg)
        .await
        .expect("route_once renotify");
    assert_eq!(renotify.jobs_inserted, 3, "{renotify:?}");
    assert!(outbox_processed(&pool, outbox3).await);
    assert_eq!(job_rows(&pool, alert).await.len(), 6);
}

#[sqlx::test(migrations = "../../migrations")]
async fn router_skips_bad_targets_and_marks_malformed_processed(pool: PgPool) {
    let dir = directory();
    let cfg = RouterConfig::default();

    // A: unknown team (skipped) next to a valid user target (still routed).
    let a = insert_alert(&pool, "open", &alert_title(&unique_tag())).await;
    let out_a = insert_notify(
        &pool,
        a,
        1,
        json!([{"to": "team:ghost", "channels": ["email"]}, {"to": "user:anna", "channels": ["email"]}]),
    )
    .await;
    // B: malformed payload.
    let b = insert_alert(&pool, "open", &alert_title(&unique_tag())).await;
    let out_b = insert_outbox_raw(&pool, "notify", b, json!({"not": "a notify payload"})).await;
    // C: no `channels` -> default (email); unsupported channel and no-address person -> skipped.
    let c = insert_alert(&pool, "open", &alert_title(&unique_tag())).await;
    let out_c = insert_notify(
        &pool,
        c,
        1,
        json!([
            {"to": "user:bob"},
            {"to": "user:carol", "channels": ["sms"]},
            {"to": "user:dave", "channels": ["email"]}
        ]),
    )
    .await;

    let stats = route_once(&pool, &dir, &cfg)
        .await
        .expect("a bad target must not fail the batch");
    assert_eq!(stats.outbox_rows, 3, "{stats:?}");
    assert_eq!(stats.malformed, 1, "{stats:?}");
    assert_eq!(stats.jobs_inserted, 2, "{stats:?}");
    assert!(stats.skipped >= 1, "{stats:?}");
    for id in [out_a, out_b, out_c] {
        assert!(
            outbox_processed(&pool, id).await,
            "outbox row {id} must be processed"
        );
    }

    let jobs_a = job_rows(&pool, a).await;
    assert_eq!(jobs_a.len(), 1);
    assert_eq!(jobs_a[0].recipient_id, "anna");
    assert!(job_rows(&pool, b).await.is_empty());
    let jobs_c = job_rows(&pool, c).await;
    assert_eq!(
        jobs_c.len(),
        1,
        "carol (sms) and dave (no email) get no job"
    );
    assert_eq!(jobs_c[0].recipient_id, "bob");
    assert_eq!(jobs_c[0].channel, "email");
}

// ---------------------------------------------------------------------------------------------
// 2. Worker + real EmailChannel -> Mailpit
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn worker_sends_through_mailpit(pool: PgPool) {
    let tag = unique_tag();
    let title = alert_title(&tag);
    let alert = insert_alert(&pool, "open", &title).await;
    let job_id = insert_job(&pool, alert, "anna", &format!("anna-{tag}@example.test")).await;

    let channel = email_channel();
    let stats = work_once_bounded(&pool, &channel, &worker_cfg("w-email")).await;
    let job = job_row(&pool, job_id).await;
    assert_eq!(stats.claimed, 1, "{stats:?}");
    assert_eq!(
        stats.sent, 1,
        "{stats:?} last_error={:?} class={:?}",
        job.last_error, job.last_error_class
    );

    assert_eq!(job.status, "sent");
    assert_eq!(job.attempts, 1);
    assert!(job.sent_at.is_some(), "sent_at");
    assert!(job.provider_receipt.is_some(), "provider_receipt");
    assert!(
        job.lease_token.is_none() && job.locked_until.is_none(),
        "lease cleared"
    );

    let found = wait_for_messages(&tag, 1, Duration::from_secs(10)).await;
    assert_eq!(found.messages_count, 1, "{found:?}");
    let mine = messages_for_job(&found, job_id);
    assert_eq!(
        mine.len(),
        1,
        "Message-ID <{job_id}@beacon> expected in {found:?}"
    );
    assert_eq!(mine[0].message_id, format!("{job_id}@beacon"));
    assert!(
        mine[0].subject.contains(&title),
        "subject {:?} should contain {title:?}",
        mine[0].subject
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn end_to_end_router_then_worker_then_mailpit(pool: PgPool) {
    let tag = unique_tag();
    let title = alert_title(&tag);
    let dir = Directory::from_parts(
        vec![
            person("anna", Some(&format!("anna-{tag}@example.test"))),
            person("bob", Some(&format!("bob-{tag}@example.test"))),
        ],
        vec![Team {
            id: "rates".into(),
            members: vec!["anna".into(), "bob".into()],
            lead: None,
            oncall: Default::default(),
        }],
    )
    .expect("directory");
    let alert = insert_alert(&pool, "open", &title).await;
    insert_notify(
        &pool,
        alert,
        1,
        json!([{"to": "team:rates", "channels": ["email", "ui"]}]),
    )
    .await;

    let routed = route_once(&pool, &dir, &RouterConfig::default())
        .await
        .expect("route_once");
    assert_eq!(routed.jobs_inserted, 2, "{routed:?}");
    let channel = email_channel();
    let stats = work_once_bounded(&pool, &channel, &worker_cfg("w-e2e")).await;
    assert_eq!(stats.sent, 2, "{stats:?}");

    let jobs = job_rows(&pool, alert).await;
    assert!(jobs.iter().all(|j| j.status == "sent"));
    let found = wait_for_messages(&tag, 2, Duration::from_secs(10)).await;
    let got: HashSet<Uuid> = found.messages.iter().filter_map(|m| m.job_id()).collect();
    let want: HashSet<Uuid> = jobs.iter().map(|j| j.id).collect();
    assert_eq!(found.messages_count, 2, "{found:?}");
    assert_eq!(got, want);
}

// ---------------------------------------------------------------------------------------------
// 3. Ack / resolve before the worker runs
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn acked_or_resolved_alert_cancels_job_without_sending(pool: PgPool) {
    for status in ["acked", "resolved"] {
        let alert = insert_alert(&pool, "open", &alert_title(&unique_tag())).await;
        let job_id = insert_job(&pool, alert, "anna", "anna@example.test").await;
        // The alert changes after the job was routed but before any worker looked at it.
        update_alert_status(&pool, alert, status).await;

        let (fake, channel) = FakeChannel::new().into_channel();
        let stats = work_once_bounded(&pool, &channel, &worker_cfg("w-cancel")).await;
        assert_eq!(stats.cancelled, 1, "{status}: {stats:?}");
        assert_eq!(stats.sent, 0, "{status}: {stats:?}");

        let job = job_row(&pool, job_id).await;
        assert_eq!(job.status, "cancelled", "{status}");
        assert!(job.sent_at.is_none());
        assert!(job.last_error.is_some(), "cancel reason recorded");
        assert!(job.lease_token.is_none() && job.locked_until.is_none());
        assert_eq!(fake.calls(), 0, "{status}: channel must not be called");
        assert!(fake.sent().is_empty());
    }
    // A job for a missing alert can no longer exist: delivery_jobs_alert_fk enforces it.
    let missing = Uuid::new_v4();
    let err = sqlx::query(
        "INSERT INTO delivery_jobs (alert_id, notify_seq, recipient_id, channel, address) \
         VALUES ($1, 1, 'anna', 'email', 'anna@example.test')",
    )
    .bind(missing)
    .execute(&pool)
    .await
    .expect_err("FK must reject a job for a missing alert");
    assert!(err.to_string().contains("delivery_jobs_alert_fk"), "{err}");
}

#[sqlx::test(migrations = "../../migrations")]
async fn acked_or_resolved_alert_sends_no_email(pool: PgPool) {
    let channel = email_channel();
    for status in ["acked", "resolved"] {
        let tag = unique_tag();
        let alert = insert_alert(&pool, status, &alert_title(&tag)).await;
        let job_id = insert_job(&pool, alert, "anna", &format!("anna-{tag}@example.test")).await;

        let stats = work_once_bounded(&pool, &channel, &worker_cfg("w-cancel-email")).await;
        assert_eq!(stats.cancelled, 1, "{status}: {stats:?}");
        assert_eq!(job_row(&pool, job_id).await.status, "cancelled");

        // Give a (buggy) late send a moment to land, then expect silence.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let found = mailpit_search(&tag).await;
        assert_eq!(
            found.messages_count, 0,
            "{status}: Mailpit should have nothing for {job_id}: {found:?}"
        );
    }
}

// ---------------------------------------------------------------------------------------------
// 4. Failures and retries
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn smtp_down_is_retried_with_backoff_then_sent(pool: PgPool) {
    let alert = insert_alert(&pool, "open", &alert_title(&unique_tag())).await;
    let job_id = insert_job(&pool, alert, "anna", "anna@example.test").await;

    let dead = dead_email_channel();
    let stats = work_once_bounded(&pool, &dead, &worker_cfg("w-down")).await;
    assert_eq!(stats.claimed, 1, "{stats:?}");
    assert_eq!(stats.retried, 1, "{stats:?}");

    let job = job_row(&pool, job_id).await;
    assert_eq!(job.status, "pending", "last_error: {:?}", job.last_error);
    assert_eq!(job.attempts, 1);
    assert_eq!(
        job.last_error_class.as_deref(),
        Some("transient"),
        "last_error: {:?}",
        job.last_error
    );
    assert!(job.last_error.is_some());
    assert!(job.lease_token.is_none() && job.locked_until.is_none() && job.locked_by.is_none());
    assert!(
        db_now_gt(&pool, job_id, "due_at").await,
        "due_at must be in the future (backoff)"
    );

    // Not due yet: a worker with a healthy channel finds nothing.
    let (fake, good) = FakeChannel::new().into_channel();
    let idle = work_once_bounded(&pool, &good, &worker_cfg("w-up")).await;
    assert_eq!(idle.claimed, 0, "{idle:?}");

    // Make it due; a working channel delivers it.
    sqlx::query("UPDATE delivery_jobs SET due_at = now() WHERE id = $1")
        .bind(job_id)
        .execute(&pool)
        .await
        .unwrap();
    let stats = work_once_bounded(&pool, &good, &worker_cfg("w-up")).await;
    assert_eq!(stats.sent, 1, "{stats:?}");
    let job = job_row(&pool, job_id).await;
    assert_eq!(job.status, "sent");
    assert_eq!(job.attempts, 2);
    assert!(job.sent_at.is_some() && job.provider_receipt.is_some());
    assert_eq!(fake.sent(), vec![job_id]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn transient_failure_on_last_attempt_is_dead(pool: PgPool) {
    let alert = insert_alert(&pool, "open", &alert_title(&unique_tag())).await;
    let job_id = insert_job_max(&pool, alert, "anna", "anna@example.test", 1).await;

    let (fake, channel) = FakeChannel::new()
        .always_failing(SendError::Transient("smtp 451 try later".into()))
        .into_channel();
    let stats = work_once_bounded(&pool, &channel, &worker_cfg("w-last")).await;
    assert_eq!(stats.dead, 1, "{stats:?}");

    let job = job_row(&pool, job_id).await;
    assert_eq!(job.status, "dead");
    assert_eq!(job.attempts, 1);
    assert_eq!(job.last_error_class.as_deref(), Some("transient"));
    assert!(
        job.last_error
            .as_deref()
            .unwrap_or_default()
            .contains("451"),
        "{:?}",
        job.last_error
    );
    assert!(job.lease_token.is_none() && job.locked_until.is_none());
    assert_eq!(fake.calls(), 1);

    // Dead jobs are never picked up again.
    let again = work_once_bounded(&pool, &channel, &worker_cfg("w-last")).await;
    assert_eq!(again.claimed, 0);
}

#[sqlx::test(migrations = "../../migrations")]
async fn permanent_error_is_dead_at_once(pool: PgPool) {
    let alert = insert_alert(&pool, "open", &alert_title(&unique_tag())).await;
    let job_id = insert_job(&pool, alert, "anna", "anna@example.test").await; // max_attempts 8

    let (fake, channel) = FakeChannel::new()
        .always_failing(SendError::Permanent("smtp 550 no such user".into()))
        .into_channel();
    let stats = work_once_bounded(&pool, &channel, &worker_cfg("w-perm")).await;
    assert_eq!(stats.dead, 1, "{stats:?}");

    let job = job_row(&pool, job_id).await;
    assert_eq!(job.status, "dead");
    assert_eq!(job.attempts, 1);
    assert_eq!(job.last_error_class.as_deref(), Some("permanent"));
    assert_eq!(fake.calls(), 1);
}

// ---------------------------------------------------------------------------------------------
// 5. Leases
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn expired_lease_is_reclaimed_and_stale_finalize_is_fenced(pool: PgPool) {
    let alert = insert_alert(&pool, "open", &alert_title(&unique_tag())).await;
    let job_id = insert_job(&pool, alert, "anna", "anna@example.test").await;
    let (cfg_a, cfg_b) = (worker_cfg("worker-a"), worker_cfg("worker-b"));

    // Worker A claims.
    let claimed_a = claim_batch(&pool, "email", &cfg_a).await.expect("claim a");
    assert_eq!(claimed_a.dead_on_claim, 0);
    assert_eq!(claimed_a.jobs.len(), 1);
    let job_a = claimed_a.jobs[0].clone();
    let token_a = job_a.lease_token.expect("lease token on claimed job");
    assert_eq!(job_a.id, job_id);
    assert_eq!(job_a.attempts, 1);
    let row = job_row(&pool, job_id).await;
    assert_eq!(row.status, "in_flight");
    assert_eq!(row.locked_by.as_deref(), Some("worker-a"));
    assert_eq!(row.lease_token, Some(token_a));
    assert!(
        db_now_gt(&pool, job_id, "locked_until").await,
        "lease is live"
    );
    let updated_after_claim = row.updated_at;

    // While the lease is live nobody else gets the job.
    let early = claim_batch(&pool, "email", &cfg_b)
        .await
        .expect("claim b early");
    assert!(early.jobs.is_empty(), "live lease must not be stolen");

    // Simulate expiry (A is "stuck").
    sqlx::query(
        "UPDATE delivery_jobs SET locked_until = now() - interval '1 second' WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Worker B reclaims with a fresh token and attempts = 2.
    let claimed_b = claim_batch(&pool, "email", &cfg_b).await.expect("claim b");
    assert_eq!(claimed_b.jobs.len(), 1);
    let job_b = claimed_b.jobs[0].clone();
    let token_b = job_b.lease_token.expect("token b");
    assert_ne!(token_a, token_b);
    assert_eq!(job_b.attempts, 2);
    let row = job_row(&pool, job_id).await;
    assert_eq!(row.status, "in_flight");
    assert_eq!(row.locked_by.as_deref(), Some("worker-b"));
    assert_eq!(row.attempts, 2);
    assert!(
        row.updated_at > updated_after_claim,
        "updated_at must advance on reclaim"
    );
    let updated_after_reclaim = row.updated_at;

    // A wakes up and tries every finalizer with the old token: all fenced.
    let r = receipt();
    assert!(
        !finalize_sent(&pool, job_id, token_a, &r).await.unwrap(),
        "stale sent"
    );
    assert!(
        !finalize_cancelled(&pool, job_id, token_a, "alert acked")
            .await
            .unwrap(),
        "stale cancel"
    );
    let failed = finalize_failed(
        &pool,
        &job_a,
        token_a,
        &SendError::Transient("late".into()),
        &BackoffPolicy::default(),
    )
    .await
    .unwrap();
    assert_eq!(failed, None, "stale failed");
    let row = job_row(&pool, job_id).await;
    assert_eq!(
        row.status, "in_flight",
        "stale finalizers must not change the row"
    );
    assert_eq!(row.lease_token, Some(token_b));
    assert_eq!(row.updated_at, updated_after_reclaim);

    // B finalizes.
    assert!(finalize_sent(&pool, job_id, token_b, &r).await.unwrap());
    let row = job_row(&pool, job_id).await;
    assert_eq!(row.status, "sent");
    assert_eq!(row.attempts, 2);
    assert!(row.sent_at.is_some() && row.provider_receipt.is_some());
    assert!(row.lease_token.is_none() && row.locked_until.is_none());
    assert!(
        row.updated_at > updated_after_reclaim,
        "updated_at must advance on finalize"
    );
    // And a second finalize with the same token is a no-op.
    assert!(!finalize_sent(&pool, job_id, token_b, &r).await.unwrap());
}

#[sqlx::test(migrations = "../../migrations")]
async fn expired_lease_without_attempts_left_is_dead_lettered(pool: PgPool) {
    let alert = insert_alert(&pool, "open", &alert_title(&unique_tag())).await;
    let exhausted: Uuid = sqlx::query_scalar(
        "INSERT INTO delivery_jobs \
           (alert_id, notify_seq, recipient_id, channel, address, status, attempts, max_attempts, \
            locked_by, locked_until, lease_token, updated_at) \
         VALUES ($1, 1, 'exhausted', 'email', 'x@example.test', 'in_flight', 3, 3, \
                 'worker-x', now() - interval '1 second', gen_random_uuid(), now() - interval '1 hour') \
         RETURNING id",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .unwrap();
    let fresh = insert_job(&pool, alert, "fresh", "fresh@example.test").await;
    let before = job_row(&pool, exhausted).await.updated_at;

    let claimed = claim_batch(&pool, "email", &worker_cfg("worker-b"))
        .await
        .expect("claim");
    assert_eq!(claimed.dead_on_claim, 1);
    let ids: Vec<Uuid> = claimed.jobs.iter().map(|j| j.id).collect();
    assert_eq!(
        ids,
        vec![fresh],
        "the exhausted job must not be returned as claimed"
    );

    let dead = job_row(&pool, exhausted).await;
    assert_eq!(dead.status, JobStatus::Dead.as_str());
    assert_eq!(dead.last_error_class.as_deref(), Some("ambiguous"));
    assert_eq!(
        dead.last_error.as_deref(),
        Some("max attempts exceeded after lease expiry")
    );
    assert_eq!(dead.attempts, 3, "attempts unchanged");
    assert!(dead.lease_token.is_none() && dead.locked_until.is_none());
    assert!(
        dead.updated_at > before,
        "updated_at must advance when dead-lettered"
    );

    // Nothing left to reclaim for the dead job.
    let again = claim_batch(&pool, "email", &worker_cfg("worker-c"))
        .await
        .expect("claim again");
    assert!(again.jobs.is_empty() && again.dead_on_claim == 0);
}

// ---------------------------------------------------------------------------------------------
// 6. Concurrency
// ---------------------------------------------------------------------------------------------

const LOAD_JOBS: i64 = 1_000;
const LOAD_WORKERS: usize = 8;

/// One alert with `n` distinct recipients, inserted directly. Returns the alert id.
async fn insert_many_jobs(pool: &PgPool, title: &str, n: i64) -> Uuid {
    let alert = insert_alert(pool, "open", title).await;
    sqlx::query(
        "INSERT INTO delivery_jobs (alert_id, notify_seq, recipient_id, channel, address) \
         SELECT $1, 1, 'r' || g, 'email', 'r' || g || '@example.test' FROM generate_series(1, $2::int) g",
    )
    .bind(alert)
    .bind(n)
    .execute(pool)
    .await
    .expect("insert jobs");
    alert
}

/// Run `workers` looping `work_once` until no pending/in_flight jobs remain (or `limit`).
async fn drain(pool: &PgPool, channel: &Arc<dyn Channel>, workers: usize, limit: Duration) {
    let mut handles = Vec::new();
    for i in 0..workers {
        let (pool, channel) = (pool.clone(), channel.clone());
        handles.push(tokio::spawn(async move {
            let cfg = worker_cfg(&format!("load-worker-{i}"));
            loop {
                let stats = beacon_delivery::worker::work_once(&pool, &channel, &cfg)
                    .await
                    .expect("work_once");
                if stats.claimed == 0 {
                    if open_job_count(&pool).await == 0 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
        }));
    }
    let joined = tokio::time::timeout(limit, async {
        for h in handles {
            h.await.expect("worker task panicked");
        }
    })
    .await;
    assert!(
        joined.is_ok(),
        "workers did not drain {LOAD_JOBS} jobs within {limit:?}"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn concurrent_workers_send_each_job_exactly_once_fake(pool: PgPool) {
    let pool = big_pool(&pool, 20).await;
    let alert = insert_many_jobs(&pool, &alert_title(&unique_tag()), LOAD_JOBS).await;
    let (fake, channel) = FakeChannel::new()
        .with_delay(Duration::from_millis(2))
        .into_channel();

    let t0 = Instant::now();
    drain(&pool, &channel, LOAD_WORKERS, Duration::from_secs(120)).await;
    let elapsed = t0.elapsed();
    println!(
        "fake channel: {LOAD_JOBS} jobs, {LOAD_WORKERS} workers in {elapsed:.2?} ({:.0} jobs/s)",
        LOAD_JOBS as f64 / elapsed.as_secs_f64()
    );

    let jobs = job_rows(&pool, alert).await;
    assert_eq!(jobs.len() as i64, LOAD_JOBS);
    assert!(jobs.iter().all(|j| j.status == "sent"), "all jobs sent");
    assert!(jobs.iter().all(|j| j.attempts == 1), "no job claimed twice");

    let sent = fake.sent();
    let unique: HashSet<Uuid> = sent.iter().copied().collect();
    assert_eq!(
        sent.len() as i64,
        LOAD_JOBS,
        "each job sent exactly once (no duplicates, no losses)"
    );
    assert_eq!(unique, jobs.iter().map(|j| j.id).collect::<HashSet<_>>());
    assert_eq!(fake.calls() as i64, LOAD_JOBS);
}

#[sqlx::test(migrations = "../../migrations")]
async fn concurrent_workers_deliver_1000_emails_to_mailpit(pool: PgPool) {
    let pool = big_pool(&pool, 20).await;
    let tag = unique_tag();
    let alert = insert_many_jobs(&pool, &alert_title(&tag), LOAD_JOBS).await;
    let channel = email_channel();

    let t0 = Instant::now();
    drain(&pool, &channel, LOAD_WORKERS, Duration::from_secs(180)).await;
    let elapsed = t0.elapsed();
    println!(
        "mailpit: {LOAD_JOBS} emails, {LOAD_WORKERS} workers in {elapsed:.2?} ({:.0} msgs/s)",
        LOAD_JOBS as f64 / elapsed.as_secs_f64()
    );

    let jobs = job_rows(&pool, alert).await;
    assert_eq!(jobs.len() as i64, LOAD_JOBS);
    let not_sent: Vec<_> = jobs
        .iter()
        .filter(|j| j.status != "sent")
        .map(|j| (j.id, &j.status, &j.last_error))
        .collect();
    assert!(
        not_sent.is_empty(),
        "{} jobs not sent, first: {:?}",
        not_sent.len(),
        not_sent.first()
    );

    let found = wait_for_messages(&tag, LOAD_JOBS as usize, Duration::from_secs(60)).await;
    assert_eq!(
        found.messages_count as i64, LOAD_JOBS,
        "Mailpit message count"
    );
    assert_eq!(
        found.messages.len() as i64,
        LOAD_JOBS,
        "all messages returned by the API"
    );
    let got: Vec<Uuid> = found
        .messages
        .iter()
        .map(|m| m.job_id().expect("Message-ID is <job_id@beacon>"))
        .collect();
    let got_set: HashSet<Uuid> = got.iter().copied().collect();
    assert_eq!(got.len(), got_set.len(), "no Message-ID appears twice");
    assert_eq!(
        got_set,
        jobs.iter().map(|j| j.id).collect::<HashSet<_>>(),
        "Message-IDs == job ids"
    );
}
