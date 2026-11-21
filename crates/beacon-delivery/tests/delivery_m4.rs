//! M4 delivery integration suite: the channel breaker, deferral, one-hop fallback jobs,
//! and the SMS outage scenario (SPEC §2.3, §5.6, §9 scenario c).
//!
//! Needs Postgres (`DATABASE_URL`; sqlx creates a throwaway database per test). The outage
//! tests start the fake gateway (`crate::gateway`) in-process, so no Mailpit is involved.
//!
//! Run: `cargo test -p beacon-delivery --test delivery_m4 -- --nocapture`

mod common;

use std::collections::HashSet;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use beacon_delivery::breaker::{self, BreakerPolicy, Gate};
use beacon_delivery::channel::gateway_client::{GatewayChannel, GatewayClientConfig, GatewayKind};
use beacon_delivery::channel::{BoxFuture, Channel, OutboundMessage, Receipt, SendError};
use beacon_delivery::gateway::{self, GatewayState, OutageMode};
use beacon_delivery::model::DeliveryJob;
use beacon_delivery::worker::{self, WorkStats, claim_batch};
use beacon_delivery::{BackoffPolicy, WorkerConfig};
use chrono::Utc;
use common::{insert_alert, job_row, job_rows, unique_tag, work_once_bounded};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

/// Short policy so breaker transitions happen in test time.
fn short_breaker() -> BreakerPolicy {
    BreakerPolicy {
        threshold: 3,
        cooldown: Duration::from_millis(300),
        probe_lease: Duration::from_secs(1),
    }
}

fn m4_cfg(worker_id: &str) -> WorkerConfig {
    WorkerConfig {
        worker_id: worker_id.to_string(),
        breaker: short_breaker(),
        ..WorkerConfig::default()
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

/// Insert one pending job on an arbitrary channel with a route_info.
async fn insert_job_on(
    pool: &PgPool,
    alert_id: Uuid,
    notify_seq: i32,
    recipient: &str,
    channel: &str,
    address: &str,
    route_info: Value,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO delivery_jobs \
           (alert_id, notify_seq, recipient_id, channel, address, route_info) \
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
    )
    .bind(alert_id)
    .bind(notify_seq)
    .bind(recipient)
    .bind(channel)
    .bind(address)
    .bind(route_info)
    .fetch_one(pool)
    .await
    .expect("insert job")
}

async fn insert_sms_job(
    pool: &PgPool,
    alert_id: Uuid,
    recipient: &str,
    address: &str,
    route_info: Value,
) -> Uuid {
    insert_job_on(pool, alert_id, 1, recipient, "sms", address, route_info).await
}

/// Route info with a one-hop fallback (SPEC §5.6), as the M4 router freezes it.
fn fallback_route(fallback_channel: &str, address: &str) -> Value {
    json!({
        "target": "oncall:settlements-primary",
        "rule_id": "settlements-workflow-failed",
        "reason": "new",
        "fallback": { "channel": fallback_channel, "address": address },
    })
}

async fn breaker_probe_token(pool: &PgPool, channel: &str) -> Option<Uuid> {
    sqlx::query_scalar("SELECT probe_token FROM channel_breakers WHERE channel = $1")
        .bind(channel)
        .fetch_one(pool)
        .await
        .expect("breaker row")
}

/// sms jobs still pending or in flight.
async fn sms_open_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM delivery_jobs \
         WHERE channel = 'sms' AND status IN ('pending', 'in_flight')",
    )
    .fetch_one(pool)
    .await
    .expect("count open sms jobs")
}

fn add_stats(total: &mut WorkStats, s: &WorkStats) {
    total.claimed += s.claimed;
    total.sent += s.sent;
    total.cancelled += s.cancelled;
    total.retried += s.retried;
    total.dead += s.dead;
    total.dead_on_claim += s.dead_on_claim;
    total.lost_lease += s.lost_lease;
    total.deferred += s.deferred;
    total.fallbacks += s.fallbacks;
}

// ---------------------------------------------------------------------------------------------
// Local fake with a configurable channel name
// ---------------------------------------------------------------------------------------------

/// Like `common::FakeChannel`, but the channel NAME is configurable and results are scripted,
/// so one suite can fake `sms` and `email` separately.
struct Scripted {
    name: &'static str,
    state: Mutex<ScriptedState>,
    calls: AtomicUsize,
}

struct ScriptedState {
    failures: VecDeque<SendError>,
    always_fail: Option<SendError>,
    sent: Vec<Uuid>,
}

impl Scripted {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            state: Mutex::new(ScriptedState {
                failures: VecDeque::new(),
                always_fail: None,
                sent: Vec::new(),
            }),
            calls: AtomicUsize::new(0),
        }
    }

    /// Fail the next `times` sends with `err`, then succeed.
    fn failing(self, err: SendError, times: usize) -> Self {
        self.state.lock().unwrap().failures = std::iter::repeat_n(err, times).collect();
        self
    }

    /// Fail every send with `err`.
    fn always_failing(self, err: SendError) -> Self {
        self.state.lock().unwrap().always_fail = Some(err);
        self
    }

    fn into_channel(self) -> (Arc<Scripted>, Arc<dyn Channel>) {
        let fake = Arc::new(self);
        let dynamic: Arc<dyn Channel> = fake.clone();
        (fake, dynamic)
    }

    /// Job ids of successful sends, in order.
    fn sent(&self) -> Vec<Uuid> {
        self.state.lock().unwrap().sent.clone()
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl Channel for Scripted {
    fn name(&self) -> &'static str {
        self.name
    }

    fn send<'a>(&'a self, msg: &'a OutboundMessage) -> BoxFuture<'a, Result<Receipt, SendError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut st = self.state.lock().unwrap();
            if let Some(err) = st.always_fail.clone() {
                return Err(err);
            }
            if let Some(err) = st.failures.pop_front() {
                return Err(err);
            }
            st.sent.push(msg.job_id);
            Ok(Receipt {
                provider_message_id: format!("<{}@fake-{}>", msg.job_id, self.name),
                detail: json!({ "fake": self.name }),
            })
        })
    }
}

// ---------------------------------------------------------------------------------------------
// 1. Breaker lifecycle: open, block, probe (fail), probe (succeed), drain
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn breaker_opens_blocks_probes_and_recovers(pool: PgPool) {
    // A long cooldown makes the phase windows deterministic: every reopen pushes `open_until`
    // far past the end of the test, so at most one probe can be taken per phase. The window
    // boundaries are moved explicitly, exactly like the M0 lease tests move `locked_until`;
    // real cooldown timing is covered by the outage tests further down.
    let cfg = WorkerConfig {
        worker_id: "w-brk".into(),
        breaker: BreakerPolicy {
            threshold: 3,
            cooldown: Duration::from_secs(30),
            probe_lease: Duration::from_secs(1),
        },
        ..WorkerConfig::default()
    };
    let alert = insert_alert(&pool, "open", &format!("breaker m4 {}", unique_tag())).await;
    let _a = insert_sms_job(&pool, alert, "anna", "+44111", json!({})).await;
    let _b = insert_sms_job(&pool, alert, "bob", "+44222", json!({})).await;
    let _c = insert_sms_job(&pool, alert, "carol", "+44333", json!({})).await;

    // --- three transient failures open the breaker ---------------------------------------------
    let (bad_fake, bad) = Scripted::new("sms")
        .always_failing(SendError::Transient("sms provider 503".into()))
        .into_channel();
    let stats = work_once_bounded(&pool, &bad, &cfg).await;
    assert_eq!(stats.claimed, 3, "{stats:?}");
    // Every one of these was a real send attempt, so each consumes its attempt even though
    // the third failure opened the breaker: nothing is refunded.
    assert_eq!(stats.deferred, 0, "{stats:?}");
    assert_eq!(stats.retried, 3, "{stats:?}");
    assert_eq!(stats.dead + stats.sent, 0, "{stats:?}");
    assert_eq!(
        breaker::status(&pool, "sms").await.expect("status"),
        ("open".to_string(), 3)
    );

    let jobs = job_rows(&pool, alert).await;
    assert!(jobs.iter().all(|j| j.status == "pending"), "{jobs:?}");
    assert!(
        jobs.iter().all(|j| j.attempts == 1),
        "real attempts are consumed even when they open the breaker: {jobs:?}"
    );
    assert!(jobs.iter().all(|j| j.lease_token.is_none()));

    // --- while open, nothing is claimed and attempts do not move -------------------------------
    // The 30 s cooldown leaves a comfortable margin: nothing in this test can reach the next
    // probe window by itself.
    let d = insert_sms_job(&pool, alert, "dana", "+44444", json!({})).await;
    let attempts_before: Vec<(Uuid, i32)> =
        sqlx::query_as("SELECT id, attempts FROM delivery_jobs WHERE channel = 'sms' ORDER BY id")
            .fetch_all(&pool)
            .await
            .expect("attempts before");

    let (good_fake, good) = Scripted::new("sms").into_channel();
    let idle = work_once_bounded(&pool, &good, &cfg).await;
    assert_eq!(
        idle,
        WorkStats::default(),
        "open breaker claims nothing: {idle:?}"
    );
    assert_eq!(good_fake.calls(), 0);
    let d_row = job_row(&pool, d).await;
    assert_eq!(d_row.status, "pending");
    assert_eq!(
        d_row.attempts, 0,
        "an open breaker must not consume attempts"
    );
    let attempts_after: Vec<(Uuid, i32)> =
        sqlx::query_as("SELECT id, attempts FROM delivery_jobs WHERE channel = 'sms' ORDER BY id")
            .fetch_all(&pool)
            .await
            .expect("attempts after");
    assert_eq!(
        attempts_before, attempts_after,
        "attempts must not move while open"
    );

    // --- cooldown over: exactly one probe job, even with several concurrent workers ------------
    sqlx::query("UPDATE channel_breakers SET open_until = now() WHERE channel = 'sms'")
        .execute(&pool)
        .await
        .expect("cooldown over");
    // Only d is due, so the probe target is deterministic.
    sqlx::query(
        "UPDATE delivery_jobs SET due_at = now() + interval '1 hour' WHERE channel = 'sms'",
    )
    .execute(&pool)
    .await
    .expect("park backlog");
    sqlx::query("UPDATE delivery_jobs SET due_at = now() WHERE id = $1")
        .bind(d)
        .execute(&pool)
        .await
        .expect("make probe job due");

    let calls_before_probe = bad_fake.calls();
    let mut handles = Vec::new();
    for _ in 0..8 {
        let (pool, bad, cfg) = (pool.clone(), bad.clone(), cfg.clone());
        handles.push(tokio::spawn(async move {
            worker::work_once(&pool, &bad, &cfg)
                .await
                .expect("work_once")
        }));
    }
    let mut total = WorkStats::default();
    for h in handles {
        let stats = tokio::time::timeout(Duration::from_secs(30), h)
            .await
            .expect("probe round timed out")
            .expect("probe task panicked");
        add_stats(&mut total, &stats);
    }
    assert_eq!(total.claimed, 1, "exactly one probe job: {total:?}");
    assert_eq!(
        total.retried, 1,
        "the failed probe consumes its attempt: {total:?}"
    );
    assert_eq!(total.deferred, 0, "{total:?}");
    assert_eq!(total.sent + total.dead, 0, "{total:?}");
    assert_eq!(bad_fake.calls() - calls_before_probe, 1);

    // The probe job consumed its attempt and waits out the cooldown (30 s here, far above
    // its ~1 s backoff, so the cooldown floor pins `due_at`).
    let d_row = job_row(&pool, d).await;
    assert_eq!(d_row.status, "pending");
    assert_eq!(
        d_row.attempts, 1,
        "a real probe attempt consumes its attempt"
    );
    let probe_due_in = d_row.due_at - Utc::now();
    assert!(
        probe_due_in >= chrono::Duration::seconds(29),
        "due_at must respect the 30 s cooldown floor, got {probe_due_in}"
    );
    assert!(
        d_row.due_at > Utc::now(),
        "due_at in the future: {:?}",
        d_row.due_at
    );
    assert_eq!(d_row.last_error_class.as_deref(), Some("transient"));
    assert!(d_row.last_error.is_some());
    assert!(d_row.lease_token.is_none() && d_row.locked_until.is_none());
    // The backlog was untouched: only the probe job moved (0 -> 1 attempts).
    let attempts_probed: Vec<(Uuid, i32)> =
        sqlx::query_as("SELECT id, attempts FROM delivery_jobs WHERE channel = 'sms' ORDER BY id")
            .fetch_all(&pool)
            .await
            .expect("attempts after probe");
    let before_map: std::collections::HashMap<Uuid, i32> = attempts_before.into_iter().collect();
    for (id, attempts) in &attempts_probed {
        let expected = if *id == d {
            1
        } else {
            *before_map.get(id).expect("job seen before")
        };
        assert_eq!(
            *attempts, expected,
            "probe claims only one job (id={id}): {attempts_probed:?}"
        );
    }
    // The failed probe reopened the breaker.
    let (state, failures) = breaker::status(&pool, "sms").await.expect("status");
    assert_eq!(state, "open");
    assert!(failures >= 3, "{failures}");

    // --- cooldown over: a successful probe closes the breaker and the backlog drains -------------
    // Move the window (the failed probe pushed `open_until` 30 s out) and release the backlog.
    sqlx::query("UPDATE channel_breakers SET open_until = now() WHERE channel = 'sms'")
        .execute(&pool)
        .await
        .expect("cooldown over");
    sqlx::query("UPDATE delivery_jobs SET due_at = now() WHERE channel = 'sms'")
        .execute(&pool)
        .await
        .expect("release backlog");

    let (good2_fake, good2) = Scripted::new("sms").into_channel();
    let probe = work_once_bounded(&pool, &good2, &cfg).await;
    assert_eq!(probe.claimed, 1, "the probe claims one job: {probe:?}");
    assert_eq!(probe.sent, 1, "{probe:?}");
    assert_eq!(
        breaker::status(&pool, "sms").await.expect("status"),
        ("closed".to_string(), 0)
    );
    assert!(
        breaker_probe_token(&pool, "sms").await.is_none(),
        "probe cleared"
    );

    let rest = work_once_bounded(&pool, &good2, &cfg).await;
    assert_eq!(rest.claimed, 3, "{rest:?}");
    assert_eq!(rest.sent, 3, "{rest:?}");

    let jobs = job_rows(&pool, alert).await;
    assert_eq!(jobs.len(), 4);
    assert!(jobs.iter().all(|j| j.status == "sent"), "{jobs:?}");
    assert!(jobs.iter().all(|j| j.attempts <= 2), "{jobs:?}");
    let sent = good2_fake.sent();
    assert_eq!(
        sent.iter().copied().collect::<HashSet<_>>(),
        jobs.iter().map(|j| j.id).collect::<HashSet<_>>()
    );
}

// ---------------------------------------------------------------------------------------------
// 2. Probe takeover after a crashed probe worker
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn crashed_probe_worker_loses_its_probe_to_a_takeover(pool: PgPool) {
    let policy = short_breaker();

    // First acquire creates the row as closed.
    assert_eq!(
        breaker::acquire(&pool, "sms", &policy)
            .await
            .expect("acquire"),
        Gate::Closed
    );
    // Simulate a breaker opened by three failures whose cooldown has passed.
    sqlx::query(
        "UPDATE channel_breakers \
         SET state = 'open', consecutive_failures = 3, opened_at = now(), \
             open_until = now() - interval '1 second' \
         WHERE channel = 'sms'",
    )
    .execute(&pool)
    .await
    .expect("force open");

    let Gate::Probe { token: token1 } = breaker::acquire(&pool, "sms", &policy)
        .await
        .expect("acquire")
    else {
        panic!("probe expected");
    };
    assert_eq!(
        breaker::status(&pool, "sms").await.expect("status").0,
        "half_open"
    );
    assert_eq!(breaker_probe_token(&pool, "sms").await, Some(token1));

    // While the probe lease is live, every other worker sees Open: no second probe.
    assert_eq!(
        breaker::acquire(&pool, "sms", &policy)
            .await
            .expect("acquire"),
        Gate::Open
    );

    // The probe worker "crashes": it never reports. After probe_until, another worker takes
    // the probe over with a fresh token, so half_open never deadlocks the channel.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let Gate::Probe { token: token2 } = breaker::acquire(&pool, "sms", &policy)
        .await
        .expect("acquire")
    else {
        panic!("takeover probe expected");
    };
    assert_ne!(token1, token2, "the takeover mints a fresh token");
    assert_eq!(breaker_probe_token(&pool, "sms").await, Some(token2));

    // A stale token's reports change nothing at all.
    breaker::record_failure(&pool, "sms", Some(token1), &policy)
        .await
        .expect("stale record_failure");
    let (state, failures) = breaker::status(&pool, "sms").await.expect("status");
    assert_eq!(
        state, "half_open",
        "stale failure must not change the state"
    );
    assert_eq!(failures, 3, "stale failure must not count");
    assert_eq!(breaker_probe_token(&pool, "sms").await, Some(token2));

    breaker::record_success(&pool, "sms", Some(token1))
        .await
        .expect("stale record_success");
    assert_eq!(
        breaker::status(&pool, "sms").await.expect("status"),
        ("half_open".to_string(), 3),
        "stale success must not close the breaker"
    );
    assert_eq!(breaker_probe_token(&pool, "sms").await, Some(token2));

    // A stale failure on a closed breaker does not count either.
    breaker::record_success(&pool, "sms", Some(token2))
        .await
        .expect("holder record_success");
    assert_eq!(
        breaker::status(&pool, "sms").await.expect("status"),
        ("closed".to_string(), 0)
    );
    assert!(
        !breaker::record_failure(&pool, "sms", Some(token1), &policy)
            .await
            .expect("stale failure on closed")
    );
    assert_eq!(
        breaker::status(&pool, "sms").await.expect("status"),
        ("closed".to_string(), 0),
        "stale token must not touch a closed breaker"
    );
}

// ---------------------------------------------------------------------------------------------
// 3. Retry-After below the threshold
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn retry_after_failure_waits_for_the_provider_delay(pool: PgPool) {
    let cfg = m4_cfg("w-ra");
    let alert = insert_alert(&pool, "open", &format!("retry-after m4 {}", unique_tag())).await;
    let job_id = insert_sms_job(&pool, alert, "anna", "+44111", json!({})).await;

    let (fake, channel) = Scripted::new("sms")
        .failing(
            SendError::RetryAfter("sms provider asks to wait".into(), Duration::from_secs(5)),
            1,
        )
        .into_channel();
    let stats = work_once_bounded(&pool, &channel, &cfg).await;
    assert_eq!(stats.claimed, 1, "{stats:?}");
    assert_eq!(
        stats.retried, 1,
        "below the threshold: no deferral: {stats:?}"
    );
    assert_eq!(stats.deferred, 0, "{stats:?}");
    assert_eq!(fake.calls(), 1);

    let job = job_row(&pool, job_id).await;
    assert_eq!(job.status, "pending");
    assert_eq!(job.attempts, 1);
    assert_eq!(job.last_error_class.as_deref(), Some("transient"));
    let due_in = job.due_at - Utc::now();
    assert!(
        due_in >= chrono::Duration::seconds(4),
        "due_at must honour Retry-After (5 s), got {due_in}"
    );
    assert_eq!(
        breaker::status(&pool, "sms").await.expect("status"),
        ("closed".to_string(), 1),
        "one failure counted, breaker still closed"
    );

    // Not due yet: no claim, and the job is not wedged once it becomes due.
    let (good_fake, good) = Scripted::new("sms").into_channel();
    let idle = work_once_bounded(&pool, &good, &cfg).await;
    assert_eq!(idle.claimed, 0, "{idle:?}");
    assert_eq!(good_fake.calls(), 0);

    sqlx::query("UPDATE delivery_jobs SET due_at = now() WHERE id = $1")
        .bind(job_id)
        .execute(&pool)
        .await
        .expect("make due");
    let sent = work_once_bounded(&pool, &good, &cfg).await;
    assert_eq!(sent.sent, 1, "{sent:?}");
    let job = job_row(&pool, job_id).await;
    assert_eq!(job.status, "sent");
    assert_eq!(job.attempts, 2);
    assert_eq!(good_fake.sent(), vec![job_id]);
}

// ---------------------------------------------------------------------------------------------
// 4. One-hop fallback
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn hostile_retry_after_is_clamped_to_ten_minutes(pool: PgPool) {
    use beacon_delivery::channel::gateway_client::MAX_RETRY_AFTER;

    let cfg = m4_cfg("w-ra-clamp");
    let alert = insert_alert(&pool, "open", &format!("retry clamp m4 {}", unique_tag())).await;
    let job_id = insert_sms_job(&pool, alert, "anna", "+44111", json!({})).await;

    // A "wait 31.7 years" answer from the provider: the retry delay must clamp to
    // MAX_RETRY_AFTER (600 s), not park the job for decades.
    let (fake, channel) = Scripted::new("sms")
        .failing(
            SendError::RetryAfter(
                "gateway says wait forever".into(),
                Duration::from_secs(1_000_000_000),
            ),
            1,
        )
        .into_channel();
    let stats = work_once_bounded(&pool, &channel, &cfg).await;
    assert_eq!(stats.claimed, 1, "{stats:?}");
    assert_eq!(stats.retried, 1, "{stats:?}");
    assert_eq!(stats.dead, 0, "{stats:?}");
    assert_eq!(fake.calls(), 1);

    let job = job_row(&pool, job_id).await;
    assert_eq!(job.status, "pending");
    assert_eq!(job.last_error_class.as_deref(), Some("transient"));
    let due_in = job.due_at - Utc::now();
    assert!(
        due_in >= chrono::Duration::seconds(595),
        "the clamped provider delay (600 s) must still win over backoff, got {due_in}"
    );
    assert!(
        due_in <= chrono::Duration::seconds(605),
        "due_at must clamp to MAX_RETRY_AFTER ({} s), got {due_in}",
        MAX_RETRY_AFTER.as_secs()
    );

    // And the job is claimable again once the clamped delay is over.
    sqlx::query("UPDATE delivery_jobs SET due_at = now() WHERE id = $1")
        .bind(job_id)
        .execute(&pool)
        .await
        .expect("make due");
    let (good_fake, good) = Scripted::new("sms").into_channel();
    let sent = work_once_bounded(&pool, &good, &cfg).await;
    assert_eq!(sent.sent, 1, "{sent:?}");
    assert_eq!(good_fake.sent(), vec![job_id]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn dead_job_creates_exactly_one_fallback_job(pool: PgPool) {
    let cfg = m4_cfg("w-fb");
    let alert = insert_alert(&pool, "open", &format!("fallback m4 {}", unique_tag())).await;
    let sms_id = insert_sms_job(
        &pool,
        alert,
        "bob",
        "+44111",
        fallback_route("email", "bob-fallback@example.test"),
    )
    .await;

    let (fake, channel) = Scripted::new("sms")
        .always_failing(SendError::Permanent("sms 550 invalid number".into()))
        .into_channel();
    let stats = work_once_bounded(&pool, &channel, &cfg).await;
    assert_eq!(stats.dead, 1, "{stats:?}");
    assert_eq!(stats.fallbacks, 1, "{stats:?}");
    assert_eq!(fake.calls(), 1);

    let sms = job_row(&pool, sms_id).await;
    assert_eq!(sms.status, "dead");
    assert_eq!(sms.last_error_class.as_deref(), Some("permanent"));

    // Exactly one fallback job, on the routed channel, for the same notification.
    let fallbacks: Vec<DeliveryJob> =
        sqlx::query_as("SELECT * FROM delivery_jobs WHERE fallback_of = $1")
            .bind(sms_id)
            .fetch_all(&pool)
            .await
            .expect("fallback rows");
    assert_eq!(fallbacks.len(), 1, "exactly one fallback: {fallbacks:?}");
    let fb = &fallbacks[0];
    assert_eq!(fb.alert_id, alert);
    assert_eq!(fb.notify_seq, 1);
    assert_eq!(fb.recipient_id, "bob");
    assert_eq!(fb.channel, "email");
    assert_eq!(fb.address, "bob-fallback@example.test");
    assert_eq!(fb.status, "pending");
    assert_eq!(fb.attempts, 0);
    assert_eq!(fb.fallback_of, Some(sms_id));
    // Provenance chain (SPEC §5.6).
    assert_eq!(fb.route_info["target"], "oncall:settlements-primary");
    assert_eq!(fb.route_info["rule_id"], "settlements-workflow-failed");
    assert_eq!(fb.route_info["reason"], "new");
    assert_eq!(fb.route_info["fallback_from"], "sms");
    assert_eq!(
        fb.route_info["fallback_reason"],
        json!("sms 550 invalid number"),
        "the fallback records why the original channel died"
    );
    assert!(
        fb.route_info.get("fallback").is_none(),
        "the fallback job must never fall back again"
    );
    assert_eq!(
        job_rows(&pool, alert).await.len(),
        2,
        "one sms job + one fallback job, nothing else"
    );

    // An email worker delivers the fallback job.
    let (efake, echan) = Scripted::new("email").into_channel();
    let estats = work_once_bounded(&pool, &echan, &cfg).await;
    assert_eq!(estats.sent, 1, "{estats:?}");
    let fb_row = job_row(&pool, fb.id).await;
    assert_eq!(fb_row.status, "sent");
    assert!(fb_row.sent_at.is_some());
    assert_eq!(efake.sent(), vec![fb.id]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn fallback_job_that_dies_creates_no_second_hop(pool: PgPool) {
    let cfg = m4_cfg("w-fb2");
    let alert = insert_alert(&pool, "open", &format!("fallback hop m4 {}", unique_tag())).await;
    let sms_id = insert_sms_job(
        &pool,
        alert,
        "bob",
        "+44111",
        fallback_route("email", "bob-fallback@example.test"),
    )
    .await;

    // The sms job dies permanently -> one email fallback.
    let (_, sms_chan) = Scripted::new("sms")
        .always_failing(SendError::Permanent("sms 550 invalid number".into()))
        .into_channel();
    let stats = work_once_bounded(&pool, &sms_chan, &cfg).await;
    assert_eq!(stats.fallbacks, 1, "{stats:?}");
    let fb_id: Uuid = sqlx::query_scalar("SELECT id FROM delivery_jobs WHERE fallback_of = $1")
        .bind(sms_id)
        .fetch_one(&pool)
        .await
        .expect("fallback id");

    // The fallback email job dies permanently too: NO further job (one hop).
    let (_, echan) = Scripted::new("email")
        .always_failing(SendError::Permanent("smtp 550 no such user".into()))
        .into_channel();
    let stats = work_once_bounded(&pool, &echan, &cfg).await;
    assert_eq!(stats.dead, 1, "{stats:?}");
    assert_eq!(stats.fallbacks, 0, "a fallback never falls back: {stats:?}");
    assert_eq!(
        job_rows(&pool, alert).await.len(),
        2,
        "still exactly two jobs"
    );
    assert_eq!(job_row(&pool, fb_id).await.status, "dead");

    // And a dead job without a `fallback` key creates nothing at all.
    let alert2 = insert_alert(&pool, "open", &format!("no fallback m4 {}", unique_tag())).await;
    let plain = insert_sms_job(&pool, alert2, "bob", "+44222", json!({})).await;
    let stats = work_once_bounded(&pool, &sms_chan, &cfg).await;
    assert_eq!(stats.dead, 1, "{stats:?}");
    assert_eq!(stats.fallbacks, 0, "{stats:?}");
    assert_eq!(job_rows(&pool, alert2).await.len(), 1);
    assert_eq!(job_row(&pool, plain).await.status, "dead");
}

#[sqlx::test(migrations = "../../migrations")]
async fn existing_job_on_the_fallback_channel_prevents_a_duplicate(pool: PgPool) {
    let cfg = m4_cfg("w-fb3");
    let alert = insert_alert(&pool, "open", &format!("fallback dup m4 {}", unique_tag())).await;
    let sms_id = insert_sms_job(
        &pool,
        alert,
        "bob",
        "+44111",
        fallback_route("email", "bob-fallback@example.test"),
    )
    .await;
    // The person already has an email job for this notification.
    let existing = insert_job_on(
        &pool,
        alert,
        1,
        "bob",
        "email",
        "bob@example.test",
        json!({"target": "oncall:settlements-primary"}),
    )
    .await;

    let (_, sms_chan) = Scripted::new("sms")
        .always_failing(SendError::Permanent("sms 550 invalid number".into()))
        .into_channel();
    let stats = work_once_bounded(&pool, &sms_chan, &cfg).await;
    assert_eq!(stats.dead, 1, "{stats:?}");
    assert_eq!(
        stats.fallbacks, 0,
        "the idempotency conflict skips the insert: {stats:?}"
    );

    let jobs = job_rows(&pool, alert).await;
    assert_eq!(jobs.len(), 2, "no duplicate email job: {jobs:?}");
    let email: Vec<&DeliveryJob> = jobs.iter().filter(|j| j.channel == "email").collect();
    assert_eq!(email.len(), 1);
    assert_eq!(email[0].id, existing, "the pre-existing job is untouched");
    assert_eq!(email[0].fallback_of, None);
    assert_eq!(email[0].route_info["target"], "oncall:settlements-primary");
    let sms = job_row(&pool, sms_id).await;
    assert_eq!(sms.status, "dead");
}

#[sqlx::test(migrations = "../../migrations")]
async fn dead_on_claim_creates_the_fallback(pool: PgPool) {
    let cfg = m4_cfg("w-doc");
    let alert = insert_alert(&pool, "open", &format!("dead on claim m4 {}", unique_tag())).await;
    // An in_flight sms job whose lease expired with no attempts left, with a fallback route.
    let dead_id: Uuid = sqlx::query_scalar(
        "INSERT INTO delivery_jobs \
           (alert_id, notify_seq, recipient_id, channel, address, route_info, status, \
            attempts, max_attempts, locked_by, locked_until, lease_token, updated_at) \
         VALUES ($1, 1, 'bob', 'sms', '+44111', $2, 'in_flight', 3, 3, 'worker-x', \
                 now() - interval '1 second', gen_random_uuid(), now() - interval '1 hour') \
         RETURNING id",
    )
    .bind(alert)
    .bind(fallback_route("email", "bob-fallback@example.test"))
    .fetch_one(&pool)
    .await
    .expect("insert exhausted job");

    let claimed = claim_batch(&pool, "sms", &cfg).await.expect("claim");
    assert_eq!(claimed.dead_on_claim, 1, "{claimed:?}");
    assert_eq!(
        claimed.fallbacks, 1,
        "the dead-on-claim path also falls back: {claimed:?}"
    );
    assert!(claimed.jobs.is_empty());

    let dead = job_row(&pool, dead_id).await;
    assert_eq!(dead.status, "dead");
    assert_eq!(dead.attempts, 3, "attempts unchanged by dead-lettering");

    let fallbacks: Vec<DeliveryJob> =
        sqlx::query_as("SELECT * FROM delivery_jobs WHERE fallback_of = $1")
            .bind(dead_id)
            .fetch_all(&pool)
            .await
            .expect("fallback rows");
    assert_eq!(fallbacks.len(), 1, "{fallbacks:?}");
    let fb = &fallbacks[0];
    assert_eq!(fb.channel, "email");
    assert_eq!(fb.alert_id, alert);
    assert_eq!(fb.recipient_id, "bob");
    assert_eq!(
        fb.route_info["fallback_reason"],
        json!("max attempts exceeded after lease expiry")
    );
    assert_eq!(fb.route_info["fallback_from"], "sms");
}

// ---------------------------------------------------------------------------------------------
// 5. Outage scenarios against the real fake gateway (SPEC §9 scenario c)
// ---------------------------------------------------------------------------------------------

/// Run the outage scenario: 50 alerts with one sms job each, an outage of `secs` seconds,
/// 4 concurrent worker loops, then reconciliation against `gateway_receipts`.
async fn run_outage_scenario(pool: PgPool, mode: OutageMode, secs: u64) {
    const N: usize = 50;
    const OUTAGE_JOBS: usize = N;
    let pool = big_pool(&pool, 16).await;

    // The fake gateway on an ephemeral port, sharing the test database.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let state = GatewayState::new(pool.clone());
    let srv_state = state.clone();
    let bound: Arc<OnceLock<SocketAddr>> = Arc::new(OnceLock::new());
    let bound_setter = bound.clone();
    let srv = tokio::spawn(async move {
        let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr");
        gateway::serve(srv_state, addr, shutdown_rx, move |a| {
            let _ = bound_setter.set(a);
        })
        .await
    });
    let addr = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(a) = bound.get() {
                return *a;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("gateway did not bind");

    let channel: Arc<dyn Channel> = Arc::new(
        GatewayChannel::new(
            GatewayKind::Sms,
            GatewayClientConfig {
                base_url: format!("http://{addr}"),
                public_url: "http://localhost:5173".into(),
                timeout: Duration::from_secs(5),
                token: None,
            },
        )
        .expect("build gateway channel"),
    );

    // 50 open alerts, one sms job each, unique addresses.
    let mut job_ids = Vec::with_capacity(OUTAGE_JOBS);
    for i in 0..OUTAGE_JOBS {
        let alert = insert_alert(&pool, "open", &format!("outage {i} {}", unique_tag())).await;
        job_ids
            .push(insert_sms_job(&pool, alert, "bob", &format!("+4477009{i:04}"), json!({})).await);
    }

    let cfg = WorkerConfig {
        worker_id: "w-outage".into(),
        wake: beacon_delivery::wake::Wake::never(),
        poll_interval: Duration::from_millis(50),
        batch_size: 50,
        lease: Duration::from_secs(5),
        concurrency: 8,
        backoff: BackoffPolicy {
            base: Duration::from_millis(100),
            cap: Duration::from_secs(1),
        },
        breaker: BreakerPolicy {
            threshold: 3,
            cooldown: Duration::from_millis(300),
            probe_lease: Duration::from_secs(1),
        },
    };

    state.set_outage("sms", mode, secs);
    let t0 = Instant::now();

    // 4 concurrent worker loops until no pending/in_flight sms jobs remain.
    let mut handles = Vec::new();
    for w in 0..4 {
        let (pool, channel, cfg) = (pool.clone(), channel.clone(), cfg.clone());
        handles.push(tokio::spawn(async move {
            let cfg = WorkerConfig {
                worker_id: format!("w-outage-{w}"),
                ..cfg
            };
            loop {
                let stats = worker::work_once(&pool, &channel, &cfg)
                    .await
                    .expect("work_once");
                if stats.claimed == 0 {
                    if sms_open_count(&pool).await == 0 {
                        break;
                    }
                    tokio::time::sleep(cfg.poll_interval).await;
                }
            }
        }));
    }
    let drained = tokio::time::timeout(Duration::from_secs(60), async {
        for h in handles {
            h.await.expect("worker task panicked");
        }
    })
    .await;
    assert!(
        drained.is_ok(),
        "workers did not drain the outage within 60 s"
    );
    let elapsed = t0.elapsed();
    let drain_after_outage = elapsed.saturating_sub(Duration::from_secs(secs));

    // Stop the gateway.
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), srv).await;

    // Every job sent, none dead, no fallbacks.
    let jobs: Vec<DeliveryJob> = sqlx::query_as("SELECT * FROM delivery_jobs WHERE id = ANY($1)")
        .bind(&job_ids)
        .fetch_all(&pool)
        .await
        .expect("job rows");
    assert_eq!(jobs.len(), OUTAGE_JOBS);
    let not_sent: Vec<_> = jobs.iter().filter(|j| j.status != "sent").collect();
    assert!(
        not_sent.is_empty(),
        "{} jobs not sent, first: {:?}",
        not_sent.len(),
        not_sent.first()
    );
    let dead: Vec<_> = jobs.iter().filter(|j| j.status == "dead").collect();
    assert!(dead.is_empty(), "0 lost: {dead:?}");
    let max_attempts = jobs.iter().map(|j| j.attempts).max().unwrap_or(0);
    assert!(
        max_attempts <= 4,
        "attempts must stay <= threshold + 1, got {max_attempts}"
    );

    let fallbacks: i64 =
        sqlx::query_scalar("SELECT count(*) FROM delivery_jobs WHERE fallback_of = ANY($1)")
            .bind(&job_ids)
            .fetch_one(&pool)
            .await
            .expect("fallback count");
    assert_eq!(fallbacks, 0, "a pure outage must not kill any job");

    // Reconciliation against the gateway's own receipts.
    let (receipt_rows, distinct_receipts, duplicates): (i64, i64, i64) = sqlx::query_as(
        "SELECT count(*), count(DISTINCT job_id), COALESCE(SUM(duplicates), 0) \
         FROM gateway_receipts WHERE job_id = ANY($1)",
    )
    .bind(&job_ids)
    .fetch_one(&pool)
    .await
    .expect("receipt stats");
    assert_eq!(
        distinct_receipts as usize, OUTAGE_JOBS,
        "every expected sms job reconciled against a persisted receipt"
    );
    let receipt_jobs: HashSet<Uuid> =
        sqlx::query_scalar("SELECT DISTINCT job_id FROM gateway_receipts WHERE job_id = ANY($1)")
            .bind(&job_ids)
            .fetch_all(&pool)
            .await
            .expect("receipt job ids")
            .into_iter()
            .collect();
    assert_eq!(
        receipt_jobs,
        job_ids.iter().copied().collect::<HashSet<_>>()
    );

    println!(
        "outage {mode:?} for {secs} s: drained in {elapsed:.2?} \
         (~{drain_after_outage:.2?} after the outage ended); \
         receipts={receipt_rows} distinct={distinct_receipts} duplicates={duplicates} \
         max_attempts={max_attempts}"
    );

    if mode == OutageMode::Down {
        assert_eq!(
            receipt_rows as usize, OUTAGE_JOBS,
            "one receipt row per job"
        );
        assert_eq!(
            duplicates, 0,
            "nothing is accepted while the channel is down"
        );
    } else {
        // lose_response: the gateway persisted before dropping the response, so the
        // at-least-once retry visibly duplicates — exactly one receipt row per job.
        assert_eq!(
            receipt_rows as usize, OUTAGE_JOBS,
            "one receipt row per job"
        );
        assert!(
            duplicates >= 1,
            "duplicates must be counted, got {duplicates}"
        );
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn sms_outage_down_mode_loses_nothing_and_duplicates_nothing(pool: PgPool) {
    run_outage_scenario(pool, OutageMode::Down, 3).await;
}

#[sqlx::test(migrations = "../../migrations")]
async fn sms_outage_lose_response_persists_before_the_outage_reply(pool: PgPool) {
    run_outage_scenario(pool, OutageMode::LoseResponse, 2).await;
}

// ---------------------------------------------------------------------------------------------
// Review fixes: breaker re-checked before each send; an idle probe is released
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn jobs_queued_behind_a_send_slot_do_not_send_once_the_breaker_opens(pool: PgPool) {
    let cfg = WorkerConfig {
        worker_id: "w-slot".into(),
        concurrency: 1,
        breaker: BreakerPolicy {
            threshold: 1,
            cooldown: Duration::from_secs(30),
            probe_lease: Duration::from_secs(30),
        },
        ..WorkerConfig::default()
    };
    let alert = insert_alert(&pool, "open", &format!("slot {}", unique_tag())).await;
    for (who, to) in [("anna", "+44111"), ("bob", "+44222"), ("carol", "+44333")] {
        insert_sms_job(&pool, alert, who, to, json!({})).await;
    }
    let (fake, bad) = Scripted::new("sms")
        .always_failing(SendError::Transient("sms provider 503".into()))
        .into_channel();

    let stats = work_once_bounded(&pool, &bad, &cfg).await;
    assert_eq!(stats.claimed, 3, "{stats:?}");
    // The first job is a real send: it opens the breaker (threshold 1) and consumes its
    // attempt. The two jobs still waiting for the send slot never reach the provider, so
    // only they defer with a refund.
    assert_eq!(stats.retried, 1, "{stats:?}");
    assert_eq!(stats.deferred, 2, "{stats:?}");
    assert_eq!(
        fake.calls(),
        1,
        "only the send that opened the breaker goes out"
    );
    let jobs = job_rows(&pool, alert).await;
    assert!(jobs.iter().all(|j| j.status == "pending"), "{jobs:?}");
    assert_eq!(
        jobs.iter().filter(|j| j.attempts == 1).count(),
        1,
        "the real attempt is consumed: {jobs:?}"
    );
    assert_eq!(
        jobs.iter().filter(|j| j.attempts == 0).count(),
        2,
        "jobs that never sent keep their refund: {jobs:?}"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_probe_with_nothing_to_send_is_released_at_once(pool: PgPool) {
    let cfg = WorkerConfig {
        worker_id: "w-idle-probe".into(),
        breaker: BreakerPolicy {
            threshold: 1,
            cooldown: Duration::from_secs(30),
            probe_lease: Duration::from_secs(30),
        },
        ..WorkerConfig::default()
    };
    sqlx::query(
        "INSERT INTO channel_breakers (channel, state, consecutive_failures, opened_at, open_until) \
         VALUES ('sms', 'open', 1, now(), now())",
    )
    .execute(&pool)
    .await
    .expect("open breaker past its cooldown");
    let (_fake, good) = Scripted::new("sms").into_channel();

    // The probe is taken, but nothing is due.
    let idle = work_once_bounded(&pool, &good, &cfg).await;
    assert_eq!(idle.claimed, 0, "{idle:?}");

    // A job arriving right after is probed at once, not after the 30 s probe lease.
    let alert = insert_alert(&pool, "open", &format!("idle probe {}", unique_tag())).await;
    insert_sms_job(&pool, alert, "anna", "+44111", json!({})).await;
    let probe = work_once_bounded(&pool, &good, &cfg).await;
    assert_eq!(probe.sent, 1, "{probe:?}");
    assert_eq!(
        breaker::status(&pool, "sms").await.expect("status").0,
        "closed"
    );
}

// ---------------------------------------------------------------------------------------------
// 6. Retry horizon (SPEC §9c) and exhaustion without refunds
// ---------------------------------------------------------------------------------------------

/// SPEC §9c needs the retry horizon to stay well above the 60 s outage, so a 60 s outage
/// drains with 0 dead. This pins the arithmetic from first principles (no 60 s sleep):
/// probes are spaced at least `cooldown` apart, every real probe consumes one attempt, and
/// the failed probe's `due_at` push spreads probes across jobs via the claim's `due_at`
/// ordering, so a batch of `N` jobs survives at least `N * max_attempts * cooldown`.
#[test]
fn retry_horizon_stays_above_sixty_seconds() {
    // Production defaults: backoff base 1 s / cap 60 s, breaker cooldown 5 s, DB
    // `delivery_jobs.max_attempts` default 8 (see migration
    // `20261120203000_delivery_create_delivery_jobs.sql`).
    let backoff = BackoffPolicy::default();
    let breaker = BreakerPolicy::default();
    assert_eq!(backoff.base, Duration::from_secs(1));
    assert_eq!(backoff.cap, Duration::from_secs(60));
    assert_eq!(breaker.cooldown, Duration::from_secs(5));
    let max_attempts: i32 = 8;

    // Single job, expected (mean) horizon: full-jitter uniform in `[0, upper]` has mean
    // `upper/2` (the `base/2` floor only matters for the first attempt), floored by the
    // cooldown each attempt. Uppers for attempts 1..=7 are 1,2,4,8,16,32,60 (capped).
    let mut expected_secs = 0.0;
    for k in 1..max_attempts {
        let upper = (backoff.base.as_secs_f64() * 2f64.powi(k - 1)).min(backoff.cap.as_secs_f64());
        expected_secs += (upper / 2.0).max(breaker.cooldown.as_secs_f64());
    }
    // 5+5+5+5+8+16+30 = 74 s.
    assert!(
        expected_secs > 60.0,
        "single-job expected horizon must exceed the 60 s outage, got {expected_secs:.1} s"
    );

    // The SPEC §9c batch spreads probes across jobs: 50 jobs * 8 attempts, one probe per
    // cooldown window minimum.
    let batch_min_secs = 50.0 * max_attempts as f64 * breaker.cooldown.as_secs_f64();
    assert!(
        batch_min_secs > 60.0,
        "50-job batch minimum horizon must exceed 60 s, got {batch_min_secs:.1} s"
    );

    // The faster policy used by the outage tests below (base 100 ms / cap 1 s, cooldown
    // 300 ms): same batch argument gives 50 * 8 * 0.3 = 120 s.
    let test_batch_min_secs = 50.0 * 8.0 * 0.3;
    assert!(
        test_batch_min_secs > 60.0,
        "test-policy batch minimum horizon must exceed 60 s, got {test_batch_min_secs:.1} s"
    );
}

/// A horizon-long outage does kill: with a short policy and an always-failing channel, a job
/// consumes one attempt per probe (no refunds for real sends) and eventually reaches `dead`
/// with exactly one one-hop fallback job.
#[sqlx::test(migrations = "../../migrations")]
async fn sustained_outage_exhausts_attempts_and_creates_one_fallback(pool: PgPool) {
    let cfg = WorkerConfig {
        worker_id: "w-exhaust".into(),
        breaker: BreakerPolicy {
            threshold: 2,
            cooldown: Duration::from_millis(50),
            probe_lease: Duration::from_secs(1),
        },
        backoff: BackoffPolicy {
            base: Duration::from_millis(10),
            cap: Duration::from_millis(50),
        },
        ..WorkerConfig::default()
    };
    let alert = insert_alert(&pool, "open", &format!("exhaust m4 {}", unique_tag())).await;
    let sms_id = insert_sms_job(
        &pool,
        alert,
        "bob",
        "+44111",
        fallback_route("email", "bob-fallback@example.test"),
    )
    .await;
    // Small budget so the test exhausts it in a handful of iterations.
    sqlx::query("UPDATE delivery_jobs SET max_attempts = 5 WHERE id = $1")
        .bind(sms_id)
        .execute(&pool)
        .await
        .expect("set max_attempts");

    let (fake, channel) = Scripted::new("sms")
        .always_failing(SendError::Transient("sms provider 503".into()))
        .into_channel();

    // Drive the worker, fast-forwarding past every cooldown and retry delay (as if the outage
    // lasted longer than the horizon): each pass is one real attempt.
    let mut total = WorkStats::default();
    let mut died = false;
    for _ in 0..20 {
        sqlx::query("UPDATE delivery_jobs SET due_at = now() WHERE id = $1")
            .bind(sms_id)
            .execute(&pool)
            .await
            .expect("make due");
        sqlx::query(
            "UPDATE channel_breakers SET open_until = now() - interval '1 second' \
             WHERE channel = 'sms'",
        )
        .execute(&pool)
        .await
        .expect("cooldown over");
        let stats = work_once_bounded(&pool, &channel, &cfg).await;
        add_stats(&mut total, &stats);
        if stats.dead == 1 {
            died = true;
            break;
        }
    }
    assert!(died, "an always-failing job must eventually die: {total:?}");
    assert_eq!(
        fake.calls(),
        5,
        "one send per attempt, no phantom retries: {total:?}"
    );
    assert_eq!(
        total.deferred, 0,
        "real attempts are never refunded: {total:?}"
    );

    let sms = job_row(&pool, sms_id).await;
    assert_eq!(sms.status, "dead");
    assert_eq!(
        sms.attempts, 5,
        "every real attempt consumed its attempt: {sms:?}"
    );
    assert_eq!(sms.last_error_class.as_deref(), Some("transient"));

    let fallbacks: Vec<DeliveryJob> =
        sqlx::query_as("SELECT * FROM delivery_jobs WHERE fallback_of = $1")
            .bind(sms_id)
            .fetch_all(&pool)
            .await
            .expect("fallback rows");
    assert_eq!(fallbacks.len(), 1, "exactly one fallback: {fallbacks:?}");
    assert_eq!(fallbacks[0].channel, "email");
    assert_eq!(fallbacks[0].address, "bob-fallback@example.test");
    assert_eq!(fallbacks[0].status, "pending");
}

/// Round-2 review (board #439): with the breaker open, the failure before a job's FINAL attempt
/// waits at least `max(cooldown, backoff cap)`, independent of jitter, so one job cannot burn
/// every attempt inside a 60 s outage. Earlier failures keep the normal floor.
#[sqlx::test(migrations = "../../migrations")]
async fn penultimate_failure_while_open_waits_at_least_the_backoff_cap(pool: PgPool) {
    let policy = BackoffPolicy::default(); // base 1 s, cap 60 s
    let cooldown = BreakerPolicy::default().cooldown; // 5 s
    let cfg = WorkerConfig {
        worker_id: "w-floor".into(),
        ..WorkerConfig::default()
    };
    let alert = insert_alert(&pool, "open", &format!("floor m4 {}", unique_tag())).await;
    let err = SendError::Transient("sms provider 503".into());

    // (attempts before the claim, expected minimum delay, expected maximum delay)
    for (before_claim, min_s, max_s) in [(6, 60.0, 61.0), (2, 5.0, 8.5)] {
        let id = insert_sms_job(
            &pool,
            alert,
            &format!("p{before_claim}"),
            "+44111",
            json!({}),
        )
        .await;
        sqlx::query("UPDATE delivery_jobs SET attempts = $2, max_attempts = 8 WHERE id = $1")
            .bind(id)
            .bind(before_claim)
            .execute(&pool)
            .await
            .expect("set attempts");
        let claimed = claim_batch(&pool, "sms", &cfg).await.expect("claim");
        let job = claimed
            .jobs
            .iter()
            .find(|j| j.id == id)
            .expect("claimed")
            .clone();
        assert_eq!(job.attempts, before_claim + 1);
        let token = job.lease_token.expect("lease token");
        let status =
            worker::finalize_failed_while_open(&pool, &job, token, &err, &policy, cooldown)
                .await
                .expect("finalize");
        assert_eq!(status, Some(beacon_delivery::model::JobStatus::Pending));
        let secs: f64 = sqlx::query_scalar(
            "SELECT extract(epoch from due_at - updated_at)::float8 FROM delivery_jobs WHERE id = $1",
        )
        .bind(id)
        .fetch_one(&pool)
        .await
        .expect("due_at");
        assert!(
            secs >= min_s - 0.01 && secs <= max_s,
            "attempt {} of 8: due_at - updated_at = {secs:.2}s, want [{min_s}, {max_s}]",
            job.attempts
        );
    }
}
