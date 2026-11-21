//! Delivery for Beacon (team split part (b)), M0 scope.
//!
//! Pipeline (docs/SPEC.md §2, board #27/#35):
//!
//! ```text
//! aggregator (a) ── one PG tx ──► alerts + outbox(kind='notify', payload = NotifyPayload)
//!                                        │
//!   router::route_once     claims outbox rows (FOR UPDATE SKIP LOCKED), expands the route
//!                          via config::Directory, inserts delivery_jobs ON CONFLICT DO NOTHING
//!                          and marks the outbox row processed, all in one tx
//!                                        │
//!   worker::work_once      claims due jobs with a lease, re-checks alert status, sends outside
//!                          any tx through a channel::Channel, finalizes fenced by lease_token
//! ```
//!
//! M4 adds per-person preferences (`prefs`), SMS/phone via the fake gateway (`gateway`,
//! `channel::gateway_client`), a durable per-channel `breaker` and one-hop fallback jobs.
//! Throttles, escalation, approvals and runbooks come later.

pub mod actions;
pub mod alert_ops;
pub mod api;
pub mod approvals;
pub mod audit;
pub mod breaker;
pub mod channel;
pub mod clock;
pub mod config;
pub mod email_link;
pub mod escalation;
pub mod gateway;
pub mod model;
pub mod overrides;
pub mod prefs;
pub mod router;
pub mod rules;
pub mod runbook;
pub mod throttle;
pub mod worker;

use std::sync::Arc;
use std::time::Duration;

/// Migrations for the whole workspace live in the repo-root `migrations/` directory.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// Settings for the router loop.
#[derive(Debug, Clone)]
pub struct RouterConfig {
    /// Sleep between polls when the previous poll found nothing.
    pub poll_interval: Duration,
    /// Max outbox rows claimed per transaction.
    pub batch_size: i64,
    /// Time used for preference windows; frozen into the job's `route_info.decision`.
    pub clock: Arc<dyn clock::Clock>,
    /// Demo/test knob: replaces every rule's `escalate_after` (env `ESCALATE_AFTER_OVERRIDE_MS`).
    pub escalate_after_override: Option<Duration>,
    /// Run the throttle digest pass before each batch. With several router tasks
    /// (`ROUTER_TASKS`) only one needs it; the others would only repeat empty queries.
    pub digest: bool,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(250),
            batch_size: 100,
            clock: Arc::new(clock::SystemClock),
            escalate_after_override: None,
            digest: true,
        }
    }
}

/// Retry policy: capped, jittered exponential backoff.
#[derive(Debug, Clone)]
pub struct BackoffPolicy {
    pub base: Duration,
    pub cap: Duration,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            cap: Duration::from_secs(60),
        }
    }
}

/// Settings for one channel worker.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Written to `delivery_jobs.locked_by`.
    pub worker_id: String,
    /// Sleep between polls when the previous poll found nothing.
    pub poll_interval: Duration,
    /// Max jobs claimed per claim transaction.
    pub batch_size: i64,
    /// Lease length written to `locked_until`.
    pub lease: Duration,
    /// Max sends in flight at once for this worker.
    pub concurrency: usize,
    pub backoff: BackoffPolicy,
    pub breaker: breaker::BreakerPolicy,
    /// M7 (SPEC §6.3): signing config for the approval links email prompts carry.
    /// `None` (the default) renders the link-less "Decide in Beacon" form — the prompt
    /// email still names the request, it just cannot carry a deciding link.
    pub email_link: Option<email_link::EmailLinkConfig>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: format!("worker-{}", uuid::Uuid::new_v4()),
            poll_interval: Duration::from_millis(250),
            batch_size: 50,
            lease: Duration::from_secs(30),
            concurrency: 16,
            backoff: BackoffPolicy::default(),
            breaker: breaker::BreakerPolicy::default(),
            email_link: None,
        }
    }
}

/// Cooperative shutdown signal: loops exit when the value becomes `true`.
pub type Shutdown = tokio::sync::watch::Receiver<bool>;
