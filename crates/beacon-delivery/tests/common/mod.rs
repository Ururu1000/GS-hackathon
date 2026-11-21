//! Shared helpers for the delivery integration suite.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beacon_delivery::WorkerConfig;
use beacon_delivery::channel::email::{EmailChannel, SmtpConfig};
use beacon_delivery::channel::{BoxFuture, Channel, OutboundMessage, Receipt, SendError};
use beacon_delivery::model::DeliveryJob;
use beacon_delivery::worker::{self, WorkStats};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

// ---------------------------------------------------------------------------------------------
// Database helpers
// ---------------------------------------------------------------------------------------------

/// Insert an open-able alert row with every NOT NULL column of the core alerts table. `status` is `open | acked | resolved`.
pub async fn insert_alert(pool: &PgPool, status: &str, title: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO alerts (id, rule_id, rule_revision, rule_name, team, fingerprint, \
                             dedup_window_ms, status, severity, color, title, body, \
                             first_seen, last_seen, source, schema, notify_seq) \
         VALUES ($1, 'rates-rejected-trades', 1, 'Rates rejected trades', 'rates', $2, \
                 600000, $3, 'warning', 'amber', $4, 'Reason: LIMIT_BREACH', \
                 now(), now(), 'test', 'trade.rejected.v1', 1)",
    )
    .bind(id)
    .bind(format!("fp-{id}"))
    .bind(status)
    .bind(title)
    .execute(pool)
    .await
    .expect("insert alert");
    id
}

/// Insert an outbox `notify` row whose payload is a valid `NotifyPayload`. Returns the outbox id.
pub async fn insert_notify(
    pool: &PgPool,
    alert_id: Uuid,
    notify_seq: i32,
    route_json: Value,
) -> i64 {
    let payload = json!({
        "alert_id": alert_id,
        "notify_seq": notify_seq,
        "rule_id": "rates-rejected-trades",
        "rule_revision": 1,
        "team": "rates",
        "severity": "warning",
        "reason": "new",
        "route": route_json,
        "created_at": chrono::Utc::now(),
    });
    insert_outbox_raw(pool, "notify", alert_id, payload).await
}

/// Insert an arbitrary outbox row (used for malformed payloads).
pub async fn insert_outbox_raw(pool: &PgPool, kind: &str, alert_id: Uuid, payload: Value) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO outbox (kind, alert_id, payload) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(kind)
    .bind(alert_id)
    .bind(payload)
    .fetch_one(pool)
    .await
    .expect("insert outbox row")
}

/// `outbox.processed_at IS NOT NULL` for one row.
pub async fn outbox_processed(pool: &PgPool, outbox_id: i64) -> bool {
    sqlx::query_scalar("SELECT processed_at IS NOT NULL FROM outbox WHERE id = $1")
        .bind(outbox_id)
        .fetch_one(pool)
        .await
        .expect("read outbox row")
}

pub async fn update_alert_status(pool: &PgPool, alert_id: Uuid, status: &str) {
    sqlx::query("UPDATE alerts SET status = $2 WHERE id = $1")
        .bind(alert_id)
        .bind(status)
        .execute(pool)
        .await
        .expect("update alert status");
}

/// Insert one pending email job (notify_seq 1, max_attempts 8). Returns the job id.
pub async fn insert_job(pool: &PgPool, alert_id: Uuid, recipient: &str, address: &str) -> Uuid {
    insert_job_max(pool, alert_id, recipient, address, 8).await
}

pub async fn insert_job_max(
    pool: &PgPool,
    alert_id: Uuid,
    recipient: &str,
    address: &str,
    max_attempts: i32,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO delivery_jobs (alert_id, notify_seq, recipient_id, channel, address, max_attempts) \
         VALUES ($1, 1, $2, 'email', $3, $4) RETURNING id",
    )
    .bind(alert_id)
    .bind(recipient)
    .bind(address)
    .bind(max_attempts)
    .fetch_one(pool)
    .await
    .expect("insert job")
}

/// All jobs of an alert, ordered by recipient then channel.
pub async fn job_rows(pool: &PgPool, alert_id: Uuid) -> Vec<DeliveryJob> {
    sqlx::query_as::<_, DeliveryJob>(
        "SELECT * FROM delivery_jobs WHERE alert_id = $1 ORDER BY recipient_id, channel",
    )
    .bind(alert_id)
    .fetch_all(pool)
    .await
    .expect("read jobs")
}

pub async fn job_row(pool: &PgPool, job_id: Uuid) -> DeliveryJob {
    sqlx::query_as::<_, DeliveryJob>("SELECT * FROM delivery_jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(pool)
        .await
        .expect("read job")
}

/// Jobs still pending or in flight (all alerts).
pub async fn open_job_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM delivery_jobs WHERE status IN ('pending', 'in_flight')",
    )
    .fetch_one(pool)
    .await
    .expect("count open jobs")
}

// ---------------------------------------------------------------------------------------------
// Worker helpers
// ---------------------------------------------------------------------------------------------

pub fn worker_cfg(worker_id: &str) -> WorkerConfig {
    WorkerConfig {
        worker_id: worker_id.to_string(),
        ..WorkerConfig::default()
    }
}

/// `work_once` bounded by a timeout.
pub async fn work_once_bounded(
    pool: &PgPool,
    channel: &Arc<dyn Channel>,
    cfg: &WorkerConfig,
) -> WorkStats {
    tokio::time::timeout(
        Duration::from_secs(60),
        worker::work_once(pool, channel, cfg),
    )
    .await
    .expect("work_once timed out")
    .expect("work_once db error")
}

// ---------------------------------------------------------------------------------------------
// FakeChannel
// ---------------------------------------------------------------------------------------------

/// In-memory channel named `email`. Records the job id of every *successful* send.
pub struct FakeChannel {
    sent: Mutex<Vec<Uuid>>,
    failures: Mutex<VecDeque<SendError>>,
    always_fail: Mutex<Option<SendError>>,
    delay: Duration,
    calls: AtomicUsize,
}

impl FakeChannel {
    pub fn new() -> Self {
        Self {
            sent: Mutex::new(Vec::new()),
            failures: Mutex::new(VecDeque::new()),
            always_fail: Mutex::new(None),
            delay: Duration::ZERO,
            calls: AtomicUsize::new(0),
        }
    }

    /// Fail the next `times` sends with `err`, then succeed.
    pub fn failing(self, err: SendError, times: usize) -> Self {
        *self.failures.lock().unwrap() = std::iter::repeat_n(err, times).collect();
        self
    }

    /// Fail every send with `err`.
    pub fn always_failing(self, err: SendError) -> Self {
        *self.always_fail.lock().unwrap() = Some(err);
        self
    }

    /// Sleep this long inside every `send`.
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    pub fn into_channel(self) -> (Arc<FakeChannel>, Arc<dyn Channel>) {
        let fake = Arc::new(self);
        let dynamic: Arc<dyn Channel> = fake.clone();
        (fake, dynamic)
    }

    /// Job ids of successful sends, in order.
    pub fn sent(&self) -> Vec<Uuid> {
        self.sent.lock().unwrap().clone()
    }

    /// Number of `send` calls, successful or not.
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl Default for FakeChannel {
    fn default() -> Self {
        Self::new()
    }
}

impl Channel for FakeChannel {
    fn name(&self) -> &'static str {
        "email"
    }

    fn send<'a>(&'a self, msg: &'a OutboundMessage) -> BoxFuture<'a, Result<Receipt, SendError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            if let Some(err) = self.always_fail.lock().unwrap().clone() {
                return Err(err);
            }
            if let Some(err) = self.failures.lock().unwrap().pop_front() {
                return Err(err);
            }
            self.sent.lock().unwrap().push(msg.job_id);
            Ok(Receipt {
                provider_message_id: format!("<{}@fake>", msg.job_id),
                detail: json!({ "fake": true }),
            })
        })
    }
}

// ---------------------------------------------------------------------------------------------
// SMTP / Mailpit helpers
// ---------------------------------------------------------------------------------------------

pub fn smtp_host() -> String {
    std::env::var("SMTP_HOST").unwrap_or_else(|_| "localhost".into())
}

pub fn smtp_port() -> u16 {
    std::env::var("SMTP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(1025)
}

/// The real `EmailChannel` pointed at `SMTP_HOST:SMTP_PORT` (default localhost:1025).
pub fn email_channel() -> Arc<dyn Channel> {
    Arc::new(
        EmailChannel::new(SmtpConfig {
            host: smtp_host(),
            port: smtp_port(),
            // Tests share one Mailpit with the 1,000-email load test running in parallel; under
            // that flood a single SMTP exchange was observed to take > 6 s. The 5 s production
            // default would then (correctly) record an ambiguous timeout and retry, which makes
            // the single-send assertions flaky.
            timeout: Duration::from_secs(30),
            ..SmtpConfig::default()
        })
        .expect("build EmailChannel"),
    )
}

/// An `EmailChannel` pointed at a local port nothing listens on (connection refused).
pub fn dead_email_channel() -> Arc<dyn Channel> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    Arc::new(
        EmailChannel::new(SmtpConfig {
            host: "127.0.0.1".into(),
            port,
            timeout: Duration::from_secs(2),
            ..SmtpConfig::default()
        })
        .expect("build EmailChannel"),
    )
}

/// A unique alphanumeric token to put in alert titles; searched with `subject:<tag>`.
pub fn unique_tag() -> String {
    format!("TAG{}", &Uuid::new_v4().simple().to_string()[..16])
}

pub fn mailpit_url() -> String {
    std::env::var("MAILPIT_URL").unwrap_or_else(|_| "http://localhost:8025".into())
}

#[derive(Debug, Clone)]
pub struct MailMsg {
    /// Message-ID with angle brackets stripped, e.g. `<uuid>@beacon` -> `uuid@beacon`.
    pub message_id: String,
    pub subject: String,
}

impl MailMsg {
    /// The job id encoded in a `<{job_id}@beacon>` Message-ID.
    pub fn job_id(&self) -> Option<Uuid> {
        self.message_id
            .split('@')
            .next()
            .and_then(|s| Uuid::parse_str(s).ok())
    }
}

#[derive(Debug, Clone)]
pub struct MailSearch {
    /// Mailpit's own `messages_count` for the query.
    pub messages_count: usize,
    pub messages: Vec<MailMsg>,
}

/// One search: all messages whose subject contains `tag`.
pub async fn mailpit_search(tag: &str) -> MailSearch {
    let resp = reqwest::Client::new()
        .get(format!("{}/api/v1/search", mailpit_url()))
        .query(&[
            ("query", format!("subject:{tag}")),
            ("limit", "5000".to_string()),
        ])
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .unwrap_or_else(|e| panic!("Mailpit unreachable at {}: {e}", mailpit_url()))
        .error_for_status()
        .expect("Mailpit search status");
    let body: Value = resp.json().await.expect("Mailpit search JSON");
    let messages = body["messages"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|m| MailMsg {
                    message_id: m["MessageID"]
                        .as_str()
                        .unwrap_or_default()
                        .trim_matches(|c| c == '<' || c == '>')
                        .to_string(),
                    subject: m["Subject"].as_str().unwrap_or_default().to_string(),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let messages_count = body["messages_count"]
        .as_u64()
        .map(|n| n as usize)
        .unwrap_or(messages.len());
    MailSearch {
        messages_count,
        messages,
    }
}

/// Poll Mailpit until at least `at_least` messages carry `tag` (or `within` elapses; the last
/// result is returned either way so the caller's assertion shows what was found).
pub async fn wait_for_messages(tag: &str, at_least: usize, within: Duration) -> MailSearch {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let found = mailpit_search(tag).await;
        if found.messages_count >= at_least || tokio::time::Instant::now() >= deadline {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Messages with `tag` whose Message-ID is `<{job_id}@beacon>`.
pub fn messages_for_job(found: &MailSearch, job_id: Uuid) -> Vec<&MailMsg> {
    found
        .messages
        .iter()
        .filter(|m| m.job_id() == Some(job_id))
        .collect()
}
