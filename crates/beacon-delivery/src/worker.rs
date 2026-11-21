//! Channel worker: lease due jobs, re-check the alert, send outside any tx, finalize by token.

use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use sqlx::PgPool;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::breaker::{self, Gate};
use crate::channel::gateway_client::MAX_RETRY_AFTER;
use crate::channel::{AlertView, Channel, Receipt, SendError};
use crate::model::{AlertStatus, DeliveryJob, JobStatus};
use crate::{BackoffPolicy, Shutdown, WorkerConfig};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WorkStats {
    pub claimed: u64,
    pub sent: u64,
    pub cancelled: u64,
    pub retried: u64,
    pub dead: u64,
    /// Expired leases with no attempts left, dead-lettered during claim.
    pub dead_on_claim: u64,
    /// Finalize found the lease_token changed (another worker took over).
    pub lost_lease: u64,
    /// Put back to `pending` with the attempt refunded because the channel breaker was open.
    pub deferred: u64,
    /// Fallback jobs inserted for jobs that died (one hop, SPEC §5.6), including
    /// dead-on-claim ones.
    pub fallbacks: u64,
}

/// Outcome of a claim: jobs this worker now holds plus how many expired leases were
/// dead-lettered instead of being reclaimed, and how many fallback jobs were inserted for
/// those dead-lettered rows (SPEC §5.6).
#[derive(Debug, Default)]
pub struct Claimed {
    pub jobs: Vec<DeliveryJob>,
    pub dead_on_claim: u64,
    pub fallbacks: u64,
}

/// One short transaction:
/// 1. `in_flight` jobs of `channel` whose lease expired AND `attempts >= max_attempts` become
///    `dead` with `last_error_class = 'ambiguous'`,
///    `last_error = 'max attempts exceeded after lease expiry'`. Each such row gets at most
///    one fallback job (SPEC §5.6) in the same transaction.
/// 2. Claim up to `cfg.batch_size` jobs that are `pending` and due, or `in_flight` with an
///    expired lease (and attempts left), `FOR UPDATE SKIP LOCKED`: set `in_flight`,
///    `locked_by`, `locked_until = now() + lease`, a fresh `lease_token`, `attempts + 1`.
///
/// Every UPDATE sets `updated_at = now()`.
pub async fn claim_batch(
    pool: &PgPool,
    channel: &str,
    cfg: &WorkerConfig,
) -> Result<Claimed, sqlx::Error> {
    let mut tx = pool.begin().await?;

    let dead_ids: Vec<(Uuid,)> = sqlx::query_as(
        "UPDATE delivery_jobs \
         SET status = 'dead', last_error_class = 'ambiguous', \
             last_error = 'max attempts exceeded after lease expiry', \
             lease_token = NULL, locked_until = NULL, locked_by = NULL, updated_at = now() \
         WHERE id IN ( \
             SELECT id FROM delivery_jobs \
             WHERE channel = $1 AND status = 'in_flight' AND locked_until < now() \
               AND attempts >= max_attempts \
             FOR UPDATE SKIP LOCKED) \
         RETURNING id",
    )
    .bind(channel)
    .fetch_all(&mut *tx)
    .await?;
    let dead_on_claim = dead_ids.len() as u64;

    let mut fallbacks = 0u64;
    for (id,) in &dead_ids {
        if insert_fallback_job(&mut tx, *id).await? {
            fallbacks += 1;
        }
    }
    if fallbacks > 0 {
        info!(
            count = fallbacks,
            "fallback jobs created for jobs dead-lettered on claim"
        );
    }

    let jobs = sqlx::query_as::<_, DeliveryJob>(
        "WITH c AS ( \
             SELECT id FROM delivery_jobs \
             WHERE channel = $1 \
               AND ((status = 'pending' AND due_at <= now()) \
                 OR (status = 'in_flight' AND locked_until < now() AND attempts < max_attempts)) \
             ORDER BY due_at \
             LIMIT $2 \
             FOR UPDATE SKIP LOCKED) \
         UPDATE delivery_jobs j \
         SET status = 'in_flight', locked_by = $3, \
             locked_until = now() + make_interval(secs => $4::float8), \
             lease_token = gen_random_uuid(), attempts = j.attempts + 1, updated_at = now() \
         FROM c WHERE j.id = c.id \
         RETURNING j.*",
    )
    .bind(channel)
    .bind(cfg.batch_size)
    .bind(&cfg.worker_id)
    .bind(cfg.lease.as_secs_f64())
    .fetch_all(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(Claimed {
        jobs,
        dead_on_claim,
        fallbacks,
    })
}

/// Insert the one-hop fallback job for the freshly dead job `dead_id` (SPEC §5.6), inside the
/// caller's transaction: the dead row must already be `dead` (with its terminal
/// `last_error`) in this transaction.
///
/// At most one row, ever:
/// - `fallback_of IS NULL` on the dead job means a fallback never falls back again (one hop);
/// - `ON CONFLICT (alert_id, recipient_id, channel, notify_seq) DO NOTHING` skips the insert
///   when the person already has any job on the fallback channel for the same notification.
///
/// The fallback job's `route_info` keeps the provenance chain: `target`, `rule_id` and
/// `reason` from the original route, plus `fallback_from` (the channel that died) and
/// `fallback_reason` (the dead job's terminal error). A dead job without a `fallback`
/// object (or with null channel/address) inserts nothing. Returns `true` when a row was
/// inserted.
async fn insert_fallback_job(
    tx: &mut sqlx::PgConnection,
    dead_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let res = sqlx::query(
        "INSERT INTO delivery_jobs \
             (alert_id, notify_seq, recipient_id, channel, address, route_info, fallback_of) \
         SELECT j.alert_id, j.notify_seq, j.recipient_id, \
                j.route_info->'fallback'->>'channel', \
                j.route_info->'fallback'->>'address', \
                jsonb_build_object('target', j.route_info->'target', \
                                    'rule_id', j.route_info->'rule_id', \
                                    'reason', j.route_info->'reason', \
                                    'fallback_from', j.channel, \
                                    'fallback_reason', j.last_error), \
                j.id \
         FROM delivery_jobs j \
         WHERE j.id = $1 AND j.status = 'dead' AND j.fallback_of IS NULL \
           AND j.route_info ? 'fallback' \
           AND j.route_info->'fallback'->>'channel' IS NOT NULL \
           AND j.route_info->'fallback'->>'address' IS NOT NULL \
         ON CONFLICT (alert_id, recipient_id, channel, notify_seq) DO NOTHING",
    )
    .bind(dead_id)
    .execute(&mut *tx)
    .await?;
    Ok(res.rows_affected() == 1)
}

/// Finalizers: each is `UPDATE ... WHERE id = $1 AND lease_token = $2 AND status = 'in_flight'`
/// and returns `false` if the lease was lost (0 rows). Each clears the lease columns and sets
/// `updated_at = now()`.
///
/// `finalize_sent` sets `sent` and `sent_at`, stores `provider_receipt =
/// {"provider_message_id": .., "detail": ..}`, and keeps any earlier `last_error` as history.
pub async fn finalize_sent(
    pool: &PgPool,
    job_id: Uuid,
    token: Uuid,
    receipt: &Receipt,
) -> Result<bool, sqlx::Error> {
    let receipt_json = serde_json::json!({
        "provider_message_id": receipt.provider_message_id,
        "detail": receipt.detail,
    });
    let res = sqlx::query(
        "UPDATE delivery_jobs \
         SET status = 'sent', sent_at = now(), provider_receipt = $3, \
             lease_token = NULL, locked_until = NULL, locked_by = NULL, updated_at = now() \
         WHERE id = $1 AND lease_token = $2 AND status = 'in_flight'",
    )
    .bind(job_id)
    .bind(token)
    .bind(receipt_json)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() == 1)
}

/// `cancelled`, `last_error = reason` (e.g. "alert acked").
pub async fn finalize_cancelled(
    pool: &PgPool,
    job_id: Uuid,
    token: Uuid,
    reason: &str,
) -> Result<bool, sqlx::Error> {
    let res = sqlx::query(
        "UPDATE delivery_jobs \
         SET status = 'cancelled', last_error = $3, \
             lease_token = NULL, locked_until = NULL, locked_by = NULL, updated_at = now() \
         WHERE id = $1 AND lease_token = $2 AND status = 'in_flight'",
    )
    .bind(job_id)
    .bind(token)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() == 1)
}

/// Permanent errors, or any error when `attempts >= max_attempts`, -> `dead`; every death
/// also inserts at most one fallback job (SPEC §5.6) in the same transaction. Otherwise ->
/// `pending` with `due_at = now() + max(backoff_delay(policy, attempts), min(retry_after,
/// MAX_RETRY_AFTER))`: the provider's requested minimum delay wins when larger, but it can
/// never exceed the 10-minute cap. Returns `Some(new_status)` or `None` if the lease was lost.
///
/// `extra_floor` is an additional minimum delay (the channel breaker's cooldown for a real
/// send that failed while the breaker is open): the retry waits out the cooldown as well as
/// its own backoff, so the next attempt cannot come back before the next probe window.
pub async fn finalize_failed(
    pool: &PgPool,
    job: &DeliveryJob,
    token: Uuid,
    err: &SendError,
    policy: &BackoffPolicy,
) -> Result<Option<crate::model::JobStatus>, sqlx::Error> {
    Ok(
        finalize_failed_inner(pool, job, token, err, policy, Duration::ZERO)
            .await?
            .map(|outcome| outcome.status),
    )
}

/// Like [`finalize_failed`], but the retry delay also respects `cooldown`:
/// `due_at = now() + max(backoff_delay(policy, attempts), cooldown,
/// min(retry_after, MAX_RETRY_AFTER))`.
///
/// Use this for a REAL send attempt that failed while the breaker is (or just became) open:
/// the attempt is consumed (`attempts` stays incremented), and the job waits out the
/// cooldown before it can be probed again. Jobs that never reached the provider (breaker
/// opened while they waited for a send slot) go through [`finalize_deferred`] instead and
/// keep their refund.
///
/// # Retry horizon (SPEC §9 scenario c)
///
/// A failed probe pushes its own job's `due_at` out by `max(backoff, cooldown, Retry-After)`,
/// while [`claim_batch`] always takes the earliest-`due_at` job first, so consecutive probes
/// round-robin across jobs instead of hammering one row. Probes are spaced at least `cooldown`
/// apart (one probe per `open_until` window), and every probe consumes one attempt, so a batch
/// of `N` jobs survives at least `N * max_attempts * cooldown` worst case before the first
/// death: the SPEC §9c batch (`N = 50`, `max_attempts = 8`, test cooldown `0.3 s`) gives
/// `50 * 8 * 0.3 = 120 s > 60 s`, and production defaults (`N = 50`, `max_attempts = 8`,
/// cooldown `5 s`) give `2000 s`. A single job's *expected* horizon with production defaults
/// (backoff base `1 s`, cap `60 s`: per-attempt uppers `1,2,4,8,16,32,60 s`, mean `upper/2`,
/// floored by the `5 s` cooldown) is `5+5+5+5+8+16+30 = 74 s > 60 s`; independent of jitter,
/// the failure before the final attempt waits at least `max(cooldown, backoff cap)` (60 s by
/// default), so even the worst case is `6 * 5 s + 60 s = 90 s > 60 s`. `retry_horizon_stays_above_sixty_seconds`
/// in `tests/delivery_m4.rs` pins this arithmetic; the sustained-outage test there pins the
/// other end (a horizon-long outage does reach `dead` with exactly one fallback).
pub async fn finalize_failed_while_open(
    pool: &PgPool,
    job: &DeliveryJob,
    token: Uuid,
    err: &SendError,
    policy: &BackoffPolicy,
    cooldown: Duration,
) -> Result<Option<crate::model::JobStatus>, sqlx::Error> {
    Ok(
        finalize_failed_inner(pool, job, token, err, policy, cooldown)
            .await?
            .map(|outcome| outcome.status),
    )
}

/// What `finalize_failed_inner` did to one job.
#[derive(Debug)]
struct FailedOutcome {
    status: JobStatus,
    /// Fallback jobs inserted because the job died (0 or 1).
    fallbacks: u64,
}

async fn finalize_failed_inner(
    pool: &PgPool,
    job: &DeliveryJob,
    token: Uuid,
    err: &SendError,
    policy: &BackoffPolicy,
    cooldown_floor: Duration,
) -> Result<Option<FailedOutcome>, sqlx::Error> {
    let dead = matches!(err, SendError::Permanent(_)) || job.attempts >= job.max_attempts;
    let mut tx = pool.begin().await?;
    let res = if dead {
        sqlx::query(
            "UPDATE delivery_jobs \
             SET status = 'dead', last_error = $3, last_error_class = $4, \
                 lease_token = NULL, locked_until = NULL, locked_by = NULL, updated_at = now() \
             WHERE id = $1 AND lease_token = $2 AND status = 'in_flight'",
        )
        .bind(job.id)
        .bind(token)
        .bind(err.message())
        .bind(err.class().as_str())
        .execute(&mut *tx)
        .await?
    } else {
        // The provider's requested minimum delay wins over our own backoff when it is
        // larger, but it is clamped to MAX_RETRY_AFTER (10 minutes): a hostile
        // or broken Retry-After must not park a job for years, whatever any channel
        // produced (the gateway channel clamps its own values as well; this is defence in
        // depth for every other channel). `cooldown_floor` is the breaker's cooldown, nonzero
        // only for a real attempt that failed while the breaker is open: the job must wait
        // out the cooldown as well as its own backoff before it can be probed again.
        let mut delay = backoff_delay(policy, job.attempts)
            .max(cooldown_floor)
            .max(err.retry_after().unwrap_or_default().min(MAX_RETRY_AFTER));
        // Deterministic floor before the FINAL attempt while the breaker is open: jitter alone
        // could let one job burn every attempt inside a short outage (worst case
        // `(max_attempts - 2) * cooldown`). Waiting at least `max(cooldown, backoff cap)` here
        // makes the single-job worst-case horizon `6 * 5 s + 60 s = 90 s > 60 s` with the
        // defaults, while short test policies (small cap) stay fast.
        if !cooldown_floor.is_zero() && job.attempts == job.max_attempts - 1 {
            delay = delay.max(cooldown_floor.max(policy.cap));
        }
        sqlx::query(
            "UPDATE delivery_jobs \
             SET status = 'pending', last_error = $3, last_error_class = $4, \
                 due_at = now() + make_interval(secs => $5::float8), \
                 lease_token = NULL, locked_until = NULL, locked_by = NULL, updated_at = now() \
             WHERE id = $1 AND lease_token = $2 AND status = 'in_flight'",
        )
        .bind(job.id)
        .bind(token)
        .bind(err.message())
        .bind(err.class().as_str())
        .bind(delay.as_secs_f64())
        .execute(&mut *tx)
        .await?
    };
    if res.rows_affected() != 1 {
        // Lease lost: nothing was written; dropping the tx rolls it back.
        return Ok(None);
    }

    let mut fallbacks = 0u64;
    if dead && insert_fallback_job(&mut tx, job.id).await? {
        fallbacks += 1;
        info!(job_id = %job.id, alert_id = %job.alert_id, from_channel = job.channel,
              "fallback job created for dead delivery");
    }
    tx.commit().await?;
    Ok(Some(FailedOutcome {
        status: if dead {
            JobStatus::Dead
        } else {
            JobStatus::Pending
        },
        fallbacks,
    }))
}

/// Put a claimed job back to `pending` WITHOUT consuming its attempt: the job never
/// reached the provider in this attempt (the channel breaker opened while it waited for a
/// send slot), so the send must wait out the cooldown instead of counting against
/// `max_attempts` ("deferred jobs don't consume attempts", SPEC §2.3). Sets
/// `attempts = attempts - 1` (the refund), `due_at = now() + delay`, records the error, clears
/// the lease; fenced by `token` like the other finalizers. Returns `false` if the lease was
/// lost.
///
/// This is ONLY for sends that never happened. A real send attempt that failed always consumes
/// its attempt, even when the breaker is or becomes open: those go through
/// [`finalize_failed_while_open`], never here.
pub async fn finalize_deferred(
    pool: &PgPool,
    job: &DeliveryJob,
    token: Uuid,
    err: &SendError,
    delay: Duration,
) -> Result<bool, sqlx::Error> {
    let res = sqlx::query(
        "UPDATE delivery_jobs \
         SET status = 'pending', attempts = attempts - 1, last_error = $3, \
             last_error_class = $4, due_at = now() + make_interval(secs => $5::float8), \
             lease_token = NULL, locked_until = NULL, locked_by = NULL, updated_at = now() \
         WHERE id = $1 AND lease_token = $2 AND status = 'in_flight'",
    )
    .bind(job.id)
    .bind(token)
    .bind(err.message())
    .bind(err.class().as_str())
    .bind(delay.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(res.rows_affected() == 1)
}

/// Full-jitter exponential backoff: uniform in `[0, min(cap, base * 2^(attempts-1))]`,
/// with a floor of `base / 2` so a retry is never immediate. `attempts` is the count
/// after the failed attempt (>= 1). The result never exceeds `cap`.
pub fn backoff_delay(policy: &BackoffPolicy, attempts: i32) -> Duration {
    let exp = (attempts.max(1) - 1).min(62);
    let cap = policy.cap.as_secs_f64();
    let upper = (policy.base.as_secs_f64() * 2f64.powi(exp)).min(cap);
    let floor = policy.base.as_secs_f64() / 2.0;
    let jittered = rand::rng().random_range(0.0..=upper);
    Duration::from_secs_f64(jittered.max(floor).min(cap))
}

/// What happened to one claimed job.
enum Outcome {
    Sent,
    Cancelled,
    Retried,
    /// Put back to `pending` with the attempt refunded: the job never reached the
    /// provider (breaker opened while it waited for a send slot).
    Deferred,
    /// Died; the payload is how many fallback jobs were created for it.
    Dead(u64),
    LostLease,
}

async fn finalize_cancelled_logged(
    pool: &PgPool,
    job: &DeliveryJob,
    token: Uuid,
    reason: &str,
) -> Result<Outcome, sqlx::Error> {
    if finalize_cancelled(pool, job.id, token, reason).await? {
        info!(job_id = %job.id, alert_id = %job.alert_id, reason, "delivery cancelled");
        Ok(Outcome::Cancelled)
    } else {
        warn!(job_id = %job.id, "lease lost before cancel");
        Ok(Outcome::LostLease)
    }
}

async fn process_job(
    pool: &PgPool,
    channel: &dyn Channel,
    cfg: &WorkerConfig,
    job: DeliveryJob,
    // `Some(token)` when this job is the breaker probe (this worker holds the channel's
    // probe lease): the send's outcome must be reported against that token.
    probe: Option<Uuid>,
) -> Result<Outcome, sqlx::Error> {
    // The claim always sets a token; a job without one cannot be finalized.
    let Some(token) = job.lease_token else {
        warn!(job_id = %job.id, "claimed job has no lease token");
        return Ok(Outcome::LostLease);
    };

    // Plain read: no lock, no transaction, no connection held across the send.
    let alert: Option<(String, String, String, String)> =
        sqlx::query_as("SELECT status, title, body, severity FROM alerts WHERE id = $1")
            .bind(job.alert_id)
            .fetch_optional(pool)
            .await?;
    let Some((status, title, body, severity)) = alert else {
        return finalize_cancelled_logged(pool, &job, token, "alert not found").await;
    };
    if !AlertStatus::parse(&status).is_some_and(AlertStatus::is_deliverable) {
        return finalize_cancelled_logged(pool, &job, token, &format!("alert {status}")).await;
    }

    let alert = AlertView {
        id: job.alert_id,
        status,
        severity,
        title,
        body,
    };
    // A job that waited for a send slot must not send if the breaker opened meanwhile.
    if probe.is_none() && breaker::is_not_closed(pool, channel.name()).await? {
        let err = SendError::Transient("channel breaker open".into());
        return Ok(
            if finalize_deferred(pool, &job, token, &err, cfg.breaker.cooldown).await? {
                Outcome::Deferred
            } else {
                Outcome::LostLease
            },
        );
    }
    let msg = channel.render(&alert, &job);
    match channel.send(&msg).await {
        Ok(receipt) => {
            let finalized = finalize_sent(pool, job.id, token, &receipt).await?;
            // The provider accepted this message, so the channel is healthy. Report it even
            // if the job lease was lost: a probe closing the breaker is never wrong here, and
            // a normal success only resets a `closed` row's failure streak.
            breaker::record_success(pool, channel.name(), probe).await?;
            if finalized {
                info!(job_id = %job.id, alert_id = %job.alert_id, attempts = job.attempts,
                      provider_message_id = %receipt.provider_message_id, "delivery sent");
                Ok(Outcome::Sent)
            } else {
                warn!(job_id = %job.id, "lease lost after send; message was delivered");
                Ok(Outcome::LostLease)
            }
        }
        Err(err) => {
            if matches!(err, SendError::Permanent(_)) {
                // A permanent error means the address is wrong, not the provider: it says
                // nothing about channel health, so it never touches the breaker.
                finish_failed(pool, &job, token, &err, &cfg.backoff).await
            } else {
                // Transient / ambiguous / Retry-After: counts against the channel.
                let open =
                    breaker::record_failure(pool, channel.name(), probe, &cfg.breaker).await?;
                if open {
                    // The breaker is open (this failure may have opened it): this WAS a real
                    // send attempt, so the attempt is consumed. The job goes back to `pending`
                    // with `due_at = now() + max(backoff, cooldown, Retry-After clamped)` so it
                    // respects the cooldown; the backoff growth spreads consecutive probes
                    // across jobs via the claim's `due_at` ordering. A sustained outage therefore
                    // exhausts `max_attempts` instead of looping on refunds, and the job reaches
                    // `dead` (+ one-hop fallback) once the horizon is spent.
                    match finalize_failed_inner(
                        pool,
                        &job,
                        token,
                        &err,
                        &cfg.backoff,
                        cfg.breaker.cooldown,
                    )
                    .await?
                    {
                        Some(outcome) if outcome.status == JobStatus::Dead => {
                            info!(job_id = %job.id, attempts = job.attempts, channel = channel.name(),
                                  class = err.class().as_str(), error = err.message(),
                                  fallbacks = outcome.fallbacks,
                                  "delivery dead during channel breaker open");
                            Ok(Outcome::Dead(outcome.fallbacks))
                        }
                        Some(_) => {
                            warn!(job_id = %job.id, attempts = job.attempts, channel = channel.name(),
                                  class = err.class().as_str(), error = err.message(),
                                  "delivery failed while breaker open, will retry after cooldown");
                            Ok(Outcome::Retried)
                        }
                        None => {
                            warn!(job_id = %job.id, "lease lost while deferring");
                            Ok(Outcome::LostLease)
                        }
                    }
                } else {
                    finish_failed(pool, &job, token, &err, &cfg.backoff).await
                }
            }
        }
    }
}

/// Finalize a send failure through the normal retry/dead path (breaker stays as it is).
async fn finish_failed(
    pool: &PgPool,
    job: &DeliveryJob,
    token: Uuid,
    err: &SendError,
    policy: &BackoffPolicy,
) -> Result<Outcome, sqlx::Error> {
    match finalize_failed_inner(pool, job, token, err, policy, Duration::ZERO).await? {
        Some(outcome) if outcome.status == JobStatus::Dead => {
            info!(job_id = %job.id, attempts = job.attempts, class = err.class().as_str(),
                  error = err.message(), fallbacks = outcome.fallbacks, "delivery dead");
            Ok(Outcome::Dead(outcome.fallbacks))
        }
        Some(_) => {
            warn!(job_id = %job.id, attempts = job.attempts, class = err.class().as_str(),
                  error = err.message(), "delivery failed, will retry");
            Ok(Outcome::Retried)
        }
        None => {
            warn!(job_id = %job.id, "lease lost while recording send failure");
            Ok(Outcome::LostLease)
        }
    }
}

/// Consult the channel breaker, then claim one batch and process it with up to
/// `cfg.concurrency` sends in flight:
/// - breaker `open` → claim nothing (returns an all-zero [`WorkStats`], so the run loop sleeps);
/// - this worker holds the probe → claim exactly ONE job and report its outcome against the
///   probe token;
/// - breaker `closed` → a normal batch.
///
/// For each job: read `alerts.status, title, body, severity` (no lock); if the alert is
/// missing or not deliverable -> `finalize_cancelled`; else build an `OutboundMessage` and
/// `channel.send` (no transaction open), then finalize. Returns once every claimed job is
/// finalized.
pub async fn work_once(
    pool: &PgPool,
    channel: &Arc<dyn Channel>,
    cfg: &WorkerConfig,
) -> Result<WorkStats, sqlx::Error> {
    let gate = breaker::acquire(pool, channel.name(), &cfg.breaker).await?;
    let probe = match gate {
        Gate::Open => return Ok(WorkStats::default()),
        Gate::Closed => None,
        Gate::Probe { token } => Some(token),
    };

    // The probe holder claims exactly one job: a single send speaks for the whole channel.
    let claimed = if probe.is_some() {
        let mut probe_cfg = cfg.clone();
        probe_cfg.batch_size = 1;
        claim_batch(pool, channel.name(), &probe_cfg).await?
    } else {
        claim_batch(pool, channel.name(), cfg).await?
    };

    let mut stats = WorkStats {
        claimed: claimed.jobs.len() as u64,
        dead_on_claim: claimed.dead_on_claim,
        fallbacks: claimed.fallbacks,
        ..WorkStats::default()
    };
    if claimed.dead_on_claim > 0 {
        info!(
            count = claimed.dead_on_claim,
            "dead-lettered expired leases with no attempts left"
        );
    }

    let sem = Arc::new(Semaphore::new(cfg.concurrency.max(1)));
    let cfg_arc = Arc::new(cfg.clone());
    let mut set: JoinSet<Result<Outcome, sqlx::Error>> = JoinSet::new();
    let mut first_err: Option<sqlx::Error> = None;

    fn tally(
        stats: &mut WorkStats,
        first_err: &mut Option<sqlx::Error>,
        res: Result<Result<Outcome, sqlx::Error>, tokio::task::JoinError>,
    ) {
        match res {
            Ok(Ok(Outcome::Sent)) => stats.sent += 1,
            Ok(Ok(Outcome::Cancelled)) => stats.cancelled += 1,
            Ok(Ok(Outcome::Retried)) => stats.retried += 1,
            Ok(Ok(Outcome::Deferred)) => stats.deferred += 1,
            Ok(Ok(Outcome::Dead(fallbacks))) => {
                stats.dead += 1;
                stats.fallbacks += fallbacks;
            }
            Ok(Ok(Outcome::LostLease)) => stats.lost_lease += 1,
            Ok(Err(e)) => {
                error!(error = %e, "job processing failed; lease will expire and the job be reclaimed");
                first_err.get_or_insert(e);
            }
            Err(e) => error!(error = %e, "job task panicked or was cancelled"),
        }
    }

    for job in claimed.jobs {
        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore never closed");
        // Reap finished tasks as we go so results are not held for the whole batch.
        while let Some(res) = set.try_join_next() {
            tally(&mut stats, &mut first_err, res);
        }
        // With a probe gate, the single claimed job is the probe job.
        let job_probe = probe;
        let pool = pool.clone();
        let channel = Arc::clone(channel);
        let cfg = Arc::clone(&cfg_arc);
        set.spawn(async move {
            let _permit = permit;
            process_job(&pool, channel.as_ref(), &cfg, job, job_probe).await
        });
    }
    while let Some(res) = set.join_next().await {
        tally(&mut stats, &mut first_err, res);
    }
    if let Some(token) = probe {
        breaker::release_probe(pool, channel.name(), token).await?;
    }

    match first_err {
        Some(e) => Err(e),
        None => Ok(stats),
    }
}

/// Loop `work_once` until shutdown; poll again at once after a non-empty batch, else sleep
/// `cfg.poll_interval`. Logs and backs off on DB errors instead of exiting.
pub async fn run_worker(
    pool: PgPool,
    channel: Arc<dyn Channel>,
    cfg: WorkerConfig,
    shutdown: Shutdown,
) {
    let mut shutdown = shutdown;
    info!(worker_id = %cfg.worker_id, channel = channel.name(), "delivery worker started");
    while !*shutdown.borrow() {
        match work_once(&pool, &channel, &cfg).await {
            Ok(stats) if stats.claimed > 0 || stats.dead_on_claim > 0 => continue,
            Ok(_) => {
                tokio::select! {
                    _ = tokio::time::sleep(cfg.poll_interval) => {}
                    res = shutdown.changed() => {
                        if res.is_err() {
                            // Sender dropped: nobody can signal shutdown any more; keep polling.
                            tokio::time::sleep(cfg.poll_interval).await;
                        }
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "delivery worker iteration failed");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = shutdown.changed() => {}
                }
            }
        }
    }
    info!(worker_id = %cfg.worker_id, "delivery worker stopped");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_within_bounds() {
        let p = BackoffPolicy {
            base: Duration::from_secs(1),
            cap: Duration::from_secs(60),
        };
        for attempts in 1..=40 {
            let upper = (2f64.powi((attempts - 1).min(62))).min(60.0);
            for _ in 0..200 {
                let d = backoff_delay(&p, attempts).as_secs_f64();
                assert!(d >= 0.5, "attempts={attempts} d={d} below floor");
                assert!(
                    d <= upper.max(0.5) + 1e-9,
                    "attempts={attempts} d={d} above {upper}"
                );
                assert!(d <= 60.0);
            }
        }
    }

    #[test]
    fn backoff_first_attempt_is_between_floor_and_base() {
        let p = BackoffPolicy {
            base: Duration::from_secs(2),
            cap: Duration::from_secs(60),
        };
        for _ in 0..500 {
            let d = backoff_delay(&p, 1);
            assert!(
                d >= Duration::from_secs(1) && d <= Duration::from_secs(2),
                "{d:?}"
            );
        }
    }

    #[test]
    fn backoff_respects_small_cap_and_odd_attempts() {
        let p = BackoffPolicy {
            base: Duration::from_secs(10),
            cap: Duration::from_secs(3),
        };
        for attempts in [-5, 0, 1, 2, i32::MAX] {
            for _ in 0..100 {
                assert!(backoff_delay(&p, attempts) <= Duration::from_secs(3));
            }
        }
    }

    #[test]
    fn backoff_grows_to_cap() {
        let p = BackoffPolicy {
            base: Duration::from_secs(1),
            cap: Duration::from_secs(60),
        };
        let max_seen = (0..2000).map(|_| backoff_delay(&p, 10)).max().unwrap();
        assert!(max_seen > Duration::from_secs(30), "{max_seen:?}");
    }
}
