//! Channel worker: lease due jobs, re-check the alert, send outside any tx, finalize by token.

use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use sqlx::PgPool;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use uuid::Uuid;

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
}

/// Outcome of a claim: jobs this worker now holds plus how many expired leases were
/// dead-lettered instead of being reclaimed.
#[derive(Debug, Default)]
pub struct Claimed {
    pub jobs: Vec<DeliveryJob>,
    pub dead_on_claim: u64,
}

/// One short transaction:
/// 1. `in_flight` jobs of `channel` whose lease expired AND `attempts >= max_attempts` become
///    `dead` with `last_error_class = 'ambiguous'`,
///    `last_error = 'max attempts exceeded after lease expiry'`.
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

    let dead_on_claim = sqlx::query(
        "UPDATE delivery_jobs \
         SET status = 'dead', last_error_class = 'ambiguous', \
             last_error = 'max attempts exceeded after lease expiry', \
             lease_token = NULL, locked_until = NULL, locked_by = NULL, updated_at = now() \
         WHERE id IN ( \
             SELECT id FROM delivery_jobs \
             WHERE channel = $1 AND status = 'in_flight' AND locked_until < now() \
               AND attempts >= max_attempts \
             FOR UPDATE SKIP LOCKED)",
    )
    .bind(channel)
    .execute(&mut *tx)
    .await?
    .rows_affected();

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
    })
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

/// Permanent errors, or any error when `attempts >= max_attempts`, -> `dead`.
/// Otherwise -> `pending` with `due_at = now() + backoff_delay(policy, attempts)`.
/// Returns `Some(new_status)` or `None` if the lease was lost.
pub async fn finalize_failed(
    pool: &PgPool,
    job: &DeliveryJob,
    token: Uuid,
    err: &SendError,
    policy: &BackoffPolicy,
) -> Result<Option<crate::model::JobStatus>, sqlx::Error> {
    let dead = matches!(err, SendError::Permanent(_)) || job.attempts >= job.max_attempts;
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
        .execute(pool)
        .await?
    } else {
        let delay = backoff_delay(policy, job.attempts);
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
        .execute(pool)
        .await?
    };
    if res.rows_affected() != 1 {
        return Ok(None);
    }
    Ok(Some(if dead {
        JobStatus::Dead
    } else {
        JobStatus::Pending
    }))
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
    Dead,
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
    let msg = channel.render(&alert, &job);
    match channel.send(&msg).await {
        Ok(receipt) => {
            if finalize_sent(pool, job.id, token, &receipt).await? {
                info!(job_id = %job.id, alert_id = %job.alert_id, attempts = job.attempts,
                      provider_message_id = %receipt.provider_message_id, "delivery sent");
                Ok(Outcome::Sent)
            } else {
                warn!(job_id = %job.id, "lease lost after send; message was delivered");
                Ok(Outcome::LostLease)
            }
        }
        Err(err) => match finalize_failed(pool, &job, token, &err, &cfg.backoff).await? {
            Some(JobStatus::Dead) => {
                info!(job_id = %job.id, attempts = job.attempts, class = err.class().as_str(),
                      error = err.message(), "delivery dead");
                Ok(Outcome::Dead)
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
        },
    }
}

/// Claim one batch and process it with up to `cfg.concurrency` sends in flight. For each job:
/// read `alerts.status, title, body, severity` (no lock); if the alert is missing or not
/// deliverable -> `finalize_cancelled`; else build an `OutboundMessage` and `channel.send`
/// (no transaction open), then finalize. Returns once every claimed job is finalized.
pub async fn work_once(
    pool: &PgPool,
    channel: &Arc<dyn Channel>,
    cfg: &WorkerConfig,
) -> Result<WorkStats, sqlx::Error> {
    let claimed = claim_batch(pool, channel.name(), cfg).await?;
    let mut stats = WorkStats {
        claimed: claimed.jobs.len() as u64,
        dead_on_claim: claimed.dead_on_claim,
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
            Ok(Ok(Outcome::Dead)) => stats.dead += 1,
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
        let pool = pool.clone();
        let channel = Arc::clone(channel);
        let cfg = Arc::clone(&cfg_arc);
        set.spawn(async move {
            let _permit = permit;
            process_job(&pool, channel.as_ref(), &cfg, job).await
        });
    }
    while let Some(res) = set.join_next().await {
        tally(&mut stats, &mut first_err, res);
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
