//! Durable per-channel circuit breaker (SPEC §2.3, §5.6), table `channel_breakers`.
//!
//! States:
//! - `closed`: workers claim normally. Each transient/ambiguous send failure increments
//!   `consecutive_failures`; any success resets it to 0. Reaching `threshold` opens the breaker:
//!   `state='open', open_until = now() + cooldown`.
//! - `open`: workers claim NOTHING for this channel; pending jobs wait without consuming attempts.
//! - after `open_until`: exactly one worker wins the probe: `state='half_open'`, fresh
//!   `probe_token`, `probe_until = now() + probe_lease` (15 s). It claims ONE job.
//!   - probe sent OK → `closed`, failures 0, probe fields cleared.
//!   - probe failed transiently → `open` again with a new `open_until`; the probe job is a
//!     REAL send attempt, so it consumes its attempt and goes back to `pending` with
//!     `due_at = now() + max(backoff, cooldown, Retry-After clamped)` ("failed real attempts
//!     consume attempts, even while the breaker is open"; only jobs that never reached the
//!     provider are deferred with a refund).
//!   - probe worker crashed: once `probe_until` passes, another worker may take the probe
//!     (no deadlock in `half_open`).
//! - Permanent errors (bad address) never count: they say nothing about the provider.
//!
//! Every UPDATE sets `updated_at = now()`. A missing row means `closed` (created on first use).

use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct BreakerPolicy {
    pub threshold: i32,
    pub cooldown: Duration,
    pub probe_lease: Duration,
}

impl Default for BreakerPolicy {
    fn default() -> Self {
        Self {
            threshold: 5,
            cooldown: Duration::from_secs(5),
            probe_lease: Duration::from_secs(15),
        }
    }
}

/// What a worker may do on this channel right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    /// Claim a normal batch.
    Closed,
    /// Claim nothing.
    Open,
    /// This worker holds the probe: claim exactly one job and report with this token.
    Probe { token: Uuid },
}

/// Read the gate, atomically taking the probe if it is due (open past `open_until`, or
/// half_open past `probe_until`). One short transaction / single UPDATE ... RETURNING.
///
/// The probe is taken by one atomic `UPDATE ... RETURNING`: concurrent acquires serialize on
/// the row lock, and after the winner flips the state to `half_open` with a live probe lease
/// every loser's predicate no longer matches, so exactly one worker holds the probe per
/// window. A `half_open` row whose `probe_until` has passed means the probe worker died:
/// anyone may take the probe over (fresh token, fresh lease).
pub async fn acquire(
    pool: &PgPool,
    channel: &str,
    policy: &BreakerPolicy,
) -> Result<Gate, sqlx::Error> {
    let mut tx = pool.begin().await?;

    // A missing row means `closed`; create it so the probe UPDATE and every later call have a
    // row to work on.
    sqlx::query("INSERT INTO channel_breakers (channel) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(channel)
        .execute(&mut *tx)
        .await?;

    // Take the probe only when it is due. `probe_lease` bounds how long a crashed probe
    // worker can hold the channel in `half_open`.
    let probe: Option<(Uuid,)> = sqlx::query_as(
        "UPDATE channel_breakers \
         SET state = 'half_open', probe_token = gen_random_uuid(), \
             probe_until = now() + make_interval(secs => $2::float8), updated_at = now() \
         WHERE channel = $1 \
           AND ((state = 'open' AND open_until <= now()) \
             OR (state = 'half_open' AND probe_until < now())) \
         RETURNING probe_token",
    )
    .bind(channel)
    .bind(policy.probe_lease.as_secs_f64())
    .fetch_optional(&mut *tx)
    .await?;

    let gate = match probe {
        Some((token,)) => Gate::Probe { token },
        None => {
            let (state,): (String,) =
                sqlx::query_as("SELECT state FROM channel_breakers WHERE channel = $1")
                    .bind(channel)
                    .fetch_one(&mut *tx)
                    .await?;
            if state == "closed" {
                Gate::Closed
            } else {
                // `open` before the cooldown ends, or `half_open` with a live probe lease.
                Gate::Open
            }
        }
    };
    tx.commit().await?;
    Ok(gate)
}

/// A send on `channel` succeeded (probe or normal): close / reset.
///
/// - `probe = Some(token)`: the probe send succeeded; close the breaker (`failures = 0`,
///   probe fields cleared), but only while this worker still holds the token. A takeover or
///   an already-closed row makes this a no-op, so a slow probe can never close a breaker that
///   has since been reopened.
/// - `probe = None`: a normal success resets the failure streak **only while the row is
///   `closed`**. While a probe is in flight (or the channel is open) a stale in-flight
///   success must not close the breaker out from under the probe holder or mask newer
///   failures: it is ignored.
pub async fn record_success(
    pool: &PgPool,
    channel: &str,
    probe: Option<Uuid>,
) -> Result<(), sqlx::Error> {
    match probe {
        Some(token) => {
            sqlx::query(
                "UPDATE channel_breakers \
                 SET state = 'closed', consecutive_failures = 0, probe_token = NULL, \
                     probe_until = NULL, updated_at = now() \
                 WHERE channel = $1 AND state = 'half_open' AND probe_token = $2",
            )
            .bind(channel)
            .bind(token)
            .execute(pool)
            .await?;
        }
        None => {
            sqlx::query(
                "UPDATE channel_breakers \
                 SET consecutive_failures = 0, updated_at = now() \
                 WHERE channel = $1 AND state = 'closed'",
            )
            .bind(channel)
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}

/// A transient or ambiguous send failure. Returns `true` if the breaker is (now) open, so the
/// caller defers instead of retrying. With `probe = Some(token)` it reopens immediately (only if
/// the token still matches); otherwise it increments and opens at `threshold`.
///
/// - `probe = Some(token)` and the token matches: reopen now (`open_until = now() +
///   cooldown`, probe cleared) and return `true`.
/// - `probe = Some(token)` and the token is stale (taken over, or already closed): change
///   nothing — the current probe owner owns the state — and just report whether the row is
///   currently not `closed`.
/// - `probe = None`: increment `consecutive_failures` while `closed`; the first failure to
///   reach `threshold` opens the breaker in the same statement (concurrent failures cannot
///   overshoot, because once `open` no row matches `state = 'closed'`). Failures that arrive
///   while the row is `open` or `half_open` do not extend the window; they only report the
///   state. A missing row is created first so the very first failure still counts.
pub async fn record_failure(
    pool: &PgPool,
    channel: &str,
    probe: Option<Uuid>,
    policy: &BreakerPolicy,
) -> Result<bool, sqlx::Error> {
    match probe {
        Some(token) => {
            let reopened = sqlx::query(
                "UPDATE channel_breakers \
                 SET state = 'open', consecutive_failures = consecutive_failures + 1, \
                     opened_at = now(), \
                     open_until = now() + make_interval(secs => $3::float8), \
                     probe_token = NULL, probe_until = NULL, updated_at = now() \
                 WHERE channel = $1 AND state = 'half_open' AND probe_token = $2",
            )
            .bind(channel)
            .bind(token)
            .bind(policy.cooldown.as_secs_f64())
            .execute(pool)
            .await?;
            if reopened.rows_affected() == 1 {
                return Ok(true);
            }
            is_not_closed(pool, channel).await
        }
        None => {
            sqlx::query(
                "INSERT INTO channel_breakers (channel) VALUES ($1) ON CONFLICT DO NOTHING",
            )
            .bind(channel)
            .execute(pool)
            .await?;
            let state: Option<(String,)> = sqlx::query_as(
                "UPDATE channel_breakers \
                 SET consecutive_failures = consecutive_failures + 1, \
                     state = CASE WHEN consecutive_failures + 1 >= $2 THEN 'open' ELSE state END, \
                     opened_at = CASE WHEN consecutive_failures + 1 >= $2 \
                                      THEN now() ELSE opened_at END, \
                     open_until = CASE WHEN consecutive_failures + 1 >= $2 \
                                       THEN now() + make_interval(secs => $3::float8) \
                                       ELSE open_until END, \
                     updated_at = now() \
                 WHERE channel = $1 AND state = 'closed' \
                 RETURNING state",
            )
            .bind(channel)
            .bind(policy.threshold)
            .bind(policy.cooldown.as_secs_f64())
            .fetch_optional(pool)
            .await?;
            match state {
                // The failure was counted while closed: report whether it opened the breaker.
                Some((state,)) => Ok(state != "closed"),
                // The row is open or half_open: report that without changing it.
                None => is_not_closed(pool, channel).await,
            }
        }
    }
}

/// The probe holder finished without a send result (nothing to claim, alert cancelled,
/// permanent error): hand the probe back (`open`, `open_until` unchanged and already past) so
/// the next `acquire` can take it at once instead of waiting out `probe_lease`. No-op once
/// the token no longer matches (the probe already closed or reopened the breaker).
pub async fn release_probe(pool: &PgPool, channel: &str, token: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE channel_breakers \
         SET state = 'open', probe_token = NULL, probe_until = NULL, updated_at = now() \
         WHERE channel = $1 AND state = 'half_open' AND probe_token = $2",
    )
    .bind(channel)
    .bind(token)
    .execute(pool)
    .await?;
    Ok(())
}

/// Current row for diagnostics/tests: `(state, consecutive_failures)`; `("closed", 0)` if none.
pub async fn status(pool: &PgPool, channel: &str) -> Result<(String, i32), sqlx::Error> {
    let row: Option<(String, i32)> = sqlx::query_as(
        "SELECT state, consecutive_failures FROM channel_breakers WHERE channel = $1",
    )
    .bind(channel)
    .fetch_optional(pool)
    .await?;
    Ok(row.unwrap_or_else(|| ("closed".to_string(), 0)))
}

/// Whether the channel's breaker is currently not `closed` (a missing row is `closed`).
pub async fn is_not_closed(pool: &PgPool, channel: &str) -> Result<bool, sqlx::Error> {
    let state: Option<(String,)> =
        sqlx::query_as("SELECT state FROM channel_breakers WHERE channel = $1")
            .bind(channel)
            .fetch_optional(pool)
            .await?;
    Ok(state.is_some_and(|(state,)| state != "closed"))
}
