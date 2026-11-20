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
//!   - probe failed transiently → `open` again with a new `open_until`; the probe job is
//!     *deferred*: back to `pending`, its attempt refunded (`attempts = attempts - 1`), `due_at =
//!     open_until` ("deferred jobs don't consume attempts").
//!   - probe worker crashed: once `probe_until` passes, another worker may take the probe
//!     (no deadlock in `half_open`).
//! - Permanent errors (bad address) never count: they say nothing about the provider.
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
pub async fn acquire(
    pool: &PgPool,
    channel: &str,
    policy: &BreakerPolicy,
) -> Result<Gate, sqlx::Error> {
    let _ = (pool, channel, policy);
    todo!("agent 3")
}

/// A send on `channel` succeeded (probe or normal): close / reset.
pub async fn record_success(
    pool: &PgPool,
    channel: &str,
    probe: Option<Uuid>,
) -> Result<(), sqlx::Error> {
    let _ = (pool, channel, probe);
    todo!("agent 3")
}

/// A transient or ambiguous send failure. Returns `true` if the breaker is (now) open, so the
/// caller defers instead of retrying. With `probe = Some(token)` it reopens immediately (only if
/// the token still matches); otherwise it increments and opens at `threshold`.
pub async fn record_failure(
    pool: &PgPool,
    channel: &str,
    probe: Option<Uuid>,
    policy: &BreakerPolicy,
) -> Result<bool, sqlx::Error> {
    let _ = (pool, channel, probe, policy);
    todo!("agent 3")
}

/// Current row for diagnostics/tests: `(state, consecutive_failures)`; `("closed", 0)` if none.
pub async fn status(pool: &PgPool, channel: &str) -> Result<(String, i32), sqlx::Error> {
    let _ = (pool, channel);
    todo!("agent 3")
}
