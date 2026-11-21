//! Wake-ups over Postgres `LISTEN/NOTIFY`, so the router and the channel workers react to
//! new work at once instead of waiting out their poll interval (SPEC §9, M6 latency).
//!
//! One [`Wake::listen`] per process and channel holds one dedicated connection (its own
//! one-connection pool, outside the shared pool's sizing) and bumps a [`watch`] counter on
//! every notification. Each loop calls [`Wake::mark_seen`] before it looks for work and
//! [`Wake::changed`] while it sleeps: a notification that lands during a pass is never
//! missed, and a burst of them collapses into one extra pass.
//!
//! Polling stays as the fallback: a lost connection, a missed notification or a job whose
//! `due_at` is in the future (a retry backoff) is still picked up by the next poll.

use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::{PgListener, PgPoolOptions};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::Shutdown;

/// The engine's outbox channel: the aggregator sends it whenever it writes outbox rows
/// (`beacon_engine::OUTBOX_CHANNEL`; delivery does not depend on the engine crate).
pub const OUTBOX_CHANNEL: &str = "beacon_outbox";

/// Sent by a statement trigger on every `INSERT INTO delivery_jobs`
/// (`migrations/20261121070000_delivery_jobs_notify.sql`), whoever inserts: the router,
/// fallbacks, escalation steps, approval prompts, throttle digests.
pub const JOBS_CHANNEL: &str = "beacon_delivery_jobs";

/// A shared wake-up signal. Cheap to clone; every clone tracks what it has seen on its own.
#[derive(Debug, Clone)]
pub struct Wake {
    rx: watch::Receiver<u64>,
    /// The listener task ended (shutdown): stop waiting on it so loops don't spin.
    closed: bool,
}

impl Wake {
    /// Spawn the listener task for `channel` and return the first handle.
    pub fn listen(pool: &PgPool, channel: &'static str, shutdown: Shutdown) -> Self {
        let (tx, rx) = watch::channel(0u64);
        // A pool of one, never recycled: PgListener uses it only to reconnect.
        let listen_pool = PgPoolOptions::new()
            .max_connections(1)
            .max_lifetime(None)
            .idle_timeout(None)
            .connect_lazy_with((*pool.connect_options()).clone());
        tokio::spawn(run_listener(listen_pool, channel, tx, shutdown));
        Self { rx, closed: false }
    }

    /// A handle that never fires: loops built with it only poll.
    pub fn never() -> Self {
        let (_tx, rx) = watch::channel(0u64);
        Self { rx, closed: true }
    }

    /// Forget notifications so far. Call right before looking for work.
    pub fn mark_seen(&mut self) {
        self.rx.mark_unchanged();
    }

    /// Resolves once a notification arrived after the last [`Wake::mark_seen`]
    /// (immediately if one already did). Pending forever once the listener is gone.
    pub async fn changed(&mut self) {
        if !self.closed && self.rx.changed().await.is_ok() {
            return;
        }
        self.closed = true;
        std::future::pending::<()>().await
    }
}

async fn run_listener(
    pool: PgPool,
    channel: &'static str,
    tx: watch::Sender<u64>,
    mut shutdown: Shutdown,
) {
    let bump = || tx.send_modify(|n| *n = n.wrapping_add(1));
    'connect: loop {
        let listener = async {
            let mut l = PgListener::connect_with(&pool).await?;
            l.listen(channel).await?;
            Ok::<_, sqlx::Error>(l)
        };
        let mut listener = tokio::select! {
            res = listener => match res {
                Ok(l) => l,
                Err(err) => {
                    warn!(channel, %err, "LISTEN failed; polling only, retrying in 5s");
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(5)) => continue 'connect,
                        _ = shutdown.changed() => break 'connect,
                    }
                }
            },
            _ = shutdown.changed() => break 'connect,
        };
        info!(channel, "listening for wake-ups");
        // Anything sent before LISTEN took effect: one pass picks it up.
        bump();
        loop {
            tokio::select! {
                res = listener.recv() => match res {
                    Ok(_) => bump(),
                    Err(err) => {
                        // `recv` reconnects on the next call; notifications in the gap are
                        // lost, so wake everyone once and let them look.
                        warn!(channel, %err, "LISTEN connection lost; reconnecting");
                        bump();
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                            _ = shutdown.changed() => break 'connect,
                        }
                    }
                },
                _ = shutdown.changed() => break 'connect,
            }
            if *shutdown.borrow() {
                break 'connect;
            }
        }
    }
    pool.close().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn never_stays_pending() {
        let mut w = Wake::never();
        let res = tokio::time::timeout(Duration::from_millis(20), w.changed()).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn bump_during_a_pass_is_not_missed_and_bursts_collapse() {
        let (tx, rx) = watch::channel(0u64);
        let mut w = Wake { rx, closed: false };
        w.mark_seen();
        // Three notifications while "working".
        for _ in 0..3 {
            tx.send_modify(|n| *n += 1);
        }
        tokio::time::timeout(Duration::from_millis(20), w.changed())
            .await
            .expect("a notification during the pass wakes the next wait");
        // The burst was one wake-up: nothing more is pending.
        w.mark_seen();
        let res = tokio::time::timeout(Duration::from_millis(20), w.changed()).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn closed_listener_does_not_spin() {
        let (tx, rx) = watch::channel(0u64);
        let mut w = Wake { rx, closed: false };
        drop(tx);
        let res = tokio::time::timeout(Duration::from_millis(20), w.changed()).await;
        assert!(res.is_err());
        assert!(w.closed);
    }
}
