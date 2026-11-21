//! Wake-ups over Postgres `LISTEN/NOTIFY`, so the router and the channel workers react to
//! new work at once instead of waiting out their poll interval (SPEC §9, M6 latency).
//!
//! One [`Wakes::listen`] per process and Postgres channel holds one dedicated connection (its
//! own one-connection pool, outside the shared pool's sizing). Each notification payload
//! (`""` for the outbox, the delivery channel name for jobs) maps to one [`Wake`], and a
//! notification wakes **one** idle loop of that key (`Notify::notify_one`): loops compete for
//! rows with `SKIP LOCKED`, so waking all of them only buys empty passes. When every loop is
//! busy one wake-up is kept, so a burst of notifications collapses into one extra pass, and
//! a notification that lands during a pass is never missed.
//!
//! Polling stays as the fallback: a lost connection, a missed notification or a job whose
//! `due_at` is in the future (a retry backoff) is still picked up by the next poll.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::{PgListener, PgPoolOptions};
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::Shutdown;

/// The engine's outbox channel: the aggregator sends it whenever it writes outbox rows
/// (`beacon_engine::OUTBOX_CHANNEL`; delivery does not depend on the engine crate). The
/// payload is empty: use [`OUTBOX_KEY`].
pub const OUTBOX_CHANNEL: &str = "beacon_outbox";
pub const OUTBOX_KEY: &str = "";

/// Sent by a statement trigger after every `INSERT INTO delivery_jobs`, once per delivery
/// channel inserted, with that channel's name as the payload
/// (`migrations/20261121070000_delivery_jobs_notify.sql`), whoever inserts: the router,
/// fallbacks, escalation steps, approval prompts, throttle digests.
pub const JOBS_CHANNEL: &str = "beacon_delivery_jobs";

/// Wake-triggered passes of one loop start at least this far apart, so a steady stream of
/// notifications under load still lets work batch up (a timed poll is not paced).
pub const MIN_GAP: Duration = Duration::from_millis(20);

/// The wake-ups of one Postgres channel, by payload.
#[derive(Debug, Clone, Default)]
pub struct Wakes {
    keys: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
}

impl Wakes {
    /// Spawn the listener task for `channel`.
    pub fn listen(pool: &PgPool, channel: &'static str, shutdown: Shutdown) -> Self {
        let wakes = Self::default();
        // A pool of one, never recycled: PgListener uses it only to reconnect.
        let listen_pool = PgPoolOptions::new()
            .max_connections(1)
            .max_lifetime(None)
            .idle_timeout(None)
            .connect_lazy_with((*pool.connect_options()).clone());
        tokio::spawn(run_listener(listen_pool, channel, wakes.clone(), shutdown));
        wakes
    }

    /// The wake-up for notifications whose payload is `key`. Every loop that serves the
    /// same key should share it (clones share).
    pub fn get(&self, key: &str) -> Wake {
        let notify = self
            .keys
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_default()
            .clone();
        Wake(Some(notify))
    }

    fn wake(&self, key: &str) {
        if let Some(n) = self.keys.lock().unwrap().get(key) {
            n.notify_one();
        }
    }

    fn wake_all(&self) {
        for n in self.keys.lock().unwrap().values() {
            n.notify_one();
        }
    }
}

/// One key's wake-up. `Wake::never()` (the configs' default) never fires: such a loop only
/// polls.
#[derive(Debug, Clone, Default)]
pub struct Wake(Option<Arc<Notify>>);

impl Wake {
    pub fn never() -> Self {
        Self(None)
    }

    /// Resolves once this loop is chosen for a notification, or at once when one arrived
    /// while every loop of the key was busy. Pending forever for `Wake::never()`.
    pub async fn notified(&self) {
        match &self.0 {
            Some(n) => n.notified().await,
            None => std::future::pending().await,
        }
    }

    /// Sleep out the rest of [`MIN_GAP`] since `pass_started`.
    pub async fn pace(pass_started: Instant) {
        tokio::time::sleep_until(pass_started + MIN_GAP).await;
    }
}

async fn run_listener(pool: PgPool, channel: &'static str, wakes: Wakes, mut shutdown: Shutdown) {
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
        // Anything sent before LISTEN took effect: one pass per key picks it up.
        wakes.wake_all();
        loop {
            tokio::select! {
                res = listener.recv() => match res {
                    Ok(n) => wakes.wake(n.payload()),
                    Err(err) => {
                        // `recv` reconnects on the next call; notifications in the gap are
                        // lost, so wake every key once and let them look.
                        warn!(channel, %err, "LISTEN connection lost; reconnecting");
                        wakes.wake_all();
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

    async fn fires(w: &Wake) -> bool {
        tokio::time::timeout(Duration::from_millis(20), w.notified())
            .await
            .is_ok()
    }

    #[tokio::test]
    async fn never_stays_pending() {
        assert!(!fires(&Wake::never()).await);
    }

    #[tokio::test]
    async fn burst_while_busy_is_kept_once() {
        let wakes = Wakes::default();
        let w = wakes.get("sms");
        // Three notifications while the loop is busy (not waiting).
        for _ in 0..3 {
            wakes.wake("sms");
        }
        assert!(
            fires(&w).await,
            "a notification during the pass is not missed"
        );
        assert!(!fires(&w).await, "the burst was one wake-up");
    }

    #[tokio::test]
    async fn one_notification_wakes_one_waiter_of_its_key_only() {
        let wakes = Wakes::default();
        let (a, b, email) = (wakes.get("sms"), wakes.get("sms"), wakes.get("email"));
        let (ta, tb) = (
            tokio::spawn(async move { fires(&a).await }),
            tokio::spawn(async move { fires(&b).await }),
        );
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        wakes.wake("sms");
        let woken = [ta.await.unwrap(), tb.await.unwrap()];
        assert_eq!(woken.iter().filter(|w| **w).count(), 1, "{woken:?}");
        assert!(!fires(&email).await, "another key is not woken");
        wakes.wake("unknown"); // no loop serves it: ignored
    }

    #[tokio::test]
    async fn wake_all_reaches_every_key() {
        let wakes = Wakes::default();
        let (sms, email) = (wakes.get("sms"), wakes.get("email"));
        wakes.wake_all();
        assert!(fires(&sms).await && fires(&email).await);
    }
}
