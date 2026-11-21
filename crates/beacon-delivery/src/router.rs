//! M0 router: outbox `kind='notify'` rows -> `delivery_jobs`, one transaction per batch.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use sqlx::PgPool;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::config::Directory;
use crate::model::NotifyPayload;
use crate::{RouterConfig, Shutdown};

/// Channels the router can create jobs for in M0. `ui` is delivered by the alert feed itself
/// and never becomes a job; any other channel is skipped with a warning until it exists.
pub const SUPPORTED_JOB_CHANNELS: &[&str] = &["email"];

/// Channels used when a route entry has no explicit `channels:` (M0; preferences come at M4).
pub const DEFAULT_CHANNELS: &[&str] = &["email"];

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RouteStats {
    /// Outbox rows marked processed.
    pub outbox_rows: u64,
    /// `delivery_jobs` rows actually inserted (conflicts excluded).
    pub jobs_inserted: u64,
    /// Rows whose payload failed to parse (marked processed, logged).
    pub malformed: u64,
    /// Recipients skipped (unknown target, no address for the channel, unsupported channel).
    pub skipped: u64,
}

/// Claim up to `cfg.batch_size` unprocessed notify rows with `FOR UPDATE SKIP LOCKED`, insert
/// their jobs `ON CONFLICT (alert_id, recipient_id, channel, notify_seq) DO NOTHING`, and set
/// `processed_at = now()`, all in ONE transaction.
pub async fn route_once(
    pool: &PgPool,
    dir: &Directory,
    cfg: &RouterConfig,
) -> Result<RouteStats, sqlx::Error> {
    let mut tx = pool.begin().await?;

    let rows: Vec<(i64, Uuid, serde_json::Value)> = sqlx::query_as(
        "SELECT id, alert_id, payload FROM outbox \
         WHERE kind = 'notify' AND processed_at IS NULL \
         ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED",
    )
    .bind(cfg.batch_size)
    .fetch_all(&mut *tx)
    .await?;

    let mut stats = RouteStats::default();
    if rows.is_empty() {
        tx.commit().await?;
        return Ok(stats);
    }

    for (outbox_id, alert_id, payload) in &rows {
        let payload: NotifyPayload = match serde_json::from_value(payload.clone()) {
            Ok(p) => p,
            Err(err) => {
                // Still marked processed below: a poison row must not block the queue.
                warn!(outbox_id, %alert_id, %err, "malformed notify payload, dropping");
                stats.malformed += 1;
                continue;
            }
        };

        // (recipient, channel) pairs already planned for this payload.
        let mut seen: HashSet<(String, &str)> = HashSet::new();

        for entry in &payload.route {
            let people = match dir.expand(&entry.to) {
                Ok(p) => p,
                Err(err) => {
                    warn!(outbox_id, %alert_id, target = %entry.to, %err, "cannot expand route target, skipping");
                    stats.skipped += 1;
                    continue;
                }
            };
            let channels: Vec<&str> = match &entry.channels {
                Some(c) => c.iter().map(String::as_str).collect(),
                None => DEFAULT_CHANNELS.to_vec(),
            };

            for person in people {
                for &channel in &channels {
                    if channel == "ui" {
                        continue;
                    }
                    if !SUPPORTED_JOB_CHANNELS.contains(&channel) {
                        warn!(outbox_id, %alert_id, recipient = %person.id, channel, "unsupported channel, skipping");
                        stats.skipped += 1;
                        continue;
                    }
                    let address = match channel {
                        "email" => person.contact.email.as_deref(),
                        _ => None,
                    };
                    let Some(address) = address else {
                        warn!(outbox_id, %alert_id, recipient = %person.id, channel, "no address for channel, skipping");
                        stats.skipped += 1;
                        continue;
                    };
                    if !seen.insert((person.id.clone(), channel)) {
                        continue;
                    }

                    let route_info = json!({
                        "target": entry.to,
                        "rule_id": payload.rule_id,
                        "rule_revision": payload.rule_revision,
                        "reason": payload.reason,
                        "outbox_id": outbox_id,
                    });
                    let res = sqlx::query(
                        "INSERT INTO delivery_jobs \
                           (alert_id, notify_seq, recipient_id, channel, address, route_info) \
                         VALUES ($1, $2, $3, $4, $5, $6) \
                         ON CONFLICT (alert_id, recipient_id, channel, notify_seq) DO NOTHING",
                    )
                    .bind(alert_id)
                    .bind(payload.notify_seq)
                    .bind(&person.id)
                    .bind(channel)
                    .bind(address)
                    .bind(route_info)
                    .execute(&mut *tx)
                    .await?;
                    stats.jobs_inserted += res.rows_affected();
                }
            }
        }
    }

    let ids: Vec<i64> = rows.iter().map(|(id, _, _)| *id).collect();
    sqlx::query("UPDATE outbox SET processed_at = now() WHERE id = ANY($1)")
        .bind(&ids)
        .execute(&mut *tx)
        .await?;
    stats.outbox_rows = ids.len() as u64;

    tx.commit().await?;
    Ok(stats)
}

/// Loop `route_once` until shutdown. Polls again immediately after a full batch, otherwise
/// sleeps `cfg.poll_interval`. Logs and backs off on DB errors instead of exiting.
pub async fn run_router(pool: PgPool, dir: Arc<Directory>, cfg: RouterConfig, shutdown: Shutdown) {
    let mut shutdown = shutdown;
    while !*shutdown.borrow() {
        let wait = match route_once(&pool, &dir, &cfg).await {
            Ok(stats) => {
                if stats != RouteStats::default() {
                    debug!(?stats, "router batch");
                }
                if i64::try_from(stats.outbox_rows).is_ok_and(|n| n >= cfg.batch_size) {
                    continue; // full batch: more is probably waiting
                }
                cfg.poll_interval
            }
            Err(err) => {
                error!(%err, "router batch failed, retrying in 1s");
                Duration::from_secs(1)
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            // `Err` means the sender is gone, so nobody can signal shutdown any more: stop.
            res = shutdown.changed() => {
                if res.is_err() {
                    break;
                }
            }
        }
    }
}
