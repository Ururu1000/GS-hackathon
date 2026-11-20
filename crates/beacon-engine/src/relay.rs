//! Relay role: outbox `kind='alert_update'` → Kafka `alerts.updates`.
//!
//! Claims pending rows with `FOR UPDATE SKIP LOCKED` (any number of relays,
//! no leader), publishes the alert's **current** full state keyed by alert id,
//! and marks the rows processed in the same transaction once the broker has
//! acknowledged. Several rows for one alert collapse into one publish; each
//! record carries the alert `version`, so consumers drop stale ones.
//! Woken by `NOTIFY beacon_outbox`, with a short polling fallback.

use std::time::Duration;

use anyhow::{Context, Result};
use beacon_core::model::outbox_kind;
use beacon_ingest::kafka::{self, ALERTS_UPDATES};
use futures::future::join_all;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use sqlx::postgres::PgListener;
use sqlx::{PgPool, Row};
use tokio::sync::watch;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::OUTBOX_CHANNEL;
use crate::feed::{FEED_COLUMNS, row_to_feed};

const BATCH: i64 = 1000;
const POLL: Duration = Duration::from_millis(250);

pub async fn run_relay(bootstrap: String, pool: PgPool, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let producer = kafka::producer(&bootstrap)?;
    let mut listener = PgListener::connect_with(&pool).await.context("relay LISTEN connection")?;
    listener.listen(OUTBOX_CHANNEL).await?;
    info!("relay started");
    loop {
        // Drain everything pending, then wait for a NOTIFY (or the poll timeout).
        loop {
            match relay_once(&pool, &producer).await {
                Ok(n) if n as i64 >= BATCH => continue,
                Ok(_) => break,
                Err(e) => {
                    error!(error = format!("{e:#}"), "relay batch failed; will retry");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    break;
                }
            }
        }
        tokio::select! {
            _ = shutdown.changed() => break,
            n = listener.recv() => {
                if let Err(e) = n {
                    warn!(error = %e, "relay listener error");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
            _ = tokio::time::sleep(POLL) => {}
        }
    }
    Ok(())
}

/// Relay one batch. Returns the number of outbox rows claimed.
pub async fn relay_once(pool: &PgPool, producer: &FutureProducer) -> Result<usize> {
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "SELECT id, alert_id FROM outbox
         WHERE kind = $1 AND processed_at IS NULL
         ORDER BY id LIMIT $2
         FOR UPDATE SKIP LOCKED",
    )
    .bind(outbox_kind::ALERT_UPDATE)
    .bind(BATCH)
    .fetch_all(&mut *tx)
    .await?;
    if rows.is_empty() {
        return Ok(0);
    }
    let outbox_ids: Vec<i64> = rows.iter().map(|r| r.get("id")).collect();
    let mut alert_ids: Vec<Uuid> = rows.iter().map(|r| r.get("alert_id")).collect();
    alert_ids.sort();
    alert_ids.dedup();

    let alerts = sqlx::query(&format!("SELECT {FEED_COLUMNS} FROM alerts WHERE id = ANY($1)"))
        .bind(&alert_ids)
        .fetch_all(&mut *tx)
        .await?;
    let payloads: Vec<(String, Vec<u8>)> = alerts
        .iter()
        .map(|r| {
            let a = row_to_feed(r)?;
            Ok((a.id.to_string(), serde_json::to_vec(&a)?))
        })
        .collect::<Result<_>>()?;
    let sends = payloads.iter().map(|(key, payload)| {
        producer.send(
            FutureRecord::to(ALERTS_UPDATES).key(key).payload(payload),
            Timeout::After(Duration::from_secs(5)),
        )
    });
    let results = join_all(sends).await;
    if let Some((e, _)) = results.into_iter().find_map(Result::err) {
        // Roll back: the rows stay pending and are relayed again (at-least-once).
        anyhow::bail!("publishing alert updates: {e}");
    }
    sqlx::query("UPDATE outbox SET processed_at = now() WHERE id = ANY($1)")
        .bind(&outbox_ids)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(outbox_ids.len())
}
