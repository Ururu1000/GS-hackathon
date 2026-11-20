//! Per-replica fan-out of `alerts.updates`.
//!
//! Each API replica consumes the topic in its **own** consumer group from the
//! latest offset, so every replica sees every update; there is nothing to
//! commit. Updates go to a bounded broadcast channel; an SSE client that falls
//! behind gets `Lagged` and is closed, so the browser reconnects and resyncs
//! from a fresh snapshot (SPEC §8).

use std::time::Duration;

use anyhow::Result;
use beacon_core::FeedAlert;
use beacon_ingest::kafka::{self, ALERTS_UPDATES};
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tokio::sync::{broadcast, watch};
use tracing::{info, warn};

pub const CHANNEL_CAPACITY: usize = 4096;

pub async fn run_feed(
    bootstrap: String,
    updates: broadcast::Sender<FeedAlert>,
    ready: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let group = format!("beacon-api-{}", uuid::Uuid::now_v7().simple());
    let consumer: StreamConsumer = kafka::base_config(&bootstrap)
        .set("group.id", &group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "latest")
        .set("fetch.wait.max.ms", "5")
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "1000")
        .create()?;
    consumer.subscribe(&[ALERTS_UPDATES])?;
    info!(%group, "api feed consumer started");
    let mut assigned = false;
    loop {
        let msg = tokio::select! {
            _ = shutdown.changed() => break,
            m = tokio::time::timeout(Duration::from_millis(200), consumer.recv()) => m,
        };
        if !assigned && consumer.assignment().map(|a| a.count() > 0).unwrap_or(false) {
            assigned = true;
            let _ = ready.send(true);
            info!("api feed consumer assigned; ready");
        }
        match msg {
            Err(_) => continue,
            Ok(Err(e)) => {
                warn!(error = %e, "alerts.updates receive error");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Ok(Ok(m)) => match m.payload().map(serde_json::from_slice::<FeedAlert>) {
                // No receivers is fine: nobody is watching right now.
                Some(Ok(a)) => {
                    let _ = updates.send(a);
                }
                Some(Err(e)) => warn!(error = %e, "undecodable alert update; skipped"),
                None => {}
            },
        }
    }
    let _ = ready.send(false);
    Ok(())
}
