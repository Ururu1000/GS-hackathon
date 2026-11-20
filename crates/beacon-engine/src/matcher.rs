//! Matcher role: `events.raw` → normalize + match → `alerts.matches`.
//!
//! Stateless. Matches are keyed by fingerprint so every match for one
//! incident lands on the same aggregator partition. Input offsets are stored
//! only after every match produced from them is acknowledged (at-least-once;
//! the stable `match_id` lets M1 deduplicate replays).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use beacon_core::{ConfigSnapshot, Event};
use beacon_ingest::kafka::{self, ALERTS_MATCHES, EVENTS_RAW};
use futures::future::join_all;
use rdkafka::Message;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

pub const GROUP: &str = "beacon-matcher";
const FLUSH_EVERY: usize = 2000;
const FLUSH_INTERVAL: Duration = Duration::from_millis(20);

pub async fn run_matcher(
    bootstrap: String,
    config: Arc<ConfigSnapshot>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let consumer = kafka::consumer(&bootstrap, GROUP, &[EVENTS_RAW], true)?;
    let producer = kafka::producer(&bootstrap)?;
    info!(rules = config.rules.len(), schemas = config.mappings.len(), "matcher started");

    let mut pending: Vec<DeliveryFuture> = Vec::new();
    let mut offsets: HashMap<i32, i64> = HashMap::new();
    let mut last_flush = Instant::now();
    let (mut seen, mut matched, mut last_stats) = (0u64, 0u64, Instant::now());
    loop {
        if last_stats.elapsed() >= Duration::from_secs(5) {
            let secs = last_stats.elapsed().as_secs_f64();
            info!(events_per_s = (seen as f64 / secs) as u64, matches_per_s = (matched as f64 / secs) as u64, "matcher throughput");
            (seen, matched, last_stats) = (0, 0, Instant::now());
        }
        let recv = tokio::time::timeout(FLUSH_INTERVAL, consumer.recv());
        let msg = tokio::select! {
            _ = shutdown.changed() => break,
            m = recv => m,
        };
        match msg {
            Err(_) => {} // idle tick: fall through to flush
            Ok(Err(e)) => {
                warn!(error = %e, "matcher receive error");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(Ok(m)) => {
                seen += 1;
                offsets.insert(m.partition(), m.offset());
                match m.payload().map(serde_json::from_slice::<Event>) {
                    Some(Ok(event)) => {
                        let (_, matches, diags) = config.process(event);
                        if !diags.is_empty() {
                            debug!(?diags, "mapping diagnostics");
                        }
                        matched += matches.len() as u64;
                        for rec in matches {
                            let payload = encode_bounded(rec)?;
                            let (key, payload) = payload;
                            pending.push(send(&producer, &key, payload).await?);
                        }
                    }
                    Some(Err(e)) => error!(offset = m.offset(), error = %e, "undecodable event in events.raw; skipped"),
                    None => {}
                }
            }
        }
        if !offsets.is_empty() && (pending.len() >= FLUSH_EVERY || last_flush.elapsed() >= FLUSH_INTERVAL) {
            flush(&consumer, &mut pending, &mut offsets).await?;
            last_flush = Instant::now();
        }
    }
    flush(&consumer, &mut pending, &mut offsets).await?;
    let _ = consumer.commit_consumer_state(CommitMode::Sync);
    Ok(())
}

/// Stay well under the broker's 1 MiB record limit, including JSON escaping.
const MAX_MATCH_BYTES: usize = 900 * 1024;

/// Serialize a match; if escaping still makes it too large, drop the copied
/// card fields and key preview (the fingerprint is already computed from the
/// full key, so dedup is unaffected) and say so in the diagnostics.
fn encode_bounded(mut rec: beacon_core::MatchRecord) -> Result<(String, Vec<u8>)> {
    let payload = serde_json::to_vec(&rec)?;
    if payload.len() <= MAX_MATCH_BYTES {
        return Ok((rec.fingerprint, payload));
    }
    warn!(rule = %rec.rule_id, bytes = payload.len(), "match too large; dropping copied fields");
    rec.fields.clear();
    rec.key.clear();
    rec.event = None;
    rec.diagnostics = vec![format!("match was {} bytes; card fields dropped", payload.len())];
    let payload = serde_json::to_vec(&rec)?;
    // What is left is ids, two rendered strings capped at 8 KiB and the rule's
    // configured route; if even that is too large the config is broken, so fail
    // loudly rather than let the producer reject it.
    anyhow::ensure!(payload.len() <= MAX_MATCH_BYTES, "match for rule {} is {} bytes even without fields", rec.rule_id, payload.len());
    Ok((rec.fingerprint.clone(), payload))
}

/// Enqueue without blocking; if the local queue is full, wait and retry. Any
/// other producer error fails the role, so the supervisor restarts it from the
/// last stored offset. (Match size is bounded in beacon-core, so a size
/// rejection cannot be caused by a valid event.)
async fn send(producer: &FutureProducer, key: &str, payload: Vec<u8>) -> Result<DeliveryFuture> {
    loop {
        match producer.send_result(FutureRecord::to(ALERTS_MATCHES).key(key).payload(&payload)) {
            Ok(f) => return Ok(f),
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                warn!("producer queue full; backing off");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err((e, _)) => anyhow::bail!("producing match {key} ({} bytes): {e}", payload.len()),
        }
    }
}

/// Wait for all produced matches, then store the input offsets they came from.
/// A failed delivery returns an error without storing offsets: the role
/// restarts from the last committed offset and reprocesses the events.
async fn flush(consumer: &StreamConsumer, pending: &mut Vec<DeliveryFuture>, offsets: &mut HashMap<i32, i64>) -> Result<()> {
    let results = join_all(pending.drain(..)).await;
    let failed = results
        .iter()
        .filter(|r| !matches!(r, Ok(Ok(_))))
        .count();
    if failed > 0 {
        offsets.clear();
        anyhow::bail!("{failed} match deliveries failed; restarting from the last committed offset");
    }
    for (partition, offset) in offsets.drain() {
        if let Err(e) = consumer.store_offset(EVENTS_RAW, partition, offset) {
            warn!(partition, error = %e, "store offset failed");
        }
    }
    Ok(())
}
