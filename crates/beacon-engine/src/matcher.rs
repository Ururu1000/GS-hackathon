//! Matcher role: `events.raw` → normalize + match → `alerts.matches`.
//!
//! Stateless. Matches are keyed by fingerprint so every match for one
//! incident lands on the same aggregator partition.
//!
//! **Exactly-once (default):** matches are produced in a Kafka transaction
//! that also commits the consumed `events.raw` offsets (consume→produce).
//! Aggregators read `read_committed`, so a crash or rebalance mid-batch
//! leaves no visible duplicate: the aborted batch is simply redone.
//! `MATCHER_TRANSACTIONS=false` falls back to at-least-once (offsets stored
//! after the matches are acknowledged).
//!
//! Duplicate event ids (an HTTP retry with the same `Idempotency-Key`, a
//! Kafka source re-publish) are dropped within a bounded recent-id window per
//! matcher; events.raw is keyed by event id, so duplicates meet on one partition.

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

/// `config` is live: rule edits published through the API arrive here without
/// a restart (see `config_store::watch_config`). `samples`, when given,
/// receives a few recent events per schema for the test panel and discovery.
pub async fn run_matcher(
    bootstrap: String,
    config: watch::Receiver<Arc<ConfigSnapshot>>,
    samples: Option<SampleSink>,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    if std::env::var("MATCHER_TRANSACTIONS").map(|v| v != "false").unwrap_or(true) {
        run_transactional(bootstrap, config, samples, shutdown).await
    } else {
        run_at_least_once(bootstrap, config, samples, shutdown).await
    }
}

/// Bounded window of recently seen event ids (duplicate suppression).
pub struct RecentIds {
    set: std::collections::HashSet<String>,
    order: std::collections::VecDeque<(Instant, String)>,
    ttl: Duration,
    cap: usize,
}

impl RecentIds {
    pub fn new(ttl: Duration, cap: usize) -> Self {
        RecentIds { set: Default::default(), order: Default::default(), ttl, cap }
    }

    /// True if `id` was seen within the window (and records it otherwise).
    /// Only ids that can legitimately repeat are tracked: client-supplied
    /// `Idempotency-Key` ids (`http:...`) and Kafka-source ids (`kafka:...`).
    /// Server-generated UUIDs never repeat, which keeps the window small.
    pub fn seen(&mut self, id: &str) -> bool {
        if !(id.starts_with("http:") || id.starts_with("kafka:")) {
            return false;
        }
        let now = Instant::now();
        while let Some((t, _)) = self.order.front() {
            if now.duration_since(*t) > self.ttl || self.order.len() > self.cap {
                let (_, old) = self.order.pop_front().unwrap();
                self.set.remove(&old);
            } else {
                break;
            }
        }
        if self.set.contains(id) {
            return true;
        }
        self.set.insert(id.to_string());
        self.order.push_back((now, id.to_string()));
        false
    }
}

const RECENT_ID_TTL: Duration = Duration::from_secs(15 * 60);
const RECENT_ID_CAP: usize = 100_000;
const TXN_TIMEOUT: Duration = Duration::from_secs(30);

async fn run_transactional(
    bootstrap: String,
    config: watch::Receiver<Arc<ConfigSnapshot>>,
    samples: Option<SampleSink>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    use rdkafka::producer::Producer;
    let consumer: StreamConsumer = kafka::base_config(&bootstrap)
        .set("group.id", GROUP)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed")
        .set("fetch.wait.max.ms", "5")
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "1000")
        .create()?;
    consumer.subscribe(&[EVENTS_RAW])?;
    let producer: FutureProducer = kafka::base_config(&bootstrap)
        .set("transactional.id", format!("beacon-matcher-{}", uuid::Uuid::now_v7().simple()))
        // Commits happen every ~20 ms; a short timeout bounds how long a crashed
        // matcher's open transaction can stall read_committed readers.
        .set("transaction.timeout.ms", "10000")
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("linger.ms", "2")
        .set("compression.type", "lz4")
        .set("queue.buffering.max.messages", "500000")
        .create()?;
    tokio::task::block_in_place(|| producer.init_transactions(TXN_TIMEOUT))?;
    let res = transactional_loop(&consumer, &producer, config, samples, &mut shutdown).await;
    if res.is_err() {
        // Don't leave an open transaction stalling read_committed readers.
        let _ = tokio::task::block_in_place(|| producer.abort_transaction(TXN_TIMEOUT));
    }
    res
}

async fn transactional_loop(
    consumer: &StreamConsumer,
    producer: &FutureProducer,
    config: watch::Receiver<Arc<ConfigSnapshot>>,
    samples: Option<SampleSink>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    use rdkafka::producer::Producer;
    {
        let c = config.borrow();
        info!(revision = c.revision, rules = c.rules.len(), schemas = c.mappings.len(), "matcher started (transactional)");
    }
    let mut recent = RecentIds::new(RECENT_ID_TTL, RECENT_ID_CAP);
    // Transaction commit cadence: longer means fewer blocking commits (more
    // throughput per worker) at the cost of a higher latency floor.
    let txn_flush = Duration::from_millis(std::env::var("MATCHER_TXN_FLUSH_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(50));
    let mut offsets: HashMap<i32, i64> = HashMap::new();
    let (mut in_txn, mut in_batch, mut batch_start) = (false, 0usize, Instant::now());
    let (mut seen, mut matched, mut dupes, mut last_stats) = (0u64, 0u64, 0u64, Instant::now());
    loop {
        if last_stats.elapsed() >= Duration::from_secs(5) {
            let secs = last_stats.elapsed().as_secs_f64();
            info!(events_per_s = (seen as f64 / secs) as u64, matches_per_s = (matched as f64 / secs) as u64, duplicates = dupes, "matcher throughput");
            (seen, matched, dupes, last_stats) = (0, 0, 0, Instant::now());
        }
        let msg = tokio::select! {
            _ = shutdown.changed() => break,
            m = tokio::time::timeout(FLUSH_INTERVAL, consumer.recv()) => m,
        };
        match msg {
            Err(_) => {}
            Ok(Err(e)) => {
                warn!(error = %e, "matcher receive error");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(Ok(m)) => {
                if !in_txn {
                    producer.begin_transaction()?;
                    (in_txn, in_batch, batch_start) = (true, 0, Instant::now());
                }
                seen += 1;
                in_batch += 1;
                offsets.insert(m.partition(), m.offset());
                match m.payload().map(serde_json::from_slice::<Event>) {
                    Some(Ok(event)) if recent.seen(&event.id) => dupes += 1,
                    Some(Ok(event)) => {
                        let snapshot = config.borrow().clone();
                        let (event, matches, _diags) = snapshot.process(event);
                        if let Some(s) = &samples {
                            s.offer(&event);
                        }
                        matched += matches.len() as u64;
                        for rec in matches {
                            let (key, payload) = encode_bounded(rec)?;
                            // The transaction commit waits for delivery.
                            drop(send(producer, &key, payload).await?);
                        }
                    }
                    Some(Err(e)) => error!(offset = m.offset(), error = %e, "undecodable event in events.raw; skipped"),
                    None => {}
                }
            }
        }
        if in_txn && (in_batch >= FLUSH_EVERY || batch_start.elapsed() >= txn_flush) {
            commit_txn(consumer, producer, &mut offsets)?;
            in_txn = false;
        }
    }
    if in_txn {
        commit_txn(consumer, producer, &mut offsets)?;
    }
    Ok(())
}

/// Commit the produced matches together with the consumed offsets. On any
/// error the transaction is aborted and the role restarts from the last
/// committed offsets, so nothing is lost or doubled.
fn commit_txn(consumer: &StreamConsumer, producer: &FutureProducer, offsets: &mut HashMap<i32, i64>) -> Result<()> {
    use rdkafka::producer::Producer;
    use rdkafka::{Offset, TopicPartitionList};
    let mut tpl = TopicPartitionList::new();
    for (p, o) in offsets.drain() {
        tpl.add_partition_offset(EVENTS_RAW, p, Offset::Offset(o + 1))?;
    }
    let res = tokio::task::block_in_place(|| -> Result<()> {
        let meta = consumer.group_metadata().ok_or_else(|| anyhow::anyhow!("no consumer group metadata"))?;
        producer.send_offsets_to_transaction(&tpl, &meta, TXN_TIMEOUT)?;
        producer.commit_transaction(TXN_TIMEOUT)?;
        Ok(())
    });
    if let Err(e) = res {
        let _ = tokio::task::block_in_place(|| producer.abort_transaction(TXN_TIMEOUT));
        anyhow::bail!("matcher transaction failed (aborted, will redo from committed offsets): {e:#}");
    }
    Ok(())
}

async fn run_at_least_once(
    bootstrap: String,
    config: watch::Receiver<Arc<ConfigSnapshot>>,
    samples: Option<SampleSink>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let consumer = kafka::consumer(&bootstrap, GROUP, &[EVENTS_RAW], true)?;
    let producer = kafka::producer(&bootstrap)?;
    {
        let c = config.borrow();
        info!(revision = c.revision, rules = c.rules.len(), schemas = c.mappings.len(), "matcher started");
    }

    let mut recent = RecentIds::new(RECENT_ID_TTL, RECENT_ID_CAP);
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
                    Some(Ok(event)) if recent.seen(&event.id) => {}
                    Some(Ok(event)) => {
                        let snapshot = config.borrow().clone();
                        let (event, matches, diags) = snapshot.process(event);
                        if let Some(s) = &samples {
                            s.offer(&event);
                        }
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

/// Rate-limited, lossy sampling of recent events into `event_samples`: at most
/// one event per schema every [`SAMPLE_EVERY`], written off the hot path.
#[derive(Clone)]
pub struct SampleSink {
    tx: tokio::sync::mpsc::Sender<Event>,
    last: Arc<std::sync::Mutex<HashMap<String, Instant>>>,
}

pub const SAMPLE_EVERY: Duration = Duration::from_secs(2);
const MAX_TRACKED_SCHEMAS: usize = 10_000;

const SAMPLES_KEPT: i64 = 2000;

impl SampleSink {
    /// Spawn the writer task.
    pub fn start(pool: sqlx::PgPool) -> SampleSink {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(256);
        tokio::spawn(async move {
            let mut since_prune = 0usize;
            while let Some(e) = rx.recv().await {
                let raw = beacon_core::matcher::bounded_to(beacon_core::matcher::redact(e.raw), beacon_core::model::MAX_NOTIFY_RAW);
                let fields = beacon_core::matcher::bounded_to(serde_json::Value::Object(e.fields), 4096);
                let res = sqlx::query(
                    "INSERT INTO event_samples (event_id, source, schema, received_at, fields, raw) VALUES ($1, $2, $3, $4, $5, $6)",
                )
                .bind(&e.id)
                .bind(&e.source)
                .bind(&e.schema)
                .bind(e.ingested_at)
                .bind(strip_nul(fields))
                .bind(strip_nul(raw))
                .execute(&pool)
                .await;
                if let Err(err) = res {
                    warn!(error = %err, "event sample not stored");
                }
                since_prune += 1;
                if since_prune >= 100 {
                    since_prune = 0;
                    let _ = sqlx::query("DELETE FROM event_samples WHERE id < (SELECT max(id) FROM event_samples) - $1")
                        .bind(SAMPLES_KEPT)
                        .execute(&pool)
                        .await;
                }
            }
        });
        SampleSink { tx, last: Default::default() }
    }

    pub fn offer(&self, e: &Event) {
        let now = Instant::now();
        {
            let mut last = self.last.lock().unwrap();
            if last.get(&e.schema).is_some_and(|t| now.duration_since(*t) < SAMPLE_EVERY) {
                return;
            }
            // Senders control unknown shapes, so the map is capped: past the
            // cap, stale entries are dropped (worst case: a few extra samples).
            if last.len() >= MAX_TRACKED_SCHEMAS {
                last.retain(|_, t| now.duration_since(*t) < SAMPLE_EVERY);
                if last.len() >= MAX_TRACKED_SCHEMAS {
                    return;
                }
            }
            last.insert(e.schema.clone(), now);
        }
        // Lossy by design: never block matching on sample storage.
        let _ = self.tx.try_send(e.clone());
    }
}

/// Postgres jsonb rejects \u0000; samples are display-only, so drop NULs.
fn strip_nul(v: serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::String(s) => serde_json::Value::String(s.replace('\0', "")),
        serde_json::Value::Array(a) => serde_json::Value::Array(a.into_iter().map(strip_nul).collect()),
        serde_json::Value::Object(o) => serde_json::Value::Object(o.into_iter().map(|(k, v)| (k.replace('\0', ""), strip_nul(v))).collect()),
        other => other,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recent_ids_window() {
        let mut r = RecentIds::new(Duration::from_secs(60), 3);
        assert!(!r.seen("http:a"));
        assert!(r.seen("http:a"));
        for id in ["http:b", "http:c", "kafka:d", "kafka:e"] {
            assert!(!r.seen(id));
        }
        assert!(!r.seen("http:a"), "evicted past the cap");
        assert!(!r.seen("0196-uuid") && !r.seen("0196-uuid"), "server ids are not tracked");
    }
}
