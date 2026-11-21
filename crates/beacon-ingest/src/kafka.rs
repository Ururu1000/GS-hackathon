//! Kafka plumbing shared by every role.

use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::StreamConsumer;
use rdkafka::producer::FutureProducer;
use rdkafka::types::RDKafkaErrorCode;
use tracing::info;

/// Raw events from every source, keyed by event id.
pub const EVENTS_RAW: &str = "events.raw";
/// Rule matches, keyed by incident fingerprint (rule + dedup tuple).
pub const ALERTS_MATCHES: &str = "alerts.matches";
/// Full alert snapshots for the live feed, keyed by alert id.
pub const ALERTS_UPDATES: &str = "alerts.updates";
/// Payloads that could not be decoded, with the reason in a header.
pub const EVENTS_QUARANTINE: &str = "events.quarantine";

/// Fixed for the demo (spec §2.4): changing it remaps keys.
pub const PARTITIONS: i32 = 24;

pub fn bootstrap_from_env() -> String {
    std::env::var("BEACON_KAFKA").unwrap_or_else(|_| "localhost:19092".to_string())
}

pub fn base_config(bootstrap: &str) -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", bootstrap);
    c.set("client.id", "beacon");
    // Docker publishes the broker on IPv4 only; skip the ::1 attempt for `localhost`.
    c.set("broker.address.family", "v4");
    c
}

/// Idempotent, acks=all producer with a short linger for batching.
pub fn producer(bootstrap: &str) -> Result<FutureProducer> {
    base_config(bootstrap)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("linger.ms", "2")
        .set("batch.num.messages", "10000")
        .set("compression.type", "lz4")
        .set("queue.buffering.max.messages", "500000")
        .set("message.timeout.ms", "10000")
        .create()
        .context("creating Kafka producer")
}

/// A consumer that only commits offsets we explicitly store after processing
/// (at-least-once; M1 replaces this with fenced checkpoints for the aggregator).
pub fn consumer(bootstrap: &str, group: &str, topics: &[&str], from_beginning: bool) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = consumer_config(bootstrap, group, from_beginning)
        .create()
        .context("creating Kafka consumer")?;
    rdkafka::consumer::Consumer::subscribe(&consumer, topics).context("subscribing")?;
    Ok(consumer)
}

/// The settings behind [`consumer`], for callers that add their own (e.g. a
/// Kafka source's TLS/SASL settings) before creating it.
pub fn consumer_config(bootstrap: &str, group: &str, from_beginning: bool) -> ClientConfig {
    let mut c = base_config(bootstrap);
    c
        .set("group.id", group)
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "500")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", if from_beginning { "earliest" } else { "latest" })
        .set("isolation.level", "read_committed")
        .set("fetch.wait.max.ms", "5")
        .set("partition.assignment.strategy", "cooperative-sticky")
        // Detect a crashed member in seconds, not the 45 s default, so its
        // partitions move to a live replica quickly.
        .set("session.timeout.ms", "10000")
        .set("heartbeat.interval.ms", "1000");
    c
}

/// Create topics with `partitions` if they don't exist (idempotent).
pub async fn ensure_named_topics(bootstrap: &str, names: &[String], partitions: i32) -> Result<()> {
    if names.is_empty() {
        return Ok(());
    }
    let admin: AdminClient<DefaultClientContext> = base_config(bootstrap).create().context("creating Kafka admin client")?;
    let topics: Vec<NewTopic> = names.iter().map(|t| NewTopic::new(t, partitions, TopicReplication::Fixed(1))).collect();
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    for r in admin.create_topics(&topics, &opts).await.context("creating topics")? {
        match r {
            Ok(t) => info!(topic = %t, partitions, "created topic"),
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((t, e)) => anyhow::bail!("creating topic {t}: {e}"),
        }
    }
    Ok(())
}

/// Create the internal topics if they don't exist (idempotent).
pub async fn ensure_topics(bootstrap: &str) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> =
        base_config(bootstrap).create().context("creating Kafka admin client")?;
    let topics: Vec<NewTopic> = [EVENTS_RAW, ALERTS_MATCHES, ALERTS_UPDATES, EVENTS_QUARANTINE]
        .iter()
        .map(|t| {
            NewTopic::new(t, PARTITIONS, TopicReplication::Fixed(1))
                .set("retention.ms", "86400000")
        })
        .collect();
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let results = admin.create_topics(&topics, &opts).await.context("creating topics")?;
    for r in results {
        match r {
            Ok(t) => info!(topic = %t, partitions = PARTITIONS, "created topic"),
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((t, e)) => anyhow::bail!("creating topic {t}: {e}"),
        }
    }
    Ok(())
}
