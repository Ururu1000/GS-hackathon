//! External Kafka topics as a source (spec §5.1).
//!
//! A new source *instance* is a YAML file in `config/sources/`:
//!
//! ```yaml
//! id: kafka:workflows            # becomes Event.source
//! type: kafka
//! bootstrap: localhost:19092     # optional, defaults to BEACON_KAFKA
//! topics: [workflow-events]
//! group: beacon-src-workflows    # optional
//! start: earliest                # first-start offset policy: earliest | latest
//! ```
//!
//! The upstream offset is stored only after the event is durably in
//! `events.raw`. Undecodable payloads go to `events.quarantine`.

use std::path::Path as FsPath;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use beacon_core::Event;
use rdkafka::Message;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;
use serde::Deserialize;
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::kafka::{self, EVENTS_QUARANTINE};
use crate::publisher::{EventPublisher, PublishError};

#[derive(Debug, Clone, Deserialize)]
pub struct SourceSpec {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub bootstrap: Option<String>,
    #[serde(default)]
    pub topics: Vec<String>,
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default = "earliest")]
    pub start: String,
}

fn earliest() -> String {
    "earliest".into()
}

pub fn load_sources(dir: &FsPath) -> Result<Vec<SourceSpec>> {
    let specs: Vec<SourceSpec> = beacon_core::config::load_yaml_files(dir)?;
    for s in &specs {
        if s.kind != "kafka" {
            bail!("source `{}`: unsupported type `{}` (M0 supports kafka; HTTP needs no config)", s.id, s.kind);
        }
        if s.topics.is_empty() {
            bail!("source `{}`: `topics` must not be empty", s.id);
        }
        if !matches!(s.start.as_str(), "earliest" | "latest") {
            bail!("source `{}`: `start` must be earliest or latest", s.id);
        }
    }
    Ok(specs)
}

pub async fn run_kafka_source(spec: SourceSpec, default_bootstrap: String, publisher: EventPublisher, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let bootstrap = spec.bootstrap.clone().unwrap_or(default_bootstrap);
    let group = spec.group.clone().unwrap_or_else(|| format!("beacon-src-{}", spec.id.replace(':', "-")));
    let topics: Vec<&str> = spec.topics.iter().map(String::as_str).collect();
    let consumer = kafka::consumer(&bootstrap, &group, &topics, spec.start == "earliest")
        .with_context(|| format!("source {}", spec.id))?;
    info!(source = %spec.id, ?topics, %group, "kafka source started");
    loop {
        let msg = tokio::select! {
            _ = shutdown.changed() => break,
            m = consumer.recv() => m,
        };
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                warn!(source = %spec.id, error = %e, "kafka source receive error");
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let id = format!("{}:{}:{}:{}", spec.id, msg.topic(), msg.partition(), msg.offset());
        let parsed = msg
            .payload()
            .ok_or_else(|| "empty payload".to_string())
            .and_then(|p| serde_json::from_slice::<serde_json::Value>(p).map_err(|e| e.to_string()));
        let mut backoff = Duration::from_millis(100);
        loop {
            let res = match &parsed {
                Ok(raw) => publisher.publish(&Event::new(id.clone(), spec.id.clone(), raw.clone())).await,
                Err(reason) => quarantine(&publisher, &id, msg.payload().unwrap_or_default(), reason).await,
            };
            match res {
                Ok(()) => break,
                Err(e) => {
                    // Never skip: the upstream offset is not stored until the publish succeeds.
                    warn!(source = %spec.id, error = %e, "publish failed, retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                    if *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
        if let Err(e) = consumer.store_offset_from_message(&msg) {
            error!(source = %spec.id, error = %e, "store offset failed");
        }
    }
    let _ = consumer.commit_consumer_state(CommitMode::Sync);
    Ok(())
}

async fn quarantine(publisher: &EventPublisher, id: &str, payload: &[u8], reason: &str) -> Result<(), PublishError> {
    warn!(%id, %reason, "quarantining undecodable payload");
    let headers = OwnedHeaders::new().insert(Header { key: "reason", value: Some(reason) });
    let record = FutureRecord::to(EVENTS_QUARANTINE).key(id).payload(payload).headers(headers);
    publisher
        .producer()
        .send(record, Timeout::After(Duration::from_secs(5)))
        .await
        .map(|_| ())
        .map_err(|(e, _)| PublishError::Broker(e.to_string()))
}
