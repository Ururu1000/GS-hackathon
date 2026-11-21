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
//! security:                      # optional, for an external broker
//!   protocol: sasl_ssl           # plaintext | ssl | sasl_plaintext | sasl_ssl
//!   sasl_mechanism: SCRAM-SHA-512
//!   username_env: TRADES_KAFKA_USER      # secrets come from the environment,
//!   password_env: TRADES_KAFKA_PASSWORD  # never from the YAML file
//!   ca_location: /etc/ssl/certs/ca.pem   # optional
//! ```
//!
//! Source topics may not be Beacon's own internal topics: consuming
//! `events.raw` as a source would republish every event to itself forever.
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

use crate::kafka::{self, ALERTS_MATCHES, ALERTS_UPDATES, EVENTS_QUARANTINE, EVENTS_RAW};
use crate::publisher::{EventPublisher, PublishError};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
    #[serde(default)]
    pub security: Option<SecuritySpec>,
}

/// TLS/SASL settings for an external broker. Credentials are named by
/// environment variable so they never sit in the config directory.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecuritySpec {
    pub protocol: String,
    #[serde(default)]
    pub sasl_mechanism: Option<String>,
    #[serde(default)]
    pub username_env: Option<String>,
    #[serde(default)]
    pub password_env: Option<String>,
    #[serde(default)]
    pub ca_location: Option<String>,
}

impl SecuritySpec {
    fn validate(&self, source: &str) -> Result<()> {
        let sasl = match self.protocol.as_str() {
            "plaintext" | "ssl" => false,
            "sasl_plaintext" | "sasl_ssl" => true,
            other => bail!("source `{source}`: security.protocol `{other}` must be plaintext, ssl, sasl_plaintext or sasl_ssl"),
        };
        if sasl {
            match self.sasl_mechanism.as_deref() {
                Some("PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512") => {}
                other => bail!("source `{source}`: sasl_mechanism must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512 (got {other:?})"),
            }
            if self.username_env.is_none() || self.password_env.is_none() {
                bail!("source `{source}`: SASL needs username_env and password_env");
            }
        } else if self.sasl_mechanism.is_some() || self.username_env.is_some() || self.password_env.is_some() {
            bail!("source `{source}`: SASL settings need protocol sasl_plaintext or sasl_ssl");
        }
        for var in [&self.username_env, &self.password_env].into_iter().flatten() {
            if var.is_empty() || !var.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                bail!("source `{source}`: `{var}` is not an environment variable name");
            }
        }
        Ok(())
    }

    /// Adds these settings to a consumer config, reading the credentials now.
    fn apply(&self, source: &str, config: &mut rdkafka::ClientConfig) -> Result<()> {
        config.set("security.protocol", self.protocol.as_str());
        if let Some(m) = &self.sasl_mechanism {
            config.set("sasl.mechanism", m.as_str());
        }
        for (key, var) in [("sasl.username", &self.username_env), ("sasl.password", &self.password_env)] {
            if let Some(var) = var {
                let value = std::env::var(var).with_context(|| format!("source `{source}`: environment variable {var} is not set"))?;
                config.set(key, value);
            }
        }
        if let Some(ca) = &self.ca_location {
            config.set("ssl.ca.location", ca.as_str());
        }
        Ok(())
    }
}

const INTERNAL_TOPICS: [&str; 4] = [EVENTS_RAW, ALERTS_MATCHES, ALERTS_UPDATES, EVENTS_QUARANTINE];

/// Kafka's own topic-name rule: 1–249 of `[A-Za-z0-9._-]`.
fn valid_topic(t: &str) -> bool {
    !t.is_empty() && t.len() <= 249 && t != "." && t != ".." && t.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// Source ids become `Event.source`, part of every event id and the default
/// consumer group, so they stay short and printable.
fn valid_source_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 128 && id.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, ':' | '.' | '_' | '-'))
}

fn earliest() -> String {
    "earliest".into()
}

pub fn load_sources(dir: &FsPath) -> Result<Vec<SourceSpec>> {
    let specs: Vec<SourceSpec> = beacon_core::config::load_yaml_files(dir)?;
    validate_sources(&specs)?;
    Ok(specs)
}

pub fn validate_sources(specs: &[SourceSpec]) -> Result<()> {
    let mut ids = std::collections::HashSet::new();
    let mut groups = std::collections::HashSet::new();
    for s in specs {
        if !valid_source_id(&s.id) {
            bail!("source id `{}` must be 1-128 characters of [A-Za-z0-9:._-]", s.id);
        }
        if !ids.insert(s.id.as_str()) {
            bail!("source `{}` is defined twice", s.id);
        }
        if s.kind != "kafka" {
            bail!("source `{}`: unsupported type `{}` (M0 supports kafka; HTTP needs no config)", s.id, s.kind);
        }
        if s.topics.is_empty() {
            bail!("source `{}`: `topics` must not be empty", s.id);
        }
        for t in &s.topics {
            if !valid_topic(t) {
                bail!("source `{}`: `{t}` is not a valid Kafka topic name", s.id);
            }
            // Only Beacon's own broker carries the internal topics.
            if s.bootstrap.is_none() && INTERNAL_TOPICS.contains(&t.as_str()) {
                bail!("source `{}`: `{t}` is an internal Beacon topic and cannot be a source", s.id);
            }
        }
        let group = group_id(s);
        if !valid_topic(&group) {
            bail!("source `{}`: consumer group `{group}` must be 1-249 characters of [A-Za-z0-9._-]", s.id);
        }
        // Two sources in one group would split each other's partitions.
        if !groups.insert((s.bootstrap.clone(), group.clone())) {
            bail!("source `{}`: consumer group `{group}` is already used by another source", s.id);
        }
        if !matches!(s.start.as_str(), "earliest" | "latest") {
            bail!("source `{}`: `start` must be earliest or latest", s.id);
        }
        if let Some(sec) = &s.security {
            sec.validate(&s.id)?;
        }
    }
    Ok(())
}

fn group_id(spec: &SourceSpec) -> String {
    spec.group.clone().unwrap_or_else(|| format!("beacon-src-{}", spec.id.replace(':', "-")))
}

pub async fn run_kafka_source(spec: SourceSpec, default_bootstrap: String, publisher: EventPublisher, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let bootstrap = spec.bootstrap.clone().unwrap_or(default_bootstrap);
    let group = group_id(&spec);
    let topics: Vec<&str> = spec.topics.iter().map(String::as_str).collect();
    let mut config = kafka::consumer_config(&bootstrap, &group, spec.start == "earliest");
    if let Some(sec) = &spec.security {
        sec.apply(&spec.id, &mut config)?;
    }
    let consumer: rdkafka::consumer::StreamConsumer =
        config.create().with_context(|| format!("source {}: creating Kafka consumer", spec.id))?;
    consumer.subscribe(&topics).with_context(|| format!("source {}: subscribing", spec.id))?;
    info!(source = %spec.id, ?topics, %group, "kafka source started");
    loop {
        // Collect a small batch, publish it concurrently, then store the
        // upstream offsets only after every event in it is durably written.
        let first = tokio::select! {
            _ = shutdown.changed() => break,
            m = consumer.recv() => m,
        };
        let mut batch: Vec<rdkafka::message::OwnedMessage> = Vec::with_capacity(SOURCE_BATCH);
        let mut bytes = 0usize;
        match first {
            Ok(m) => {
                bytes += m.payload_len();
                batch.push(m.detach());
            }
            Err(e) => {
                warn!(source = %spec.id, error = %e, "kafka source receive error");
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        }
        let deadline = tokio::time::Instant::now() + SOURCE_LINGER;
        while !batch_full(batch.len(), bytes) {
            match tokio::time::timeout_at(deadline, consumer.recv()).await {
                Ok(Ok(m)) => {
                    bytes += m.payload_len();
                    batch.push(m.detach());
                }
                _ => break,
            }
        }
        let results = futures::future::join_all(batch.iter().map(|m| forward(&spec.id, &publisher, m, shutdown.clone()))).await;
        if results.iter().any(|ok| !ok) {
            // Shutting down mid-retry: nothing from this batch is stored.
            return Ok(());
        }
        let mut last: std::collections::HashMap<(String, i32), i64> = std::collections::HashMap::new();
        for m in &batch {
            let e = last.entry((m.topic().to_string(), m.partition())).or_insert(m.offset());
            *e = (*e).max(m.offset());
        }
        for ((topic, partition), offset) in last {
            if let Err(e) = consumer.store_offset(&topic, partition, offset) {
                error!(source = %spec.id, error = %e, "store offset failed");
            }
        }
    }
    let _ = consumer.commit_consumer_state(CommitMode::Sync);
    Ok(())
}

const SOURCE_BATCH: usize = 500;
/// Payload bytes held per batch, so a producer of large messages can't pin
/// hundreds of MiB per source.
const SOURCE_BATCH_BYTES: usize = 8 * 1024 * 1024;
const SOURCE_LINGER: Duration = Duration::from_millis(10);

/// A batch closes at the message count or the byte budget, whichever comes first.
fn batch_full(count: usize, bytes: usize) -> bool {
    count >= SOURCE_BATCH || bytes >= SOURCE_BATCH_BYTES
}

/// Publish one upstream message (or quarantine it), retrying transient broker
/// errors. Returns false only when shutting down before it was written.
async fn forward(source_id: &str, publisher: &EventPublisher, msg: &rdkafka::message::OwnedMessage, shutdown: watch::Receiver<bool>) -> bool {
    let id = format!("{}:{}:{}:{}", source_id, msg.topic(), msg.partition(), msg.offset());
    let parsed = msg
        .payload()
        .ok_or_else(|| "empty payload".to_string())
        .and_then(|p| serde_json::from_slice::<serde_json::Value>(p).map_err(|e| e.to_string()));
    // Undecodable or oversized payloads go to quarantine; either way the
    // upstream offset is stored only after the broker acknowledged a write.
    let mut quarantine_reason: Option<String> = parsed.as_ref().err().cloned();
    let mut backoff = Duration::from_millis(100);
    loop {
        let res = match (&parsed, &quarantine_reason) {
            (_, Some(reason)) => quarantine(publisher, &id, msg.payload().unwrap_or_default(), reason).await,
            (Ok(raw), None) => publisher.publish(&Event::new(id.clone(), source_id.to_string(), raw.clone())).await,
            (Err(_), None) => unreachable!("parse errors always set a quarantine reason"),
        };
        match res {
            Ok(()) => return true,
            Err(PublishError::TooLarge) if quarantine_reason.is_none() => {
                let len = msg.payload().map_or(0, <[u8]>::len);
                quarantine_reason = Some(format!("too large for events.raw ({len} bytes)"));
            }
            Err(e) => {
                warn!(source = %source_id, error = %e, "publish failed, retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
                if *shutdown.borrow() {
                    return false;
                }
            }
        }
    }
}

/// Quarantined copies are capped so the quarantine write itself always fits.
const QUARANTINE_MAX: usize = 64 * 1024;

async fn quarantine(publisher: &EventPublisher, id: &str, payload: &[u8], reason: &str) -> Result<(), PublishError> {
    warn!(%id, %reason, "quarantining payload");
    let payload = &payload[..payload.len().min(QUARANTINE_MAX)];
    let headers = OwnedHeaders::new().insert(Header { key: "reason", value: Some(reason) });
    let record = FutureRecord::to(EVENTS_QUARANTINE).key(id).payload(payload).headers(headers);
    publisher
        .producer()
        .send(record, Timeout::After(Duration::from_secs(5)))
        .await
        .map(|_| ())
        .map_err(|(e, _)| PublishError::Broker(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(yaml: &str) -> SourceSpec {
        serde_saphyr::from_str(yaml).unwrap()
    }

    const OK: &str = "id: kafka:workflows\ntype: kafka\ntopics: [workflow-events]\n";

    #[test]
    fn batches_close_at_count_or_bytes() {
        assert!(!batch_full(1, 1024));
        assert!(batch_full(SOURCE_BATCH, 10));
        // Three ~1 MiB messages are fine; eight reach the 8 MiB budget.
        let mib = 1024 * 1024;
        assert!(!batch_full(3, 3 * mib));
        assert!(batch_full(8, 8 * mib));
    }

    #[test]
    fn accepts_the_seed_sources() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/sources");
        let specs = load_sources(&dir).unwrap();
        assert_eq!(specs.len(), 2);
    }

    #[test]
    fn rejects_internal_topics_on_the_local_broker() {
        let s = spec("id: kafka:loop\ntype: kafka\ntopics: [events.raw]\n");
        let err = validate_sources(&[s]).unwrap_err().to_string();
        assert!(err.contains("internal"), "{err}");
        // On someone else's broker the name is theirs to use.
        let s = spec("id: kafka:ext\ntype: kafka\nbootstrap: ext:9092\ntopics: [events.raw]\n");
        validate_sources(&[s]).unwrap();
    }

    #[test]
    fn rejects_duplicate_ids_and_groups() {
        let err = validate_sources(&[spec(OK), spec(OK)]).unwrap_err().to_string();
        assert!(err.contains("defined twice"), "{err}");
        let a = spec("id: a\ntype: kafka\ntopics: [t1]\ngroup: shared\n");
        let b = spec("id: b\ntype: kafka\ntopics: [t2]\ngroup: shared\n");
        let err = validate_sources(&[a, b]).unwrap_err().to_string();
        assert!(err.contains("already used"), "{err}");
    }

    #[test]
    fn rejects_bad_names_and_unknown_keys() {
        assert!(validate_sources(&[spec("id: \"bad id\"\ntype: kafka\ntopics: [t]\n")]).is_err());
        assert!(validate_sources(&[spec("id: a\ntype: kafka\ntopics: [\"bad/topic\"]\n")]).is_err());
        // A typo must not silently fall back to the local broker.
        assert!(serde_saphyr::from_str::<SourceSpec>("id: a\ntype: kafka\ntopics: [t]\nbootsrap: x:9092\n").is_err());
    }

    #[test]
    fn security_takes_credentials_from_the_environment_only() {
        let base = "id: kafka:ext\ntype: kafka\nbootstrap: ext:9092\ntopics: [t]\n";
        let ok = spec(&format!("{base}security: {{protocol: sasl_ssl, sasl_mechanism: SCRAM-SHA-512, username_env: SRC_USER, password_env: SRC_PASS}}\n"));
        validate_sources(&[ok]).unwrap();
        let no_creds = spec(&format!("{base}security: {{protocol: sasl_ssl, sasl_mechanism: PLAIN}}\n"));
        assert!(validate_sources(&[no_creds]).is_err());
        let bad_proto = spec(&format!("{base}security: {{protocol: tls}}\n"));
        assert!(validate_sources(&[bad_proto]).is_err());
        // There is no field for an inline password.
        assert!(serde_saphyr::from_str::<SourceSpec>(&format!("{base}security: {{protocol: sasl_ssl, password: hunter2}}\n")).is_err());
    }

    #[test]
    fn apply_fails_when_the_secret_is_missing() {
        let sec = SecuritySpec {
            protocol: "sasl_ssl".into(),
            sasl_mechanism: Some("PLAIN".into()),
            username_env: Some("BEACON_TEST_SURELY_UNSET_USER".into()),
            password_env: Some("BEACON_TEST_SURELY_UNSET_PASS".into()),
            ca_location: None,
        };
        let mut c = rdkafka::ClientConfig::new();
        let err = sec.apply("kafka:ext", &mut c).unwrap_err().to_string();
        assert!(err.contains("BEACON_TEST_SURELY_UNSET_USER"), "{err}");
    }
}
