//! Getting events in (spec §5.1).
//!
//! - [`http`]: `POST /ingest/{source}` accepts any JSON and answers `202 {id}`
//!   only after the broker has the event (`429` when saturated, `503` when the
//!   broker fails).
//! - [`kafka_source`]: consumes external Kafka topics described by
//!   `config/sources/*.yaml` and commits upstream offsets only after a durable
//!   publish.
//! - [`kafka`]: shared Kafka helpers (topic names, client config, topic setup).

pub mod http;
pub mod kafka;
pub mod kafka_source;
pub mod publisher;

pub use publisher::{EventPublisher, PublishError};
