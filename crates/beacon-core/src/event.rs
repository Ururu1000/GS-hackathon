//! The internal event envelope (spec §4.1).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    /// Stable id: kept across HTTP retries (Idempotency-Key) or derived from
    /// the upstream topic/partition/offset.
    pub id: String,
    /// Transport source instance, e.g. `http:alertmanager`, `kafka:workflows`.
    pub source: String,
    pub ingested_at: DateTime<Utc>,
    /// Detected schema, or `unknown:<structural signature>`. Filled by the matcher.
    #[serde(default)]
    pub schema: String,
    /// Typed canonical fields from the schema mapping (may be partial).
    #[serde(default)]
    pub fields: Map<String, Value>,
    /// The original payload, untouched.
    pub raw: Value,
}

impl Event {
    pub fn new(id: String, source: String, raw: Value) -> Self {
        Event {
            id,
            source,
            ingested_at: Utc::now(),
            schema: String::new(),
            fields: Map::new(),
            raw,
        }
    }
}
