//! Publishing events to `events.raw` with a durable-ack contract.

use std::time::Duration;

use beacon_core::Event;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use crate::kafka::EVENTS_RAW;

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    /// Local queue is full: back off (HTTP 429).
    #[error("broker queue full")]
    Saturated,
    /// The event is larger than the broker accepts (HTTP 413).
    #[error("event too large for the broker")]
    TooLarge,
    /// The broker failed or the outcome is unknown (HTTP 503).
    #[error("broker error: {0}")]
    Broker(String),
}

#[derive(Clone)]
pub struct EventPublisher {
    producer: FutureProducer,
    topic: String,
    timeout: Duration,
}

impl EventPublisher {
    pub fn new(producer: FutureProducer) -> Self {
        EventPublisher {
            producer,
            topic: EVENTS_RAW.to_string(),
            timeout: Duration::from_secs(5),
        }
    }

    /// Resolves only after the broker acknowledged the write.
    pub async fn publish(&self, event: &Event) -> Result<(), PublishError> {
        let payload = serde_json::to_vec(event).map_err(|e| PublishError::Broker(e.to_string()))?;
        let record = FutureRecord::to(&self.topic).key(&event.id).payload(&payload);
        match self.producer.send(record, Timeout::After(self.timeout)).await {
            Ok(_) => Ok(()),
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => Err(PublishError::Saturated),
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge), _)) => Err(PublishError::TooLarge),
            Err((e, _)) => Err(PublishError::Broker(e.to_string())),
        }
    }

    pub fn producer(&self) -> &FutureProducer {
        &self.producer
    }
}
