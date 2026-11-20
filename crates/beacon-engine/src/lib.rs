//! The stateful middle of the pipeline (part a):
//!
//! ```text
//! events.raw ─► matcher ─► alerts.matches ─► aggregator ─► Postgres alerts + outbox
//!                                                             └─► relay ─► alerts.updates
//! ```
//!
//! M0 is at-least-once end to end; M1 adds fenced checkpoints (exact counts
//! across rebalances) and matcher transactions.

pub mod aggregator;
pub mod feed;
pub mod matcher;
pub mod relay;

/// Postgres channel the aggregator notifies after committing outbox rows.
pub const OUTBOX_CHANNEL: &str = "beacon_outbox";
