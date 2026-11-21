//! Transport-neutral core of Beacon.
//!
//! Everything here is pure: no Kafka, no Postgres, no HTTP. The matcher, the
//! rule test endpoint and the unit tests all use the same code, so a rule
//! behaves identically in preview and in production.

pub mod condition;
pub mod config;
pub mod event;
pub mod mapping;
pub mod matcher;
pub mod model;
pub mod path;
pub mod rule;
pub mod severity;
pub mod template;

pub use condition::{Condition, ConditionSpec, Op};
pub use config::{ConfigSnapshot, load_config_dir};
pub use event::Event;
pub use matcher::{MatchKind, MatchRecord};
pub use model::{FeedAlert, NotifyPayload};
pub use path::Path;
