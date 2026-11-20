//! Per-person channel preferences and time windows (SPEC §4.4), M4.
//!
//! Routing precedence (SPEC §4.4):
//! 1. Route `channels:` given → those (union across routes reaching the same person).
//! 2. Otherwise the person's FIRST matching preference rule,
//! 3. then the team default (`Team::default_channels`),
//! 4. then `[ui]` (no job).
//!
//! The decision is made once, at routing time, and frozen on the job (`route_info`).
//!
//! YAML shape (`config/people/<id>.yaml`, `preferences:` list, first match wins):
//! ```yaml
//! preferences:
//!   - match: { severity: critical, service: payments-api }   # all keys optional, AND-ed
//!     channels: [sms]
//!     when: { between: "08:00-20:00", tz: Europe/London }    # optional, half-open [start, end)
//!     otherwise: [email]                                     # used when `when` is false; optional
//!   - match: { severity: warning }
//!     channels: [email]
//!   - default: [ui]                                          # matches everything
//! ```
//! A rule with `when` and no `otherwise` does not match outside its window (evaluation goes on to
//! the next rule). `match` keys: `severity`, `service`, `team`, `rule`, `environment`; a key whose
//! context value is missing compares false (same as rules).

use chrono::{DateTime, NaiveTime, Utc};
use serde::Serialize;

use crate::config::{ConfigError, Person, Team};

/// Facts about the alert a preference can match on. Built by the router from `NotifyPayload`
/// plus the optional `context` object of the payload (board #100/#109).
#[derive(Debug, Clone, Default)]
pub struct RoutingContext {
    pub severity: String,
    pub team: String,
    pub rule_id: String,
    pub service: Option<String>,
    pub environment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PrefMatch {
    pub severity: Option<String>,
    pub service: Option<String>,
    pub team: Option<String>,
    pub rule: Option<String>,
    pub environment: Option<String>,
}

/// Half-open local-time window `[start, end)` in `tz`. `start > end` wraps past midnight;
/// `start == end` is rejected at load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeWindow {
    pub start: NaiveTime,
    pub end: NaiveTime,
    pub tz: chrono_tz::Tz,
}

impl TimeWindow {
    /// Parse `between: "HH:MM-HH:MM"` + `tz: <IANA name>`.
    pub fn parse(between: &str, tz: &str) -> Result<Self, ConfigError> {
        let _ = (between, tz);
        todo!("agent 1")
    }

    /// True if `now`, converted to local time in `tz`, is in `[start, end)`.
    pub fn contains(&self, now: DateTime<Utc>) -> bool {
        let _ = now;
        todo!("agent 1")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreferenceRule {
    Match {
        matcher: PrefMatch,
        channels: Vec<String>,
        when: Option<TimeWindow>,
        otherwise: Option<Vec<String>>,
    },
    Default {
        channels: Vec<String>,
    },
}

/// Parse and validate a person's raw `preferences:` value (as loaded from YAML into JSON).
/// Unknown channel names (not in `known_channels`), bad windows/time zones, unknown match keys,
/// and rules after a `default` (unreachable) are `ConfigError::Invalid` naming the person.
pub fn parse_preferences(
    person_id: &str,
    raw: &serde_json::Value,
    known_channels: &[&str],
) -> Result<Vec<PreferenceRule>, ConfigError> {
    let _ = (person_id, raw, known_channels);
    todo!("agent 1")
}

/// Why a channel set was chosen; serialized into `delivery_jobs.route_info.decision`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum Decision {
    /// The route listed `channels:`.
    Explicit,
    /// `index` is the position in `preferences`; `window` is `in`, `out` or `none`.
    Preference {
        index: usize,
        window: &'static str,
    },
    TeamDefault {
        team: String,
    },
    /// Nothing matched: UI only.
    UiDefault,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    /// May contain `ui`; the router skips it (UI delivery is the alert feed itself).
    pub channels: Vec<String>,
    pub decision: Decision,
}

/// Steps 2–4 of the precedence above (step 1, explicit route channels, is the router's).
/// `team` is the team whose default applies: the routed team for `team:`/`oncall:` targets,
/// else the alert's team if the person is a member, else `None`.
pub fn resolve_channels(
    person: &Person,
    team: Option<&Team>,
    ctx: &RoutingContext,
    now: DateTime<Utc>,
) -> Resolution {
    let _ = (person, team, ctx, now);
    todo!("agent 1")
}
