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
//! the next rule).
//!
//! **Off-hours digest.** `otherwise` may name the pseudo-channel `digest` (alone or next to real
//! channels): outside the window, the notification is held and sent as part of ONE email at the
//! window's next start ("overnight: 3 alerts, 1 resolved"), see `crate::offhours`. `digest` is
//! only valid in `otherwise`, because the window's start is the release time. Critical alerts are
//! never held (the router sends `digest` as email for them), and escalation steps and runbook
//! actions, which are urgent by definition, resolve `digest` to email via [`resolve_channels`]. `match` keys: `severity`, `service`, `team`, `rule`, `environment`; a key whose
//! context value is missing compares false (same as rules).

use chrono::{DateTime, Duration, NaiveTime, TimeZone, Utc};
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
        let tz: chrono_tz::Tz = tz
            .parse()
            .map_err(|_| ConfigError::Invalid(format!("unknown time zone {tz:?}")))?;
        let bad = |what: &str| ConfigError::Invalid(format!("bad window {between:?}: {what}"));
        let (start_s, end_s) = between
            .split_once('-')
            .ok_or_else(|| bad("want \"HH:MM-HH:MM\""))?;
        let start = parse_hm(start_s).ok_or_else(|| bad("want \"HH:MM-HH:MM\""))?;
        let end = parse_hm(end_s).ok_or_else(|| bad("want \"HH:MM-HH:MM\""))?;
        if start == end {
            return Err(bad("start equals end (the window would be empty)"));
        }
        Ok(Self { start, end, tz })
    }

    /// True if `now`, converted to local time in `tz`, is in `[start, end)`.
    pub fn contains(&self, now: DateTime<Utc>) -> bool {
        // Instant -> local: always a valid local time, so DST gaps never bite.
        let t = now.with_timezone(&self.tz).time();
        if self.start <= self.end {
            self.start <= t && t < self.end
        } else {
            // Wraps past midnight: [start, 24:00) ∪ [00:00, end).
            t >= self.start || t < self.end
        }
    }
}

impl TimeWindow {
    /// The first instant strictly after `now` at which the window opens (local `start` in `tz`).
    /// A start time that falls in a DST gap that day opens at the first valid local time after it
    /// (an hour later); an ambiguous one (DST fold) opens at its earlier instant.
    pub fn next_start(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        let today = now.with_timezone(&self.tz).date_naive();
        for day in 0..=2 {
            let date = today + Duration::days(day);
            let local = date.and_time(self.start);
            let opens = self.tz.from_local_datetime(&local).earliest().or_else(|| {
                // DST gap: the wall-clock start does not exist that day.
                self.tz
                    .from_local_datetime(&(local + Duration::hours(1)))
                    .earliest()
            });
            if let Some(t) = opens.map(|t| t.with_timezone(&Utc))
                && t > now
            {
                return t;
            }
        }
        // Unreachable for real time zones (a start exists at least every other day).
        now + Duration::days(1)
    }
}

/// The pseudo-channel that holds a notification for the off-hours digest (`otherwise` only).
pub const DIGEST: &str = "digest";

/// `H:MM` or `HH:MM`, 24-hour.
fn parse_hm(s: &str) -> Option<NaiveTime> {
    let (h, m) = s.split_once(':')?;
    let digits = |x: &str| !x.is_empty() && x.bytes().all(|b| b.is_ascii_digit());
    if h.len() > 2 || m.len() != 2 || !digits(h) || !digits(m) {
        return None;
    }
    NaiveTime::from_hms_opt(h.parse().ok()?, m.parse().ok()?, 0)
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
    // `null` / missing -> no rules.
    if raw.is_null() {
        return Ok(Vec::new());
    }
    let at = |i: usize, what: &str| {
        ConfigError::Invalid(format!("person {person_id}: preferences[{i}]: {what}"))
    };
    let entries = raw.as_array().ok_or_else(|| {
        ConfigError::Invalid(format!("person {person_id}: preferences must be a list"))
    })?;

    let channels = |i: usize,
                    v: &serde_json::Value,
                    what: &str,
                    allow_digest: bool|
     -> Result<Vec<String>, ConfigError> {
        let list = v
            .as_array()
            .filter(|l| !l.is_empty())
            .ok_or_else(|| at(i, &format!("{what} must be a non-empty list of channels")))?;
        let mut out = Vec::with_capacity(list.len());
        for c in list {
            let name = c
                .as_str()
                .ok_or_else(|| at(i, &format!("{what} entries must be strings")))?;
            if name == DIGEST {
                if !allow_digest {
                    return Err(at(
                        i,
                        &format!(
                            "`digest` is only allowed in `otherwise` (it is sent when the \
                                 `when` window next opens), not in {what}"
                        ),
                    ));
                }
            } else if !known_channels.contains(&name) {
                return Err(at(i, &format!("unknown channel {name:?}")));
            }
            if out.iter().any(|c| c == name) {
                continue;
            }
            out.push(name.to_string());
        }
        Ok(out)
    };

    let mut rules = Vec::with_capacity(entries.len());
    let mut have_default = false;
    for (i, entry) in entries.iter().enumerate() {
        if have_default {
            return Err(at(i, "unreachable: rule after a `default` entry"));
        }
        let obj = entry
            .as_object()
            .ok_or_else(|| at(i, "entry must be a mapping"))?;

        // A `default` entry matches everything; nothing may follow it.
        if let Some(default) = obj.get("default") {
            for other in ["match", "channels", "when", "otherwise"] {
                if obj.contains_key(other) {
                    return Err(at(
                        i,
                        &format!("a `default` entry cannot also have `{other}`"),
                    ));
                }
            }
            rules.push(PreferenceRule::Default {
                channels: channels(i, default, "`default`", false)?,
            });
            have_default = true;
            continue;
        }

        // A match rule: `match` (all keys optional, AND-ed) + `channels` + optional
        // `when` window and `otherwise` channels.
        let matcher = match obj.get("match") {
            Some(v) if !v.is_null() => {
                let mobj = v
                    .as_object()
                    .ok_or_else(|| at(i, "`match` must be a mapping"))?;
                let mut matcher = PrefMatch::default();
                for (key, value) in mobj {
                    let what = MATCH_KEYS
                        .contains(&key.as_str())
                        .then_some(key.as_str())
                        .ok_or_else(|| {
                            at(
                                i,
                                &format!(
                                    "unknown match key {key:?} \
                                     (want severity, service, team, rule or environment)"
                                ),
                            )
                        })?;
                    let s = value.as_str().ok_or_else(|| {
                        at(i, &format!("match key {key:?} must have a string value"))
                    })?;
                    match what {
                        "severity" => matcher.severity = Some(s.to_string()),
                        "service" => matcher.service = Some(s.to_string()),
                        "team" => matcher.team = Some(s.to_string()),
                        "rule" => matcher.rule = Some(s.to_string()),
                        "environment" => matcher.environment = Some(s.to_string()),
                        _ => unreachable!("MATCH_KEYS entries are exhaustive"),
                    }
                }
                matcher
            }
            // No `default` (above) and no `match`: not a rule at all.
            _ => return Err(at(i, "entry must have `match` or `default`")),
        };

        let channels_v = obj
            .get("channels")
            .ok_or_else(|| at(i, "missing `channels`"))?;
        let rule_channels = channels(i, channels_v, "`channels`", false)?;

        let when = match obj.get("when") {
            None => None,
            Some(v) if v.is_null() => None,
            Some(v) => {
                let wobj = v
                    .as_object()
                    .ok_or_else(|| at(i, "`when` must be a mapping"))?;
                let between = wobj
                    .get("between")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| at(i, "`when` needs a `between` string \"HH:MM-HH:MM\""))?;
                let tz = wobj
                    .get("tz")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| at(i, "`when` needs a `tz` string (IANA name)"))?;
                match TimeWindow::parse(between, tz) {
                    Ok(w) => Some(w),
                    Err(ConfigError::Invalid(m)) => return Err(at(i, &m)),
                    Err(other) => return Err(other),
                }
            }
        };

        let otherwise = match obj.get("otherwise") {
            None => None,
            Some(v) if v.is_null() => None,
            Some(v) => {
                if when.is_none() {
                    return Err(at(i, "`otherwise` requires `when`"));
                }
                Some(channels(i, v, "`otherwise`", true)?)
            }
        };

        rules.push(PreferenceRule::Match {
            matcher,
            channels: rule_channels,
            when,
            otherwise,
        });
    }
    Ok(rules)
}

/// Match keys a preference rule may use (SPEC §4.4).
const MATCH_KEYS: [&str; 5] = ["severity", "service", "team", "rule", "environment"];

/// Every key set in `matcher` must equal the context value; a key whose context value is
/// missing compares false (so the rule does not match).
fn matches_ctx(matcher: &PrefMatch, ctx: &RoutingContext) -> bool {
    matcher.severity.as_ref().is_none_or(|v| v == &ctx.severity)
        && matcher
            .service
            .as_ref()
            .is_none_or(|v| ctx.service.as_deref() == Some(v.as_str()))
        && matcher.team.as_ref().is_none_or(|v| v == &ctx.team)
        && matcher.rule.as_ref().is_none_or(|v| v == &ctx.rule_id)
        && matcher
            .environment
            .as_ref()
            .is_none_or(|v| ctx.environment.as_deref() == Some(v.as_str()))
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
    /// Set when `channels` contains [`DIGEST`]: the instant the off-hours window reopens,
    /// when the held notification is released in the person's digest email.
    pub hold_until: Option<DateTime<Utc>>,
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
    let mut r = resolve_with_hold(person, team, ctx, now);
    if r.hold_until.take().is_some() {
        // Urgent callers (escalation steps, runbook actions) never hold: digest -> email.
        let mut channels: Vec<String> = Vec::with_capacity(r.channels.len());
        for c in r.channels.drain(..) {
            let c = if c == DIGEST { "email".to_string() } else { c };
            if !channels.contains(&c) {
                channels.push(c);
            }
        }
        r.channels = channels;
    }
    r
}

/// Like [`resolve_channels`], but keeps the [`DIGEST`] pseudo-channel and sets `hold_until`
/// (the window's next start) when the matching rule's `otherwise` holds the notification.
/// Used by the router for an alert's notifications.
pub fn resolve_with_hold(
    person: &Person,
    team: Option<&Team>,
    ctx: &RoutingContext,
    now: DateTime<Utc>,
) -> Resolution {
    for (index, rule) in person.rules.iter().enumerate() {
        match rule {
            PreferenceRule::Match {
                matcher,
                channels,
                when,
                otherwise,
            } => {
                if !matches_ctx(matcher, ctx) {
                    continue;
                }
                match when.as_ref().map(|w| w.contains(now)) {
                    // No window: the rule applies at any time.
                    None => {
                        return Resolution {
                            channels: channels.clone(),
                            decision: Decision::Preference {
                                index,
                                window: "none",
                            },
                            hold_until: None,
                        };
                    }
                    Some(true) => {
                        return Resolution {
                            channels: channels.clone(),
                            decision: Decision::Preference {
                                index,
                                window: "in",
                            },
                            hold_until: None,
                        };
                    }
                    // Outside the window.
                    Some(false) => match otherwise {
                        Some(channels) => {
                            let hold_until = channels
                                .iter()
                                .any(|c| c == DIGEST)
                                .then(|| when.as_ref().map(|w| w.next_start(now)))
                                .flatten();
                            return Resolution {
                                channels: channels.clone(),
                                decision: Decision::Preference {
                                    index,
                                    window: "out",
                                },
                                hold_until,
                            };
                        }
                        // No `otherwise`: this rule passes; the next one gets a chance.
                        None => continue,
                    },
                }
            }
            PreferenceRule::Default { channels } => {
                return Resolution {
                    channels: channels.clone(),
                    decision: Decision::Preference {
                        index,
                        window: "none",
                    },
                    hold_until: None,
                };
            }
        }
    }
    if let Some(t) = team
        && let Some(default) = &t.default_channels
    {
        return Resolution {
            channels: default.clone(),
            decision: Decision::TeamDefault { team: t.id.clone() },
            hold_until: None,
        };
    }
    Resolution {
        channels: vec!["ui".to_string()],
        decision: Decision::UiDefault,
        hold_until: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Contact, KNOWN_CHANNELS};
    use chrono::TimeZone;
    use serde_json::json;
    use std::collections::HashMap;

    fn utc(s: &str) -> DateTime<Utc> {
        s.parse().expect("valid RFC3339 timestamp")
    }

    fn ctx(severity: &str, service: Option<&str>) -> RoutingContext {
        RoutingContext {
            severity: severity.to_string(),
            team: "payments".to_string(),
            rule_id: "anna-payments-critical".to_string(),
            service: service.map(str::to_string),
            environment: None,
        }
    }

    /// Anna as configured in the seed `config/people/anna.yaml`: critical+payments-api ->
    /// sms in [08:00, 20:00) Europe/London (else email), warning -> email, default ui.
    fn anna() -> Person {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/people/anna.yaml");
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
        let p: Person = serde_saphyr::from_str(&text).unwrap_or_else(|e| panic!("anna.yaml: {e}"));
        let raw = p.preferences.as_ref().expect("anna has preferences");
        Person {
            rules: parse_preferences("anna", raw, KNOWN_CHANNELS).expect("anna prefs parse"),
            ..p
        }
    }

    fn resolve_anna(now: DateTime<Utc>, severity: &str, service: Option<&str>) -> Resolution {
        let person = anna();
        resolve_channels(&person, None, &ctx(severity, service), now)
    }

    /// Unwrap the window of a `Preference` decision (panics on any other decision).
    fn window(r: &Resolution) -> &'static str {
        match r.decision {
            Decision::Preference { window, .. } => window,
            ref other => panic!("expected a preference decision, got {other:?}"),
        }
    }

    // -- window boundaries --------------------------------------------------------------

    #[test]
    fn anna_window_boundaries_in_gmt_winter() {
        // Mid-January: London == GMT == UTC. Half-open [08:00, 20:00).
        for (at, want, window) in [
            ("2026-01-15T07:59:59Z", "email", "out"),
            ("2026-01-15T08:00:00Z", "sms", "in"),
            ("2026-01-15T19:59:59Z", "sms", "in"),
            ("2026-01-15T20:00:00Z", "email", "out"),
        ] {
            let r = resolve_anna(utc(at), "critical", Some("payments-api"));
            assert_eq!(r.channels, [want], "{at}: {:?}", r.decision);
            assert_eq!(
                r.decision,
                Decision::Preference { index: 0, window },
                "{at}"
            );
        }
    }

    #[test]
    fn anna_window_follows_london_dst_spring() {
        // 2026-03-29 01:00Z: London jumps GMT -> BST (UTC+1). Same instants land on
        // different sides of the local window across the change.
        let cases = [
            ("2026-03-28T07:30:00Z", "email", "out"), // 07:30 GMT
            ("2026-03-30T07:30:00Z", "sms", "in"),    // 08:30 BST
            ("2026-03-30T19:30:00Z", "email", "out"), // 20:30 BST
        ];
        for (at, want, w) in cases {
            let r = resolve_anna(utc(at), "critical", Some("payments-api"));
            assert_eq!(r.channels, [want], "{at}: {:?}", r.decision);
            assert_eq!(window(&r), w, "{at}");
        }
    }

    #[test]
    fn anna_window_follows_london_dst_autumn() {
        // 2026-10-25 01:00Z: London falls back BST -> GMT.
        let cases = [
            ("2026-10-24T07:30:00Z", "sms", "in"),    // 08:30 BST
            ("2026-10-26T07:30:00Z", "email", "out"), // 07:30 GMT
            ("2026-10-24T19:30:00Z", "email", "out"), // 20:30 BST
            ("2026-10-26T19:30:00Z", "sms", "in"),    // 19:30 GMT
        ];
        for (at, want, w) in cases {
            let r = resolve_anna(utc(at), "critical", Some("payments-api"));
            assert_eq!(r.channels, [want], "{at}: {:?}", r.decision);
            assert_eq!(window(&r), w, "{at}");
        }
    }

    #[test]
    fn overnight_window_wraps_midnight() {
        let w = TimeWindow::parse("22:00-06:00", "Europe/London").unwrap();
        // January: London == GMT, so UTC instants are local times.
        let jan = |h, m, s| Utc.with_ymd_and_hms(2026, 1, 15, h, m, s).unwrap();
        assert!(w.contains(jan(23, 0, 0)));
        assert!(w.contains(jan(5, 59, 59)));
        assert!(!w.contains(jan(6, 0, 0)), "half-open: end excluded");
        assert!(!w.contains(jan(21, 59, 59)));
        assert!(!w.contains(jan(12, 0, 0)));
    }

    // -- rule evaluation ----------------------------------------------------------------

    #[test]
    fn first_matching_rule_wins() {
        let raw = json!([
            {"match": {"severity": "critical"}, "channels": ["sms"]},
            {"match": {"severity": "critical"}, "channels": ["email"]},
            {"default": ["ui"]}
        ]);
        let person = Person {
            rules: parse_preferences("x", &raw, KNOWN_CHANNELS).unwrap(),
            id: "x".into(),
            name: None,
            contact: Contact {
                email: None,
                phone: None,
            },
            preferences: None,
        };
        let r = resolve_channels(
            &person,
            None,
            &ctx("critical", None),
            utc("2026-01-15T12:00:00Z"),
        );
        assert_eq!(r.channels, ["sms"]);
        assert_eq!(
            r.decision,
            Decision::Preference {
                index: 0,
                window: "none",
            }
        );
    }

    #[test]
    fn rule_needing_a_missing_context_value_does_not_match() {
        // Anna's rule 0 also matches on service: without a service in the context it never
        // applies; rule 1 (warning) does not match critical; the default wins.
        let r = resolve_anna(utc("2026-01-15T12:00:00Z"), "critical", None);
        assert_eq!(r.channels, ["ui"]);
        assert_eq!(
            r.decision,
            Decision::Preference {
                index: 2,
                window: "none",
            }
        );
        // Matching is case-sensitive.
        let r = resolve_anna(
            utc("2026-01-15T12:00:00Z"),
            "Critical",
            Some("payments-api"),
        );
        assert_eq!(r.channels, ["ui"]);
    }

    #[test]
    fn anna_warning_is_email_in_hours_and_held_outside() {
        // 10:00 London (GMT): email now, nothing held.
        let person = anna();
        let r = resolve_with_hold(
            &person,
            None,
            &ctx("warning", None),
            utc("2026-01-15T10:00:00Z"),
        );
        assert_eq!(r.channels, ["email"]);
        assert_eq!(
            r.decision,
            Decision::Preference {
                index: 1,
                window: "in"
            }
        );
        assert_eq!(r.hold_until, None);

        // 03:00: held until 08:00 the same morning.
        let r = resolve_with_hold(
            &person,
            None,
            &ctx("warning", None),
            utc("2026-01-15T03:00:00Z"),
        );
        assert_eq!(r.channels, [DIGEST]);
        assert_eq!(
            r.decision,
            Decision::Preference {
                index: 1,
                window: "out"
            }
        );
        assert_eq!(r.hold_until, Some(utc("2026-01-15T08:00:00Z")));

        // 21:00: held until 08:00 the NEXT morning.
        let r = resolve_with_hold(
            &person,
            None,
            &ctx("warning", None),
            utc("2026-01-15T21:00:00Z"),
        );
        assert_eq!(r.hold_until, Some(utc("2026-01-16T08:00:00Z")));

        // Urgent callers (escalation, actions) never hold: digest becomes email.
        let r = resolve_anna(utc("2026-01-15T03:00:00Z"), "warning", None);
        assert_eq!(r.channels, ["email"]);
        assert_eq!(r.hold_until, None);
    }

    #[test]
    fn next_start_follows_dst_and_wraps() {
        let w = TimeWindow::parse("08:00-20:00", "Europe/London").unwrap();
        // Summer (BST): 08:00 local is 07:00Z.
        assert_eq!(
            w.next_start(utc("2026-07-01T02:00:00Z")),
            utc("2026-07-01T07:00:00Z")
        );
        // Exactly at the start: strictly after, so tomorrow.
        assert_eq!(
            w.next_start(utc("2026-07-01T07:00:00Z")),
            utc("2026-07-02T07:00:00Z")
        );
        // Across the spring change (2026-03-29): Saturday night GMT -> Sunday 08:00 BST.
        assert_eq!(
            w.next_start(utc("2026-03-28T22:00:00Z")),
            utc("2026-03-29T07:00:00Z")
        );

        // A start inside the spring-forward gap (01:00-02:00 London) opens an hour later.
        let gap = TimeWindow::parse("01:30-06:00", "Europe/London").unwrap();
        assert_eq!(
            gap.next_start(utc("2026-03-29T00:00:00Z")),
            utc("2026-03-29T01:30:00Z")
        );

        // A wrapping night window opens at its evening start.
        let night = TimeWindow::parse("22:00-06:00", "UTC").unwrap();
        assert_eq!(
            night.next_start(utc("2026-01-15T12:00:00Z")),
            utc("2026-01-15T22:00:00Z")
        );
    }

    #[test]
    fn digest_is_only_valid_in_otherwise() {
        let ok = json!([{ "match": {}, "channels": ["email"],
                          "when": {"between": "08:00-20:00", "tz": "UTC"},
                          "otherwise": ["ui", "digest"] }]);
        assert!(parse_preferences("a", &ok, KNOWN_CHANNELS).is_ok());
        for raw in [
            json!([{ "match": {}, "channels": ["digest"] }]),
            json!([{ "default": ["digest"] }]),
            json!([{ "match": {}, "channels": ["digest"],
                     "when": {"between": "08:00-20:00", "tz": "UTC"}, "otherwise": ["email"] }]),
        ] {
            let msg = match parse_preferences("a", &raw, KNOWN_CHANNELS) {
                Err(ConfigError::Invalid(m)) => m,
                other => panic!("{raw} should be rejected, got {other:?}"),
            };
            assert!(
                msg.contains("`digest` is only allowed in `otherwise`"),
                "{msg}"
            );
        }
    }

    #[test]
    fn team_default_then_ui_default() {
        let person = Person {
            id: "bob".into(),
            name: None,
            contact: Contact {
                email: None,
                phone: None,
            },
            preferences: None,
            rules: Vec::new(),
        };
        let now = utc("2026-01-15T12:00:00Z");
        let plain = Team {
            id: "rates".into(),
            members: Vec::new(),
            lead: None,
            oncall: HashMap::new(),
            default_channels: None,
        };

        let r = resolve_channels(&person, None, &ctx("warning", None), now);
        assert_eq!(r.channels, ["ui"]);
        assert_eq!(r.decision, Decision::UiDefault);

        let r = resolve_channels(&person, Some(&plain), &ctx("warning", None), now);
        assert_eq!(r.channels, ["ui"]);
        assert_eq!(r.decision, Decision::UiDefault);

        let with_default = Team {
            default_channels: Some(vec!["email".into()]),
            ..plain
        };
        let r = resolve_channels(&person, Some(&with_default), &ctx("warning", None), now);
        assert_eq!(r.channels, ["email"]);
        assert_eq!(
            r.decision,
            Decision::TeamDefault {
                team: "rates".into(),
            }
        );
    }

    // -- parsing / validation ----------------------------------------------------------

    #[test]
    fn null_and_empty_preferences_are_no_rules() {
        assert!(
            parse_preferences("a", &serde_json::Value::Null, KNOWN_CHANNELS)
                .unwrap()
                .is_empty()
        );
        assert!(
            parse_preferences("a", &json!([]), KNOWN_CHANNELS)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn time_window_rejects_bad_input() {
        for between in [
            "08:00-25:00",
            "0800-2000",
            "08:00",
            "08:00-20:00:00",
            "8am-9pm",
        ] {
            assert!(
                TimeWindow::parse(between, "Europe/London").is_err(),
                "{between} must not parse"
            );
        }
        assert!(
            TimeWindow::parse("08:00-08:00", "Europe/London").is_err(),
            "start == end is an empty window"
        );
        let err = TimeWindow::parse("08:00-20:00", "Europe/Nowhere").unwrap_err();
        assert!(err.to_string().contains("Europe/Nowhere"), "{err}");
        assert!(TimeWindow::parse("08:00-20:00", "Europe/London").is_ok());
    }

    #[test]
    fn parse_preferences_rejects_malformed_entries() {
        for raw in [
            json!([{ "channels": ["email"] }]), // neither match nor default
            json!([{ "match": {} }]),           // missing channels
            json!([{ "match": {}, "channels": [] }]), // empty channels
            json!([{ "match": {}, "channels": ["fax"] }]), // unknown channel
            json!([{ "match": {}, "channels": [3] }]), // non-string channel
            json!([{ "match": {}, "default": ["ui"] }]), // both forms
            json!([{ "default": ["ui"], "when": {} }]), // default with when
            json!([{ "match": {"servce": "payments-api"}, "channels": ["email"] }]), // unknown match key
            json!([{ "match": {"severity": 3}, "channels": ["email"] }]), // non-string match value
            json!([{ "default": ["ui"] }, { "match": {"severity": "warning"}, "channels": ["email"] }]), // after default
            json!([{ "match": {}, "channels": ["email"], "otherwise": ["sms"] }]), // otherwise without when
            json!([{ "match": {}, "channels": ["email"], "when": {"between": "08:00-20:00"} }]), // when without tz
            json!([{ "match": {}, "channels": ["email"], "when": {"tz": "Europe/London"} }]), // when without between
            json!([{ "match": {}, "channels": ["email"],
                     "when": {"between": "08:00-20:00", "tz": "Europe/Nowhere"} }]), // bad tz
            json!([{ "match": {}, "channels": ["email"],
                     "when": {"between": "8am-9pm", "tz": "Europe/London"} }]), // bad time
            json!([{ "match": {}, "channels": ["email"],
                     "when": {"between": "08:00-08:00", "tz": "Europe/London"} }]), // start == end
            json!(["not a mapping"]),
        ] {
            let msg = match parse_preferences("anna", &raw, KNOWN_CHANNELS) {
                Err(ConfigError::Invalid(m)) => m,
                other => panic!("{raw} should be rejected, got {other:?}"),
            };
            assert!(msg.starts_with("person anna: preferences["), "{raw}: {msg}");
        }
        // The whole `preferences:` value must be a list.
        let msg = match parse_preferences("anna", &json!("nope"), KNOWN_CHANNELS) {
            Err(ConfigError::Invalid(m)) => m,
            other => panic!("should be rejected, got {other:?}"),
        };
        assert_eq!(msg, "person anna: preferences must be a list");
    }

    #[test]
    fn parse_preferences_accepts_anna_and_empty_match() {
        // The seed file parses to three rules.
        let person = anna();
        assert_eq!(person.rules.len(), 3);

        // An empty `match` matches everything.
        let raw = json!([{ "match": {}, "channels": ["email"] }]);
        let person = Person {
            rules: parse_preferences("x", &raw, KNOWN_CHANNELS).unwrap(),
            id: "x".into(),
            name: None,
            contact: Contact {
                email: None,
                phone: None,
            },
            preferences: None,
        };
        let r = resolve_channels(
            &person,
            None,
            &ctx("whatever", None),
            utc("2026-01-15T12:00:00Z"),
        );
        assert_eq!(r.channels, ["email"]);
    }
}
