//! The parts of a rule delivery acts on (M5a): `escalate_after` and `throttle`.
//!
//! Source of truth, in order:
//! 1. The notify payload, frozen when the notification was written (board #132, optional keys
//!    `escalate_after_ms: u64` and `throttle: {max, per_ms}`), so a rule edit never changes an
//!    alert already in flight.
//! 2. Otherwise the rule file in `config/rules/<id>.yaml` (owned by part (a)), read at startup:
//!    `escalate_after: 10m` and `throttle: { max: 20, per: 1m }` (SPEC §4.3).
//!
//! Rule files are parsed loosely as JSON values (only `id`, `escalate_after`, `throttle`), so
//! a rule field delivery doesn't know about never breaks delivery; an unparseable file or value
//! is logged and skipped, not fatal (the rules belong to another part).

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use serde_json::Value;
use tracing::warn;

use crate::throttle::ThrottleSpec;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RulePolicy {
    pub escalate_after: Option<Duration>,
    pub throttle: Option<ThrottleSpec>,
    /// The rule's `runbook:` id (M5b).
    pub runbook: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RulePolicies {
    pub by_rule: HashMap<String, RulePolicy>,
}

impl RulePolicies {
    /// Read `<dir>/*.yaml|*.yml`. A missing directory gives an empty set.
    pub fn load(dir: &Path) -> Self {
        let mut by_rule = HashMap::new();
        let Ok(entries) = std::fs::read_dir(dir) else {
            return Self::default();
        };
        let mut paths: Vec<_> = entries
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| {
                p.extension().and_then(|e| e.to_str()).is_some_and(|e| {
                    e.eq_ignore_ascii_case("yaml") || e.eq_ignore_ascii_case("yml")
                })
            })
            .collect();
        paths.sort();
        for path in paths {
            let parsed = std::fs::read_to_string(&path)
                .map_err(|e| e.to_string())
                .and_then(|t| serde_saphyr::from_str::<Value>(&t).map_err(|e| e.to_string()));
            let value = match parsed {
                Ok(v) => v,
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "rule file unreadable, ignoring for delivery");
                    continue;
                }
            };
            let Some(id) = value.get("id").and_then(Value::as_str) else {
                continue;
            };
            by_rule.insert(
                id.to_string(),
                policy_from_rule(&path.display().to_string(), &value),
            );
        }
        Self { by_rule }
    }

    /// The effective policy for one notification: payload keys win over the rule file.
    pub fn for_payload(&self, rule_id: &str, raw_payload: &Value) -> RulePolicy {
        let mut p = self.by_rule.get(rule_id).cloned().unwrap_or_default();
        if let Some(ms) = raw_payload.get("escalate_after_ms").and_then(Value::as_u64) {
            p.escalate_after = Some(Duration::from_millis(ms));
        }
        if let Some(r) = raw_payload.get("runbook").and_then(Value::as_str) {
            p.runbook = Some(r.to_string());
        }
        if let Some(t) = raw_payload.get("throttle").filter(|t| !t.is_null()) {
            match ThrottleSpec::from_payload_value(t) {
                Ok(spec) => p.throttle = Some(spec),
                Err(e) => warn!(rule_id, error = %e, "bad payload.throttle, using the rule file's"),
            }
        }
        p
    }
}

fn policy_from_rule(path: &str, rule: &Value) -> RulePolicy {
    let escalate_after = match rule.get("escalate_after") {
        None | Some(Value::Null) => None,
        Some(v) => match v.as_str().and_then(parse_duration) {
            Some(d) => Some(d),
            None => {
                warn!(path, value = %v, "bad escalate_after, ignoring");
                None
            }
        },
    };
    let throttle = match rule.get("throttle") {
        None | Some(Value::Null) => None,
        Some(v) => match ThrottleSpec::from_rule_value(v) {
            Ok(s) => Some(s),
            Err(e) => {
                warn!(path, error = %e, "bad throttle, ignoring");
                None
            }
        },
    };
    let runbook = rule
        .get("runbook")
        .and_then(Value::as_str)
        .map(str::to_string);
    RulePolicy {
        escalate_after,
        throttle,
        runbook,
    }
}

/// `"90s"`, `"10m"`, `"1h30m"`, `"2h 37min"`: the humantime syntax `RuleSpec` parses rule
/// files with, so a rule the engine accepts never loses its `escalate_after` here.
pub fn parse_duration(s: &str) -> Option<Duration> {
    humantime_serde::re::humantime::parse_duration(s.trim()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("10m"), Some(Duration::from_secs(600)));
        assert_eq!(parse_duration("1h30m"), Some(Duration::from_secs(5400)));
        assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_duration("90s"), Some(Duration::from_secs(90)));
        assert_eq!(parse_duration("2h 37min"), Some(Duration::from_secs(9420)));
        for bad in ["", "m", "10", "10x", "-1m"] {
            assert_eq!(parse_duration(bad), None, "{bad}");
        }
    }

    #[test]
    fn seed_rules_load_and_payload_overrides() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/rules");
        let p = RulePolicies::load(&dir);
        let s = &p.by_rule["settlements-workflow-failed"];
        assert_eq!(s.escalate_after, Some(Duration::from_secs(600)));
        let raw = serde_json::json!({"escalate_after_ms": 5000});
        let eff = p.for_payload("settlements-workflow-failed", &raw);
        assert_eq!(eff.escalate_after, Some(Duration::from_secs(5)));
        assert_eq!(
            p.for_payload("no-such-rule", &serde_json::json!({})),
            RulePolicy::default()
        );
    }
}
