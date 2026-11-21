//! Normalize + match (spec §5.2). Stateless: a pure function of the config
//! snapshot and the event, so any matcher replica can handle any event.

use std::collections::{BTreeSet, HashMap};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::condition::{Condition, Op};
use crate::config::ConfigSnapshot;
use crate::event::Event;
use crate::mapping::{Diagnostic, structural_signature};
use crate::model::{AlertField, MAX_NOTIFY_RAW, NotifyContext, NotifyEvent};
use crate::path::Path;
use crate::rule::{CompiledRule, RouteSpec};
use crate::severity::Severity;
use crate::template;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchKind {
    /// The rule's `when` matched: open or bump an alert.
    Fire,
    /// The rule's `auto_resolve.recover_when` matched: resolve the alert with the same key.
    Recover,
}

/// One rule matching one event. Published to `alerts.matches`, keyed by `fingerprint`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatchRecord {
    /// Stable across replays: `(event_id, rule_id)` for fires, plus `:recover` for recoveries.
    pub match_id: String,
    pub kind: MatchKind,
    pub rule_id: String,
    pub rule_revision: i64,
    pub rule_name: String,
    pub team: String,
    /// Hash of rule id + dedup key values: the incident identity.
    pub fingerprint: String,
    pub key: Vec<Value>,
    pub dedup_window_ms: u64,
    pub severity: String,
    pub color: String,
    pub title: String,
    pub body: String,
    pub fields: Vec<AlertField>,
    pub route: Vec<RouteSpec>,
    pub event_id: String,
    pub source: String,
    pub schema: String,
    pub ingested_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<String>,
    /// Canonical event facts passed on to delivery in the notify payload.
    #[serde(default)]
    pub context: NotifyContext,
    /// Bounded event data for runbook selection and templates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<NotifyEvent>,
    /// `auto_resolve.quiet_for`: resolve the alert after this long without a match.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quiet_for_ms: Option<u64>,
    /// Grouped rules: identity of the group tuple without the time bucket.
    /// A recovery resolves every open digest with this key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_key: Option<String>,
}

/// Values copied from the event into a match (dedup key values, card fields)
/// are capped so a match always fits a Kafka record, whatever the event holds.
pub const MAX_PROJECTED_VALUE: usize = 4096;

/// Replace an oversized value with a truncated string preview.
fn bounded(v: Value) -> Value {
    bounded_to(v, MAX_PROJECTED_VALUE)
}

/// Keys whose values must be masked before leaving the hot path.
fn is_secret_key(k: &str) -> bool {
    const SECRET: &[&str] = &[
        "password",
        "passwd",
        "secret",
        "token",
        "authorization",
        "api_key",
        "apikey",
        "cookie",
        "private_key",
        "credential",
        "access_key",
        "session_id",
        "ssn",
        "credit_card",
        "card_number",
        "phone",
        "mobile",
        "telephone",
        "msisdn",
    ];
    let lk = k.to_ascii_lowercase();
    SECRET.iter().any(|s| lk.contains(s))
}

pub fn is_secret_path_str(p: &str) -> bool {
    p.split(['.', '/']).any(is_secret_key)
}

fn is_secret_path(p: &Path) -> bool {
    is_secret_path_str(p.as_str())
}

/// Scrub inline credentials (`Bearer ...`, `Basic ...`, `scheme://user:pass@`),
/// labeled phone numbers, JSON-preview secret values, email addresses, and
/// formatted phone numbers from strings before storage or display.
pub fn redact_str(s: &str) -> String {
    use regex::Regex;
    use std::sync::OnceLock;
    static AUTH_RE: OnceLock<Regex> = OnceLock::new();
    static URL_CRED_RE: OnceLock<Regex> = OnceLock::new();
    static LABELED_PHONE_RE: OnceLock<Regex> = OnceLock::new();
    static JSON_SECRET_RE: OnceLock<Regex> = OnceLock::new();
    static EMAIL_RE: OnceLock<Regex> = OnceLock::new();

    let auth = AUTH_RE.get_or_init(|| {
        Regex::new(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}").expect("valid regex")
    });
    let url_cred = URL_CRED_RE.get_or_init(|| {
        Regex::new(r"(?i)([a-z][a-z0-9+.-]*://)[^\s/@:]+:[^\s/@]+@").expect("valid regex")
    });
    let labeled_phone = LABELED_PHONE_RE.get_or_init(|| {
        Regex::new(r#"(?i)\b(phone|mobile|tel|telephone|msisdn)(\s*[:=]\s*["']?)(\+?\d{7,15})(["']?)"#)
            .expect("valid regex")
    });
    let json_secret = JSON_SECRET_RE.get_or_init(|| {
        Regex::new(r#"(?i)("(?:[^"]*(?:password|passwd|secret|token|authorization|api_key|apikey|cookie|private_key|credential|access_key|session_id|ssn|credit_card|card_number|phone|mobile|telephone|msisdn)[^"]*)"\s*:\s*)(?:"(?:\\.|[^"\\])*(?:"|$)|-?\d+(?:\.\d+)?)"#)
            .expect("valid regex")
    });
    let email = EMAIL_RE.get_or_init(|| {
        Regex::new(r"(?i)\b([A-Z0-9._%+-]+)@([A-Z0-9.-]+\.[A-Z]{2,})\b").expect("valid regex")
    });

    let s = auth.replace_all(s, "$1 [redacted]");
    let s = url_cred.replace_all(&s, "${1}[redacted]@");
    let s = json_secret.replace_all(&s, r#"${1}"[redacted]""#);
    let s = labeled_phone.replace_all(&s, "${1}${2}[redacted]${4}");
    let s = email.replace_all(&s, |caps: &regex::Captures| {
        let user = &caps[1];
        let domain = &caps[2];
        let first: String = user.chars().take(1).collect();
        format!("{first}***@{domain}")
    });
    mask_phones(&s)
}

/// Mask phone-number-like sequences (e.g. `+44 7700 900123`, `+447700900123`,
/// `555-123-4567`) outside complete ISO date/timestamp spans, without
/// disturbing timestamps, IPv4s, or plain numeric codes.
pub fn mask_phones(s: &str) -> String {
    use regex::Regex;
    use std::sync::OnceLock;
    static TS_RE: OnceLock<Regex> = OnceLock::new();
    static PHONE_RE: OnceLock<Regex> = OnceLock::new();
    let ts_re = TS_RE.get_or_init(|| {
        Regex::new(r"\b\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?\b")
            .expect("valid regex")
    });
    let phone_re = PHONE_RE.get_or_init(|| {
        Regex::new(r"(?:\+\d[\d .()-]{5,18}\d|\b(?:\(\d{2,4}\)|\d{3,5})[ .()-]\d[\d .()-]{4,14}\d\b)")
            .expect("valid regex")
    });
    let mask_chunk = |chunk: &str| -> String {
        phone_re
            .replace_all(chunk, |caps: &regex::Captures| {
                let m = &caps[0];
                let digits: Vec<char> = m.chars().filter(|c| c.is_ascii_digit()).collect();
                let is_ipv4 = m.split('.').count() == 4 && m.split('.').all(|p| p.parse::<u8>().is_ok());
                if is_ipv4 || digits.len() < 7 || digits.len() > 15 {
                    return m.to_string();
                }
                let head: String = m.chars().take_while(|c| *c != ' ').take(3).collect();
                let tail: String = digits[digits.len() - 3..].iter().collect();
                format!("{head} *** {tail}")
            })
            .into_owned()
    };
    let mut out = String::with_capacity(s.len());
    let mut pos = 0;
    for ts in ts_re.find_iter(s) {
        out.push_str(&mask_chunk(&s[pos..ts.start()]));
        out.push_str(ts.as_str());
        pos = ts.end();
    }
    out.push_str(&mask_chunk(&s[pos..]));
    out
}

/// Mask values under secret-looking keys (password, token, authorization...)
/// and inline credentials before an event leaves the hot path for storage or display.
pub fn redact(v: Value) -> Value {
    match v {
        Value::Object(o) => Value::Object(
            o.into_iter()
                .map(|(k, v)| {
                    if is_secret_key(&k) { (k, Value::String("[redacted]".into())) } else { (k, redact(v)) }
                })
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.into_iter().map(redact).collect()),
        Value::String(s) => {
            if (s.starts_with('{') || s.starts_with('[')) && s.ends_with('…') {
                Value::String("[truncated]".into())
            } else {
                Value::String(redact_str(&s))
            }
        }
        other => other,
    }
}

/// Cap a JSON value at `max` serialized bytes, replacing it with a truncated
/// string preview when larger.
pub fn bounded_to(v: Value, max: usize) -> Value {
    let text = match &v {
        Value::String(s) if s.len() <= max => return v,
        Value::String(s) => s.clone(),
        Value::Array(_) | Value::Object(_) => {
            let t = v.to_string();
            if t.len() <= max {
                return v;
            }
            t
        }
        _ => return v,
    };
    let mut cut = max;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    Value::String(format!("{}…", &text[..cut]))
}

/// Pre-index: rules whose condition requires `path == value` for some top-level
/// conjunct are only evaluated when the event has that value. Everything else
/// is in the fallback bucket. The full predicate always runs on candidates, so
/// the index can only skip work, never change results.
#[derive(Debug, Default, Clone)]
pub struct RuleIndex {
    /// path → (canonical value → rule positions)
    by_path: Vec<(Path, HashMap<String, Vec<usize>>)>,
    fallback: Vec<usize>,
}

impl RuleIndex {
    pub fn build<'a>(conds: impl Iterator<Item = (usize, Option<&'a Condition>)>) -> RuleIndex {
        let mut idx = RuleIndex::default();
        for (pos, cond) in conds {
            let Some(cond) = cond else { continue };
            match selector(cond) {
                Some((path, values)) => {
                    let slot = match idx.by_path.iter().position(|(p, _)| p == path) {
                        Some(i) => i,
                        None => {
                            idx.by_path.push((path.clone(), HashMap::new()));
                            idx.by_path.len() - 1
                        }
                    };
                    for v in values {
                        idx.by_path[slot].1.entry(canonical(v)).or_default().push(pos);
                    }
                }
                None => idx.fallback.push(pos),
            }
        }
        idx
    }

    pub fn candidates(&self, event: &Event) -> BTreeSet<usize> {
        let mut out: BTreeSet<usize> = self.fallback.iter().copied().collect();
        for (path, map) in &self.by_path {
            if let Some(v) = path.resolve(event)
                && let Some(rules) = map.get(&canonical(v.as_ref())) {
                    out.extend(rules.iter().copied());
                }
        }
        out
    }

    pub fn indexed_paths(&self) -> usize {
        self.by_path.len()
    }
}

/// A necessary equality: a top-level `eq`/`in` leaf, or one directly under a top-level `all`.
/// Preference: schema, fields.type, source, then any other path.
fn selector(cond: &Condition) -> Option<(&Path, Vec<&Value>)> {
    let leaves: Vec<&crate::condition::Leaf> = match cond {
        Condition::Leaf(l) => vec![l],
        Condition::All(cs) => cs
            .iter()
            .filter_map(|c| match c {
                Condition::Leaf(l) => Some(l),
                _ => None,
            })
            .collect(),
        _ => vec![],
    };
    let usable = |l: &&crate::condition::Leaf| match l.op {
        // Numbers are excluded: 1 and 1.0 are equal but canonicalize differently.
        Op::Eq => indexable(&l.value),
        Op::In => l.value.as_array().is_some_and(|a| !a.is_empty() && a.iter().all(indexable)),
        _ => false,
    };
    let rank = |l: &crate::condition::Leaf| match l.path.as_str() {
        "schema" => 0,
        "fields.type" => 1,
        "source" => 2,
        _ => 3,
    };
    let best = leaves.into_iter().filter(usable).min_by_key(|l| rank(l))?;
    let values = match best.op {
        Op::In => best.value.as_array().unwrap().iter().collect(),
        _ => vec![&best.value],
    };
    Some((&best.path, values))
}

fn indexable(v: &Value) -> bool {
    matches!(v, Value::String(_) | Value::Bool(_))
}

fn canonical(v: &Value) -> String {
    match v {
        Value::String(s) => format!("s:{s}"),
        Value::Bool(b) => format!("b:{b}"),
        other => format!("j:{other}"),
    }
}

impl ConfigSnapshot {
    /// Detect the schema and extract canonical fields, in place.
    ///
    /// An explicit source-scoped mapping wins. Otherwise the highest-priority
    /// detector whose tests pass; a tie between different schemas at the top
    /// priority becomes `unknown` with a diagnostic.
    pub fn normalize(&self, event: &mut Event) -> Vec<Diagnostic> {
        let detected: Vec<_> = self.mappings.iter().filter(|m| m.detects(event)).collect();
        let chosen = if let Some(m) = detected.iter().find(|m| m.is_source_scoped()) {
            Some(*m)
        } else if let Some(top) = detected.first() {
            let tied: Vec<_> = detected
                .iter()
                .filter(|m| m.priority == top.priority && m.schema != top.schema)
                .collect();
            if tied.is_empty() {
                Some(*top)
            } else {
                let names: Vec<_> = std::iter::once(top.schema.as_str())
                    .chain(tied.iter().map(|m| m.schema.as_str()))
                    .collect();
                event.schema = structural_signature(&event.raw);
                return vec![Diagnostic {
                    field: "schema".into(),
                    message: format!("ambiguous: {} detectors tie at priority {}: {names:?}", names.len(), top.priority),
                }];
            }
        } else {
            None
        };
        match chosen {
            Some(m) => {
                event.schema = m.schema.clone();
                let (fields, diags) = m.extract(event);
                event.fields = fields;
                diags
            }
            None => {
                event.schema = structural_signature(&event.raw);
                vec![]
            }
        }
    }

    /// Evaluate the candidate rules against a normalized event.
    pub fn match_event(&self, event: &Event) -> Vec<MatchRecord> {
        let mut out = Vec::new();
        let recovering: BTreeSet<usize> = self
            .recover_index
            .candidates(event)
            .into_iter()
            .filter(|&i| {
                let r = &self.rules[i];
                r.spec.enabled && r.recover_when.as_ref().is_some_and(|c| c.eval(event))
            })
            .collect();
        for &i in &recovering {
            out.push(self.record(&self.rules[i], event, MatchKind::Recover));
        }
        for i in self.fire_index.candidates(event) {
            let r = &self.rules[i];
            // A recovery signal never opens an alert for the same rule.
            if r.spec.enabled && !recovering.contains(&i) && r.when.eval(event) {
                out.push(self.record(r, event, MatchKind::Fire));
            }
        }
        out
    }

    /// Normalize then match; the event is returned with its schema and fields filled.
    pub fn process(&self, mut event: Event) -> (Event, Vec<MatchRecord>, Vec<Diagnostic>) {
        let diags = self.normalize(&mut event);
        let matches = self.match_event(&event);
        (event, matches, diags)
    }

    fn record(&self, rule: &CompiledRule, event: &Event, kind: MatchKind) -> MatchRecord {
        let mut diagnostics = Vec::new();
        // Grouped rules (`group:`) key incidents by the group tuple plus the UTC
        // time bucket, so each tuple gets one alert per window (a digest whose
        // count grows live). Otherwise the dedup key tuple is the identity.
        let (key_paths, window_ms, bucket) = match &rule.group {
            Some(g) => {
                let w = g.window.as_millis().max(1) as i64;
                let start = event.ingested_at.timestamp_millis().div_euclid(w) * w;
                (&g.by, w as u64, Some((start, start + w)))
            }
            None => (&rule.dedup_key, rule.spec.dedup.window.as_millis() as u64, None),
        };
        let mut key = Vec::with_capacity(key_paths.len() + 1);
        let mut missing = false;
        for p in key_paths {
            match p.resolve(event) {
                Some(v) => key.push(v.into_owned()),
                None => {
                    missing = true;
                    let what = if bucket.is_some() { "group key" } else { "dedup key" };
                    diagnostics.push(format!("{what} `{p}` missing; alert not merged"));
                    key.push(Value::Null);
                }
            }
        }
        // The group tuple alone identifies the series; recoveries use it so
        // they close digests of every window, not just the current one.
        let group_key = bucket.map(|_| fingerprint(rule.id(), &key, None));
        if let (Some((start, _)), MatchKind::Fire) = (bucket, kind) {
            key.push(Value::from(start));
        }
        let fingerprint = fingerprint(rule.id(), &key, missing.then_some(event.id.as_str()));
        let key: Vec<Value> = key.into_iter().map(bounded).collect();
        let severity = rule
            .spec
            .severity
            .or_else(|| event.fields.get("severity").and_then(Value::as_str).and_then(Severity::normalize))
            .unwrap_or(Severity::Warning);
        let (title, body, fields, notify_event) = if kind == MatchKind::Fire {
            let redacted_raw = redact(event.raw.clone());
            let redacted_fields = match redact(Value::Object(event.fields.clone())) {
                Value::Object(m) => m,
                _ => serde_json::Map::new(),
            };
            let redacted_id = redact_str(&event.id);
            let ctx = template::context_from(&redacted_raw, &redacted_fields, &event.schema, &event.source, &redacted_id);
            let title = template::render(&self.env, &format!("{}:title", rule.id()), &ctx);
            let body = template::render(&self.env, &format!("{}:body", rule.id()), &ctx);
            let fields = rule
                .fields
                .iter()
                .filter_map(|(p, label)| {
                    p.resolve(event).map(|v| AlertField {
                        path: p.to_string(),
                        label: label.clone(),
                        value: if is_secret_path(p) {
                            Value::String("[redacted]".into())
                        } else {
                            bounded(redact(v.into_owned()))
                        },
                    })
                })
                .collect::<Vec<_>>();
            let mut fields = fields;
            if let Some((start, end)) = bucket {
                fields.push(AlertField { path: "group.window".into(), label: "Window".into(), value: Value::String(window_label(start, end)) });
            }
            // Delivery may match a team runbook even without an explicit rule binding.
            let notify_event = Some(NotifyEvent {
                fields: match bounded_to(Value::Object(redacted_fields), 4096) {
                    Value::Object(m) => m,
                    _ => serde_json::Map::new(),
                },
                raw: bounded_to(redacted_raw, MAX_NOTIFY_RAW),
            });
            (title, body, fields, notify_event)
        } else {
            (String::new(), String::new(), Vec::new(), None)
        };
        MatchRecord {
            match_id: match kind {
                MatchKind::Fire => format!("{}:{}", event.id, rule.id()),
                MatchKind::Recover => format!("{}:{}:recover", event.id, rule.id()),
            },
            kind,
            rule_id: rule.id().to_string(),
            rule_revision: rule.revision,
            rule_name: rule.name().to_string(),
            team: rule.spec.team.clone(),
            fingerprint,
            key,
            dedup_window_ms: window_ms,
            quiet_for_ms: rule.spec.auto_resolve.as_ref().and_then(|a| a.quiet_for).map(|d| d.as_millis() as u64),
            group_key,
            severity: severity.as_str().to_string(),
            color: rule
                .spec
                .present
                .color
                .clone()
                .unwrap_or_else(|| severity.default_color().to_string()),
            title,
            body,
            fields,
            route: rule.spec.route.clone(),
            event_id: event.id.clone(),
            source: event.source.clone(),
            schema: event.schema.clone(),
            ingested_at: event.ingested_at,
            diagnostics,
            event: notify_event,
            context: NotifyContext {
                service: field_str(event, "service"),
                environment: field_str(event, "environment"),
                event_type: field_str(event, "type"),
                source: Some(event.source.clone()),
                schema: Some(event.schema.clone()),
            },
        }
    }
}

/// Canonical facts are short identifiers; anything longer is cut at 256 bytes.
fn field_str(event: &Event, name: &str) -> Option<String> {
    let mut s = event.fields.get(name).and_then(|v| match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(_) | Value::Bool(_) => Some(v.to_string()),
        _ => None,
    })?;
    if s.len() > 256 {
        let mut cut = 256;
        while !s.is_char_boundary(cut) {
            cut -= 1;
        }
        s.truncate(cut);
    }
    Some(s)
}

/// "09:15–09:20 UTC" (date added when the window isn't within one day).
fn window_label(start_ms: i64, end_ms: i64) -> String {
    use chrono::{TimeZone, Utc};
    let (s, e) = (Utc.timestamp_millis_opt(start_ms).single(), Utc.timestamp_millis_opt(end_ms).single());
    match (s, e) {
        (Some(s), Some(e)) if e - s < chrono::Duration::days(1) => format!("{}–{} UTC", s.format("%H:%M"), e.format("%H:%M")),
        (Some(s), Some(e)) => format!("{}–{} UTC", s.format("%Y-%m-%d %H:%M"), e.format("%Y-%m-%d %H:%M")),
        _ => String::new(),
    }
}

/// Incident identity: rule id + dedup key values. When a key field is missing
/// the event id is mixed in, so unrelated events are never silently merged.
pub fn fingerprint(rule_id: &str, key: &[Value], unique: Option<&str>) -> String {
    let mut h = Sha256::new();
    h.update(rule_id.as_bytes());
    h.update([0]);
    h.update(serde_json::to_vec(key).unwrap_or_default());
    if let Some(u) = unique {
        h.update([0]);
        h.update(u.as_bytes());
    }
    hex::encode(&h.finalize()[..16])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn projected_values_are_bounded() {
        assert_eq!(bounded(json!("short")), json!("short"));
        assert_eq!(bounded(json!(42)), json!(42));
        let big = bounded(json!("x".repeat(100_000)));
        assert!(big.as_str().unwrap().len() <= MAX_PROJECTED_VALUE + 4);
        let obj = bounded(json!({"blob": "y".repeat(100_000)}));
        assert!(obj.is_string(), "large objects become a truncated preview");
    }

    #[test]
    fn redacts_secret_keys_inline_creds_and_phones() {
        let v = redact(json!({
            "password": "s3cr3t",
            "phone": 7700900123i64,
            "nested": {"api_key": "k123", "msg": "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload contact bob@example.test or +44 7700 900123"},
            "url": "smtp://alice:hunter2@mail.internal:587",
            "ip": "192.168.100.200",
            "ts": "2026-10-23 09:14:02",
            "ok": "trade-42"
        }));
        assert_eq!(v["password"], json!("[redacted]"));
        assert_eq!(v["phone"], json!("[redacted]"));
        assert_eq!(v["nested"]["api_key"], json!("[redacted]"));
        assert_eq!(
            v["nested"]["msg"],
            json!("Authorization: Bearer [redacted] contact b***@example.test or +44 *** 123")
        );
        assert_eq!(v["url"], json!("smtp://[redacted]@mail.internal:587"));
        assert_eq!(v["ip"], json!("192.168.100.200"));
        assert_eq!(v["ts"], json!("2026-10-23 09:14:02"));
        assert_eq!(v["ok"], json!("trade-42"));

        assert_eq!(mask_phones("dial +44 7700 900123 failed at 2026-10-23 09:14:02"), "dial +44 *** 123 failed at 2026-10-23 09:14:02");
        assert_eq!(mask_phones("code 550 2026-10-23 09:14:02"), "code 550 2026-10-23 09:14:02");
        assert_eq!(mask_phones("dial +44 7700 900123 2026-10-23 09:14:02"), "dial +44 *** 123 2026-10-23 09:14:02");
        assert_eq!(mask_phones("sms to +447700900123 rejected (550)"), "sms to +44 *** 123 rejected (550)");
        assert_eq!(redact_str("phone=07700900123 ts=1761210842000"), "phone=[redacted] ts=1761210842000");
        assert_eq!(
            redact_str(r#"{"password":"hunter2","ok":1,"token":"trunc…"#),
            r#"{"password":"[redacted]","ok":1,"token":"[redacted]""#
        );
    }

    #[test]
    fn match_redacts_templates_fields_event_id_and_notify_event() {
        let rule: crate::rule::RuleSpec = serde_json::from_value(json!({
            "id": "r1",
            "team": "payments",
            "when": {"path": "raw.ok", "op": "eq", "value": true},
            "route": [{"to": "team:payments"}],
            "present": {
                "title": "Alert {{ raw.headers.authorization }} ({{ id }})",
                "body": "Contact {{ raw.note }}",
                "fields": [{"path": "raw.headers.authorization", "label": "Auth"}, {"path": "raw.note", "label": "Note"}]
            },
            "dedup": {"key": ["raw.id"]},
            "runbook": "rb-1"
        }))
        .unwrap();
        let snap = ConfigSnapshot::build(1, vec![], vec![rule]).unwrap();
        let mut ev = Event::new(
            "http:src:bob@example.test".into(),
            "http:test".into(),
            json!({
                "ok": true,
                "id": "k1",
                "headers": {"authorization": "Bearer topsecret12345"},
                "note": "Bearer topsecret12345 bob@example.test"
            }),
        );
        ev.fields.insert("api_key".into(), json!("secret-val"));
        ev.fields.insert("msg".into(), json!("Bearer topsecret12345"));
        let m1 = snap.match_event(&ev);
        assert_eq!(m1.len(), 1);
        assert_eq!(m1[0].title, "Alert [redacted] (http:src:b***@example.test)");
        assert_eq!(m1[0].body, "Contact Bearer [redacted] b***@example.test");
        assert_eq!(m1[0].fields[0].value, json!("[redacted]"));
        assert_eq!(m1[0].fields[1].value, json!("Bearer [redacted] b***@example.test"));
        let notify_ev = m1[0].event.as_ref().unwrap();
        assert_eq!(notify_ev.fields.get("api_key"), Some(&json!("[redacted]")));
        assert_eq!(notify_ev.fields.get("msg"), Some(&json!("Bearer [redacted]")));
    }
}
