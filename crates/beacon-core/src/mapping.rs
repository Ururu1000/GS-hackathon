//! Schema mappings: recognise a payload shape and extract canonical fields
//! (spec §4.2). Config, not code: a new schema is a YAML file.
//!
//! ```yaml
//! schema: prometheus.alert
//! priority: 50
//! detect:
//!   source: http:alertmanager        # optional
//!   all:
//!     - { path: alertname, op: exists }
//!     - { path: status, op: in, value: [firing, resolved] }
//! fields:
//!   type:        { path: alertname }
//!   severity:    { path: labels.severity, transform: lower, enum: severity }
//!   occurred_at: { path: startsAt, as: rfc3339 }
//!   resolved:    { path: status, equals: resolved }
//! ```

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::condition::{Condition, ConditionError, ConditionSpec};
use crate::event::Event;
use crate::path::{Path, PathError, Root};
use crate::severity::Severity;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMappingSpec {
    pub schema: String,
    #[serde(default)]
    pub priority: i32,
    pub detect: DetectSpec,
    #[serde(default)]
    pub fields: BTreeMap<String, FieldSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectSpec {
    /// Restrict detection to one source instance.
    #[serde(default)]
    pub source: Option<String>,
    #[serde(flatten)]
    pub condition: ConditionSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSpec {
    /// Path relative to the raw payload (`labels.severity`, `/a~1b`). Omit for a constant.
    #[serde(default)]
    pub path: Option<String>,
    /// Constant value (or fallback when the path is missing).
    #[serde(default)]
    pub value: Option<Value>,
    #[serde(default)]
    pub transform: Option<Transform>,
    /// `severity` normalizes to info|warning|error|critical.
    #[serde(default, rename = "enum")]
    pub enum_: Option<EnumKind>,
    /// Typed conversion.
    #[serde(default, rename = "as")]
    pub as_: Option<As>,
    /// Produce a boolean: is the value equal to this?
    #[serde(default)]
    pub equals: Option<Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transform {
    Lower,
    Upper,
    Trim,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EnumKind {
    Severity,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum As {
    String,
    Number,
    Bool,
    /// RFC 3339 string → normalized UTC RFC 3339.
    Rfc3339,
    /// Epoch milliseconds → RFC 3339.
    EpochMs,
    /// Epoch seconds → RFC 3339.
    EpochS,
}

#[derive(Debug, thiserror::Error)]
pub enum MappingError {
    #[error("schema `{schema}`: detect: {source}")]
    Detect {
        schema: String,
        #[source]
        source: ConditionError,
    },
    #[error("schema `{schema}`: field `{field}`: {source}")]
    FieldPath {
        schema: String,
        field: String,
        #[source]
        source: PathError,
    },
    #[error("schema `{schema}`: field `{field}` needs a `path` or a `value`")]
    FieldEmpty { schema: String, field: String },
    #[error("schema name must not be empty or start with `unknown:`")]
    BadName,
}

#[derive(Debug, Clone)]
pub struct SchemaMapping {
    pub schema: String,
    pub priority: i32,
    source: Option<String>,
    detect: Condition,
    fields: Vec<(String, CompiledField)>,
}

#[derive(Debug, Clone)]
struct CompiledField {
    path: Option<Path>,
    spec: FieldSpec,
}

/// A non-fatal problem found while mapping an event.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Diagnostic {
    pub field: String,
    pub message: String,
}

impl SchemaMapping {
    pub fn compile(spec: &SchemaMappingSpec) -> Result<SchemaMapping, MappingError> {
        if spec.schema.trim().is_empty() || spec.schema.starts_with("unknown:") {
            return Err(MappingError::BadName);
        }
        let detect = Condition::compile_with_default_root(&spec.detect.condition, Root::Raw)
            .map_err(|source| MappingError::Detect {
                schema: spec.schema.clone(),
                source,
            })?;
        let mut fields = Vec::new();
        for (name, f) in &spec.fields {
            if f.path.is_none() && f.value.is_none() {
                return Err(MappingError::FieldEmpty {
                    schema: spec.schema.clone(),
                    field: name.clone(),
                });
            }
            let path = f
                .path
                .as_deref()
                .map(|p| Path::parse_with_default_root(p, Root::Raw))
                .transpose()
                .map_err(|source| MappingError::FieldPath {
                    schema: spec.schema.clone(),
                    field: name.clone(),
                    source,
                })?;
            fields.push((
                name.clone(),
                CompiledField {
                    path,
                    spec: f.clone(),
                },
            ));
        }
        Ok(SchemaMapping {
            schema: spec.schema.clone(),
            priority: spec.priority,
            source: spec.detect.source.clone(),
            detect,
            fields,
        })
    }

    /// Does this mapping recognise the event? (Source scoping + discriminator tests.)
    pub fn detects(&self, event: &Event) -> bool {
        if let Some(src) = &self.source
            && src != &event.source {
                return false;
            }
        self.detect.eval(event)
    }

    pub fn is_source_scoped(&self) -> bool {
        self.source.is_some()
    }

    /// Extract canonical fields. Failed optional conversions leave the field
    /// absent and record a diagnostic.
    pub fn extract(&self, event: &Event) -> (Map<String, Value>, Vec<Diagnostic>) {
        let mut out = Map::new();
        let mut diags = Vec::new();
        for (name, f) in &self.fields {
            let found = f.path.as_ref().and_then(|p| p.resolve_in(&event.raw)).cloned();
            let Some(v) = found.or_else(|| f.spec.value.clone()) else {
                continue;
            };
            match convert(v, &f.spec) {
                Ok(v) => {
                    out.insert(name.clone(), v);
                }
                Err(message) => diags.push(Diagnostic {
                    field: name.clone(),
                    message,
                }),
            }
        }
        // Derived: a numeric severity rank so rules can say `>= warning`.
        if let Some(rank) = out
            .get("severity")
            .and_then(Value::as_str)
            .and_then(Severity::normalize)
            .map(Severity::rank)
        {
            out.insert("severity_rank".into(), Value::from(rank));
        }
        (out, diags)
    }
}

fn convert(mut v: Value, spec: &FieldSpec) -> Result<Value, String> {
    if let Some(eq) = &spec.equals {
        return Ok(Value::Bool(&v == eq));
    }
    if let (Some(t), Value::String(s)) = (spec.transform, &v) {
        v = Value::String(match t {
            Transform::Lower => s.to_lowercase(),
            Transform::Upper => s.to_uppercase(),
            Transform::Trim => s.trim().to_string(),
        });
    }
    if let Some(EnumKind::Severity) = spec.enum_ {
        let s = scalar_string(&v).ok_or("severity is not a string")?;
        let sev = Severity::normalize(&s).ok_or_else(|| format!("unknown severity `{s}`"))?;
        v = Value::String(sev.as_str().into());
    }
    if let Some(a) = spec.as_ {
        v = match a {
            As::String => Value::String(scalar_string(&v).ok_or("not a scalar")?),
            As::Number => match &v {
                Value::Number(_) => v,
                Value::String(s) => serde_json::from_str::<serde_json::Number>(s.trim())
                    .map(Value::Number)
                    .map_err(|_| format!("`{s}` is not a number"))?,
                _ => return Err("not a number".into()),
            },
            As::Bool => match &v {
                Value::Bool(_) => v,
                Value::String(s) if s.eq_ignore_ascii_case("true") => Value::Bool(true),
                Value::String(s) if s.eq_ignore_ascii_case("false") => Value::Bool(false),
                _ => return Err("not a boolean".into()),
            },
            As::Rfc3339 => {
                let s = v.as_str().ok_or("timestamp is not a string")?;
                let t = DateTime::parse_from_rfc3339(s).map_err(|e| format!("bad RFC 3339 `{s}`: {e}"))?;
                Value::String(rfc3339(t.with_timezone(&Utc)))
            }
            As::EpochMs | As::EpochS => {
                let n = v.as_i64().ok_or("epoch is not an integer")?;
                let t = match a {
                    As::EpochMs => Utc.timestamp_millis_opt(n).single(),
                    _ => Utc.timestamp_opt(n, 0).single(),
                }
                .ok_or("epoch out of range")?;
                Value::String(rfc3339(t))
            }
        };
    }
    Ok(v)
}

fn scalar_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

pub fn rfc3339(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Bounds on unknown-schema signature traversal so a large or deeply nested
/// payload cannot amplify into excessive CPU or memory allocation.
const MAX_SIG_DEPTH: usize = 32;
const MAX_SIG_NODES: usize = 4096;

/// Structural signature of an unknown payload: sorted escaped paths with array
/// positions normalized and values ignored. Only used to group samples for
/// discovery (optional keys split shapes, equal shapes collide).
pub fn structural_signature(raw: &Value) -> String {
    let mut paths = BTreeSet::new();
    let mut remaining = MAX_SIG_NODES;
    fn go(v: &Value, prefix: &str, depth: usize, remaining: &mut usize, out: &mut BTreeSet<String>) {
        if *remaining == 0 {
            return;
        }
        *remaining -= 1;
        if depth >= MAX_SIG_DEPTH {
            out.insert(format!("{prefix}/..."));
            return;
        }
        match v {
            Value::Object(m) => {
                if m.is_empty() {
                    out.insert(format!("{prefix}/{{}}"));
                }
                for (k, v) in m {
                    if *remaining == 0 {
                        break;
                    }
                    let p = format!("{prefix}/{}", k.replace('~', "~0").replace('/', "~1"));
                    go(v, &p, depth + 1, remaining, out);
                }
            }
            Value::Array(items) => {
                let arr_prefix = format!("{prefix}/[]");
                if items.is_empty() {
                    out.insert(arr_prefix);
                    return;
                }
                for v in items {
                    if *remaining == 0 {
                        break;
                    }
                    go(v, &arr_prefix, depth + 1, remaining, out);
                }
            }
            _ => {
                out.insert(prefix.to_string());
            }
        }
    }
    go(raw, "", 0, &mut remaining, &mut paths);
    let mut h = Sha256::new();
    h.update(b"sig-v1\n");
    for p in &paths {
        h.update(p.as_bytes());
        h.update(b"\n");
    }
    format!("unknown:{}", &hex::encode(h.finalize())[..12])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn alertmanager() -> SchemaMapping {
        let spec: SchemaMappingSpec = serde_saphyr::from_str(
            r#"
schema: prometheus.alert
priority: 50
detect:
  all:
    - { path: alertname, op: exists }
    - { path: status, op: in, value: [firing, resolved] }
fields:
  type:        { path: alertname }
  severity:    { path: labels.severity, transform: lower, enum: severity }
  service:     { path: labels.service }
  occurred_at: { path: startsAt, as: rfc3339 }
  resolved:    { path: status, equals: resolved }
"#,
        )
        .unwrap();
        SchemaMapping::compile(&spec).unwrap()
    }

    #[test]
    fn detects_and_extracts() {
        let m = alertmanager();
        let e = Event::new(
            "e".into(),
            "http:alertmanager".into(),
            json!({"alertname": "HighErrorRate", "labels": {"service": "payments-api", "severity": "CRITICAL"},
                   "startsAt": "2026-10-23T09:13:00Z", "status": "firing"}),
        );
        assert!(m.detects(&e));
        let (f, d) = m.extract(&e);
        assert!(d.is_empty(), "{d:?}");
        assert_eq!(f["type"], json!("HighErrorRate"));
        assert_eq!(f["severity"], json!("critical"));
        assert_eq!(f["severity_rank"], json!(4));
        assert_eq!(f["occurred_at"], json!("2026-10-23T09:13:00.000Z"));
        assert_eq!(f["resolved"], json!(false));
    }

    #[test]
    fn bad_conversion_is_a_diagnostic_not_a_failure() {
        let m = alertmanager();
        let e = Event::new(
            "e".into(),
            "x".into(),
            json!({"alertname": "A", "status": "firing", "labels": {"severity": "weird"}, "startsAt": "yesterday"}),
        );
        let (f, d) = m.extract(&e);
        assert!(!f.contains_key("severity"));
        assert!(!f.contains_key("occurred_at"));
        assert_eq!(d.len(), 2);
    }

    #[test]
    fn signature_ignores_values_and_order() {
        let a = structural_signature(&json!({"a": 1, "b": {"c": [1, 2]}}));
        let b = structural_signature(&json!({"b": {"c": [9]}, "a": "x"}));
        let c = structural_signature(&json!({"a": 1}));
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a.starts_with("unknown:"));
    }

    #[test]
    fn signature_bounds_large_and_deep_payloads() {
        let mut deep = json!(1);
        for _ in 0..80 {
            deep = json!({"n": deep});
        }
        assert!(structural_signature(&deep).starts_with("unknown:"));

        let wide = Value::Array((0..10_000).map(|i| json!({"k": i})).collect());
        assert!(structural_signature(&wide).starts_with("unknown:"));
    }
}
