//! Rules (spec §4.3): what a team cares about, who gets told, how it looks,
//! and how duplicates collapse.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::condition::{Condition, ConditionError, ConditionSpec};
use crate::path::{Path, PathError};
use crate::severity::Severity;

const DEFAULT_DEDUP_WINDOW: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleSpec {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    pub team: String,
    #[serde(default = "yes")]
    pub enabled: bool,
    pub when: ConditionSpec,
    /// Fixed severity; when absent, the event's `fields.severity` is used.
    #[serde(default)]
    pub severity: Option<Severity>,
    #[serde(default)]
    pub route: Vec<RouteSpec>,
    #[serde(default)]
    pub present: PresentSpec,
    #[serde(default)]
    pub dedup: DedupSpec,
    /// Grouping (M3). Parsed and validated, not applied in M0.
    #[serde(default)]
    pub group: Option<Value>,
    #[serde(default)]
    pub throttle: Option<Value>,
    #[serde(default)]
    pub runbook: Option<String>,
    #[serde(default)]
    pub auto_resolve: Option<AutoResolveSpec>,
    #[serde(default, with = "humantime_serde::option")]
    pub escalate_after: Option<Duration>,
}

fn yes() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RouteSpec {
    /// `team:<id>`, `user:<id>` or `oncall:<schedule>`; expanded by delivery.
    pub to: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channels: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PresentSpec {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub body: Option<String>,
    /// Paths to show as labelled fields on the alert card.
    #[serde(default)]
    pub fields: Vec<FieldRef>,
    #[serde(default)]
    pub color: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FieldRef {
    Path(String),
    Labelled { path: String, label: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupSpec {
    /// Typed path tuple; independent of `present` so editing a template can't split an incident.
    #[serde(default)]
    pub key: Vec<String>,
    /// Sliding inactivity window.
    #[serde(default = "default_window", with = "humantime_serde")]
    pub window: Duration,
}

impl Default for DedupSpec {
    fn default() -> Self {
        DedupSpec {
            key: Vec::new(),
            window: DEFAULT_DEDUP_WINDOW,
        }
    }
}

fn default_window() -> Duration {
    DEFAULT_DEDUP_WINDOW
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoResolveSpec {
    /// Typed recovery signal; it is routed to the same incident key.
    #[serde(default)]
    pub recover_when: Option<ConditionSpec>,
    #[serde(default, with = "humantime_serde::option")]
    pub quiet_for: Option<Duration>,
}

/// Caps that keep a match far below the Kafka record limit (each copied value
/// is also capped at 4 KiB in the matcher).
pub const MAX_FIELDS: usize = 32;
pub const MAX_DEDUP_KEY: usize = 8;

/// Compiled `group: {by: [paths], window}` (window: "5m" or seconds).
#[derive(Debug, Clone)]
pub struct GroupBy {
    pub by: Vec<Path>,
    pub window: Duration,
}

fn parse_group(v: &Value) -> Result<Option<GroupBy>, String> {
    if v.is_null() {
        return Ok(None);
    }
    let by: Vec<String> = match v.get("by") {
        None | Some(Value::Null) => vec![],
        Some(b) => serde_json::from_value(b.clone()).map_err(|_| "group.by must be a list of paths".to_string())?,
    };
    let window = match v.get("window") {
        None | Some(Value::Null) => Duration::ZERO,
        Some(Value::Number(n)) => Duration::from_secs(n.as_u64().ok_or("group.window must be a positive number of seconds")?),
        Some(Value::String(s)) => humantime_serde::re::humantime::parse_duration(s).map_err(|e| format!("group.window: {e}"))?,
        Some(_) => return Err("group.window must be like \"5m\" or a number of seconds".into()),
    };
    // A zero window means "no grouping" (the seed YAML style `by: [], window: 0`).
    if window.is_zero() {
        return Ok(None);
    }
    if window < Duration::from_secs(1) || window > Duration::from_secs(24 * 3600) {
        return Err("group.window must be between 1s and 24h".into());
    }
    let by = by.iter().map(|p| Path::parse(p).map_err(|e| format!("group.by `{p}`: {e}"))).collect::<Result<Vec<_>, _>>()?;
    Ok(Some(GroupBy { by, window }))
}

pub const COLORS: &[&str] = &["red", "orange", "amber", "yellow", "green", "blue", "purple", "grey"];

#[derive(Debug, thiserror::Error)]
pub enum RuleError {
    #[error("rule `{rule}`: `when`: {source}")]
    When {
        rule: String,
        #[source]
        source: ConditionError,
    },
    #[error("rule `{rule}`: `auto_resolve.recover_when`: {source}")]
    Recover {
        rule: String,
        #[source]
        source: ConditionError,
    },
    #[error("rule `{rule}`: dedup key `{path}`: {source}")]
    DedupKey {
        rule: String,
        path: String,
        #[source]
        source: PathError,
    },
    #[error("rule `{rule}`: field `{path}`: {source}")]
    Field {
        rule: String,
        path: String,
        #[source]
        source: PathError,
    },
    #[error("rule `{rule}`: template `{which}`: {message}")]
    Template {
        rule: String,
        which: &'static str,
        message: String,
    },
    #[error("rule `{rule}`: unknown color `{color}` (expected one of {COLORS:?})")]
    Color { rule: String, color: String },
    #[error("rule `{rule}`: route target `{to}` must start with team:, user: or oncall:")]
    Route { rule: String, to: String },
    #[error("rule id must be non-empty and use [a-z0-9-_.]")]
    BadId,
    #[error("rule `{rule}`: {message}")]
    Group { rule: String, message: String },
    #[error("rule `{rule}`: at most {MAX_FIELDS} presented fields and {MAX_DEDUP_KEY} dedup key paths")]
    TooMany { rule: String },
}

/// A rule ready to run: conditions compiled, paths parsed. Templates live in
/// the snapshot's shared minijinja environment under `<id>:title` / `<id>:body`.
#[derive(Debug, Clone)]
pub struct CompiledRule {
    pub spec: RuleSpec,
    pub revision: i64,
    /// `group:`: one alert per group tuple per UTC time bucket.
    pub group: Option<GroupBy>,
    pub when: Condition,
    pub recover_when: Option<Condition>,
    pub dedup_key: Vec<Path>,
    pub fields: Vec<(Path, String)>,
}

impl CompiledRule {
    pub fn compile(spec: RuleSpec, revision: i64) -> Result<CompiledRule, RuleError> {
        let rule = spec.id.clone();
        if rule.is_empty()
            || !rule
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "-_.".contains(c))
        {
            return Err(RuleError::BadId);
        }
        if spec.present.fields.len() > MAX_FIELDS || spec.dedup.key.len() > MAX_DEDUP_KEY {
            return Err(RuleError::TooMany { rule });
        }
        let when = Condition::compile(&spec.when).map_err(|source| RuleError::When {
            rule: rule.clone(),
            source,
        })?;
        let recover_when = spec
            .auto_resolve
            .as_ref()
            .and_then(|a| a.recover_when.as_ref())
            .map(Condition::compile)
            .transpose()
            .map_err(|source| RuleError::Recover {
                rule: rule.clone(),
                source,
            })?;
        let dedup_key = spec
            .dedup
            .key
            .iter()
            .map(|p| {
                Path::parse(p).map_err(|source| RuleError::DedupKey {
                    rule: rule.clone(),
                    path: p.clone(),
                    source,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let fields = spec
            .present
            .fields
            .iter()
            .map(|f| {
                let (p, label) = match f {
                    FieldRef::Path(p) => (p, None),
                    FieldRef::Labelled { path, label } => (path, Some(label.clone())),
                };
                let path = Path::parse(p).map_err(|source| RuleError::Field {
                    rule: rule.clone(),
                    path: p.clone(),
                    source,
                })?;
                let label = label.unwrap_or_else(|| path.label().to_string());
                Ok((path, label))
            })
            .collect::<Result<Vec<_>, RuleError>>()?;
        if let Some(c) = &spec.present.color
            && !COLORS.contains(&c.as_str()) {
                return Err(RuleError::Color {
                    rule,
                    color: c.clone(),
                });
            }
        for r in &spec.route {
            if !["team:", "user:", "oncall:"].iter().any(|p| r.to.starts_with(p)) {
                return Err(RuleError::Route {
                    rule,
                    to: r.to.clone(),
                });
            }
        }
        let group = match spec.group.as_ref() {
            None => None,
            Some(g) => parse_group(g).map_err(|message| RuleError::Group { rule: rule.clone(), message })?,
        };
        Ok(CompiledRule {
            spec,
            revision,
            group,
            when,
            recover_when,
            dedup_key,
            fields,
        })
    }

    pub fn id(&self) -> &str {
        &self.spec.id
    }

    pub fn name(&self) -> &str {
        self.spec.name.as_deref().unwrap_or(&self.spec.id)
    }

    pub fn title_template(&self) -> String {
        self.spec
            .present
            .title
            .clone()
            .unwrap_or_else(|| format!("{} ({{{{ schema }}}})", self.name()))
    }

    pub fn body_template(&self) -> String {
        self.spec.present.body.clone().unwrap_or_default()
    }
}
