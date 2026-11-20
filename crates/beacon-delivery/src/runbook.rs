//! Runbooks (SPEC §6.1): load `config/runbooks/`, select one per alert, render it from the pinned
//! event context, freeze it in `alert_runbooks`, and project it for the UI (`RunbookView`,
//! ui/contract/openapi.yaml v0.2.0).
//!
//! **Formats → one model** (`Runbook`):
//! - `*.yaml` / `*.yml` Appendix B form: `runbook: <id>`, optional `title`, `team`, `priority`,
//!   `matches: {<path>: <value>}` (equality on context keys, e.g. `type`, `error.code`), `steps:`
//!   list. A step with `action:` is `kind: action` (params = the remaining keys except
//!   `check|if|approval|resolves|label|hint|id`); `check:` text becomes its text (or a
//!   `manual` step without `action`); `if: "retry fails twice"` (pattern
//!   `"<action> fails <n|once|twice|three times>"`) makes it `kind: condition`-gated:
//!   `Condition::AfterFailures{action, count}`; `approval: required|none`; `resolves: true`.
//!   Step ids: explicit `id`, else `s1`, `s2`, ... by position (stable across revisions as long
//!   as order is kept).
//! - `*.md`: title = first `# heading` (else file stem); every list item (`-`, `*`, `1.`)
//!   becomes a `manual` step; an item mentioning "approval" gets `hint = "Needs approval
//!   (shown as guidance, not enforced)"`. `text` = the file verbatim.
//! - `*.txt`: one `manual` step "Follow the instructions below"; `text` = verbatim.
//! - `*.link.yaml`: `{runbook, title, url, team?}`; `format = link`, no steps; only http(s) urls.
//! - Runbook id = `runbook:` key or the file stem; `revision` = a stable 31-bit hash of the file
//!   bytes. Duplicate ids or unknown action names (`actions::ActionRegistry::known`) are load
//!   errors; unparseable files are errors too (runbooks are delivery's own config).
//! - **Front matter (Markdown/text files):** a `*.md`/`*.txt` file may begin with a YAML block
//!   delimited by two `---` lines; its keys (`runbook`, `title`, `team`, `priority`, `matches`)
//!   are metadata for team-scoped selection, and `text` is the remainder verbatim. A file without
//!   front matter keeps the whole file as `text`.
//!
//! **Selection:** the rule's `runbook:` id (`rules::RulePolicy` / payload) if it exists; else the
//! team-scoped runbooks (`team == alert team`) whose `matches` all equal the context, highest
//! `priority` then id; else none.
//!
//! **Rendering:** minijinja with strict undefined, autoescape off (the UI escapes), fuel limit;
//! context = `{raw, fields, context, alert: {id, title, severity, team, rule_id}}` where `raw`/
//! `fields` come from the optional `payload["event"]` (board #174) and `context` from
//! `payload["context"]`; bare names like `{{workflowId}}` resolve against `raw` first, then
//! `fields`, then `context`. A step whose template fails keeps its raw text and gets
//! `disabled_reason = "missing value for <name>"`. Params values are rendered the same way; keys
//! named like secrets (`token|password|secret|key|authorization`) are redacted to `"***"`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use chrono::SecondsFormat;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sqlx::{PgPool, Postgres, Transaction};
use tracing::warn;
use uuid::Uuid;

use crate::config::ConfigError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    Yaml,
    Markdown,
    Link,
    Text,
}

impl Format {
    pub fn as_str(self) -> &'static str {
        match self {
            Format::Yaml => "yaml",
            Format::Markdown => "markdown",
            Format::Link => "link",
            Format::Text => "text",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    Manual,
    Condition,
    Action,
}

impl StepKind {
    fn as_str(self) -> &'static str {
        match self {
            StepKind::Manual => "manual",
            StepKind::Condition => "condition",
            StepKind::Action => "action",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Condition {
    /// Available once `count` executions of `action` on this alert ended `failed`.
    AfterFailures { action: String, count: u32 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepDef {
    pub id: String,
    pub kind: StepKind,
    /// Template text.
    pub text: String,
    pub hint: Option<String>,
    pub action: Option<String>,
    /// Template params (string values).
    pub params: serde_json::Map<String, Value>,
    pub approval_required: bool,
    pub condition: Option<Condition>,
    pub resolves: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Runbook {
    pub id: String,
    pub revision: i32,
    pub title: String,
    pub format: Format,
    pub team: Option<String>,
    pub priority: i32,
    pub matches: serde_json::Map<String, Value>,
    pub url: Option<String>,
    pub text: Option<String>,
    pub steps: Vec<StepDef>,
}

/// A step as frozen in `alert_runbooks.steps` (rendered, redacted). The live `state`,
/// `runs`, `approval_request` and `done_*` fields of the UI's RunbookStep are added by
/// `runbook_view` from the executions/approvals/marks tables.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RenderedStep {
    pub id: String,
    pub kind: StepKind,
    pub text: String,
    pub hint: Option<String>,
    /// `{name, label, params, risk}` as in the contract's `RunbookStep.action`.
    pub action: Option<Value>,
    /// `none | required` (effective: also `required` when the action's risk is `high`).
    pub approval: String,
    pub condition: Option<Condition>,
    pub resolves: bool,
    /// Set when rendering failed (step disabled).
    pub render_error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RunbookLibrary {
    pub runbooks: Vec<Runbook>,
}

/// Why a runbook file could not be loaded.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LoadIssue {
    /// The file is not valid YAML.
    Syntax(String),
    /// The file parses but violates the runbook model.
    Semantic(String),
}

/// Which adapter parses a file, from its name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileKind {
    Yaml,
    Link,
    Markdown,
    Text,
}

impl FileKind {
    fn of(path: &Path) -> Option<Self> {
        let name = path.file_name()?.to_str()?;
        if name.ends_with(".link.yaml") || name.ends_with(".link.yml") {
            return Some(Self::Link);
        }
        let ext = path.extension()?.to_str()?.to_ascii_lowercase();
        match ext.as_str() {
            "yaml" | "yml" => Some(Self::Yaml),
            "md" => Some(Self::Markdown),
            "txt" => Some(Self::Text),
            _ => None,
        }
    }
}

/// Keys of a YAML step that are not action params.
const STEP_RESERVED: &[&str] = &[
    "id", "action", "check", "if", "approval", "resolves", "label", "hint",
];

/// A stable 31-bit hash of the file bytes: positive, fits `integer`, changes when the file does.
fn stable_revision(bytes: &[u8]) -> i32 {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let n = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]);
    (n & 0x7fff_ffff) as i32
}

impl RunbookLibrary {
    /// Load `<dir>/*`. A missing directory gives an empty library.
    pub fn load(dir: &Path, known_actions: &[&str]) -> Result<Self, ConfigError> {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => {
                return Err(ConfigError::Io {
                    path: dir.display().to_string(),
                    source: e,
                });
            }
        };
        let mut paths: Vec<PathBuf> = entries
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .collect();
        paths.sort();

        let mut runbooks = Vec::with_capacity(paths.len());
        let mut by_id: HashMap<String, String> = HashMap::new();
        for path in paths {
            let name = path.file_name().map(|n| n.to_string_lossy().into_owned());
            let Some(name) = name else { continue };
            if name.starts_with('.') {
                continue;
            }
            let Some(kind) = FileKind::of(&path) else {
                warn!(path = %path.display(), "runbooks: skipping file with unrecognized extension");
                continue;
            };
            let bytes = std::fs::read(&path).map_err(|e| ConfigError::Io {
                path: path.display().to_string(),
                source: e,
            })?;
            let fallback_id = default_id(&path, kind);
            let rb = match kind {
                FileKind::Link => parse_link_runbook(&bytes, &fallback_id),
                FileKind::Yaml => parse_yaml_runbook(&bytes, &fallback_id, known_actions),
                FileKind::Markdown => parse_markdown_runbook(&bytes, &fallback_id),
                FileKind::Text => parse_text_runbook(&bytes, &fallback_id),
            }
            .map_err(|issue| match issue {
                LoadIssue::Syntax(m) => ConfigError::Parse {
                    path: path.display().to_string(),
                    message: m,
                },
                LoadIssue::Semantic(m) => ConfigError::Invalid(format!("runbooks/{name}: {m}")),
            })?;
            if let Some(prev) = by_id.insert(rb.id.clone(), name.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "runbooks: duplicate id {:?} in {name} (already loaded from {prev})",
                    rb.id
                )));
            }
            runbooks.push(rb);
        }
        Ok(Self { runbooks })
    }

    pub fn get(&self, id: &str) -> Option<&Runbook> {
        self.runbooks.iter().find(|r| r.id == id)
    }

    /// Selection rules in the module doc. `tctx` is `template_context(..)`; a `matches` key is
    /// looked up as a dot path in `raw`, then `fields`, then `context`.
    pub fn select(&self, rule_runbook: Option<&str>, team: &str, tctx: &Value) -> Option<&Runbook> {
        if let Some(id) = rule_runbook
            && let Some(rb) = self.get(id)
        {
            return Some(rb);
        }
        let mut candidates: Vec<&Runbook> = self
            .runbooks
            .iter()
            .filter(|r| r.team.as_deref() == Some(team) && matches_equal(&r.matches, tctx))
            .collect();
        candidates.sort_by(|a, b| b.priority.cmp(&a.priority).then_with(|| a.id.cmp(&b.id)));
        candidates.into_iter().next()
    }
}

/// The runbook's id when the file does not name one: the file stem, minus the `.link` suffix
/// link files carry before their extension.
fn default_id(path: &Path, kind: FileKind) -> String {
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    match kind {
        FileKind::Link => stem
            .strip_suffix(".link")
            .map(str::to_string)
            .unwrap_or(stem),
        _ => stem,
    }
}

/// All `matches` keys resolve (dot path in `raw`, then `fields`, then `context`) and compare
/// equal. An empty map matches everything.
fn matches_equal(matches: &Map<String, Value>, tctx: &Value) -> bool {
    if matches.is_empty() {
        return true;
    }
    let sources = [tctx.get("raw"), tctx.get("fields"), tctx.get("context")];
    matches.iter().all(|(path, want)| {
        sources
            .into_iter()
            .flatten()
            .find_map(|src| resolve_path(src, path))
            .is_some_and(|got| got == want)
    })
}

/// Resolve a dot path against an object value; array indices are not supported.
fn resolve_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = root;
    for seg in path.split('.') {
        cur = cur.as_object()?.get(seg)?;
    }
    Some(cur)
}

// ---------------------------------------------------------------------------------------------
// Parsing: file bytes -> Runbook
// ---------------------------------------------------------------------------------------------

fn parse_yaml_runbook(
    bytes: &[u8],
    fallback_id: &str,
    known: &[&str],
) -> Result<Runbook, LoadIssue> {
    let revision = stable_revision(bytes);
    let text = String::from_utf8_lossy(bytes);
    let value: Value =
        serde_saphyr::from_str(&text).map_err(|e| LoadIssue::Syntax(e.to_string()))?;
    let obj = value
        .as_object()
        .ok_or_else(|| LoadIssue::Semantic("expected a mapping at the top level".into()))?;
    let id = id_field(obj, fallback_id)?;
    let title = str_field(obj, "title")?.unwrap_or_else(|| id.clone());
    let team = str_field(obj, "team")?;
    let priority = int_field(obj, "priority")?.unwrap_or(0);
    let matches = matches_field(obj)?;
    let steps = match obj.get("steps") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(a)) => parse_steps(a, known)?,
        Some(other) => {
            return Err(LoadIssue::Semantic(format!(
                "steps must be a list, got {other}"
            )));
        }
    };
    Ok(Runbook {
        id,
        revision,
        title,
        format: Format::Yaml,
        team,
        priority,
        matches,
        url: None,
        text: None,
        steps,
    })
}

/// Interpret one `steps:` entry list (shared by nothing yet, kept for symmetry with the model).
fn parse_steps(arr: &[Value], known: &[&str]) -> Result<Vec<StepDef>, LoadIssue> {
    let mut steps = Vec::with_capacity(arr.len());
    let mut seen: HashSet<String> = HashSet::new();
    for (i, entry) in arr.iter().enumerate() {
        let obj = entry
            .as_object()
            .ok_or_else(|| LoadIssue::Semantic(format!("step {} must be a mapping", i + 1)))?;
        let id = match obj.get("id") {
            Some(Value::String(s)) if !s.trim().is_empty() => s.clone(),
            _ => format!("s{}", i + 1),
        };
        if !seen.insert(id.clone()) {
            return Err(LoadIssue::Semantic(format!("duplicate step id {id:?}")));
        }
        let action = match obj.get("action") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => {
                if !known.contains(&s.as_str()) {
                    return Err(LoadIssue::Semantic(format!(
                        "unknown action {s:?} (step {id:?})"
                    )));
                }
                Some(s.clone())
            }
            Some(other) => {
                return Err(LoadIssue::Semantic(format!(
                    "action of step {id:?} must be a string, got {other}"
                )));
            }
        };
        let check = str_field(obj, "check")?;
        let label = str_field(obj, "label")?;
        let hint = str_field(obj, "hint")?;
        let if_phrase = str_field(obj, "if")?;
        let condition = match &if_phrase {
            Some(phrase) => Some(parse_if_phrase(phrase, known).map_err(LoadIssue::Semantic)?),
            None => None,
        };
        let approval_required = match obj.get("approval") {
            None | Some(Value::Null) => false,
            Some(Value::String(s)) if s == "required" => true,
            Some(Value::String(s)) if s == "none" => false,
            Some(other) => {
                return Err(LoadIssue::Semantic(format!(
                    "approval of step {id:?} must be \"required\" or \"none\", got {other}"
                )));
            }
        };
        let resolves = match obj.get("resolves") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(b)) => *b,
            Some(other) => {
                return Err(LoadIssue::Semantic(format!(
                    "resolves of step {id:?} must be a boolean, got {other}"
                )));
            }
        };
        // Everything else is an action param, coerced to a template string.
        let mut params = Map::new();
        for (k, v) in obj {
            if STEP_RESERVED.contains(&k.as_str()) {
                continue;
            }
            let s = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => String::new(),
                other => {
                    return Err(LoadIssue::Semantic(format!(
                        "param {k:?} of step {id:?} must be a scalar, got {other}"
                    )));
                }
            };
            params.insert(k.clone(), Value::String(s));
        }
        let kind = if action.is_some() {
            StepKind::Action
        } else if condition.is_some() {
            StepKind::Condition
        } else {
            StepKind::Manual
        };
        let text = check
            .clone()
            .or(label.clone())
            .unwrap_or_else(|| match (&action, &if_phrase) {
                (Some(a), _) => action_label(a),
                (None, Some(p)) => p.clone(),
                (None, None) => String::new(),
            });
        steps.push(StepDef {
            id,
            kind,
            text,
            hint,
            action,
            params,
            approval_required,
            condition,
            resolves,
        });
    }
    Ok(steps)
}

/// `"<action> fails <n|once|twice|three times>"`: the action word may be a prefix of one
/// known action (`retry` -> `retry_workflow`).
fn parse_if_phrase(phrase: &str, known: &[&str]) -> Result<Condition, String> {
    let lower = phrase.trim().to_lowercase();
    let words: Vec<&str> = lower.split_whitespace().collect();
    let err = |msg: String| format!("cannot parse condition {phrase:?}: {msg}");
    let Some(fails_at) = words.iter().position(|w| *w == "fails") else {
        return Err(err("expected \"<action> fails <count>\"".into()));
    };
    if fails_at == 0 {
        return Err(err("no action before \"fails\"".into()));
    }
    let word = words[fails_at - 1];
    let action = if known.contains(&word) {
        word.to_string()
    } else {
        let mut candidates: Vec<&str> = known
            .iter()
            .copied()
            .filter(|a| a.starts_with(word))
            .collect();
        match candidates.len() {
            1 => candidates.remove(0).to_string(),
            0 => return Err(err(format!("unknown action {word:?}"))),
            _ => {
                return Err(err(format!(
                    "{word:?} matches several actions ({candidates:?})"
                )));
            }
        }
    };
    let tail = &words[fails_at + 1..];
    let count = match tail {
        [] => return Err(err("missing failure count".into())),
        ["once"] => 1,
        ["twice"] => 2,
        ["three", "times"] => 3,
        [n] => n
            .parse::<u32>()
            .map_err(|_| err(format!("unknown count {tail:?}")))?,
        [n, "times"] => n
            .parse::<u32>()
            .map_err(|_| err(format!("unknown count {tail:?}")))?,
        _ => return Err(err(format!("unknown count {tail:?}"))),
    };
    Ok(Condition::AfterFailures { action, count })
}

/// Split an optional leading YAML front-matter block (module doc) from `*.md`/`*.txt` content.
/// An opening `---` that is never closed is ordinary content (a Markdown rule).
fn split_front_matter(text: &str) -> Result<(Option<Value>, String), LoadIssue> {
    let lines: Vec<&str> = text.lines().collect();
    if lines.first() != Some(&"---") {
        return Ok((None, text.to_string()));
    }
    let Some(end) = lines.iter().skip(1).position(|l| l.trim() == "---") else {
        return Ok((None, text.to_string()));
    };
    let block = lines[1..=end].join("\n");
    let body = lines[end + 2..].join("\n");
    let meta: Value = serde_saphyr::from_str(&block)
        .map_err(|e| LoadIssue::Syntax(format!("front matter: {e}")))?;
    if !meta.is_object() {
        return Err(LoadIssue::Semantic(format!(
            "front matter must be a mapping, got {meta}"
        )));
    }
    Ok((Some(meta), body))
}

fn front_matter_object(meta: &Option<Value>) -> Result<Map<String, Value>, LoadIssue> {
    match meta {
        None => Ok(Map::new()),
        Some(Value::Object(m)) => Ok(m.clone()),
        Some(other) => Err(LoadIssue::Semantic(format!(
            "front matter must be a mapping, got {other}"
        ))),
    }
}

fn parse_markdown_runbook(bytes: &[u8], fallback_id: &str) -> Result<Runbook, LoadIssue> {
    let revision = stable_revision(bytes);
    let text = String::from_utf8_lossy(bytes);
    let (meta, body) = split_front_matter(&text)?;
    let obj = front_matter_object(&meta)?;
    let id = id_field(&obj, fallback_id)?;
    let team = str_field(&obj, "team")?;
    let priority = int_field(&obj, "priority")?.unwrap_or(0);
    let matches = matches_field(&obj)?;
    let title = str_field(&obj, "title")?
        .or_else(|| {
            body.lines().find_map(|l| {
                l.trim_start()
                    .strip_prefix("# ")
                    .map(|h| h.trim().to_string())
            })
        })
        .unwrap_or_else(|| fallback_id.to_string());

    let mut steps = Vec::new();
    for line in body.lines() {
        let Some(item) = list_item_text(line) else {
            continue;
        };
        let hint = if item.to_lowercase().contains("approval") {
            Some("Needs approval (shown as guidance, not enforced)".to_string())
        } else {
            None
        };
        steps.push(StepDef {
            id: format!("s{}", steps.len() + 1),
            kind: StepKind::Manual,
            text: item,
            hint,
            action: None,
            params: Map::new(),
            approval_required: false,
            condition: None,
            resolves: false,
        });
    }
    Ok(Runbook {
        id,
        revision,
        title,
        format: Format::Markdown,
        team,
        priority,
        matches,
        url: None,
        text: Some(body),
        steps,
    })
}

/// `- item`, `* item`, `+ item` or `1. item` / `1) item`.
fn list_item_text(line: &str) -> Option<String> {
    let t = line.trim_start();
    let rest = t
        .strip_prefix("- ")
        .or_else(|| t.strip_prefix("* "))
        .or_else(|| t.strip_prefix("+ "))
        .or_else(|| ordered_item_text(t));
    rest.map(|r| r.trim().to_string())
}

fn ordered_item_text(t: &str) -> Option<&str> {
    let digits = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(0);
    if digits == 0 {
        return None;
    }
    let rest = &t[digits..];
    let rest = rest
        .strip_prefix('.')
        .or_else(|| rest.strip_prefix(')'))?
        .trim_start();
    Some(rest)
}

fn parse_text_runbook(bytes: &[u8], fallback_id: &str) -> Result<Runbook, LoadIssue> {
    let revision = stable_revision(bytes);
    let text = String::from_utf8_lossy(bytes);
    let (meta, body) = split_front_matter(&text)?;
    let obj = front_matter_object(&meta)?;
    let id = id_field(&obj, fallback_id)?;
    let title = str_field(&obj, "title")?.unwrap_or_else(|| fallback_id.to_string());
    let team = str_field(&obj, "team")?;
    let priority = int_field(&obj, "priority")?.unwrap_or(0);
    let matches = matches_field(&obj)?;
    Ok(Runbook {
        id,
        revision,
        title,
        format: Format::Text,
        team,
        priority,
        matches,
        url: None,
        text: Some(body),
        steps: vec![StepDef {
            id: "s1".into(),
            kind: StepKind::Manual,
            text: "Follow the instructions below".into(),
            hint: None,
            action: None,
            params: Map::new(),
            approval_required: false,
            condition: None,
            resolves: false,
        }],
    })
}

fn parse_link_runbook(bytes: &[u8], fallback_id: &str) -> Result<Runbook, LoadIssue> {
    let revision = stable_revision(bytes);
    let text = String::from_utf8_lossy(bytes);
    let value: Value =
        serde_saphyr::from_str(&text).map_err(|e| LoadIssue::Syntax(e.to_string()))?;
    let obj = value
        .as_object()
        .ok_or_else(|| LoadIssue::Semantic("expected a mapping at the top level".into()))?;
    let id = id_field(obj, fallback_id)?;
    let title = str_field(obj, "title")?.unwrap_or_else(|| id.clone());
    let team = str_field(obj, "team")?;
    let priority = int_field(obj, "priority")?.unwrap_or(0);
    let matches = matches_field(obj)?;
    let url = match obj.get("url") {
        Some(Value::String(s)) if s.starts_with("http://") || s.starts_with("https://") => {
            s.clone()
        }
        Some(Value::String(s)) => {
            return Err(LoadIssue::Semantic(format!(
                "link runbook url must be http(s), got {s:?}"
            )));
        }
        Some(other) => {
            return Err(LoadIssue::Semantic(format!(
                "link runbook url must be a string, got {other}"
            )));
        }
        None => {
            return Err(LoadIssue::Semantic(
                "link runbook needs an http(s) url".into(),
            ));
        }
    };
    Ok(Runbook {
        id,
        revision,
        title,
        format: Format::Link,
        team,
        priority,
        matches,
        url: Some(url),
        text: None,
        steps: Vec::new(),
    })
}

// --- shared field extraction ---------------------------------------------------------------

fn id_field(obj: &Map<String, Value>, fallback: &str) -> Result<String, LoadIssue> {
    match obj.get("runbook") {
        None | Some(Value::Null) => Ok(fallback.to_string()),
        Some(Value::String(s)) if !s.trim().is_empty() => Ok(s.clone()),
        Some(Value::String(_)) => Err(LoadIssue::Semantic("runbook id must not be empty".into())),
        Some(other) => Err(LoadIssue::Semantic(format!(
            "runbook id must be a string, got {other}"
        ))),
    }
}

fn str_field(obj: &Map<String, Value>, key: &str) -> Result<Option<String>, LoadIssue> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(other) => Err(LoadIssue::Semantic(format!(
            "{key} must be a string, got {other}"
        ))),
    }
}

fn int_field(obj: &Map<String, Value>, key: &str) -> Result<Option<i32>, LoadIssue> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v
            .as_i64()
            .and_then(|n| i32::try_from(n).ok())
            .map(Some)
            .ok_or_else(|| LoadIssue::Semantic(format!("{key} must be an integer, got {v}"))),
    }
}

fn matches_field(obj: &Map<String, Value>) -> Result<Map<String, Value>, LoadIssue> {
    match obj.get("matches") {
        None | Some(Value::Null) => Ok(Map::new()),
        Some(Value::Object(m)) => Ok(m.clone()),
        Some(other) => Err(LoadIssue::Semantic(format!(
            "matches must be a mapping, got {other}"
        ))),
    }
}

// ---------------------------------------------------------------------------------------------
// Template context and rendering
// ---------------------------------------------------------------------------------------------

/// Build the template context from a notify payload (raw JSON) and the alert row values.
pub fn template_context(raw_payload: &Value, alert: &Value) -> Value {
    let event = raw_payload.get("event").filter(|e| e.is_object());
    let pick = |key: &str| -> Value {
        event
            .and_then(|e| e.get(key))
            .filter(|v| v.is_object())
            .cloned()
            .unwrap_or_else(|| json!({}))
    };
    json!({
        "raw": pick("raw"),
        "fields": pick("fields"),
        "context": raw_payload
            .get("context")
            .filter(|v| v.is_object())
            .cloned()
            .unwrap_or_else(|| json!({})),
        "alert": alert.clone(),
    })
}

/// The object minijinja actually sees: bare names resolve against `raw` top-level keys, then
/// `fields`, then `context`, with the nested keys themselves always present.
fn merged_context(ctx: &Value) -> Value {
    let raw = ctx.get("raw").filter(|v| v.is_object());
    let fields = ctx.get("fields").filter(|v| v.is_object());
    let context = ctx.get("context").filter(|v| v.is_object());
    let mut merged = Map::new();
    for source in [raw, fields, context].into_iter().flatten() {
        for (k, v) in source.as_object().into_iter().flatten() {
            merged.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }
    for (k, v) in [
        ("raw", raw.cloned().unwrap_or_else(|| json!({}))),
        ("fields", fields.cloned().unwrap_or_else(|| json!({}))),
        ("context", context.cloned().unwrap_or_else(|| json!({}))),
        (
            "alert",
            ctx.get("alert").cloned().unwrap_or_else(|| json!({})),
        ),
    ] {
        merged.insert(k.into(), v);
    }
    Value::Object(merged)
}

/// A human label for the built-in actions (`RunbookStep.action.label`, and the default text of
/// an action step without `check:`/`label:`).
fn action_label(name: &str) -> String {
    match name {
        "http_get" => "HTTP GET request".into(),
        "retry_workflow" => "Retry workflow".into(),
        "restart_service" => "Restart service".into(),
        "open_ticket" => "Open a ticket".into(),
        "escalate" => "Escalate to another team".into(),
        "notify" => "Notify someone".into(),
        other => {
            let spaced = other.replace('_', " ");
            let mut chars = spaced.chars();
            match chars.next() {
                Some(f) => f.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        }
    }
}

/// Keys that look like secrets are never rendered or shown.
fn is_secret_key(k: &str) -> bool {
    let k = k.to_ascii_lowercase();
    ["token", "password", "secret", "key", "authorization"]
        .iter()
        .any(|s| k.contains(s))
}

/// A plain variable path: `workflowId`, `raw.error.code` — not a literal, filter or expression.
fn is_plain_path(s: &str) -> bool {
    if s.is_empty()
        || s.starts_with(|c: char| c.is_ascii_digit())
        || matches!(s, "true" | "false" | "none" | "True" | "False" | "None")
    {
        return false;
    }
    s.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
        && !s.split('.').any(str::is_empty)
}

/// The first `{{ name }}` in `text` that does not resolve in `ctx`, as
/// `"missing value for <name>"`. Complex expressions are left to minijinja's own error.
fn first_missing_name(text: &str, ctx: &Value) -> Option<String> {
    let mut rest = text;
    while let Some(start) = rest.find("{{") {
        rest = &rest[start + 2..];
        let Some(end) = rest.find("}}") else {
            break; // unterminated: minijinja reports the syntax error
        };
        let expr = rest[..end].trim();
        rest = &rest[end + 2..];
        let head = expr.split('|').next().unwrap_or("").trim();
        if is_plain_path(head) && resolve_path(ctx, head).is_none() {
            return Some(format!("missing value for {head}"));
        }
    }
    None
}

/// Render one template string against the merged context; the error is the step's
/// `render_error` (a named missing value when we can detect one, else minijinja's message).
fn render_template(
    env: &minijinja::Environment,
    source: &str,
    ctx: &Value,
) -> Result<String, String> {
    if let Some(missing) = first_missing_name(source, ctx) {
        return Err(missing);
    }
    env.render_str(source, ctx).map_err(|e| e.to_string())
}

/// Render every step; `risk_of(action_name)` gives `"low" | "high"` (from the action registry).
pub fn render(
    runbook: &Runbook,
    ctx: &Value,
    risk_of: &dyn Fn(&str) -> &'static str,
) -> Vec<RenderedStep> {
    let merged = merged_context(ctx);
    let mut env = minijinja::Environment::new();
    env.set_undefined_behavior(minijinja::UndefinedBehavior::Strict);
    env.set_auto_escape_callback(|_| minijinja::AutoEscape::None);
    env.set_fuel(Some(10_000));

    runbook
        .steps
        .iter()
        .map(|s| {
            let mut render_error = None;
            let text = match render_template(&env, &s.text, &merged) {
                Ok(t) => t,
                Err(e) => {
                    render_error = Some(e);
                    s.text.clone()
                }
            };
            let action = s.action.as_deref().map(|name| {
                let risk = risk_of(name);
                let mut params = Map::new();
                for (k, v) in &s.params {
                    if is_secret_key(k) {
                        params.insert(k.clone(), json!("***"));
                        continue;
                    }
                    let rendered = match v {
                        Value::String(tpl) => match render_template(&env, tpl, &merged) {
                            Ok(t) => json!(t),
                            Err(e) => {
                                if render_error.is_none() {
                                    render_error = Some(e);
                                }
                                json!(tpl)
                            }
                        },
                        other => other.clone(),
                    };
                    params.insert(k.clone(), rendered);
                }
                json!({
                    "name": name,
                    "label": action_label(name),
                    "params": Value::Object(params),
                    "risk": risk,
                })
            });
            let approval = if s.approval_required
                || s.action.as_deref().is_some_and(|a| risk_of(a) == "high")
            {
                "required"
            } else {
                "none"
            };
            RenderedStep {
                id: s.id.clone(),
                kind: s.kind,
                text,
                hint: s.hint.clone(),
                action,
                approval: approval.to_string(),
                condition: s.condition.clone(),
                resolves: s.resolves,
                render_error,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Freezing and projecting
// ---------------------------------------------------------------------------------------------

/// Freeze the rendered runbook for an alert (`ON CONFLICT (alert_id) DO NOTHING`); `true` if
/// inserted. Called by the router in the first-notification tx.
pub async fn attach(
    tx: &mut Transaction<'_, Postgres>,
    alert_id: Uuid,
    runbook: &Runbook,
    steps: &[RenderedStep],
    ctx: &Value,
) -> Result<bool, sqlx::Error> {
    let steps_json = serde_json::to_value(steps).unwrap_or_else(|_| json!([]));
    let res = sqlx::query(
        "INSERT INTO alert_runbooks \
             (alert_id, runbook_id, revision, title, format, url, text, steps, context) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
         ON CONFLICT (alert_id) DO NOTHING",
    )
    .bind(alert_id)
    .bind(&runbook.id)
    .bind(runbook.revision)
    .bind(&runbook.title)
    .bind(runbook.format.as_str())
    .bind(runbook.url.as_deref())
    .bind(runbook.text.as_deref())
    .bind(&steps_json)
    .bind(ctx)
    .execute(&mut **tx)
    .await?;
    Ok(res.rows_affected() == 1)
}

#[derive(Debug, sqlx::FromRow)]
struct EmailSectionRow {
    title: String,
    url: Option<String>,
    format: String,
    text: Option<String>,
    steps: Value,
}

/// Plain-text section appended to emails ("Runbook: <title>" + numbered rendered steps, with
/// "[needs approval]" / "[disabled: ...]" markers), or `None` when the alert has no runbook.
pub async fn email_section(pool: &PgPool, alert_id: Uuid) -> Result<Option<String>, sqlx::Error> {
    let row: Option<EmailSectionRow> = sqlx::query_as(
        "SELECT title, url, format, text, steps FROM alert_runbooks WHERE alert_id = $1",
    )
    .bind(alert_id)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let steps: Vec<RenderedStep> = serde_json::from_value(row.steps).unwrap_or_default();
    let mut out = format!("Runbook: {}", row.title);
    for (i, s) in steps.iter().enumerate() {
        out.push_str(&format!("\n{}. {}", i + 1, s.text));
        if s.approval == "required" {
            out.push_str(" [needs approval]");
        }
        if let Some(err) = &s.render_error {
            out.push_str(&format!(" [disabled: {err}]"));
        }
    }
    if let Some(url) = row.url.as_deref().filter(|u| !u.is_empty()) {
        out.push_str(&format!("\nFull runbook: {url}"));
    }
    if row.format == "text"
        && let Some(text) = row.text.as_deref().filter(|t| !t.is_empty())
    {
        out.push_str("\n\n");
        out.push_str(text);
    }
    Ok(Some(out))
}

/// RFC 3339 UTC with milliseconds and `Z`, the contract's `Timestamp`.
fn ts(t: chrono::DateTime<chrono::Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[derive(Debug, sqlx::FromRow)]
struct ExecRow {
    id: Uuid,
    step_id: String,
    state: String,
    requested_by: String,
    started_at: Option<chrono::DateTime<chrono::Utc>>,
    finished_at: Option<chrono::DateTime<chrono::Utc>>,
    output: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct ApprovalRow {
    id: Uuid,
    step_id: String,
    state: String,
    requested_by: String,
    requested_at: chrono::DateTime<chrono::Utc>,
    expires_at: chrono::DateTime<chrono::Utc>,
    eligible: Vec<String>,
    decided_by: Option<String>,
    decided_at: Option<chrono::DateTime<chrono::Utc>>,
    note: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct MarkRow {
    step_id: String,
    done_by: String,
    done_at: chrono::DateTime<chrono::Utc>,
}

fn approval_json(a: &ApprovalRow, actor: &str) -> Value {
    json!({
        "id": a.id.to_string(),
        "state": a.state,
        "requested_by": a.requested_by,
        "requested_at": ts(a.requested_at),
        "expires_at": ts(a.expires_at),
        "eligible": a.eligible,
        "can_decide": a.state == "pending" && a.eligible.iter().any(|e| e == actor),
        "sms_code": Value::Null,
        "decided_by": a.decided_by,
        "decided_at": a.decided_at.map(ts),
        "note": a.note,
    })
}

#[derive(Debug, sqlx::FromRow)]
struct RunbookRow {
    runbook_id: String,
    revision: i32,
    title: String,
    format: String,
    url: Option<String>,
    text: Option<String>,
    steps: Value,
}

/// The contract's `RunbookView` for `AlertDetail.runbook` (or `None`): the frozen snapshot plus
/// per-step live state for this alert instance:
/// - `state`: action steps → latest execution state (`queued|running|ok|failed|uncertain`),
///   else `awaiting_approval` (pending approval) / `rejected` (latest approval rejected), else
///   `idle`; manual steps → `done` if marked else `idle`.
/// - `disabled_reason`: render error, unmet condition ("Available after 2 failed retries (1 so
///   far)" via `actions::condition_status`), or alert resolved.
/// - `runs`: executions oldest first (`Execution` shape); `approval_request`: the latest
///   approval (`ApprovalRequest` shape, `can_decide = actor ∈ eligible && pending`,
///   `sms_code: null` (never expose the code)); `done_by`/`done_at` from `step_marks`.
pub async fn runbook_view(
    pool: &PgPool,
    alert_id: Uuid,
    actor: &str,
) -> Result<Option<Value>, sqlx::Error> {
    let row: Option<RunbookRow> = sqlx::query_as(
        "SELECT runbook_id, revision, title, format, url, text, steps \
             FROM alert_runbooks WHERE alert_id = $1",
    )
    .bind(alert_id)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let steps: Vec<RenderedStep> = serde_json::from_value(row.steps).unwrap_or_default();

    let alert_status: Option<String> =
        sqlx::query_scalar("SELECT status FROM alerts WHERE id = $1")
            .bind(alert_id)
            .fetch_optional(pool)
            .await?;
    let resolved = alert_status.as_deref() == Some("resolved");

    let execs: Vec<ExecRow> = sqlx::query_as::<_, ExecRow>(
        "SELECT id, step_id, state, requested_by, started_at, finished_at, output \
         FROM action_executions WHERE alert_id = $1 ORDER BY created_at, id",
    )
    .bind(alert_id)
    .fetch_all(pool)
    .await?;
    let approvals: Vec<ApprovalRow> = sqlx::query_as::<_, ApprovalRow>(
        "SELECT id, step_id, state, requested_by, requested_at, expires_at, eligible, \
                decided_by, decided_at, note \
         FROM approvals WHERE alert_id = $1 ORDER BY requested_at, id",
    )
    .bind(alert_id)
    .fetch_all(pool)
    .await?;
    let marks: Vec<MarkRow> = sqlx::query_as::<_, MarkRow>(
        "SELECT step_id, done_by, done_at FROM step_marks WHERE alert_id = $1",
    )
    .bind(alert_id)
    .fetch_all(pool)
    .await?;

    let mut step_jsons = Vec::with_capacity(steps.len());
    for s in &steps {
        // disabled_reason: render error, then unmet condition, then alert resolved.
        let mut disabled_reason = s.render_error.clone();
        if disabled_reason.is_none()
            && let Some(cond) = &s.condition
        {
            let (met, reason) = crate::actions::condition_status(pool, alert_id, cond).await?;
            if !met {
                disabled_reason = reason;
            }
        }
        if disabled_reason.is_none() && resolved && s.kind == StepKind::Action {
            disabled_reason = Some("Alert is resolved".to_string());
        }

        let step_execs: Vec<&ExecRow> = execs.iter().filter(|e| e.step_id == s.id).collect();
        let latest_approval = approvals.iter().rev().find(|a| a.step_id == s.id);
        let mark = marks.iter().find(|m| m.step_id == s.id);

        let state = if s.kind == StepKind::Action {
            step_execs
                .last()
                .map(|e| e.state.clone())
                .unwrap_or_else(|| match latest_approval {
                    Some(a) if a.state == "pending" => "awaiting_approval".to_string(),
                    Some(a) if a.state == "rejected" => "rejected".to_string(),
                    _ => "idle".to_string(),
                })
        } else if s.kind == StepKind::Manual && mark.is_some() {
            "done".to_string()
        } else {
            "idle".to_string()
        };

        let runs: Vec<Value> = step_execs
            .iter()
            .map(|e| {
                json!({
                    "id": e.id.to_string(),
                    "state": e.state,
                    "requested_by": e.requested_by,
                    "started_at": e.started_at.map(ts),
                    "finished_at": e.finished_at.map(ts),
                    "output": e.output,
                })
            })
            .collect();

        step_jsons.push(json!({
            "id": s.id,
            "kind": s.kind.as_str(),
            "text": s.text,
            "hint": s.hint,
            "action": s.action.clone().unwrap_or(Value::Null),
            "approval": s.approval,
            "state": state,
            "disabled_reason": disabled_reason,
            "done_by": mark.map(|m| m.done_by.clone()),
            "done_at": mark.map(|m| ts(m.done_at)),
            "runs": runs,
            "approval_request": latest_approval
                .map(|a| approval_json(a, actor))
                .unwrap_or(Value::Null),
        }));
    }

    Ok(Some(json!({
        "id": row.runbook_id,
        "revision": row.revision,
        "title": row.title,
        "format": row.format,
        "url": row.url,
        "text": row.text,
        "steps": step_jsons,
    })))
}

/// `GET /api/runbooks` items: `{id, title, format, team, steps: <count>}`.
pub fn list_items(lib: &RunbookLibrary) -> Vec<Value> {
    lib.runbooks
        .iter()
        .map(|r| {
            json!({
                "id": r.id,
                "title": r.title,
                "format": r.format.as_str(),
                "team": r.team,
                "steps": r.steps.len(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const KNOWN: &[&str] = &[
        "http_get",
        "retry_workflow",
        "restart_service",
        "open_ticket",
        "escalate",
        "notify",
    ];

    fn yaml(text: &str) -> Result<Runbook, LoadIssue> {
        parse_yaml_runbook(text.as_bytes(), "fallback", KNOWN)
    }

    const APPENDIX_B: &str = "runbook: settlement-timeout\nteam: settlements\nmatches:\n  \
        type: WORKFLOW_FAILED\n  error.code: TIMEOUT\nsteps:\n  - check: \"Is counterparty \
        service healthy?\"\n    action: http_get\n    url: \"http://counterparty-service/health\"\n  \
        - action: retry_workflow\n    workflowId: \"{{workflowId}}\"\n    approval: none\n  - if: \
        \"retry fails twice\"\n    action: escalate\n    team: counterparty-support\n    \
        approval: required\n";

    #[test]
    fn yaml_parses_appendix_b() {
        let rb = yaml(APPENDIX_B).expect("parses");
        assert_eq!(rb.id, "settlement-timeout");
        assert_eq!(rb.format, Format::Yaml);
        assert_eq!(rb.team.as_deref(), Some("settlements"));
        assert_eq!(rb.title, "settlement-timeout"); // no title -> id
        assert!(rb.revision > 0);
        assert_eq!(
            rb.matches.get("type").and_then(Value::as_str),
            Some("WORKFLOW_FAILED")
        );
        assert_eq!(rb.matches.get("error.code"), Some(&json!("TIMEOUT")));
        assert_eq!(rb.steps.len(), 3);

        let s1 = &rb.steps[0];
        assert_eq!((s1.id.as_str(), s1.kind), ("s1", StepKind::Action));
        assert_eq!(s1.text, "Is counterparty service healthy?");
        assert_eq!(s1.action.as_deref(), Some("http_get"));
        assert_eq!(
            s1.params.get("url"),
            Some(&json!("http://counterparty-service/health"))
        );
        assert!(!s1.approval_required);

        let s2 = &rb.steps[1];
        assert_eq!(s2.action.as_deref(), Some("retry_workflow"));
        assert_eq!(s2.params.get("workflowId"), Some(&json!("{{workflowId}}")));
        assert_eq!(s2.text, "Retry workflow"); // default action label
        assert!(!s2.approval_required);
        assert!(s2.condition.is_none());

        let s3 = &rb.steps[2];
        assert_eq!(s3.action.as_deref(), Some("escalate"));
        assert_eq!(s3.params.get("team"), Some(&json!("counterparty-support")));
        assert!(s3.approval_required);
        assert_eq!(
            s3.condition,
            Some(Condition::AfterFailures {
                action: "retry_workflow".into(),
                count: 2
            })
        );
    }

    #[test]
    fn yaml_step_ids_labels_resolves_condition_kind_and_priority() {
        let rb = yaml(
            "runbook: mixed\ntitle: Mixed\npriority: 7\nteam: t\nsteps:\n  - id: health\n    \
             check: Service healthy?\n    action: http_get\n    url: http://x/health\n    \
             resolves: true\n  - action: notify\n    label: Ping the on-call\n    to: \
             user:carol\n  - if: \"retry fails twice\"\n    check: Then consider escalating\n",
        )
        .expect("parses");
        assert_eq!(rb.title, "Mixed");
        assert_eq!(rb.priority, 7);

        let health = &rb.steps[0];
        assert_eq!(health.id, "health"); // explicit id
        assert!(health.resolves);
        assert_eq!(rb.steps[1].id, "s2"); // position-based
        let notify = &rb.steps[1];
        assert_eq!(notify.text, "Ping the on-call"); // label as text
        assert_eq!(notify.params.get("to"), Some(&json!("user:carol")));

        let gate = &rb.steps[2];
        assert_eq!(gate.kind, StepKind::Condition); // if: without action
        assert!(gate.action.is_none());
        assert_eq!(gate.text, "Then consider escalating");
        assert_eq!(
            gate.condition,
            Some(Condition::AfterFailures {
                action: "retry_workflow".into(),
                count: 2
            })
        );
    }

    #[test]
    fn yaml_errors() {
        // unknown action
        let e = yaml("runbook: x\nsteps:\n  - action: detonate\n").unwrap_err();
        assert!(
            matches!(e, LoadIssue::Semantic(ref m) if m.contains("detonate")),
            "{e:?}"
        );
        // bad yaml syntax
        let e = yaml("runbook: x\nsteps: [::\n").unwrap_err();
        assert!(matches!(e, LoadIssue::Syntax(_)), "{e:?}");
        // bad approval
        let e = yaml("runbook: x\nsteps:\n  - action: notify\n    approval: maybe\n").unwrap_err();
        assert!(
            matches!(e, LoadIssue::Semantic(ref m) if m.contains("approval")),
            "{e:?}"
        );
        // duplicate step ids
        let e =
            yaml("runbook: x\nsteps:\n  - id: same\n    check: a\n  - id: same\n    check: b\n")
                .unwrap_err();
        assert!(
            matches!(e, LoadIssue::Semantic(ref m) if m.contains("duplicate step id")),
            "{e:?}"
        );
        // non-scalar param
        let e = yaml("runbook: x\nsteps:\n  - action: notify\n    deep: {a: 1}\n").unwrap_err();
        assert!(
            matches!(e, LoadIssue::Semantic(ref m) if m.contains("scalar")),
            "{e:?}"
        );
        // empty runbook id
        let e = yaml("runbook: \"\"\n").unwrap_err();
        assert!(
            matches!(e, LoadIssue::Semantic(ref m) if m.contains("empty")),
            "{e:?}"
        );
        // unparseable condition phrase
        let e = yaml("runbook: x\nsteps:\n  - if: \"sometimes things break\"\n").unwrap_err();
        assert!(
            matches!(e, LoadIssue::Semantic(ref m) if m.contains("cannot parse")),
            "{e:?}"
        );
        // non-mapping top level
        let e = yaml("- just\n- a\n- list\n").unwrap_err();
        assert!(
            matches!(e, LoadIssue::Semantic(ref m) if m.contains("mapping")),
            "{e:?}"
        );
    }

    #[test]
    fn if_phrase_variants() {
        let c = parse_if_phrase("retry fails twice", KNOWN).unwrap();
        assert_eq!(
            c,
            Condition::AfterFailures {
                action: "retry_workflow".into(),
                count: 2
            }
        );
        let c = parse_if_phrase("retry_workflow fails once", KNOWN).unwrap();
        assert_eq!(
            c,
            Condition::AfterFailures {
                action: "retry_workflow".into(),
                count: 1
            }
        );
        let c = parse_if_phrase("http_get fails three times", KNOWN).unwrap();
        assert_eq!(
            c,
            Condition::AfterFailures {
                action: "http_get".into(),
                count: 3
            }
        );
        let c = parse_if_phrase("restart fails 4 times", KNOWN).unwrap();
        assert_eq!(
            c,
            Condition::AfterFailures {
                action: "restart_service".into(),
                count: 4
            }
        );
        for bad in [
            "nope fails twice",
            "fails twice",
            "retry fails",
            "retry fails lots",
            "retry failed twice",
        ] {
            assert!(parse_if_phrase(bad, KNOWN).is_err(), "{bad:?}");
        }
    }

    const APPENDIX_B_MD: &str = "---\nteam: payments\nmatches:\n  type: HighErrorRate\n---\n# \
        HighErrorRate on payments-api\n1. Open the payments dashboard and check if errors are in \
        one region only.\n2. If one region: fail traffic over to the other region (needs approval \
        from the team lead).\n3. If all regions: check the last deployment and roll it back.\n4. \
        Post an update in the #payments-incidents channel every 15 minutes.\n";

    #[test]
    fn markdown_parses_appendix_b() {
        let rb = parse_markdown_runbook(APPENDIX_B_MD.as_bytes(), "payments-high-error-rate")
            .expect("parses");
        assert_eq!(rb.id, "payments-high-error-rate");
        assert_eq!(rb.format, Format::Markdown);
        assert_eq!(rb.title, "HighErrorRate on payments-api"); // first heading
        assert_eq!(rb.team.as_deref(), Some("payments"));
        assert_eq!(rb.matches.get("type"), Some(&json!("HighErrorRate")));
        assert!(
            rb.text
                .as_deref()
                .is_some_and(|t| t.contains("# HighErrorRate"))
        );
        assert_eq!(rb.steps.len(), 4);
        for s in &rb.steps {
            assert_eq!(s.kind, StepKind::Manual);
            assert!(s.action.is_none());
            assert!(!s.approval_required);
        }
        assert_eq!(
            rb.steps[0].text,
            "Open the payments dashboard and check if errors are in one region only."
        );
        assert_eq!(
            rb.steps[1].hint.as_deref(),
            Some("Needs approval (shown as guidance, not enforced)")
        );
        assert!(rb.steps[0].hint.is_none());
    }

    #[test]
    fn markdown_plain_file_bullets_and_default_title() {
        let plain = "- first thing\n* second thing\nplain paragraph\n";
        let rb = parse_markdown_runbook(plain.as_bytes(), "notes").expect("parses");
        assert_eq!(rb.title, "notes"); // no heading -> file stem
        assert!(rb.team.is_none());
        assert_eq!(rb.steps.len(), 2); // `-` and `*` items, paragraph skipped
        assert_eq!(rb.steps[0].text, "first thing");
        assert_eq!(rb.steps[1].text, "second thing");
        // the whole file is the text (no front matter to strip)
        assert_eq!(rb.text.as_deref(), Some(plain));
    }

    #[test]
    fn text_parses_free_text_with_front_matter() {
        let raw = "---\nteam: pricing\n---\nWhen pricing latency goes over a second, it's \
                   usually the cache.\n";
        let rb = parse_text_runbook(raw.as_bytes(), "pricing-latency").expect("parses");
        assert_eq!(rb.format, Format::Text);
        assert_eq!(rb.title, "pricing-latency");
        assert_eq!(rb.team.as_deref(), Some("pricing"));
        assert_eq!(rb.steps.len(), 1);
        let s = &rb.steps[0];
        assert_eq!((s.id.as_str(), s.kind), ("s1", StepKind::Manual));
        assert_eq!(s.text, "Follow the instructions below");
        assert_eq!(
            rb.text.as_deref(),
            Some("When pricing latency goes over a second, it's usually the cache.")
        );
    }

    #[test]
    fn link_parses_and_rejects_non_http() {
        let rb = parse_link_runbook(
            "runbook: trade-rejected\ntitle: Rejected trades (wiki)\nurl: \
             https://wiki.example.test/rates/trade-rejected\nteam: rates\n"
                .as_bytes(),
            "ignored",
        )
        .expect("parses");
        assert_eq!(rb.format, Format::Link);
        assert_eq!(rb.id, "trade-rejected");
        assert_eq!(rb.title, "Rejected trades (wiki)");
        assert_eq!(
            rb.url.as_deref(),
            Some("https://wiki.example.test/rates/trade-rejected")
        );
        assert_eq!(rb.team.as_deref(), Some("rates"));
        assert!(rb.steps.is_empty());
        assert!(rb.text.is_none());

        // plain http is fine; ftp is not; a missing url is not
        let ok = parse_link_runbook(b"runbook: a\nurl: http://wiki.test/a\n", "a").unwrap();
        assert_eq!(ok.url.as_deref(), Some("http://wiki.test/a"));
        let e = parse_link_runbook(b"runbook: a\nurl: ftp://wiki.test/a\n", "a").unwrap_err();
        assert!(
            matches!(e, LoadIssue::Semantic(ref m) if m.contains("http(s)")),
            "{e:?}"
        );
        let e = parse_link_runbook(b"runbook: a\ntitle: t\n", "a").unwrap_err();
        assert!(
            matches!(e, LoadIssue::Semantic(ref m) if m.contains("needs an http(s)")),
            "{e:?}"
        );
    }

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "beacon-runbooks-{name}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn library_loads_all_formats_and_skips_unknown_files() {
        let dir = tmp("load");
        std::fs::write(dir.join("a.yaml"), APPENDIX_B).unwrap();
        std::fs::write(dir.join("payments-high-error-rate.md"), APPENDIX_B_MD).unwrap();
        std::fs::write(dir.join("c.txt"), "team: pricing in front matter only\n").unwrap();
        std::fs::write(
            dir.join("d.link.yaml"),
            "runbook: trade-rejected\nurl: https://wiki.test/r\n",
        )
        .unwrap();
        std::fs::write(dir.join("ignored.docx"), "junk").unwrap();
        std::fs::write(dir.join(".hidden.yaml"), "runbook: hidden\n").unwrap();

        let lib = RunbookLibrary::load(&dir, KNOWN).expect("loads");
        let ids: Vec<&str> = lib.runbooks.iter().map(|r| r.id.as_str()).collect();
        // sorted by file name: a.yaml, c.txt, d.link.yaml, payments-high-error-rate.md
        assert_eq!(
            ids,
            [
                "settlement-timeout",
                "c",
                "trade-rejected",
                "payments-high-error-rate"
            ]
        );
        assert_eq!(lib.get("settlement-timeout").unwrap().format, Format::Yaml);
        assert_eq!(
            lib.get("payments-high-error-rate").unwrap().format,
            Format::Markdown
        );
        assert_eq!(lib.get("c").unwrap().format, Format::Text);
        assert_eq!(lib.get("trade-rejected").unwrap().format, Format::Link);
        assert!(lib.get("hidden").is_none());

        // revisions are stable across loads and change with content
        let again = RunbookLibrary::load(&dir, KNOWN).unwrap();
        assert_eq!(
            again.get("settlement-timeout").unwrap().revision,
            lib.get("settlement-timeout").unwrap().revision
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn library_rejects_duplicate_ids() {
        let dir = tmp("dup");
        std::fs::write(dir.join("one.yaml"), "runbook: same\nsteps: []\n").unwrap();
        std::fs::write(dir.join("two.md"), "---\nrunbook: same\n---\n# Two\n").unwrap();
        let err = RunbookLibrary::load(&dir, KNOWN).unwrap_err();
        assert!(err.to_string().contains("duplicate id \"same\""), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_directory_is_an_empty_library() {
        let lib = RunbookLibrary::load(Path::new("/nonexistent-runbooks-dir"), KNOWN).unwrap();
        assert!(lib.runbooks.is_empty());
    }

    #[test]
    fn unterminated_front_matter_is_content() {
        let raw = "---\njust a horizontal rule\nthen text\n";
        let (meta, body) = split_front_matter(raw).expect("ok");
        assert!(meta.is_none());
        assert_eq!(body, raw);
        // bad front matter yaml is a load error
        let raw = "---\nteam: [unclosed\n---\nbody\n";
        assert!(split_front_matter(raw).is_err());
    }

    // --- selection ---------------------------------------------------------------------------

    fn lib_with_matches() -> RunbookLibrary {
        let mut runbooks = Vec::new();
        for (id, team, priority, matches) in [
            ("settlements-a", Some("settlements"), 1, json!({})),
            (
                "settlements-b",
                Some("settlements"),
                5,
                json!({"type": "WORKFLOW_FAILED", "error.code": "TIMEOUT"}),
            ),
            (
                "settlements-c",
                Some("settlements"),
                5,
                json!({"type": "OTHER"}),
            ),
            ("payments-x", Some("payments"), 9, json!({})),
            ("untagged", None, 99, json!({})),
        ] {
            runbooks.push(Runbook {
                id: id.into(),
                revision: 1,
                title: id.into(),
                format: Format::Yaml,
                team: team.map(str::to_string),
                priority,
                matches: matches.as_object().cloned().unwrap_or_default(),
                url: None,
                text: None,
                steps: Vec::new(),
            });
        }
        RunbookLibrary { runbooks }
    }

    fn tctx(raw: Value, fields: Value) -> Value {
        json!({"raw": raw, "fields": fields, "context": {}, "alert": {}})
    }

    #[test]
    fn select_rule_binding_wins_then_team_matches_priority_id() {
        let lib = lib_with_matches();
        // explicit rule binding beats everything (even other teams / no match)
        let rb = lib
            .select(
                Some("payments-x"),
                "settlements",
                &tctx(json!({}), json!({})),
            )
            .unwrap();
        assert_eq!(rb.id, "payments-x");
        // an unknown rule id falls through to the team scope
        let rb = lib
            .select(
                Some("no-such-runbook"),
                "settlements",
                &tctx(json!({}), json!({})),
            )
            .unwrap();
        assert_eq!(rb.id, "settlements-a"); // matches trivially, only candidate

        // team scope: dot paths into raw/fields, highest priority then id
        let ctx = tctx(
            json!({"error": {"code": "TIMEOUT"}}),
            json!({"type": "WORKFLOW_FAILED"}),
        );
        let rb = lib.select(None, "settlements", &ctx).unwrap();
        assert_eq!(rb.id, "settlements-b"); // priority 5 beats settlements-a's 1
        assert_eq!(
            lib.select(
                None,
                "settlements",
                &tctx(
                    json!({"error": {"code": "INTERNAL"}}),
                    json!({"type": "WORKFLOW_FAILED"})
                )
            )
            .unwrap()
            .id,
            "settlements-a", // b no longer matches, c never did
        );
        // a team with no runbooks, and other teams' runbooks, never match
        assert!(lib.select(None, "pricing", &ctx).is_none());
        // `fields.type = OTHER` selects the runbook that asks for it
        assert_eq!(
            lib.select(
                None,
                "settlements",
                &tctx(json!({}), json!({"type": "OTHER"}))
            )
            .unwrap()
            .id,
            "settlements-c",
        );
    }

    #[test]
    fn matches_priority_is_raw_then_fields_then_context() {
        let mut lib = lib_with_matches();
        // priority 4: below b, so the fields-fallback assertion below is not an id tie-break
        lib.runbooks.push(Runbook {
            id: "contextual".into(),
            revision: 1,
            title: "contextual".into(),
            format: Format::Yaml,
            team: Some("settlements".into()),
            priority: 4,
            matches: serde_json::Map::from_iter([("type".to_string(), json!("WORKFLOW_FAILED"))]),
            url: None,
            text: None,
            steps: Vec::new(),
        });
        // raw wins when both raw and fields carry the key
        let ctx = tctx(json!({"type": "OTHER"}), json!({"type": "WORKFLOW_FAILED"}));
        let rb = lib.select(None, "settlements", &ctx).unwrap();
        assert_eq!(rb.id, "settlements-c"); // raw.type = OTHER matches c, not b
        assert_ne!(rb.id, "contextual");
        // fields is consulted when raw lacks the key: b is out (its `error.code` resolves
        // nowhere), contextual matches `type` through `fields`.
        let ctx = tctx(json!({}), json!({"type": "WORKFLOW_FAILED"}));
        assert_eq!(
            lib.select(None, "settlements", &ctx).unwrap().id,
            "contextual"
        );
    }

    // --- template context and rendering -----------------------------------------------------

    #[test]
    fn template_context_shape() {
        let payload = json!({
            "event": {"raw": {"workflowId": "WF-1"}, "fields": {"type": "WORKFLOW_FAILED"}},
            "context": {"service": "order-settlement"},
        });
        let alert =
            json!({"id": "a", "team": "settlements", "rule_id": "r", "severity": "critical"});
        let ctx = template_context(&payload, &alert);
        assert_eq!(ctx["raw"]["workflowId"], "WF-1");
        assert_eq!(ctx["fields"]["type"], "WORKFLOW_FAILED");
        assert_eq!(ctx["context"]["service"], "order-settlement");
        assert_eq!(ctx["alert"]["id"], "a");

        // older producers without event/context: empty objects, not nulls
        let ctx = template_context(&json!({}), &json!({}));
        for k in ["raw", "fields", "context", "alert"] {
            assert!(ctx[k].is_object(), "{k} should be an empty object");
        }
    }

    fn risk_of(name: &str) -> &'static str {
        crate::actions::ActionRegistry::risk_of(name)
    }

    fn render_appendix_b(payload: &Value) -> Vec<RenderedStep> {
        let rb = yaml(APPENDIX_B).expect("parses");
        let alert = json!({"id": "a", "team": "settlements", "severity": "critical"});
        let ctx = template_context(payload, &alert);
        render(&rb, &ctx, &risk_of)
    }

    #[test]
    fn render_fills_bare_names_from_raw_first() {
        let steps = render_appendix_b(&json!({
            "event": {"raw": {"workflowId": "WF-1"}, "fields": {"workflowId": "WRONG"}},
        }));
        assert_eq!(
            steps[1].action.as_ref().unwrap()["params"],
            json!({"workflowId": "WF-1"})
        );
        assert!(steps[1].render_error.is_none());
        assert!(steps[0].render_error.is_none());
    }

    #[test]
    fn render_missing_value_keeps_raw_text_with_named_error() {
        let steps = render_appendix_b(&json!({}));
        let s2 = &steps[1];
        assert_eq!(
            s2.render_error.as_deref(),
            Some("missing value for workflowId")
        );
        // the raw template text is kept
        assert_eq!(
            s2.action.as_ref().unwrap()["params"]["workflowId"],
            json!("{{workflowId}}")
        );
        // the step text itself has no template, so it rendered fine
        assert_eq!(s2.text, "Retry workflow");

        // a missing dotted path in the step text
        let rb = yaml(
            "runbook: dotted\nsteps:\n  - check: \"Error code {{raw.error.code}}\"\n    \
             action: http_get\n    url: http://x/h\n",
        )
        .unwrap();
        let ctx = template_context(&json!({"event": {"raw": {}}}), &json!({}));
        let steps = render(&rb, &ctx, &risk_of);
        assert_eq!(
            steps[0].render_error.as_deref(),
            Some("missing value for raw.error.code")
        );
        assert_eq!(steps[0].text, "Error code {{raw.error.code}}");
    }

    #[test]
    fn render_redacts_secrets_and_escalates_high_risk_to_required() {
        let rb = yaml(concat!(
            "runbook: secrets\nsteps:\n",
            "  - action: http_get\n    url: http://x/h\n    apiKey: '{{workflowId}}'\n",
            "    auth_token: abc\n    note: plain\n",
            "  - action: restart_service\n    service: pricing-engine\n    approval: none\n",
        ))
        .unwrap();
        let ctx = template_context(
            &json!({"event": {"raw": {"workflowId": "WF-9"}}}),
            &json!({}),
        );
        let steps = render(&rb, &ctx, &risk_of);
        let params = &steps[0].action.as_ref().unwrap()["params"];
        assert_eq!(params["apiKey"], json!("***"));
        assert_eq!(params["auth_token"], json!("***"));
        assert_eq!(params["note"], json!("plain"));
        assert_eq!(params["url"], json!("http://x/h"));
        // http_get is low risk and says nothing -> none
        assert_eq!(steps[0].approval, "none");
        // restart_service is high risk: approval becomes required even though the step says none
        assert_eq!(steps[1].approval, "required");
        assert_eq!(steps[1].action.as_ref().unwrap()["risk"], json!("high"));
        assert_eq!(
            steps[1].action.as_ref().unwrap()["label"],
            json!("Restart service")
        );
    }

    #[test]
    fn render_action_shape_matches_the_contract() {
        let steps = render_appendix_b(&json!({
            "event": {"raw": {"workflowId": "WF-1"}},
        }));
        let action = steps[2].action.as_ref().unwrap();
        for key in ["name", "label", "params", "risk"] {
            assert!(action.get(key).is_some(), "missing {key}");
        }
        assert_eq!(action["name"], json!("escalate"));
        assert_eq!(action["params"]["team"], json!("counterparty-support"));
        assert_eq!(action["risk"], json!("high"));
        // approval required both by the step and by the risk
        assert_eq!(steps[2].approval, "required");
    }

    #[test]
    fn render_syntax_error_keeps_raw_text() {
        let rb = yaml(
            "runbook: broken\nsteps:\n  - check: \"Unclosed {{ workflowId\"\n    action: \
             http_get\n    url: http://x/h\n",
        )
        .unwrap();
        let ctx = template_context(&json!({}), &json!({}));
        let steps = render(&rb, &ctx, &risk_of);
        assert!(
            steps[0].render_error.is_some(),
            "a syntax error must disable the step"
        );
        assert_eq!(steps[0].text, "Unclosed {{ workflowId");
    }

    #[test]
    fn list_items_shape() {
        let lib = lib_with_matches();
        let items = list_items(&lib);
        assert_eq!(items.len(), lib.runbooks.len());
        for item in &items {
            for key in ["id", "title", "format", "team", "steps"] {
                assert!(item.get(key).is_some(), "missing {key}");
            }
        }
        let first = &items[0];
        assert_eq!(first["id"], json!("settlements-a"));
        assert_eq!(first["team"], json!("settlements"));
        assert_eq!(first["steps"], json!(0));
    }
}
