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

use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    Manual,
    Condition,
    Action,
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

impl RunbookLibrary {
    /// Load `<dir>/*`. A missing directory gives an empty library.
    pub fn load(dir: &Path, known_actions: &[&str]) -> Result<Self, ConfigError> {
        let _ = (dir, known_actions);
        todo!("agent A")
    }

    pub fn get(&self, id: &str) -> Option<&Runbook> {
        self.runbooks.iter().find(|r| r.id == id)
    }

    /// Selection rules in the module doc. `tctx` is `template_context(..)`; a `matches` key is
    /// looked up as a dot path in `raw`, then `fields`, then `context`.
    pub fn select(&self, rule_runbook: Option<&str>, team: &str, tctx: &Value) -> Option<&Runbook> {
        let _ = (rule_runbook, team, tctx);
        todo!("agent A")
    }
}

/// Build the template context from a notify payload (raw JSON) and the alert row values.
pub fn template_context(raw_payload: &Value, alert: &Value) -> Value {
    let _ = (raw_payload, alert);
    todo!("agent A")
}

/// Render every step; `risk_of(action_name)` gives `"low" | "high"` (from the action registry).
pub fn render(
    runbook: &Runbook,
    ctx: &Value,
    risk_of: &dyn Fn(&str) -> &'static str,
) -> Vec<RenderedStep> {
    let _ = (runbook, ctx, risk_of);
    todo!("agent A")
}

/// Freeze the rendered runbook for an alert (`ON CONFLICT (alert_id) DO NOTHING`); `true` if
/// inserted. Called by the router in the first-notification tx.
pub async fn attach(
    tx: &mut Transaction<'_, Postgres>,
    alert_id: Uuid,
    runbook: &Runbook,
    steps: &[RenderedStep],
    ctx: &Value,
) -> Result<bool, sqlx::Error> {
    let _ = (tx, alert_id, runbook, steps, ctx);
    todo!("agent A")
}

/// Plain-text section appended to emails ("Runbook: <title>" + numbered rendered steps, with
/// "[needs approval]" / "[disabled: ...]" markers), or `None` when the alert has no runbook.
pub async fn email_section(pool: &PgPool, alert_id: Uuid) -> Result<Option<String>, sqlx::Error> {
    let _ = (pool, alert_id);
    todo!("agent A")
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
    let _ = (pool, alert_id, actor);
    todo!("agent A")
}

/// `GET /api/runbooks` items: `{id, title, format, team, steps: <count>}`.
pub fn list_items(lib: &RunbookLibrary) -> Vec<Value> {
    let _ = lib;
    todo!("agent A")
}
