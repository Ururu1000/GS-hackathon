//! Runbook actions (SPEC §6.2): a registry of `Action`s, the `action_executions` queue and the
//! `actions` role worker. Execution happens off the alert pipeline: the API inserts a `queued`
//! row and returns 202; workers lease it (`FOR UPDATE SKIP LOCKED`, `locked_until`, fresh
//! `lease_token`; expired `running` leases are reclaimable, at most `max_attempts` = 3), run it
//! with a timeout, and finalize fenced by the token: `ok`/`failed` (output = short, redacted
//! text) or `uncertain` (timeout / crash against a non-idempotent target). Each finalize writes
//! an `audit_log` row (`kind = "step_run"`, message like `retry_workflow failed: HTTP 500`) and
//! calls `AlertOps::touch` so the UI refetches; a successful step with `resolves: true`
//! resolves the alert (`AlertOps::transition(Resolve{reason: "runbook step <id>"})`).
//!
//! **Built-in actions** (all safe: targets are the gateway's mock endpoints or config aliases):
//! - `http_get` (risk low): `{url}` must start with one of the `config/actions/targets.yaml`
//!   aliases' base URLs (alias map `{name: base_url}`; a runbook's `http://counterparty-service/
//!   health` maps via alias `counterparty-service` to the mock `GET {gateway}/mock/health/
//!   counterparty-service`). No redirects, 5 s timeout, response body capped at 4 KiB; 2xx = ok.
//! - `retry_workflow` (risk low): `{workflowId}` → `POST {gateway}/mock/workflows/{id}/retry`.
//! - `restart_service` (risk high): `{service}` → `POST {gateway}/mock/services/{name}/restart`.
//! - `open_ticket` (risk low): `{title?}` → `POST {gateway}/mock/tickets`.
//! - `escalate` (risk high): `{team}` → notify every member of `team` (Directory) on their
//!   resolved channels (prefs) for this alert: insert delivery jobs via `router::insert_jobs`
//!   with `route_info.target = "escalate:<team>"`, using `notify_seq` = the alert's current
//!   `notify_seq` and `escalation_step = 100 + n` to stay unique; audit kind `escalated`.
//! - `notify` (risk low): `{to}` (route target) → same as escalate for one target.
//!
//! **Mock targets** live on the gateway role (`gateway::router`), owned here:
//! `GET /mock/health/{name}`, `POST /mock/workflows/{id}/retry`, `POST /mock/services/{name}/
//! restart`, `POST /mock/tickets`, and `POST /api/simulator/mock {target: workflows|services|
//! health|tickets, mode: ok|fail}` to switch outcomes (default `ok`; needs the gateway token like
//! the outage endpoints). They record calls in memory only.

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::Shutdown;
use crate::alert_ops::AlertOps;
use crate::channel::BoxFuture;
use crate::config::Directory;
use crate::runbook::Condition;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Risk {
    Low,
    High,
}

impl Risk {
    pub fn as_str(self) -> &'static str {
        match self {
            Risk::Low => "low",
            Risk::High => "high",
        }
    }
}

/// What an action gets at run time.
pub struct ActionCtx<'a> {
    pub pool: &'a PgPool,
    pub dir: &'a Directory,
    pub alert_id: Uuid,
    pub execution_id: Uuid,
    pub params: &'a serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ok(String),
    Failed(String),
    Uncertain(String),
}

pub trait Action: Send + Sync {
    fn name(&self) -> &'static str;
    fn label(&self) -> &'static str;
    fn risk(&self) -> Risk;
    fn execute<'a>(&'a self, ctx: &'a ActionCtx<'a>) -> BoxFuture<'a, Outcome>;
}

#[derive(Debug, Clone)]
pub struct ActionsConfig {
    /// Base URL of the gateway role's mock targets, e.g. `http://localhost:8070`.
    pub gateway_url: String,
    /// `config/actions/targets.yaml` alias → base URL.
    pub http_targets: std::collections::HashMap<String, String>,
    pub timeout: Duration,
}

#[derive(Clone, Default)]
pub struct ActionRegistry {
    // agent B: name -> Arc<dyn Action>
}

impl ActionRegistry {
    /// All built-ins configured with `cfg`.
    pub fn builtin(cfg: ActionsConfig) -> Self {
        let _ = cfg;
        todo!("agent B")
    }

    /// Names every registry knows (used by the runbook loader without building a registry).
    pub fn known() -> &'static [&'static str] {
        &[
            "http_get",
            "retry_workflow",
            "restart_service",
            "open_ticket",
            "escalate",
            "notify",
        ]
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Action>> {
        let _ = name;
        todo!("agent B")
    }

    /// Risk by name without a registry instance (`escalate`/`restart_service` high).
    pub fn risk_of(name: &str) -> &'static str {
        match name {
            "escalate" | "restart_service" => "high",
            _ => "low",
        }
    }
}

/// Load `<config>/actions/targets.yaml` (`{targets: {alias: base_url}}`); missing → empty.
pub fn load_http_targets(
    config_root: &std::path::Path,
) -> Result<std::collections::HashMap<String, String>, crate::config::ConfigError> {
    let _ = config_root;
    todo!("agent B")
}

/// Queue one execution inside the caller's tx. Unique-violation on the live index →
/// `Ok(None)` (already queued/running). Writes the audit row `step_run` "queued".
pub async fn queue(
    tx: &mut Transaction<'_, Postgres>,
    alert_id: Uuid,
    step_id: &str,
    action: &str,
    params: &Value,
    requested_by: &str,
    approval_id: Option<Uuid>,
) -> Result<Option<Uuid>, sqlx::Error> {
    let _ = (
        tx,
        alert_id,
        step_id,
        action,
        params,
        requested_by,
        approval_id,
    );
    todo!("agent B")
}

/// `(met, reason_if_not)` for a step condition on this alert, e.g.
/// `(false, Some("Available after 2 failed retries (1 so far)"))`.
pub async fn condition_status(
    pool: &PgPool,
    alert_id: Uuid,
    cond: &Condition,
) -> Result<(bool, Option<String>), sqlx::Error> {
    let _ = (pool, alert_id, cond);
    todo!("agent B")
}

#[derive(Debug, Clone)]
pub struct ActionsWorkerConfig {
    pub worker_id: String,
    pub poll_interval: Duration,
    pub batch_size: i64,
    pub lease: Duration,
}

impl Default for ActionsWorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: format!("actions-{}", Uuid::new_v4()),
            poll_interval: Duration::from_millis(250),
            batch_size: 10,
            lease: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ActionStats {
    pub claimed: u64,
    pub ok: u64,
    pub failed: u64,
    pub uncertain: u64,
    pub lost_lease: u64,
}

/// Claim and run one batch (module doc).
pub async fn run_once(
    pool: &PgPool,
    dir: &Directory,
    registry: &ActionRegistry,
    alert_ops: &dyn AlertOps,
    cfg: &ActionsWorkerConfig,
) -> Result<ActionStats, sqlx::Error> {
    let _ = (pool, dir, registry, alert_ops, cfg);
    todo!("agent B")
}

/// Loop `run_once` until shutdown (true or sender dropped).
pub async fn run_actions(
    pool: PgPool,
    dir: Arc<Directory>,
    registry: ActionRegistry,
    alert_ops: Arc<dyn AlertOps>,
    cfg: ActionsWorkerConfig,
    shutdown: Shutdown,
) {
    let _ = (pool, dir, registry, alert_ops, cfg, shutdown);
    todo!("agent B")
}
