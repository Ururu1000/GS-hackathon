//! Runbook actions (SPEC §6.2): a registry of `Action`s, the `action_executions` queue and the
//! `actions` role worker. Execution happens off the alert pipeline: the API inserts a `queued`
//! row and returns 202; workers lease it (`FOR UPDATE SKIP LOCKED`, `locked_until`, fresh
//! `lease_token`; expired `running` leases are reclaimable, at most `max_attempts` = 3 — and
//! the lease is refreshed and re-verified against the token immediately before each execution
//! runs, so a slow predecessor in the same claimed batch cannot let a later lease lapse and
//! be reclaimed under it), run it
//! with a timeout, and finalize fenced by the token: `ok`/`failed` (output = short, redacted
//! text) or `uncertain` (timeout / crash against a non-idempotent target). Each finalize writes
//! an `audit_log` row (`kind = "step_run"`, message like `retry_workflow failed: HTTP 500`) and
//! calls `AlertOps::touch` so the UI refetches; a successful step with `resolves: true`
//! resolves the alert (`AlertOps::transition(Resolve{reason: "runbook step <id>"})`).
//!
//! Audit #11 hardening: URL path segments built from parameters are validated
//! (`encode_segment` rejects empty, `.`, `..` and anything containing `/` or `\` — the action
//! fails with "invalid path segment") and percent-encoded; outputs are redacted
//! (`redact_output`) before truncation, so URL query strings and bearer/basic credentials
//! (`Authorization:`/`token=`) never reach `action_executions.output` or the audit trail. The
//! loop also sweeps stale approvals (`approvals::expire_stale`) once per iteration.
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
//!   `notify_seq` and `escalation_step = 100 + n` (the idempotency key includes the step, so
//!   a member already paged for this notification still gets the job); audit kind `escalated`.
//! - `notify` (risk low): `{to}` (route target) → same as escalate for one target.
//!
//! **Mock targets** live on the gateway role (`gateway::router`), owned here:
//! `GET /mock/health/{name}`, `POST /mock/workflows/{id}/retry`, `POST /mock/services/{name}/
//! restart`, `POST /mock/tickets`, and `POST /api/simulator/mock {target: workflows|services|
//! health|tickets, mode: ok|fail}` to switch outcomes (default `ok`; needs the gateway token like
//! the outage endpoints). They record calls in memory only.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};
use tracing::{error, info};
use uuid::Uuid;

use crate::Shutdown;
use crate::alert_ops::{AlertOps, Transition};
use crate::channel::BoxFuture;
use crate::config::{Directory, address_for};
use crate::prefs::{self, RoutingContext};
use crate::router;
use crate::runbook::Condition;
use crate::throttle::PlannedJob;

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

impl Outcome {
    fn state(&self) -> &'static str {
        match self {
            Outcome::Ok(_) => "ok",
            Outcome::Failed(_) => "failed",
            Outcome::Uncertain(_) => "uncertain",
        }
    }

    fn output(&self) -> &str {
        match self {
            Outcome::Ok(s) | Outcome::Failed(s) | Outcome::Uncertain(s) => s,
        }
    }
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
    actions: HashMap<String, Arc<dyn Action>>,
}

impl ActionRegistry {
    /// All built-ins configured with `cfg`.
    pub fn builtin(cfg: ActionsConfig) -> Self {
        // One client for every HTTP action: the configured timeout, no redirects (a 3xx
        // from a mock target is a failure, never something to follow).
        let client = reqwest::Client::builder()
            .timeout(cfg.timeout)
            .connect_timeout(cfg.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("action http client builds");
        let mut actions: HashMap<String, Arc<dyn Action>> = HashMap::new();
        actions.insert(
            "http_get".to_string(),
            Arc::new(HttpGet {
                targets: cfg.http_targets.clone(),
                client: client.clone(),
            }),
        );
        actions.insert(
            "retry_workflow".to_string(),
            Arc::new(RetryWorkflow {
                gateway_url: cfg.gateway_url.clone(),
                client: client.clone(),
            }),
        );
        actions.insert(
            "restart_service".to_string(),
            Arc::new(RestartService {
                gateway_url: cfg.gateway_url.clone(),
                client: client.clone(),
            }),
        );
        actions.insert(
            "open_ticket".to_string(),
            Arc::new(OpenTicket {
                gateway_url: cfg.gateway_url.clone(),
                client: client.clone(),
            }),
        );
        actions.insert("escalate".to_string(), Arc::new(Escalate));
        actions.insert("notify".to_string(), Arc::new(Notify));
        Self { actions }
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
        self.actions.get(name).cloned()
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
    use crate::config::ConfigError;
    let path = config_root.join("actions/targets.yaml");
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(e) => {
            return Err(ConfigError::Io {
                path: path.display().to_string(),
                source: e,
            });
        }
    };
    let value: Value = serde_saphyr::from_str(&text).map_err(|e| ConfigError::Parse {
        path: path.display().to_string(),
        message: e.to_string(),
    })?;
    let map = match value.get("targets") {
        None | Some(Value::Null) => return Ok(HashMap::new()),
        Some(Value::Object(map)) => map,
        Some(other) => {
            return Err(ConfigError::Invalid(format!(
                "actions/targets.yaml: `targets` must be a mapping, got {other}"
            )));
        }
    };
    let mut out = HashMap::with_capacity(map.len());
    for (alias, base) in map {
        let base = base.as_str().ok_or_else(|| {
            ConfigError::Invalid(format!(
                "actions/targets.yaml: target {alias:?} must be a base URL string"
            ))
        })?;
        reqwest::Url::parse(base).map_err(|_| {
            ConfigError::Invalid(format!(
                "actions/targets.yaml: target {alias:?} has a bad base URL {base:?}"
            ))
        })?;
        out.insert(alias.clone(), base.to_string());
    }
    Ok(out)
}

/// Queue one execution inside the caller's tx. Unique-violation on the live index →
/// `Ok(None)` (already queued/running). The conflicting insert runs inside a savepoint so
/// `Ok(None)` leaves the caller's tx usable. Writes the audit row `step_run` "queued".
pub async fn queue(
    tx: &mut Transaction<'_, Postgres>,
    alert_id: Uuid,
    step_id: &str,
    action: &str,
    params: &Value,
    requested_by: &str,
    approval_id: Option<Uuid>,
) -> Result<Option<Uuid>, sqlx::Error> {
    // A failed statement aborts the whole Postgres tx; the savepoint keeps a mere
    // duplicate-run from poisoning the caller's transaction.
    sqlx::query("SAVEPOINT beacon_queue")
        .execute(&mut **tx)
        .await?;
    let id: Uuid = match sqlx::query_scalar(
        "INSERT INTO action_executions (alert_id, step_id, action, params, requested_by, approval_id) \
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
    )
    .bind(alert_id)
    .bind(step_id)
    .bind(action)
    .bind(params)
    .bind(requested_by)
    .bind(approval_id)
    .fetch_one(&mut **tx)
    .await
    {
        Ok(id) => id,
        // The live (alert, step) index: another queued/running execution covers this step.
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
            sqlx::query("ROLLBACK TO SAVEPOINT beacon_queue")
                .execute(&mut **tx)
                .await?;
            return Ok(None);
        }
        Err(e) => {
            sqlx::query("ROLLBACK TO SAVEPOINT beacon_queue")
                .execute(&mut **tx)
                .await?;
            return Err(e);
        }
    };
    sqlx::query("RELEASE SAVEPOINT beacon_queue")
        .execute(&mut **tx)
        .await?;
    crate::audit::record(
        tx,
        requested_by,
        "system",
        Some(alert_id),
        "step_run",
        "queued",
        json!({"step_id": step_id, "action": action}),
    )
    .await?;
    Ok(Some(id))
}

/// `(met, reason_if_not)` for a step condition on this alert, e.g.
/// `(false, Some("Available after 2 failed retries (1 so far)"))`.
pub async fn condition_status(
    pool: &PgPool,
    alert_id: Uuid,
    cond: &Condition,
) -> Result<(bool, Option<String>), sqlx::Error> {
    match cond {
        Condition::AfterFailures { action, count } => {
            let n: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM action_executions \
                 WHERE alert_id = $1 AND action = $2 AND state = 'failed'",
            )
            .bind(alert_id)
            .bind(action)
            .fetch_one(pool)
            .await?;
            if n >= i64::from(*count) {
                Ok((true, None))
            } else {
                let reason = if action == "retry_workflow" {
                    format!("Available after {count} failed retries ({n} so far)")
                } else {
                    format!("Available after {count} failed {action} runs ({n} so far)")
                };
                Ok((false, Some(reason)))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ActionsWorkerConfig {
    pub worker_id: String,
    pub poll_interval: Duration,
    pub batch_size: i64,
    pub lease: Duration,
    /// Upper bound for one action's `execute`; overrunning is `Uncertain` (the HTTP
    /// actions additionally enforce `ActionsConfig::timeout` on the client).
    pub timeout: Duration,
}

impl Default for ActionsWorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: format!("actions-{}", Uuid::new_v4()),
            poll_interval: Duration::from_millis(250),
            batch_size: 10,
            lease: Duration::from_secs(30),
            timeout: Duration::from_secs(30),
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

/// At most this many claims may expire before the execution goes `uncertain`
/// ("lease expired") instead of being reclaimed.
const MAX_ATTEMPTS: i32 = 3;

/// Execution output is short text: truncate to 300 chars when finalizing.
const OUTPUT_MAX_CHARS: usize = 300;

fn truncate_output(s: &str) -> String {
    if s.chars().count() <= OUTPUT_MAX_CHARS {
        s.to_string()
    } else {
        s.chars().take(OUTPUT_MAX_CHARS).collect()
    }
}

/// True at the end of a URL token (whitespace or a common closing punctuation byte).
fn is_url_end(b: u8) -> bool {
    b.is_ascii_whitespace() || matches!(b, b')' | b']' | b'>' | b'"' | b'\'' | b',')
}

/// Redact one output before it is stored or audited (audit #11): URL tokens lose everything
/// from their `?` (query strings may carry tokens — the request itself keeps them, only the
/// recorded output drops them), and credentials never survive — `Authorization: <creds>`
/// (bearer/basic headers echoed by a target) and `token=<value>` (query- or form-style
/// credentials, including `access_token=` etc.) become `[REDACTED]`. The scan is
/// case-insensitive and leaves everything else byte for byte.
fn redact_output(text: &str) -> String {
    let bytes = text.as_bytes();
    let lower: Vec<u8> = bytes.iter().map(|b| b.to_ascii_lowercase()).collect();
    let mut out = String::with_capacity(text.len());
    let push = |out: &mut String, range: std::ops::Range<usize>| {
        out.push_str(&String::from_utf8_lossy(&bytes[range]));
    };
    let mut i = 0usize;
    while i < bytes.len() {
        let rest = &lower[i..];
        // `http(s)://…?query` -> `http(s)://…`.
        if rest.starts_with(b"http://") || rest.starts_with(b"https://") {
            let mut end = i;
            while end < bytes.len() && !is_url_end(bytes[end]) {
                end += 1;
            }
            let cut = bytes[i..end]
                .iter()
                .position(|&b| b == b'?')
                .map(|q| i + q)
                .unwrap_or(end);
            push(&mut out, i..cut);
            i = end;
            continue;
        }
        // `Authorization: Bearer xyz` -> `Authorization: [REDACTED]` (to the line's end).
        if rest.starts_with(b"authorization") {
            let word_end = i + "authorization".len();
            let mut j = word_end;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() && bytes[j] != b'\n' {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b':' {
                let mut k = j + 1;
                while k < bytes.len() && bytes[k].is_ascii_whitespace() && bytes[k] != b'\n' {
                    k += 1;
                }
                let line_end = bytes[k..]
                    .iter()
                    .position(|&b| b == b'\n')
                    .map(|p| k + p)
                    .unwrap_or(bytes.len());
                out.push_str("Authorization: [REDACTED]");
                i = line_end;
                continue;
            }
            // Not the header form: keep the word as-is.
            push(&mut out, i..word_end);
            i = word_end;
            continue;
        }
        // `token=<value>` -> `token=[REDACTED]` (value ends at whitespace or `&`).
        if rest.starts_with(b"token") && bytes.get(i + 5) == Some(&b'=') {
            let mut j = i + "token=".len();
            while j < bytes.len() && !bytes[j].is_ascii_whitespace() && bytes[j] != b'&' {
                j += 1;
            }
            out.push_str("token=[REDACTED]");
            i = j;
            continue;
        }
        // No marker here: copy one full character (markers are ASCII, so `i` always sits on
        // a char boundary).
        let mut len = 1usize;
        while i + len < bytes.len() && !text.is_char_boundary(i + len) {
            len += 1;
        }
        push(&mut out, i..i + len);
        i += len;
    }
    out
}

/// One claimed execution: everything `run_once` needs to run it and finalize it.
#[derive(Debug, sqlx::FromRow)]
struct ClaimedExecution {
    id: Uuid,
    alert_id: Uuid,
    step_id: String,
    action: String,
    params: Value,
    requested_by: String,
    #[allow(dead_code)]
    approval_id: Option<Uuid>,
    #[allow(dead_code)]
    attempts: i32,
    lease_token: Uuid,
}

/// Claim and run one batch (module doc).
pub async fn run_once(
    pool: &PgPool,
    dir: &Directory,
    registry: &ActionRegistry,
    alert_ops: &dyn AlertOps,
    cfg: &ActionsWorkerConfig,
) -> Result<ActionStats, sqlx::Error> {
    let mut stats = ActionStats::default();

    // 1. Expired `running` leases past the attempt budget go `uncertain` ("lease expired")
    //    with the same audit + touch a normal finalize writes. Every UPDATE sets
    //    `updated_at = now()`.
    {
        let mut tx = pool.begin().await?;
        let expired: Vec<(Uuid, Uuid, String, String)> = sqlx::query_as(
            "UPDATE action_executions SET state = 'uncertain', output = 'lease expired', \
                finished_at = now(), lease_token = NULL, locked_until = NULL, locked_by = NULL, \
                updated_at = now() \
             WHERE state = 'running' AND locked_until < now() AND attempts >= $1 \
             RETURNING id, alert_id, action, requested_by",
        )
        .bind(MAX_ATTEMPTS)
        .fetch_all(&mut *tx)
        .await?;
        for (id, alert_id, action, requested_by) in &expired {
            let message = format!("{action} uncertain: lease expired");
            crate::audit::record(
                &mut tx,
                requested_by,
                "system",
                Some(*alert_id),
                "step_run",
                &message,
                json!({"execution_id": id, "action": action, "state": "uncertain"}),
            )
            .await?;
            alert_ops.touch(&mut tx, *alert_id).await?;
            stats.uncertain += 1;
        }
        tx.commit().await?;
    }

    // 2. Claim `queued` rows and reclaimable expired `running` leases (short tx, fenced by a
    //    fresh token; `attempts + 1` per claim).
    let claimed: Vec<ClaimedExecution> = {
        let mut tx = pool.begin().await?;
        let rows: Vec<ClaimedExecution> = sqlx::query_as(
            "WITH c AS ( \
                 SELECT id FROM action_executions \
                 WHERE state = 'queued' OR (state = 'running' AND locked_until < now()) \
                 ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED) \
             UPDATE action_executions e \
             SET state = 'running', locked_by = $2, \
                 locked_until = now() + make_interval(secs => $3::float8), \
                 lease_token = gen_random_uuid(), attempts = e.attempts + 1, \
                 started_at = COALESCE(e.started_at, now()), updated_at = now() \
             FROM c WHERE e.id = c.id \
             RETURNING e.id, e.alert_id, e.step_id, e.action, e.params, e.requested_by, \
                       e.approval_id, e.attempts, e.lease_token",
        )
        .bind(cfg.batch_size)
        .bind(&cfg.worker_id)
        .bind(cfg.lease.as_secs_f64())
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        rows
    };
    stats.claimed = claimed.len() as u64;

    // 3. Run each action outside any tx, then finalize in one tx fenced by the lease token:
    //    terminal state + `finished_at` + output (300 chars), audit `step_run`, touch, and a
    //    `resolves: true` transition when the frozen step says so.
    for c in &claimed {
        // The whole batch was leased at once but actions run sequentially: a slow first
        // action can push a later execution past its claim-time lease, and another worker
        // would then reclaim and run it concurrently. Refresh and re-verify the lease
        // immediately before each execution (the token must still be ours) and skip the
        // execution entirely when it is not.
        let renewed: Option<(Uuid,)> = sqlx::query_as(
            "UPDATE action_executions \
              SET locked_until = now() + make_interval(secs => $3::float8), updated_at = now() \
             WHERE id = $1 AND lease_token = $2 AND state = 'running' RETURNING id",
        )
        .bind(c.id)
        .bind(c.lease_token)
        .bind(cfg.lease.as_secs_f64())
        .fetch_optional(pool)
        .await?;
        if renewed.is_none() {
            // Another worker reclaimed the lease while this execution waited in the batch.
            stats.lost_lease += 1;
            continue;
        }
        let outcome = match registry.get(&c.action) {
            Some(action) => {
                let params = c.params.as_object().cloned().unwrap_or_default();
                let actx = ActionCtx {
                    pool,
                    dir,
                    alert_id: c.alert_id,
                    execution_id: c.id,
                    params: &params,
                };
                match tokio::time::timeout(cfg.timeout, action.execute(&actx)).await {
                    Ok(outcome) => outcome,
                    Err(_) => Outcome::Uncertain(format!("timed out after {:?}", cfg.timeout)),
                }
            }
            None => Outcome::Failed(format!("unknown action {}", c.action)),
        };
        let state = outcome.state();
        // Redact first, truncate after (audit #11): no query strings, no credentials.
        let output = truncate_output(&redact_output(outcome.output()));
        let mut ftx = pool.begin().await?;
        let res = sqlx::query(
            "UPDATE action_executions SET state = $2, finished_at = now(), output = $3, \
                lease_token = NULL, locked_until = NULL, locked_by = NULL, updated_at = now() \
             WHERE id = $1 AND lease_token = $4 AND state = 'running'",
        )
        .bind(c.id)
        .bind(state)
        .bind(&output)
        .bind(c.lease_token)
        .execute(&mut *ftx)
        .await?;
        if res.rows_affected() != 1 {
            // Another worker reclaimed the lease first: nothing was written.
            stats.lost_lease += 1;
            continue;
        }
        let message = match &outcome {
            Outcome::Ok(_) => format!("{} ok", c.action),
            Outcome::Failed(_) => format!("{} failed: {output}", c.action),
            Outcome::Uncertain(_) => format!("{} uncertain: {output}", c.action),
        };
        crate::audit::record(
            &mut ftx,
            &c.requested_by,
            "system",
            Some(c.alert_id),
            "step_run",
            &message,
            json!({"step_id": c.step_id, "action": c.action, "execution_id": c.id,
                   "state": state}),
        )
        .await?;
        alert_ops.touch(&mut ftx, c.alert_id).await?;
        if matches!(outcome, Outcome::Ok(_))
            && step_resolves(&mut ftx, c.alert_id, &c.step_id).await?
        {
            alert_ops
                .transition(
                    &mut ftx,
                    c.alert_id,
                    Transition::Resolve {
                        reason: format!("runbook step {}", c.step_id),
                    },
                    "system",
                )
                .await?;
        }
        ftx.commit().await?;
        match state {
            "ok" => stats.ok += 1,
            "failed" => stats.failed += 1,
            _ => stats.uncertain += 1,
        }
    }
    Ok(stats)
}

/// True when the frozen runbook snapshot (`alert_runbooks.steps`) marks `step_id` with
/// `resolves: true`. Tolerates a missing row or foreign shapes (agent A owns the writer).
async fn step_resolves(
    tx: &mut Transaction<'_, Postgres>,
    alert_id: Uuid,
    step_id: &str,
) -> Result<bool, sqlx::Error> {
    let steps: Option<Value> =
        sqlx::query_scalar("SELECT steps FROM alert_runbooks WHERE alert_id = $1")
            .bind(alert_id)
            .fetch_optional(&mut **tx)
            .await?;
    Ok(steps.as_ref().and_then(Value::as_array).is_some_and(|arr| {
        arr.iter().any(|s| {
            s.get("id").and_then(Value::as_str) == Some(step_id)
                && s.get("resolves").and_then(Value::as_bool) == Some(true)
        })
    }))
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
    let mut shutdown = shutdown;
    info!(worker_id = %cfg.worker_id, "actions worker started");
    // Saved people/teams edits (directory_overrides) reach `escalate` and `notify`
    // actions on the next pass, as they do in the router and the scheduler.
    let effective = crate::overrides::EffectiveDirectory::new(dir);
    while !*shutdown.borrow() {
        // Audit #11: sweep pending approvals past their expiry (and scrub their codes at
        // rest) once per iteration — one cheap indexed UPDATE when there is nothing to do.
        if let Err(err) = crate::approvals::expire_stale(&pool).await {
            error!(error = %err, "stale approval expiry failed");
        }
        let dir = effective.get(&pool).await;
        let wait = match run_once(&pool, &dir, &registry, alert_ops.as_ref(), &cfg).await {
            Ok(stats) => {
                if stats.claimed > 0 || stats.uncertain > 0 {
                    continue; // more work is probably waiting
                }
                cfg.poll_interval
            }
            Err(err) => {
                error!(error = %err, "actions worker iteration failed");
                Duration::from_secs(1)
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            // `Err` means the sender is gone, so nobody can signal shutdown any more: stop.
            res = shutdown.changed() => {
                if res.is_err() {
                    break;
                }
            }
        }
    }
    info!(worker_id = %cfg.worker_id, "actions worker stopped");
}

// ---------------------------------------------------------------------------------------------
// Built-in actions
// ---------------------------------------------------------------------------------------------

/// Response bodies are read with a bounded loop (at most 4 KiB), like the gateway channel.
const MAX_BODY_BYTES: usize = 4096;
/// Error snippets kept in `Outcome`s (the finalize truncates to 300 chars anyway).
const SNIPPET_MAX_CHARS: usize = 200;

fn str_param<'a>(params: &'a serde_json::Map<String, Value>, key: &str) -> Option<&'a str> {
    params.get(key).and_then(Value::as_str)
}

async fn read_capped(mut resp: reqwest::Response, cap: usize) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < cap {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                let room = cap - buf.len();
                if chunk.len() >= room {
                    buf.extend_from_slice(&chunk[..room]);
                    break;
                }
                buf.extend_from_slice(&chunk);
            }
            Ok(None) | Err(_) => break,
        }
    }
    buf
}

fn snippet(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .chars()
        .take(SNIPPET_MAX_CHARS)
        .collect()
}

/// Percent-encode one URL path segment (everything outside `unreserved`). Audit #11: the
/// segment must not be empty, `.`, `..`, or contain `/` or `\` — those could escape the mock
/// path the caller builds around it (`..` climbs, `/` and `\` split) — and the action fails
/// with "invalid path segment"; everything else is percent-encoded.
fn encode_segment(s: &str) -> Result<String, String> {
    if s.is_empty() || s == "." || s == ".." || s.contains('/') || s.contains('\\') {
        return Err("invalid path segment".to_string());
    }
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    Ok(out)
}

/// One mock-target call: 2xx is `Ok`, anything else `Failed`; a client-side timeout is
/// `Uncertain` (the target may still have acted on it).
struct HttpError {
    timeout: bool,
    message: String,
}

async fn send_get(client: &reqwest::Client, url: &str) -> Result<(u16, Vec<u8>), HttpError> {
    let resp = client.get(url).send().await.map_err(|e| HttpError {
        timeout: e.is_timeout(),
        message: e.to_string(),
    })?;
    let status = resp.status().as_u16();
    Ok((status, read_capped(resp, MAX_BODY_BYTES).await))
}

async fn send_post(
    client: &reqwest::Client,
    url: &str,
    body: Value,
) -> Result<(u16, Vec<u8>), HttpError> {
    let resp = client
        .post(url)
        .json(&body)
        .send()
        .await
        .map_err(|e| HttpError {
            timeout: e.is_timeout(),
            message: e.to_string(),
        })?;
    let status = resp.status().as_u16();
    Ok((status, read_capped(resp, MAX_BODY_BYTES).await))
}

/// Map `http://<alias>/<path>` through the alias table: the alias base REPLACES
/// `http://<alias>` and the rest of the path is appended, unless the base already ends with
/// that path. Only `http` URLs whose host is a known alias map; anything else is
/// `Failed("target not allowed")` without any request being made.
fn map_alias_url(url: &str, targets: &HashMap<String, String>) -> Result<String, String> {
    let parsed = reqwest::Url::parse(url).map_err(|_| "invalid url".to_string())?;
    if parsed.scheme() != "http" {
        return Err("target not allowed".to_string());
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| "target not allowed".to_string())?;
    let base = targets
        .get(host)
        .ok_or_else(|| "target not allowed".to_string())?;
    let base = base.trim_end_matches('/');
    let rest = parsed.path();
    let mut mapped = if base.ends_with(rest) {
        base.to_string()
    } else {
        format!("{base}{rest}")
    };
    if let Some(query) = parsed.query() {
        mapped.push('?');
        mapped.push_str(query);
    }
    Ok(mapped)
}

struct HttpGet {
    targets: HashMap<String, String>,
    client: reqwest::Client,
}

impl Action for HttpGet {
    fn name(&self) -> &'static str {
        "http_get"
    }

    fn label(&self) -> &'static str {
        "Fetch URL"
    }

    fn risk(&self) -> Risk {
        Risk::Low
    }

    fn execute<'a>(&'a self, ctx: &'a ActionCtx<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            let raw = match str_param(ctx.params, "url") {
                Some(u) => u,
                None => return Outcome::Failed("missing param: url".to_string()),
            };
            let url = match map_alias_url(raw, &self.targets) {
                Ok(u) => u,
                Err(e) => return Outcome::Failed(e),
            };
            match send_get(&self.client, &url).await {
                Ok((status, body)) if (200..300).contains(&status) => {
                    Outcome::Ok(format!("GET {url} -> HTTP {status} ({} bytes)", body.len()))
                }
                Ok((status, body)) => {
                    Outcome::Failed(format!("GET {url} -> HTTP {status}: {}", snippet(&body)))
                }
                Err(e) if e.timeout => {
                    Outcome::Uncertain(format!("GET {url} timed out: {}", e.message))
                }
                Err(e) => Outcome::Failed(format!("GET {url} request failed: {}", e.message)),
            }
        })
    }
}

struct RetryWorkflow {
    gateway_url: String,
    client: reqwest::Client,
}

impl Action for RetryWorkflow {
    fn name(&self) -> &'static str {
        "retry_workflow"
    }

    fn label(&self) -> &'static str {
        "Retry workflow"
    }

    fn risk(&self) -> Risk {
        Risk::Low
    }

    fn execute<'a>(&'a self, ctx: &'a ActionCtx<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            let id = match str_param(ctx.params, "workflowId") {
                Some(id) => id,
                None => return Outcome::Failed("missing param: workflowId".to_string()),
            };
            let url = match encode_segment(id) {
                Ok(segment) => format!(
                    "{}/mock/workflows/{}/retry",
                    self.gateway_url.trim_end_matches('/'),
                    segment
                ),
                Err(e) => return Outcome::Failed(e),
            };
            match send_post(&self.client, &url, json!({"workflowId": id})).await {
                Ok((status, _)) if (200..300).contains(&status) => {
                    Outcome::Ok(format!("workflow {id} retried (HTTP {status})"))
                }
                Ok((status, body)) => Outcome::Failed(format!(
                    "workflow {id} retry failed: HTTP {status}: {}",
                    snippet(&body)
                )),
                Err(e) if e.timeout => {
                    Outcome::Uncertain(format!("workflow {id} retry timed out: {}", e.message))
                }
                Err(e) => Outcome::Failed(format!("workflow {id} retry failed: {}", e.message)),
            }
        })
    }
}

struct RestartService {
    gateway_url: String,
    client: reqwest::Client,
}

impl Action for RestartService {
    fn name(&self) -> &'static str {
        "restart_service"
    }

    fn label(&self) -> &'static str {
        "Restart service"
    }

    fn risk(&self) -> Risk {
        Risk::High
    }

    fn execute<'a>(&'a self, ctx: &'a ActionCtx<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            let name = match str_param(ctx.params, "service") {
                Some(s) => s,
                None => return Outcome::Failed("missing param: service".to_string()),
            };
            let url = match encode_segment(name) {
                Ok(segment) => format!(
                    "{}/mock/services/{}/restart",
                    self.gateway_url.trim_end_matches('/'),
                    segment
                ),
                Err(e) => return Outcome::Failed(e),
            };
            match send_post(&self.client, &url, json!({"service": name})).await {
                Ok((status, _)) if (200..300).contains(&status) => {
                    Outcome::Ok(format!("service {name} restarted (HTTP {status})"))
                }
                Ok((status, body)) => Outcome::Failed(format!(
                    "service {name} restart failed: HTTP {status}: {}",
                    snippet(&body)
                )),
                Err(e) if e.timeout => {
                    Outcome::Uncertain(format!("service {name} restart timed out: {}", e.message))
                }
                Err(e) => Outcome::Failed(format!("service {name} restart failed: {}", e.message)),
            }
        })
    }
}

struct OpenTicket {
    gateway_url: String,
    client: reqwest::Client,
}

impl Action for OpenTicket {
    fn name(&self) -> &'static str {
        "open_ticket"
    }

    fn label(&self) -> &'static str {
        "Open ticket"
    }

    fn risk(&self) -> Risk {
        Risk::Low
    }

    fn execute<'a>(&'a self, ctx: &'a ActionCtx<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            let title = str_param(ctx.params, "title").unwrap_or("");
            let url = format!("{}/mock/tickets", self.gateway_url.trim_end_matches('/'));
            let body = json!({"title": title, "alert_id": ctx.alert_id});
            match send_post(&self.client, &url, body).await {
                Ok((status, _)) if (200..300).contains(&status) => {
                    Outcome::Ok(format!("ticket opened (HTTP {status})"))
                }
                Ok((status, body)) => {
                    Outcome::Failed(format!("ticket failed: HTTP {status}: {}", snippet(&body)))
                }
                Err(e) if e.timeout => {
                    Outcome::Uncertain(format!("ticket timed out: {}", e.message))
                }
                Err(e) => Outcome::Failed(format!("ticket failed: {}", e.message)),
            }
        })
    }
}

struct Escalate;

impl Action for Escalate {
    fn name(&self) -> &'static str {
        "escalate"
    }

    fn label(&self) -> &'static str {
        "Escalate to team"
    }

    fn risk(&self) -> Risk {
        Risk::High
    }

    fn execute<'a>(&'a self, ctx: &'a ActionCtx<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            let team = match str_param(ctx.params, "team") {
                Some(t) => t,
                None => return Outcome::Failed("missing param: team".to_string()),
            };
            match escalate_to_team(ctx.pool, ctx.dir, ctx.alert_id, ctx.execution_id, team).await {
                Ok(outcome) => outcome,
                Err(e) => Outcome::Failed(format!("database error: {e}")),
            }
        })
    }
}

/// Page every member of `team` in ONE tx (module doc): channels resolved from prefs against
/// the alert row (UI-only falls back to `email`), jobs inserted with the alert's current
/// `notify_seq` and `escalation_step = 100 + n`, plus an `escalated` audit row.
async fn escalate_to_team(
    pool: &PgPool,
    dir: &Directory,
    alert_id: Uuid,
    execution_id: Uuid,
    team: &str,
) -> Result<Outcome, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let alert: Option<(i32, String, String, String)> =
        sqlx::query_as("SELECT notify_seq, severity, team, rule_id FROM alerts WHERE id = $1")
            .bind(alert_id)
            .fetch_optional(&mut *tx)
            .await?;
    let Some((notify_seq, severity, alert_team, rule_id)) = alert else {
        return Ok(Outcome::Failed("alert not found".to_string()));
    };
    let Some(eteam) = dir.teams.get(team) else {
        return Ok(Outcome::Failed(format!("unknown team {team}")));
    };
    let now = Utc::now();
    let decided_at = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    let pref_ctx = RoutingContext {
        severity,
        team: alert_team,
        rule_id: rule_id.clone(),
        service: None,
        environment: None,
    };
    let mut inserted = 0u64;
    for (n, member_id) in eteam.members.iter().enumerate() {
        let Some(person) = dir.person(member_id) else {
            continue;
        };
        let resolved = prefs::resolve_channels(person, Some(eteam), &pref_ctx, now);
        let mut channels: Vec<String> = resolved
            .channels
            .into_iter()
            .filter(|c| c != "ui")
            .collect();
        if channels.is_empty() {
            // UI-only (the feed itself has the alert): page over email instead.
            channels.push("email".to_string());
        }
        let mut jobs = Vec::with_capacity(channels.len());
        for channel in &channels {
            let Some(address) = address_for(person, channel) else {
                continue;
            };
            jobs.push(PlannedJob {
                channel: channel.clone(),
                address: address.to_string(),
                route_info: json!({
                    "target": format!("escalate:{team}"),
                    "rule_id": rule_id,
                    "decision": resolved.decision,
                    "decided_at": decided_at,
                }),
            });
        }
        if jobs.is_empty() {
            continue;
        }
        inserted += router::insert_jobs(
            &mut tx,
            alert_id,
            notify_seq,
            member_id,
            &jobs,
            Some(100 + n as i32),
        )
        .await?;
    }
    let message = format!("escalated to {team}: {inserted} job(s)");
    let requested_by: Option<String> =
        sqlx::query_scalar("SELECT requested_by FROM action_executions WHERE id = $1")
            .bind(execution_id)
            .fetch_optional(&mut *tx)
            .await?;
    crate::audit::record(
        &mut tx,
        requested_by.as_deref().unwrap_or("system"),
        "system",
        Some(alert_id),
        "escalated",
        &message,
        json!({"team": team, "jobs": inserted}),
    )
    .await?;
    tx.commit().await?;
    Ok(Outcome::Ok(message))
}

struct Notify;

impl Action for Notify {
    fn name(&self) -> &'static str {
        "notify"
    }

    fn label(&self) -> &'static str {
        "Notify target"
    }

    fn risk(&self) -> Risk {
        Risk::Low
    }

    fn execute<'a>(&'a self, ctx: &'a ActionCtx<'a>) -> BoxFuture<'a, Outcome> {
        Box::pin(async move {
            let to = match str_param(ctx.params, "to") {
                Some(t) => t,
                None => return Outcome::Failed("missing param: to".to_string()),
            };
            match notify_target(ctx.pool, ctx.dir, ctx.alert_id, to).await {
                Ok(outcome) => outcome,
                Err(e) => Outcome::Failed(format!("database error: {e}")),
            }
        })
    }
}

/// Same as `escalate` for one route target (`team:`/`user:`/`oncall:`): expand it, resolve
/// each person's channels (UI-only falls back to `email`) and insert jobs with the alert's
/// current `notify_seq` and `escalation_step = 100 + n`. The finalize's `step_run` audit
/// covers it; no extra audit row.
async fn notify_target(
    pool: &PgPool,
    dir: &Directory,
    alert_id: Uuid,
    to: &str,
) -> Result<Outcome, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let alert: Option<(i32, String, String, String)> =
        sqlx::query_as("SELECT notify_seq, severity, team, rule_id FROM alerts WHERE id = $1")
            .bind(alert_id)
            .fetch_optional(&mut *tx)
            .await?;
    let Some((notify_seq, severity, alert_team, rule_id)) = alert else {
        return Ok(Outcome::Failed("alert not found".to_string()));
    };
    let people = match dir.expand(to) {
        Ok(p) => p.iter().map(|p| p.id.clone()).collect::<Vec<_>>(),
        Err(e) => return Ok(Outcome::Failed(format!("cannot expand target {to}: {e}"))),
    };
    let now = Utc::now();
    let decided_at = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    let pref_ctx = RoutingContext {
        severity,
        team: alert_team.clone(),
        rule_id: rule_id.clone(),
        service: None,
        environment: None,
    };
    let mut inserted = 0u64;
    for (n, member_id) in people.iter().enumerate() {
        let Some(person) = dir.person(member_id) else {
            continue;
        };
        // The team whose default channels apply (the router's precedence for this target).
        let team = match to.split_once(':') {
            Some(("team", id)) => dir.teams.get(id),
            Some(("oncall", schedule)) => dir.team_for_schedule(schedule),
            _ => dir
                .teams
                .get(alert_team.as_str())
                .filter(|t| t.members.contains(&person.id)),
        };
        let resolved = prefs::resolve_channels(person, team, &pref_ctx, now);
        let mut channels: Vec<String> = resolved
            .channels
            .into_iter()
            .filter(|c| c != "ui")
            .collect();
        if channels.is_empty() {
            channels.push("email".to_string());
        }
        let mut jobs = Vec::with_capacity(channels.len());
        for channel in &channels {
            let Some(address) = address_for(person, channel) else {
                continue;
            };
            jobs.push(PlannedJob {
                channel: channel.clone(),
                address: address.to_string(),
                route_info: json!({
                    "target": format!("notify:{to}"),
                    "rule_id": rule_id,
                    "decision": resolved.decision,
                    "decided_at": decided_at,
                }),
            });
        }
        if jobs.is_empty() {
            continue;
        }
        inserted += router::insert_jobs(
            &mut tx,
            alert_id,
            notify_seq,
            member_id,
            &jobs,
            Some(100 + n as i32),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(Outcome::Ok(format!("notified {to}: {inserted} job(s)")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn targets(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn registry_has_all_known_actions_with_matching_risk() {
        let cfg = ActionsConfig {
            gateway_url: "http://localhost:8070".to_string(),
            http_targets: HashMap::new(),
            timeout: Duration::from_secs(5),
        };
        let registry = ActionRegistry::builtin(cfg);
        for name in ActionRegistry::known() {
            let action = registry
                .get(name)
                .unwrap_or_else(|| panic!("missing {name}"));
            assert_eq!(action.name(), *name);
            assert_eq!(action.risk().as_str(), ActionRegistry::risk_of(name));
            assert!(!action.label().is_empty());
        }
        assert!(registry.get("nope").is_none());
        assert_eq!(ActionRegistry::risk_of("escalate"), "high");
        assert_eq!(ActionRegistry::risk_of("restart_service"), "high");
        assert_eq!(ActionRegistry::risk_of("http_get"), "low");
    }

    #[test]
    fn alias_mapping_replaces_host_and_appends_path() {
        let t = targets(&[(
            "counterparty-service",
            "http://localhost:8070/mock/health/counterparty-service",
        )]);
        // The Appendix B runbook URL: base does not end with "/health", so it is appended.
        assert_eq!(
            map_alias_url("http://counterparty-service/health", &t).unwrap(),
            "http://localhost:8070/mock/health/counterparty-service/health"
        );
        // Base already ending with the path is used as-is.
        let t2 = targets(&[("svc", "http://localhost:8070/mock/health")]);
        assert_eq!(
            map_alias_url("http://svc/mock/health", &t2).unwrap(),
            "http://localhost:8070/mock/health"
        );
        // Query strings survive.
        assert_eq!(
            map_alias_url("http://svc/mock/health?verbose=1", &t2).unwrap(),
            "http://localhost:8070/mock/health?verbose=1"
        );
        // Unknown alias, other schemes and garbage never map (and never send).
        assert_eq!(
            map_alias_url("http://unknown/health", &t2).unwrap_err(),
            "target not allowed"
        );
        assert_eq!(
            map_alias_url("https://svc/health", &t2).unwrap_err(),
            "target not allowed"
        );
        assert_eq!(map_alias_url("not a url", &t2).unwrap_err(), "invalid url");
    }

    #[test]
    fn load_targets_missing_file_is_empty() {
        let dir = std::env::temp_dir().join(format!(
            "beacon-actions-test-missing-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let got = load_http_targets(&dir).unwrap();
        assert!(got.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_targets_parses_and_rejects_shapes() {
        let dir =
            std::env::temp_dir().join(format!("beacon-actions-test-parse-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("actions")).unwrap();
        std::fs::write(
            dir.join("actions/targets.yaml"),
            "targets:\n  counterparty-service: \"http://localhost:8070/mock/health/counterparty-service\"\n",
        )
        .unwrap();
        let got = load_http_targets(&dir).unwrap();
        assert_eq!(
            got["counterparty-service"],
            "http://localhost:8070/mock/health/counterparty-service"
        );
        std::fs::write(dir.join("actions/targets.yaml"), "targets: [nope]\n").unwrap();
        assert!(load_http_targets(&dir).is_err());
        std::fs::write(
            dir.join("actions/targets.yaml"),
            "targets:\n  bad: \":::not a url:::\"\n",
        )
        .unwrap();
        assert!(load_http_targets(&dir).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn output_truncates_to_300_chars() {
        assert_eq!(truncate_output("short"), "short");
        let long = "x".repeat(500);
        let cut = truncate_output(&long);
        assert_eq!(cut.chars().count(), OUTPUT_MAX_CHARS);
        // Char boundary, not byte boundary.
        let wide = "€".repeat(500);
        assert_eq!(truncate_output(&wide).chars().count(), OUTPUT_MAX_CHARS);
    }

    #[test]
    fn encode_segment_rejects_traversal_and_encodes_the_rest() {
        for bad in ["", ".", "..", "a/b", "/x", "x/", "a\\b", "a\\"] {
            assert_eq!(
                encode_segment(bad).unwrap_err(),
                "invalid path segment",
                "{bad:?}"
            );
        }
        // Unreserved characters stay; everything else percent-encodes.
        assert_eq!(encode_segment("WF-1").unwrap(), "WF-1");
        assert_eq!(encode_segment("a.b~c_d-e").unwrap(), "a.b~c_d-e");
        assert_eq!(encode_segment("a b").unwrap(), "a%20b");
        assert_eq!(encode_segment("%41").unwrap(), "%2541");
        // `..` only as the WHOLE segment is traversal; embedded dots are ordinary bytes.
        assert_eq!(encode_segment("v1..beta").unwrap(), "v1..beta");
    }

    #[test]
    fn redact_output_strips_queries_and_credentials() {
        // URL query strings never survive (the request keeps them; the output does not).
        assert_eq!(
            redact_output("GET http://h/x/y?token=hunter2 -> HTTP 200 (5 bytes)"),
            "GET http://h/x/y -> HTTP 200 (5 bytes)"
        );
        assert_eq!(
            redact_output("error sending request for url (http://h/x?sig=1)"),
            "error sending request for url (http://h/x)"
        );
        assert_eq!(
            redact_output("plain text, no markers"),
            "plain text, no markers"
        );
        // Bearer/basic credentials redact to the end of the line, whatever the case.
        assert_eq!(
            redact_output("x Authorization: Bearer hunter2 tail"),
            "x Authorization: [REDACTED]"
        );
        assert_eq!(
            redact_output("authorization: Basic aGk=\nnext line"),
            "Authorization: [REDACTED]\nnext line"
        );
        // `token=` values redact up to the next `&` or whitespace; prefixes like
        // `access_token=` ride along.
        assert_eq!(
            redact_output("a=1&token=supersecret&b=2"),
            "a=1&token=[REDACTED]&b=2"
        );
        assert_eq!(
            redact_output("access_token=hunter2 ok"),
            "access_token=[REDACTED] ok"
        );
        // Non-ASCII passes through untouched.
        assert_eq!(
            redact_output("…⟨wf-9⟩ em dashes stay"),
            "…⟨wf-9⟩ em dashes stay"
        );
        // The word without the header form stays.
        assert_eq!(
            redact_output("the Authorization header was missing"),
            "the Authorization header was missing"
        );
    }

    #[test]
    fn redact_output_leaves_nothing_of_the_secrets() {
        let redacted = redact_output(
            "GET https://gw/mock?token=hunter2&x=1 failed: HTTP 500: \
             Authorization: Bearer hunter2\nretry hint: token=hunter2 here",
        );
        assert!(!redacted.contains("hunter2"), "{redacted}");
        assert!(!redacted.contains('?'), "{redacted}");
        assert_eq!(redacted.matches("[REDACTED]").count(), 2, "{redacted}");
    }
}
