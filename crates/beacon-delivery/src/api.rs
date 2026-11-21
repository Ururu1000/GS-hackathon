//! Delivery's HTTP API, mounted by beacon-api (board #172/#174), shapes and status codes from
//! ui/contract/openapi.yaml v0.2.0 (PR #10):
//!
//! | Route | Success | Errors |
//! |---|---|---|
//! | `POST /api/alerts/{id}/steps/{step}/run` `{expected_version?}` | `202 {execution_id}` | `403 forbidden` (actor not on the alert's team); `404 not_found`; `409 approval_required` (+`error.approval_id`); `409 invalid_transition` (alert resolved, step disabled/condition unmet, already queued/running, not an action step, or no one else can approve this step) |
//! | `POST /api/alerts/{id}/steps/{step}/check` `{done = true}` | `200 {}` | `403 forbidden` (team); `400 bad_request` (not a manual step); `404` |
//! | `POST /api/approvals/{id}/{approve\|reject}` `{note?}` (note <= 2000) | `200 {execution_id?}` | `403 forbidden` (not eligible, or the requester — nobody approves their own request); `404`; `409 invalid_transition` (already decided / expired / alert resolved) |
//! | `GET /api/runbooks` | `200 {items}` | |
//! | `POST /api/replies` `{from, text}` (fake-gateway callback: `X-Beacon-Timestamp: <unix seconds>` within ±300 s of now, `X-Beacon-Signature: hex(hmac_sha256(reply_secret, "<timestamp>." + raw_body))`) | `200 {outcome}`: `approved`/`rejected` (+`approval_id`; approved also `execution_id`), `acked` (+`alert_id`), `already_decided`, `wrong_code`, `too_many_attempts`, `ambiguous` (+`message`: the code matches more than one pending approval — decide in the UI), `unknown` (+`message`) | `401 unauthorized` (missing/old/future timestamp, bad/missing signature) |
//!
//! Auth: beacon-api's session/CSRF middleware inserts `Extension(Actor("user:<id>"))` before
//! these handlers; without it every route except `/api/replies` returns `401 unauthorized`.
//! `/api/replies` is authenticated by its timestamped signature instead (the caller is the
//! gateway; a replay inside the 300 s window is harmless because decisions are idempotent).
//! `run` and `check` additionally require the actor to be on the alert's team — a member, the
//! lead, or anyone in one of the team's on-call rotations (Directory) — else `403 forbidden`
//! (audit #11; `GET /api/runbooks` stays open to any authenticated user).
//! POST bodies must be `application/json` (`415 unsupported_media_type` otherwise; an empty
//! body is accepted wherever the contract allows one) — a cross-site `<form>` can never post
//! one. Errors are `{error: {code, message, approval_id?}}`; 500s are `internal` with details
//! only in logs. Every mutation writes `audit_log` and bumps the alert version
//! (`AlertOps::touch`).
//!
//! `{expected_version?}` on the run route is accepted (shape-checked: an integer or null) but
//! not enforced — the route table above lists no version-conflict error for it.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, body::Bytes};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Sha256;
use uuid::Uuid;

use crate::actions::{self, ActionRegistry};
use crate::alert_ops::AlertOps;
use crate::approvals::{
    self, DecideError, DecideOutcome, Decision, ReplyOutcome, RequestError, Via,
};
use crate::audit;
use crate::config::Directory;
use crate::runbook::{RenderedStep, StepKind};

/// The authenticated user, `user:<id>`, inserted by the mounting app.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Actor(pub String);

#[derive(Clone)]
pub struct ApiState {
    pub pool: sqlx::PgPool,
    pub dir: Arc<Directory>,
    pub registry: ActionRegistry,
    pub alert_ops: Arc<dyn AlertOps>,
    /// HMAC key for `/api/replies` (env `REPLY_SECRET`); `None` rejects every reply.
    pub reply_secret: Option<Arc<Vec<u8>>>,
}

/// The contract's error body.
#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
    pub approval_id: Option<String>,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            approval_id: None,
        }
    }

    pub fn internal(err: impl std::fmt::Display) -> Self {
        tracing::error!(error = %err, "delivery api internal error");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "internal error",
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut error = json!({ "code": self.code, "message": self.message });
        if let Some(id) = self.approval_id {
            error["approval_id"] = Value::String(id);
        }
        (self.status, axum::Json(json!({ "error": error }))).into_response()
    }
}

// ---------------------------------------------------------------------------------------------
// Error + body helpers
// ---------------------------------------------------------------------------------------------

fn unauthorized() -> ApiError {
    ApiError::new(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "missing actor: this route needs an authenticated user",
    )
}

fn forbidden(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::FORBIDDEN, "forbidden", message)
}

fn not_found(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::NOT_FOUND, "not_found", message)
}

fn bad_request(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::BAD_REQUEST, "bad_request", message)
}

fn invalid_transition(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::CONFLICT, "invalid_transition", message)
}

/// `415 unsupported_media_type`: POST bodies must be `application/json`.
fn unsupported_media_type() -> ApiError {
    ApiError::new(
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "unsupported_media_type",
        "Content-Type must be application/json",
    )
}

/// Audit #11 (CSRF): a POST body that is not empty and not `application/json` (media type
/// only — parameters like `charset` are ignored) is rejected before it is parsed, so a
/// cross-site `<form>` (which can at best send `text/plain` or form encodings) can never
/// reach a handler. Empty bodies stay allowed wherever the contract allows them.
fn require_json_body(headers: &axum::http::HeaderMap, body: &[u8]) -> Result<(), ApiError> {
    if body.is_empty() {
        return Ok(());
    }
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let media_type = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if media_type == "application/json" {
        Ok(())
    } else {
        Err(unsupported_media_type())
    }
}

/// Audit #11 (team authorization): the actor may run or check an alert's steps only when
/// they are on the alert's team — a member, the lead, or anyone in one of the team's on-call
/// rotations. A team missing from the Directory authorizes nobody.
fn on_team(dir: &Directory, team_id: &str, actor: &str) -> bool {
    let Some(person) = actor.strip_prefix("user:") else {
        return false;
    };
    let Some(team) = dir.teams.get(team_id) else {
        return false;
    };
    team.members.iter().any(|m| m == person)
        || team.lead.as_deref() == Some(person)
        || team.oncall.values().flatten().any(|p| p == person)
}

/// `{..}` from a possibly-empty JSON body; empty is `{}` (both run body forms are optional),
/// anything malformed or non-object is a `400 bad_request`.
fn parse_body(bytes: &[u8]) -> Result<Value, ApiError> {
    if bytes.is_empty() {
        return Ok(json!({}));
    }
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|e| bad_request(format!("invalid JSON body: {e}")))?;
    match value {
        Value::Object(map) => Ok(Value::Object(map)),
        _ => Err(bad_request("expected a JSON object")),
    }
}

/// Parse a path segment as a uuid; a malformed one is a 404 like an unknown id (the route
/// table has no 400 for path parsing).
fn parse_id(raw: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| not_found(format!("unknown id {raw:?}")))
}

/// All delivery API routes (table above).
pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/api/alerts/{id}/steps/{step}/run", post(run_step_handler))
        .route(
            "/api/alerts/{id}/steps/{step}/check",
            post(check_step_handler),
        )
        .route("/api/approvals/{id}/approve", post(approve_handler))
        .route("/api/approvals/{id}/reject", post(reject_handler))
        .route("/api/runbooks", get(runbooks_handler))
        .route("/api/replies", post(replies_handler))
        .with_state(state)
}

/// The run/check handlers' frozen step, loaded once per request.
///
/// Load the alert row, the frozen runbook and the named step. 404s for an unknown alert id,
/// an alert without a runbook and an unknown step id, in that order. Returns the alert
/// status, the alert's team (for the authorization check) and the step.
async fn load_step(
    state: &ApiState,
    alert_id: Uuid,
    step_id: &str,
) -> Result<(String, String, RenderedStep), ApiError> {
    let status: Option<(String, String)> =
        sqlx::query_as("SELECT status, team FROM alerts WHERE id = $1")
            .bind(alert_id)
            .fetch_optional(&state.pool)
            .await
            .map_err(ApiError::internal)?;
    let Some((status, team)) = status else {
        return Err(not_found(format!("unknown alert {alert_id}")));
    };

    let steps: Option<Value> =
        sqlx::query_scalar("SELECT steps FROM alert_runbooks WHERE alert_id = $1")
            .bind(alert_id)
            .fetch_optional(&state.pool)
            .await
            .map_err(ApiError::internal)?;
    let Some(steps) = steps else {
        return Err(not_found(format!("alert {alert_id} has no runbook")));
    };
    let steps: Vec<RenderedStep> = serde_json::from_value(steps)
        .map_err(|e| ApiError::internal(format!("cannot parse frozen runbook steps: {e}")))?;
    let Some(step) = steps.into_iter().find(|s| s.id == step_id) else {
        return Err(not_found(format!("unknown runbook step {step_id:?}")));
    };
    Ok((status, team, step))
}

// ---------------------------------------------------------------------------------------------
// POST /api/alerts/{id}/steps/{step}/run
// ---------------------------------------------------------------------------------------------

async fn run_step_handler(
    State(state): State<ApiState>,
    Path((alert_raw, step_raw)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
    actor: Option<axum::Extension<Actor>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let Some(axum::Extension(Actor(actor))) = actor else {
        return Err(unauthorized());
    };
    require_json_body(&headers, &body)?;
    let alert_id = parse_id(&alert_raw)?;

    // The body is optional; `expected_version` is shape-checked but not enforced (module doc).
    let body = parse_body(&body)?;
    if let Some(v) = body.get("expected_version")
        && !(v.is_i64() || v.is_null())
    {
        return Err(bad_request("expected_version must be an integer"));
    }

    let (status, team, step) = load_step(&state, alert_id, &step_raw).await?;

    // Audit #11: only the alert's own team may touch its steps.
    if !on_team(&state.dir, &team, &actor) {
        return Err(forbidden(
            "only a member, the lead or an on-call of the alert's team may run its steps",
        ));
    }

    // Manual (and check-only) steps are never run.
    if step.kind == StepKind::Manual || step.action.is_none() {
        return Err(invalid_transition(format!(
            "step {step_raw:?} is not an action step"
        )));
    }
    if status == "resolved" {
        return Err(invalid_transition("alert is resolved"));
    }
    if let Some(render_error) = &step.render_error {
        return Err(invalid_transition(render_error.clone()));
    }
    if let Some(cond) = &step.condition {
        let (met, reason) = actions::condition_status(&state.pool, alert_id, cond)
            .await
            .map_err(ApiError::internal)?;
        if !met {
            return Err(invalid_transition(
                reason.unwrap_or_else(|| "step condition is not met".into()),
            ));
        }
    }

    let action = step
        .action
        .as_ref()
        .expect("checked: action steps carry an action");
    let action_name = action
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::internal("frozen step action has no name"))?;
    let params = action.get("params").cloned().unwrap_or_else(|| json!({}));

    // Approval-gated steps never queue directly: the 409 carries the approval to decide.
    if step.approval == "required" {
        let mut tx = state.pool.begin().await.map_err(ApiError::internal)?;
        let approval_id = approvals::request(
            &mut tx,
            &state.dir,
            alert_id,
            &step_raw,
            action_name,
            &params,
            &actor,
        )
        .await
        .map_err(|e| match e {
            // Audit #11: the requester is the only one who could decide (never themselves) —
            // the step cannot be run at all.
            RequestError::NoEligibleApprover => {
                invalid_transition("no one else can approve this step")
            }
            RequestError::NoFreeCode | RequestError::Db(_) => ApiError::internal(e),
        })?;
        state
            .alert_ops
            .touch(&mut tx, alert_id)
            .await
            .map_err(ApiError::internal)?;
        tx.commit().await.map_err(ApiError::internal)?;
        let mut err = ApiError::new(
            StatusCode::CONFLICT,
            "approval_required",
            format!("step {step_raw:?} needs approval"),
        );
        err.approval_id = Some(approval_id.to_string());
        return Err(err);
    }

    let mut tx = state.pool.begin().await.map_err(ApiError::internal)?;
    let queued = actions::queue(
        &mut tx,
        alert_id,
        &step_raw,
        action_name,
        &params,
        &actor,
        None,
    )
    .await
    .map_err(ApiError::internal)?;
    let Some(execution_id) = queued else {
        let _ = tx.rollback().await;
        return Err(invalid_transition("step is already queued or running"));
    };
    state
        .alert_ops
        .touch(&mut tx, alert_id)
        .await
        .map_err(ApiError::internal)?;
    tx.commit().await.map_err(ApiError::internal)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "execution_id": execution_id })),
    )
        .into_response())
}

// ---------------------------------------------------------------------------------------------
// POST /api/alerts/{id}/steps/{step}/check
// ---------------------------------------------------------------------------------------------

async fn check_step_handler(
    State(state): State<ApiState>,
    Path((alert_raw, step_raw)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
    actor: Option<axum::Extension<Actor>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let Some(axum::Extension(Actor(actor))) = actor else {
        return Err(unauthorized());
    };
    require_json_body(&headers, &body)?;
    let alert_id = parse_id(&alert_raw)?;

    let body = parse_body(&body)?;
    let done = match body.get("done") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(_) => return Err(bad_request("done must be a boolean")),
    };

    let (_, team, step) = load_step(&state, alert_id, &step_raw).await?;

    // Audit #11: only the alert's own team may touch its steps.
    if !on_team(&state.dir, &team, &actor) {
        return Err(forbidden(
            "only a member, the lead or an on-call of the alert's team may check its steps",
        ));
    }

    if step.kind != StepKind::Manual {
        return Err(bad_request(format!(
            "step {step_raw:?} is not a manual step"
        )));
    }

    let mut tx = state.pool.begin().await.map_err(ApiError::internal)?;
    if done {
        sqlx::query(
            "INSERT INTO step_marks (alert_id, step_id, done_by) VALUES ($1, $2, $3) \
             ON CONFLICT (alert_id, step_id) \
             DO UPDATE SET done_by = EXCLUDED.done_by, done_at = now()",
        )
        .bind(alert_id)
        .bind(&step_raw)
        .bind(&actor)
        .execute(&mut *tx)
        .await
        .map_err(ApiError::internal)?;
    } else {
        sqlx::query("DELETE FROM step_marks WHERE alert_id = $1 AND step_id = $2")
            .bind(alert_id)
            .bind(&step_raw)
            .execute(&mut *tx)
            .await
            .map_err(ApiError::internal)?;
    }
    let message = if done {
        format!("step {step_raw} marked done")
    } else {
        format!("step {step_raw} mark cleared")
    };
    audit::record(
        &mut tx,
        &actor,
        "ui",
        Some(alert_id),
        "step_done",
        &message,
        json!({ "step_id": step_raw }),
    )
    .await
    .map_err(ApiError::internal)?;
    state
        .alert_ops
        .touch(&mut tx, alert_id)
        .await
        .map_err(ApiError::internal)?;
    tx.commit().await.map_err(ApiError::internal)?;
    Ok((StatusCode::OK, Json(json!({}))).into_response())
}

// ---------------------------------------------------------------------------------------------
// POST /api/approvals/{id}/{approve|reject}
// ---------------------------------------------------------------------------------------------

async fn approve_handler(
    state: State<ApiState>,
    Path(id_raw): Path<String>,
    headers: axum::http::HeaderMap,
    actor: Option<axum::Extension<Actor>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    require_json_body(&headers, &body)?;
    decide_http(state, id_raw, actor, body, Decision::Approve).await
}

async fn reject_handler(
    state: State<ApiState>,
    Path(id_raw): Path<String>,
    headers: axum::http::HeaderMap,
    actor: Option<axum::Extension<Actor>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    require_json_body(&headers, &body)?;
    decide_http(state, id_raw, actor, body, Decision::Reject).await
}

async fn decide_http(
    State(state): State<ApiState>,
    id_raw: String,
    actor: Option<axum::Extension<Actor>>,
    body: Bytes,
    decision: Decision,
) -> Result<Response, ApiError> {
    let Some(axum::Extension(Actor(actor))) = actor else {
        return Err(unauthorized());
    };
    let approval_id = parse_id(&id_raw)?;

    let body = parse_body(&body)?;
    let note = match body.get("note") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => {
            if s.chars().count() > 2000 {
                return Err(bad_request("note exceeds 2000 chars"));
            }
            Some(s.as_str())
        }
        Some(_) => return Err(bad_request("note must be a string")),
    };

    let outcome = match approvals::decide(
        &state.pool,
        state.alert_ops.as_ref(),
        approval_id,
        &actor,
        decision,
        Via::Ui,
        note,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(e) => {
            tracing::warn!(error = %e, approval_id = %approval_id, "approval decide failed");
            return Err(decide_error(e));
        }
    };

    match outcome {
        DecideOutcome::Approved { execution_id } => Ok((
            StatusCode::OK,
            Json(json!({ "execution_id": execution_id })),
        )
            .into_response()),
        DecideOutcome::Rejected => Ok((StatusCode::OK, Json(json!({}))).into_response()),
    }
}

/// Map a [`DecideError`] to the route table's error responses.
fn decide_error(e: DecideError) -> ApiError {
    match e {
        DecideError::Forbidden(_) => {
            ApiError::new(StatusCode::FORBIDDEN, "forbidden", e.to_string())
        }
        DecideError::NotFound => not_found(e.to_string()),
        DecideError::AlreadyDecided(_)
        | DecideError::Expired
        | DecideError::InvalidTransition(_) => invalid_transition(e.to_string()),
        DecideError::Db(_) => ApiError::internal(e),
    }
}

// ---------------------------------------------------------------------------------------------
// GET /api/runbooks
// ---------------------------------------------------------------------------------------------

async fn runbooks_handler(
    State(state): State<ApiState>,
    actor: Option<axum::Extension<Actor>>,
) -> Result<Response, ApiError> {
    let _actor = actor.map(|a| a.0).ok_or_else(unauthorized)?;
    let items = crate::runbook::list_items(&state.dir.runbooks);
    Ok((StatusCode::OK, Json(json!({ "items": items }))).into_response())
}

// ---------------------------------------------------------------------------------------------
// POST /api/replies (the fake gateway's authenticated callback)
// ---------------------------------------------------------------------------------------------

/// The signed reply window: `|now - X-Beacon-Timestamp|` may not exceed this (unix seconds).
/// A replay inside the window is harmless — decisions are idempotent (`AlreadyDecided`).
const REPLY_MAX_SKEW_SECS: i64 = 300;

#[derive(Debug, Deserialize)]
struct ReplyBody {
    from: String,
    text: String,
}

async fn replies_handler(
    State(state): State<ApiState>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    // A cross-site form can never satisfy this (audit #11), and the signature below
    // authenticates the caller (the gateway), not a session: no Actor here.
    require_json_body(&headers, &body)?;
    let Some(secret) = state.reply_secret.as_deref() else {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "no reply secret configured; SMS replies are rejected",
        ));
    };

    // Replay protection (audit #11): the signature covers the timestamp as well, and the
    // timestamp must be current. A captured reply cannot be replayed later than the window,
    // and inside the window a replay decides nothing (idempotency).
    let Some(ts_raw) = headers
        .get("x-beacon-timestamp")
        .and_then(|v| v.to_str().ok())
    else {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "missing X-Beacon-Timestamp",
        ));
    };
    let ts: i64 = ts_raw.trim().parse().map_err(|_| {
        ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "X-Beacon-Timestamp must be unix seconds",
        )
    })?;
    if (chrono::Utc::now().timestamp() - ts).abs() > REPLY_MAX_SKEW_SECS {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "X-Beacon-Timestamp is outside the allowed window",
        ));
    }

    let Some(signature) = headers
        .get("x-beacon-signature")
        .and_then(|v| v.to_str().ok())
    else {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "missing X-Beacon-Signature",
        ));
    };
    let signature = hex::decode(signature).map_err(|_| {
        ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "X-Beacon-Signature is not valid hex",
        )
    })?;
    // HMAC verification is constant-time (`verify_slice`); the message is exactly
    // "<timestamp>." + the raw body bytes the gateway signed.
    let mut mac = Hmac::<Sha256>::new_from_slice(secret)
        .map_err(|e| ApiError::internal(format!("hmac init: {e}")))?;
    mac.update(ts_raw.as_bytes());
    mac.update(b".");
    mac.update(&body);
    if mac.verify_slice(&signature).is_err() {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "signature mismatch",
        ));
    }

    let reply: ReplyBody = serde_json::from_slice(&body)
        .map_err(|e| bad_request(format!("invalid reply body: {e}")))?;
    // Mirrors the gateway's simulator validation, defence in depth for direct callers.
    if reply.from.chars().count() > 32
        || !reply
            .from
            .chars()
            .all(|c| c.is_ascii_digit() || c == '+' || c == ' ')
        || !reply.from.chars().any(|c| c.is_ascii_digit())
    {
        return Err(bad_request(
            "from must be a phone number (digits, optional +)",
        ));
    }
    if reply.text.trim().is_empty() || reply.text.chars().count() > 160 {
        return Err(bad_request("text must be 1..=160 chars"));
    }

    let outcome = approvals::handle_reply(
        &state.pool,
        &state.dir,
        state.alert_ops.as_ref(),
        &reply.from,
        &reply.text,
    )
    .await
    .map_err(ApiError::internal)?;
    Ok((StatusCode::OK, Json(reply_json(outcome))).into_response())
}

/// The `/api/replies` success body: `{outcome, ...ids}`.
fn reply_json(outcome: ReplyOutcome) -> Value {
    match outcome {
        ReplyOutcome::Approved {
            approval_id,
            execution_id,
        } => json!({
            "outcome": "approved",
            "approval_id": approval_id,
            "execution_id": execution_id,
        }),
        ReplyOutcome::Rejected { approval_id } => {
            json!({ "outcome": "rejected", "approval_id": approval_id })
        }
        ReplyOutcome::Acked { alert_id } => {
            json!({ "outcome": "acked", "alert_id": alert_id })
        }
        ReplyOutcome::AlreadyDecided => json!({ "outcome": "already_decided" }),
        ReplyOutcome::WrongCode => json!({ "outcome": "wrong_code" }),
        ReplyOutcome::TooManyAttempts => json!({ "outcome": "too_many_attempts" }),
        ReplyOutcome::Ambiguous => {
            json!({ "outcome": "ambiguous", "message": "decide in the UI" })
        }
        ReplyOutcome::Unknown(m) => json!({ "outcome": "unknown", "message": m }),
    }
}

// ---------------------------------------------------------------------------------------------
// For beacon-api
// ---------------------------------------------------------------------------------------------

/// For beacon-api's detail handler: `AlertDetail.runbook`.
pub async fn runbook_view(
    pool: &sqlx::PgPool,
    alert_id: uuid::Uuid,
    actor: &Actor,
) -> Result<Option<Value>, sqlx::Error> {
    crate::runbook::runbook_view(pool, alert_id, &actor.0).await
}

/// For beacon-api's detail handler: delivery's `AlertDetail.timeline` entries.
pub async fn timeline_entries(
    pool: &sqlx::PgPool,
    alert_id: uuid::Uuid,
) -> Result<Vec<Value>, sqlx::Error> {
    crate::audit::timeline_entries(pool, alert_id).await
}

// ---------------------------------------------------------------------------------------------
// Standalone dev server
// ---------------------------------------------------------------------------------------------

/// Standalone dev server for the `delivery-api` role until beacon-api mounts `router`. Dev/demo
/// only: binds `addr` — which MUST be loopback; anything else is refused with an io error
/// before any socket is opened (audit #11) — logs a warning, and injects the `Actor` for every
/// request. `allow_demo_header` (an `ApiState`-independent flag the binary wires, e.g. from an
/// env var) gates whether the request header `X-Beacon-Demo-Actor: user:<id>` may pick the
/// acting person (it must name a person in the Directory, else 401): when `false` the header is
/// ignored outright and every request acts as `demo_actor`.
pub async fn serve_standalone(
    state: ApiState,
    addr: std::net::SocketAddr,
    demo_actor: Actor,
    shutdown: crate::Shutdown,
    allow_demo_header: bool,
) -> std::io::Result<()> {
    if !addr.ip().is_loopback() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "delivery-api standalone mode is dev/demo only and refuses to bind \
                 non-loopback {addr}"
            ),
        ));
    }
    tracing::warn!(
        allow_demo_header,
        "delivery-api standalone mode is DEV/DEMO ONLY: {} Do not expose this port beyond \
         localhost.",
        if allow_demo_header {
            "the X-Beacon-Demo-Actor header can act as ANY person in the Directory."
        } else {
            "the X-Beacon-Demo-Actor header is disabled; every request acts as the demo actor."
        }
    );
    let dir = state.dir.clone();
    let fallback = demo_actor.0.clone();
    let app = router(state).layer(axum::middleware::from_fn(
        move |req: Request, next: Next| {
            // Clone per request: the closure must stay `Fn`, the future owns its copies.
            let dir = dir.clone();
            let fallback = fallback.clone();
            async move {
                // `X-Beacon-Demo-Actor: user:<id>` names the acting person — but only when the
                // server was started with the header allowed. Unknown names are rejected
                // instead of silently falling back.
                let actor = match (allow_demo_header, req.headers().get("x-beacon-demo-actor")) {
                    // Header disabled (or absent): every request acts as the demo actor.
                    (false, _) | (true, None) => fallback,
                    (true, Some(raw)) => match raw.to_str() {
                        Err(_) => return unauthorized().into_response(),
                        Ok(raw) => {
                            let id = raw.strip_prefix("user:").unwrap_or(raw);
                            match dir.people.contains_key(id) {
                                true => format!("user:{id}"),
                                false => return unauthorized().into_response(),
                            }
                        }
                    },
                };
                let mut req = req;
                req.extensions_mut().insert(Actor(actor));
                next.run(req).await
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let mut shutdown = shutdown;
            // `Err` means the sender is gone: nobody can signal shutdown any more, so stop.
            let _ = shutdown.wait_for(|v| *v).await;
        })
        .await
}
