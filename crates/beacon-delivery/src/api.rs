//! Delivery's HTTP API, mounted by beacon-api (board #172/#174), shapes and status codes from
//! ui/contract/openapi.yaml v0.2.0 (PR #10):
//!
//! | Route | Success | Errors |
//! |---|---|---|
//! | `POST /api/alerts/{id}/steps/{step}/run` `{expected_version?}` | `202 {execution_id}` | `404 not_found`; `409 approval_required` (+`error.approval_id`); `409 invalid_transition` (alert resolved, step disabled/condition unmet, already queued/running, not an action step) |
//! | `POST /api/alerts/{id}/steps/{step}/check` `{done = true}` | `200 {}` | `400 bad_request` (not a manual step); `404` |
//! | `POST /api/approvals/{id}/{approve\|reject}` `{note?}` (note <= 2000) | `200 {execution_id?}` | `403 forbidden`; `404`; `409 invalid_transition` (already decided / expired / alert resolved) |
//! | `GET /api/runbooks` | `200 {items}` | |
//! | `POST /api/replies` `{from, text}` (fake-gateway callback, `X-Beacon-Signature: hex(hmac_sha256(reply_secret, body))`) | `200 {outcome}` | `401 unauthorized` (bad/missing signature) |
//!
//! Auth: beacon-api's session/CSRF middleware inserts `Extension(Actor("user:<id>"))` before
//! these handlers; without it every route except `/api/replies` returns `401 unauthorized`.
//! `/api/replies` is authenticated by its signature instead (the caller is the gateway).
//! Errors are `{error: {code, message, approval_id?}}`; 500s are `internal` with details only in
//! logs. Every mutation writes `audit_log` and bumps the alert version (`AlertOps::touch`).
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
use crate::approvals::{self, DecideError, DecideOutcome, Decision, ReplyOutcome, Via};
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

fn not_found(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::NOT_FOUND, "not_found", message)
}

fn bad_request(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::BAD_REQUEST, "bad_request", message)
}

fn invalid_transition(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::CONFLICT, "invalid_transition", message)
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
/// an alert without a runbook and an unknown step id, in that order. Returns the alert status
/// and the step.
async fn load_step(
    state: &ApiState,
    alert_id: Uuid,
    step_id: &str,
) -> Result<(String, RenderedStep), ApiError> {
    let status: Option<(String,)> = sqlx::query_as("SELECT status FROM alerts WHERE id = $1")
        .bind(alert_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(ApiError::internal)?;
    let Some((status,)) = status else {
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
    Ok((status, step))
}

// ---------------------------------------------------------------------------------------------
// POST /api/alerts/{id}/steps/{step}/run
// ---------------------------------------------------------------------------------------------

async fn run_step_handler(
    State(state): State<ApiState>,
    Path((alert_raw, step_raw)): Path<(String, String)>,
    actor: Option<axum::Extension<Actor>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let Some(axum::Extension(Actor(actor))) = actor else {
        return Err(unauthorized());
    };
    let alert_id = parse_id(&alert_raw)?;

    // The body is optional; `expected_version` is shape-checked but not enforced (module doc).
    let body = parse_body(&body)?;
    if let Some(v) = body.get("expected_version")
        && !(v.is_i64() || v.is_null())
    {
        return Err(bad_request("expected_version must be an integer"));
    }

    let (status, step) = load_step(&state, alert_id, &step_raw).await?;

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
        .map_err(ApiError::internal)?;
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
    actor: Option<axum::Extension<Actor>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let Some(axum::Extension(Actor(actor))) = actor else {
        return Err(unauthorized());
    };
    let alert_id = parse_id(&alert_raw)?;

    let body = parse_body(&body)?;
    let done = match body.get("done") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(_) => return Err(bad_request("done must be a boolean")),
    };

    let (_, step) = load_step(&state, alert_id, &step_raw).await?;
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
    actor: Option<axum::Extension<Actor>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    decide_http(state, id_raw, actor, body, Decision::Approve).await
}

async fn reject_handler(
    state: State<ApiState>,
    Path(id_raw): Path<String>,
    actor: Option<axum::Extension<Actor>>,
    body: Bytes,
) -> Result<Response, ApiError> {
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
        DecideError::Forbidden => ApiError::new(StatusCode::FORBIDDEN, "forbidden", e.to_string()),
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
    // The signature authenticates the caller (the gateway), not a session: no Actor here.
    let Some(secret) = state.reply_secret.as_deref() else {
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "no reply secret configured; SMS replies are rejected",
        ));
    };
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
    // HMAC verification is constant-time (`verify_slice`); the raw body bytes are exactly
    // what was signed.
    let mut mac = Hmac::<Sha256>::new_from_slice(secret)
        .map_err(|e| ApiError::internal(format!("hmac init: {e}")))?;
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
/// only: binds `addr` (the binary defaults to 127.0.0.1), logs a warning, and injects the
/// `Actor` from the request header `X-Beacon-Demo-Actor: user:<id>` (must name a person in the
/// Directory, else 401), falling back to `demo_actor`.
pub async fn serve_standalone(
    state: ApiState,
    addr: std::net::SocketAddr,
    demo_actor: Actor,
    shutdown: crate::Shutdown,
) -> std::io::Result<()> {
    tracing::warn!(
        "delivery-api standalone mode is DEV/DEMO ONLY: the X-Beacon-Demo-Actor header can \
         act as ANY person in the Directory. Do not expose this port beyond localhost."
    );
    let dir = state.dir.clone();
    let fallback = demo_actor.0.clone();
    let app = router(state).layer(axum::middleware::from_fn(
        move |req: Request, next: Next| {
            // Clone per request: the closure must stay `Fn`, the future owns its copies.
            let dir = dir.clone();
            let fallback = fallback.clone();
            async move {
                // `X-Beacon-Demo-Actor: user:<id>` names the acting person; unknown names are
                // rejected instead of silently falling back.
                let header = req
                    .headers()
                    .get("x-beacon-demo-actor")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                let actor = match header {
                    None => fallback,
                    Some(raw) => {
                        let id = raw.strip_prefix("user:").unwrap_or(&raw);
                        match dir.people.contains_key(id) {
                            true => format!("user:{id}"),
                            false => return unauthorized().into_response(),
                        }
                    }
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
