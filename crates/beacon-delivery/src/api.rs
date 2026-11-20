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

use std::sync::Arc;

use axum::Router;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use crate::actions::ActionRegistry;
use crate::alert_ops::AlertOps;
use crate::config::Directory;

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

/// All delivery API routes (table above).
pub fn router(state: ApiState) -> Router {
    let _ = state;
    todo!(
        "agent C: routes + handlers for run/check/approvals/replies; GET /api/runbooks uses runbook::list_items"
    )
}

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
    let _ = (state, addr, demo_actor, shutdown);
    todo!("agent C")
}
