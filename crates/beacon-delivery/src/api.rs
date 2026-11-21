//! Delivery's HTTP API, mounted by beacon-api (board #172/#174), shapes and status codes from
//! ui/contract/openapi.yaml v0.2.0 (PR #10):
//!
//! | Route | Success | Errors |
//! |---|---|---|
//! | `POST /api/alerts/{id}/steps/{step}/run` `{expected_version?}` | `202 {execution_id}` | `403 forbidden` (actor not on the alert's team); `404 not_found`; `409 approval_required` (+`error.approval_id`); `409 invalid_transition` (alert resolved, step disabled/condition unmet, already queued/running, not an action step, or no one else can approve this step) |
//! | `POST /api/alerts/{id}/steps/{step}/check` `{done = true}` | `200 {}` | `403 forbidden` (team); `400 bad_request` (not a manual step); `404` |
//! | `POST /api/approvals/{id}/{approve\|reject}` `{note?}` (note <= 2000) | `200 {execution_id?}` | `403 forbidden` (not eligible, or the requester — nobody approves their own request); `404`; `409 invalid_transition` (already decided / expired / alert resolved) |
//! | `GET /api/approvals/email?a&p&e&s` (M7: the signed link an approval prompt email carries; `a` approval id, `p` person id, `e` unix expiry, `s` HMAC hex) | `200` HTML: the request's facts (action, step, alert title/id, requester, expiry, current state) and, while still pending, a POST form with hidden `a,p,e,s` and `decision=approve/reject` buttons. NEVER changes state (mail scanners prefetch links); every page is `text/html; charset=utf-8`, self-contained (no JS, no external assets), every value HTML-escaped, `Cache-Control: no-store` + `Referrer-Policy: no-referrer` | `404` HTML (no `EmailLinkConfig`, or unknown approval); `401` HTML (missing/bad params or bad signature — one generic page, nothing burned); `410` HTML (the link's `e`, or the approval's expiry, is in the past) |
//! | `POST /api/approvals/email` (`application/x-www-form-urlencoded` body `a,p,e,s,decision=approve\|reject`) — decides through the same `approvals::decide` path with actor `user:<p>` and `via = email` | `200` HTML (approved / rejected) | `404` HTML (no config / unknown approval); `401` HTML (bad params/signature); `410` HTML (expired); `403` HTML (the requester, or not eligible); `409` HTML (already decided: `<state>`, or the alert is resolved); `415` HTML (any other content type) |
//! | `GET /api/runbooks` | `200 {items}` (RunbookSummary, with `revision`) | |
//! | `GET /api/runbooks/{runbookId}` | `200 {id, revision, title, format, team, steps: [{id, kind, text, action: {name, label} \| null, approval}]}` (current library version, step templates unrendered) | `401`; `404 not_found` |
//! | `POST /api/runbooks/import` `{format, team, title, content}` (RunbookImport) | `200 {id, title, steps}` (RunbookPreview: id derived from the title, steps the parsed checklist as ImportedSteps) | `401`; `403 forbidden` (not a member or the lead of `team` — checked before the content is parsed, unknown teams authorize nobody); `415`; `422 invalid` (shape/limit failures, unparsable content, unknown action) |
//! | `POST /api/runbooks` `{...RunbookImport, ...RunbookPreview}` (publish a previewed import; the submitted, possibly edited `steps` are validated like a file's) | `201 RunbookSummary` (revision 1) | `401`; `403 forbidden`; `409 revision_conflict` (the id is already published or exists in config/runbooks); `415`; `422 invalid` (import or steps validation) |
//! | `GET /api/people/directory` | `200 {items}` of `PersonDirectoryEntry` (`{id, name, teams}`) | `401 unauthorized` |
//! | `GET /api/people` | `200 {items}` of `PersonView` | `401 unauthorized` |
//! | `GET /api/teams` | `200 {items}` of `TeamView` | `401 unauthorized` |
//! | `PUT /api/people/{personId}/preferences` `{preferences, expected_revision}` | `200 PersonView` (revision = the new one) | `401 unauthorized`; `403 forbidden` (not that person or an admin); `404 not_found`; `409 revision_conflict` (+`error.current`); `415`; `422 invalid` (preferences fail the YAML validation); `400 bad_request` (malformed body / non-integer `expected_revision`) |
//! | `PUT /api/teams/{teamId}` `{members?, lead?, oncall?, default_channels?, expected_revision}` | `200 TeamView` | `401`; `403 forbidden` (not the team's current lead or an admin); `404`; `409 revision_conflict` (+`error.current`); `415`; `422 invalid` (members/lead/on-call name unknown people, unknown channel); `400` |
//! | `POST /api/replies` `{from, text}` (fake-gateway callback: `X-Beacon-Timestamp: <unix seconds>` within ±300 s of now, `X-Beacon-Signature: hex(hmac_sha256(reply_secret, "<timestamp>." + raw_body))`) | `200 {outcome}`: `approved`/`rejected` (+`approval_id`; approved also `execution_id`), `acked` (+`alert_id`, the alert of the newest sms/phone job `sent` to the number), `already_decided` (a repeated decision, or a repeated `ACK` whose newest sent page was already acked — it never advances to an older open alert), `wrong_code`, `too_many_attempts`, `ambiguous` (+`message`: the code matches more than one pending approval — decide in the UI), `unknown` (+`message`) | `401 unauthorized` (missing/old/future timestamp, bad/missing signature) |
//!
//! Auth: beacon-api's session/CSRF middleware inserts `Extension(Actor("user:<id>"))` before
//! these handlers; without it every route except `/api/replies` and the two
//! `/api/approvals/email` routes returns `401 unauthorized`.
//! `/api/replies` is authenticated by its timestamped signature instead (the caller is the
//! gateway; a replay inside the 300 s window is harmless because decisions are idempotent).
//! The two email-approval routes ignore the session Actor ENTIRELY: the signed person `p`
//! is the actor (the HMAC in the URL/body is the authorization, not a cookie, so a
//! cross-site form post only ever decides what its author already holds — a validly
//! signed link — and CSRF protection is pointless there; this is also why they are the
//! only POST route that must NOT require JSON).
//! `run` and `check` additionally require the actor to be on the alert's team — a member, the
//! lead, or anyone in one of the team's on-call rotations (the EFFECTIVE directory, so a
//! saved team change applies) — else `403 forbidden`
//! (audit #11; `GET /api/runbooks` stays open to any authenticated user).
//! The three people/teams `GET`s are read-only views over the EFFECTIVE directory — the
//! startup YAML base with every saved override applied (`overrides`, table
//! `directory_overrides`, items sorted by `id`); each view's `revision` is the override's
//! revision, else the YAML base's constant `1`. The two `PUT`s write those overrides
//! (compare-and-set on `expected_revision`, the first save at revision 2) through the same
//! validator a YAML import uses (`overrides::apply` -> `Directory::from_parts`), and
//! `/api/people` redacts everyone except the actor to `contact: {}` and
//! `preferences: []`, so no other user's email, phone or preferences may leak.
//! The two runbook `GET`s read the effective library too (an imported runbook is listed
//! with no restart, its `revision` from its row), and the two runbook `POST`s are the
//! UI's Runbooks tab: the content goes through the same adapter a `config/runbooks`
//! file of that format takes, the preview id is derived from the title, and a publish
//! stores `kind = 'runbook'` rows of `directory_overrides` (revision 1, the actor in
//! `updated_by`, `overrides::apply_runbooks` rebuilds each stored body into the
//! effective library). Importing is for a member or the lead of the target team only
//! (the mock's rule, checked before the content is parsed). `Admin(bool)`
//! is inserted by the mounting app (beacon-api's
//! `Session.admin`; absent means not admin, and `serve_standalone` never inserts it) and
//! widens `can_edit` to every person and team — otherwise a person's `can_edit` is being
//! that person and a team's is being its lead. The same widening authorizes the two PUTs:
//! preferences are self-or-admin, a team is its-current-lead-or-admin (the CURRENT lead as
//! the effective directory has it, which prevents self-promotion by a plain member).
//! Every POST and PUT must say `Content-Type: application/json` (`415 unsupported_media_type`
//! otherwise — media type only, and even when the body is empty: an empty body WITH the
//! JSON content type is still accepted wherever the contract allows no body) — a
//! cross-site `<form>` can neither send the header nor smuggle an empty body past it.
//! Errors are `{error: {code, message, approval_id?, current?}}`; 500s are `internal` with
//! details only in logs. Every mutation writes `audit_log` and bumps the alert version
//! (`AlertOps::touch`) — the two config PUTs write `audit_log` too (actions
//! `person.preferences_updated` / `team.updated`) but touch no alert.
//!
//! `{expected_version?}` on the run route is accepted (shape-checked: an integer or null) but
//! not enforced — the route table above lists no version-conflict error for it. The config
//! PUTs DO enforce `expected_revision` (the contract's config-writes convention: a stale
//! revision is `409 revision_conflict` with `error.current`, the object as it is now).

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, RawQuery, Request, State};
use axum::http::StatusCode;
use axum::http::header;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, body::Bytes};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use uuid::Uuid;

use crate::actions::{self, ActionRegistry};
use crate::alert_ops::AlertOps;
use crate::approvals::{
    self, DecideError, DecideOutcome, Decision, ReplyOutcome, RequestError, Via,
};
use crate::audit;
use crate::config::{Directory, Person, Team};
use crate::email_link::{self, EmailLinkConfig};
use crate::overrides::{self, Overrides};
use crate::runbook::{RenderedStep, StepKind};

/// The authenticated user, `user:<id>`, inserted by the mounting app.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Actor(pub String);

/// Whether the session is an admin, inserted by the mounting app (beacon-api's
/// `Session.admin`); absent means not admin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Admin(pub bool);

#[derive(Clone)]
pub struct ApiState {
    pub pool: sqlx::PgPool,
    pub dir: Arc<Directory>,
    pub registry: ActionRegistry,
    pub alert_ops: Arc<dyn AlertOps>,
    /// HMAC key for `/api/replies` (env `REPLY_SECRET`); `None` rejects every reply.
    pub reply_secret: Option<Arc<Vec<u8>>>,
    /// M7 (SPEC §6.3): signing config for the approval links email prompts carry.
    /// `None` (the default) disables the `/api/approvals/email` routes (404) and the
    /// worker renders the prompt emails' link-less "Decide in Beacon" form.
    pub email_link: Option<EmailLinkConfig>,
}

/// The contract's error body.
#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
    pub approval_id: Option<String>,
    /// `error.current` on `revision_conflict`: the object as it is now (the contract's
    /// config-writes convention); never set for any other code.
    pub current: Option<Value>,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            approval_id: None,
            current: None,
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
        if let Some(current) = self.current {
            error["current"] = current;
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

/// `422 invalid`: a submitted config object failed the validation a YAML import goes
/// through (contract: validation failures are `invalid`).
fn invalid(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "invalid", message)
}

/// `409 revision_conflict`: someone else saved first (contract's config-writes
/// convention); the handler attaches `error.current`, the object as it is now.
fn revision_conflict(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::CONFLICT, "revision_conflict", message)
}

/// `415 unsupported_media_type`: POST bodies must be `application/json`.
fn unsupported_media_type() -> ApiError {
    ApiError::new(
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "unsupported_media_type",
        "Content-Type must be application/json",
    )
}

/// Audit #11 (CSRF): every POST and PUT must say `Content-Type: application/json` (media
/// type only — parameters like `charset` are ignored), even when its body is empty. A
/// cross-site `<form>` can at best send `text/plain` or form encodings and can never set
/// this header, so it cannot reach a handler — with a body or without one. An empty body
/// WITH the JSON content type still means "no body" wherever the contract allows one.
fn require_json_body(headers: &axum::http::HeaderMap) -> Result<(), ApiError> {
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
///
/// The router wraps `state.dir` into an [`overrides::EffectiveDirectory`] exactly once
/// and hands it to the handlers through an internal extension layer: `ApiState` stays
/// source-compatible (its `dir` remains the startup base for every existing constructor),
/// while every people/teams view, team-membership check and approvals decision below
/// reads the effective, override-applied directory instead — so a saved preference or
/// team change applies everywhere without rebuilding the state.
pub fn router(state: ApiState) -> Router {
    let effective = Arc::new(overrides::EffectiveDirectory::new(state.dir.clone()));
    Router::new()
        .route("/api/alerts/{id}/steps/{step}/run", post(run_step_handler))
        .route(
            "/api/alerts/{id}/steps/{step}/check",
            post(check_step_handler),
        )
        .route("/api/approvals/{id}/approve", post(approve_handler))
        .route("/api/approvals/{id}/reject", post(reject_handler))
        .route(
            "/api/approvals/email",
            get(email_approval_page).post(email_approval_decide),
        )
        .route(
            "/api/runbooks",
            get(runbooks_handler).post(publish_runbook_handler),
        )
        .route("/api/runbooks/{runbook_id}", get(runbook_detail_handler))
        .route("/api/runbooks/import", post(import_runbook_handler))
        .route("/api/people/directory", get(directory_handler))
        .route("/api/people", get(people_handler))
        .route("/api/teams", get(teams_handler))
        .route(
            "/api/people/{personId}/preferences",
            put(save_preferences_handler),
        )
        .route("/api/teams/{teamId}", put(save_team_handler))
        .route("/api/replies", post(replies_handler))
        .layer(axum::Extension(effective))
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
    axum::Extension(effective): axum::Extension<Arc<overrides::EffectiveDirectory>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let Some(axum::Extension(Actor(actor))) = actor else {
        return Err(unauthorized());
    };
    require_json_body(&headers)?;
    let alert_id = parse_id(&alert_raw)?;

    // The body is optional; `expected_version` is shape-checked but not enforced (module doc).
    let body = parse_body(&body)?;
    if let Some(v) = body.get("expected_version")
        && !(v.is_i64() || v.is_null())
    {
        return Err(bad_request("expected_version must be an integer"));
    }

    let (status, team, step) = load_step(&state, alert_id, &step_raw).await?;

    // The effective directory: a saved team change applies to this check too.
    let dir = effective.get(&state.pool).await;

    // Audit #11: only the alert's own team may touch its steps.
    if !on_team(&dir, &team, &actor) {
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
            // The effective directory: eligibility follows a saved team change.
            &dir,
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
    axum::Extension(effective): axum::Extension<Arc<overrides::EffectiveDirectory>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let Some(axum::Extension(Actor(actor))) = actor else {
        return Err(unauthorized());
    };
    require_json_body(&headers)?;
    let alert_id = parse_id(&alert_raw)?;

    let body = parse_body(&body)?;
    let done = match body.get("done") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(_) => return Err(bad_request("done must be a boolean")),
    };

    let (_, team, step) = load_step(&state, alert_id, &step_raw).await?;

    // The effective directory: a saved team change applies to this check too.
    let dir = effective.get(&state.pool).await;

    // Audit #11: only the alert's own team may touch its steps.
    if !on_team(&dir, &team, &actor) {
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
    require_json_body(&headers)?;
    decide_http(state, id_raw, actor, body, Decision::Approve).await
}

async fn reject_handler(
    state: State<ApiState>,
    Path(id_raw): Path<String>,
    headers: axum::http::HeaderMap,
    actor: Option<axum::Extension<Actor>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    require_json_body(&headers)?;
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
// GET/POST /api/approvals/email (M7: the signed link an approval email carries, SPEC §6.3)
// ---------------------------------------------------------------------------------------------

/// The approval (and its alert) as the email pages show it.
#[derive(sqlx::FromRow)]
struct EmailApprovalView {
    action: String,
    step_id: String,
    alert_id: Uuid,
    state: String,
    requested_by: String,
    expires_at: DateTime<Utc>,
    /// The alert's title; `NULL` only when the alerts row is gone.
    title: Option<String>,
}

/// Escape every value interpolated into an email page: the pages carry alert titles,
/// person ids and action names that nobody here controls, so text and double-quoted
/// attribute positions are both covered.
fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// One small self-contained HTML page: `text/html; charset=utf-8`, no JS, no external
/// assets. `no-store` because the query string carries the signature (it must not sit in
/// a shared cache), `no-referrer` so a followed link off the page cannot leak it either.
fn email_page(status: StatusCode, title: &str, body_html: &str) -> Response {
    let html = format!(
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
         <title>{}</title>\n</head>\n<body>\n{}\n</body>\n</html>\n",
        html_escape(title),
        body_html
    );
    (
        status,
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::REFERRER_POLICY, "no-referrer"),
        ],
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
    )
        .into_response()
}

/// The generic 401 every bad or forged link gets: one page, no detail about which field
/// failed — nothing is revealed, nothing burned (no `sms_attempts`, no state change).
fn email_link_rejected() -> Response {
    email_page(
        StatusCode::UNAUTHORIZED,
        "Invalid approval link",
        "<p>This approval link is not valid.</p>",
    )
}

fn email_link_expired() -> Response {
    email_page(
        StatusCode::GONE,
        "Link expired",
        "<p>This approval link has expired.</p>",
    )
}

/// Percent-decode one urlencoded component: `%XX` becomes that byte (an invalid escape
/// stays literal) and `+` a space (the form-encoding rule). Never fails; the result is
/// lossy UTF-8. The encoder's side is [`email_link::percent_encode`].
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let nibble = |b: u8| match b {
                    b'0'..=b'9' => Some(b - b'0'),
                    b'a'..=b'f' => Some(b - b'a' + 10),
                    b'A'..=b'F' => Some(b - b'A' + 10),
                    _ => None,
                };
                match (nibble(bytes[i + 1]), nibble(bytes[i + 2])) {
                    (Some(hi), Some(lo)) => {
                        out.push(hi * 16 + lo);
                        i += 3;
                    }
                    _ => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Parse `k=v&k=v` — a query string or an `application/x-www-form-urlencoded` body —
/// percent-decoding both sides. A pair without `=` decodes to an empty value; a repeated
/// key keeps its first value.
fn parse_urlencoded(raw: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for pair in raw.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        out.entry(percent_decode(key))
            .or_insert_with(|| percent_decode(value));
    }
    out
}

/// The signed link's `a`, `p`, `e`, `s` parsed and verified (constant time). `None` for
/// any missing or malformed field or a bad signature — both callers then answer the same
/// generic 401 and burn nothing. Returns `(approval id, person id, exp unix, sig)`.
fn verified_link_params(
    cfg: &EmailLinkConfig,
    params: &HashMap<String, String>,
) -> Option<(Uuid, String, i64, String)> {
    let approval_id: Uuid = params.get("a")?.parse().ok()?;
    let person = params.get("p")?;
    let exp_unix: i64 = params.get("e")?.parse().ok()?;
    let sig = params.get("s")?;
    if !email_link::verify(&cfg.secret, approval_id, person, exp_unix, sig) {
        return None;
    }
    Some((approval_id, person.clone(), exp_unix, sig.clone()))
}

/// `GET /api/approvals/email?a&p&e&s`: the page a mail client opens. NEVER changes state
/// (mail scanners prefetch links); it only reads. The signature is verified in constant
/// time, and a valid one still decides nothing — the form's POST does.
async fn email_approval_page(State(state): State<ApiState>, RawQuery(raw): RawQuery) -> Response {
    let Some(cfg) = state.email_link.as_ref() else {
        return email_page(
            StatusCode::NOT_FOUND,
            "Email approvals are not enabled",
            "<p>Email approvals are not enabled on this Beacon installation.</p>",
        );
    };
    let query = raw.as_deref().map(parse_urlencoded).unwrap_or_default();
    let Some((approval_id, person, exp_unix, sig)) = verified_link_params(cfg, &query) else {
        return email_link_rejected();
    };
    if exp_unix <= Utc::now().timestamp() {
        return email_link_expired();
    }
    let row: Result<Option<EmailApprovalView>, sqlx::Error> =
        sqlx::query_as::<_, EmailApprovalView>(
            "SELECT ap.action, ap.step_id, ap.alert_id, ap.state, ap.requested_by, \
                    ap.expires_at, al.title \
             FROM approvals ap LEFT JOIN alerts al ON al.id = ap.alert_id \
             WHERE ap.id = $1",
        )
        .bind(approval_id)
        .fetch_optional(&state.pool)
        .await;
    let view = match row {
        Ok(view) => view,
        Err(e) => {
            tracing::error!(error = %e, "email approval page load failed");
            return email_page(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal error",
                "<p>Something went wrong. Try the Beacon UI.</p>",
            );
        }
    };
    let Some(view) = view else {
        return email_page(
            StatusCode::NOT_FOUND,
            "Approval not found",
            "<p>This approval no longer exists.</p>",
        );
    };
    // A pending row whose expiry has already lapsed (the sweeper has not marked it yet)
    // answers like the decide path would: expired, with no form to press.
    if view.state == "pending" && view.expires_at <= Utc::now() {
        return email_link_expired();
    }
    let expires_at = view.expires_at.to_rfc3339();
    let mut body_html = format!(
        "<h1>Approval request</h1>\n\
         <p>Action: <strong>{action}</strong> (step {step_id})</p>\n\
         <p>Alert: {title} ({alert_id})</p>\n\
         <p>Requested by: {requested_by}</p>\n\
         <p>Expiry: {expires_at}</p>\n\
         <p>Current state: {state}</p>\n",
        action = html_escape(&view.action),
        step_id = html_escape(&view.step_id),
        title = html_escape(view.title.as_deref().unwrap_or("(alert removed)")),
        alert_id = html_escape(&view.alert_id.to_string()),
        requested_by = html_escape(&view.requested_by),
        expires_at = html_escape(&expires_at),
        state = html_escape(&view.state),
    );
    if view.state == "pending" {
        // The form re-carries the link's own parameters as hidden fields; only the
        // pressed `decision` button is new.
        body_html.push_str(&format!(
            "<form method=\"post\" action=\"/api/approvals/email\">\n\
             <input type=\"hidden\" name=\"a\" value=\"{a}\">\n\
             <input type=\"hidden\" name=\"p\" value=\"{p}\">\n\
             <input type=\"hidden\" name=\"e\" value=\"{e}\">\n\
             <input type=\"hidden\" name=\"s\" value=\"{s}\">\n\
             <button type=\"submit\" name=\"decision\" value=\"approve\">Approve</button>\n\
             <button type=\"submit\" name=\"decision\" value=\"reject\">Reject</button>\n\
             </form>\n",
            a = html_escape(&approval_id.to_string()),
            p = html_escape(&person),
            e = html_escape(&exp_unix.to_string()),
            s = html_escape(&sig),
        ));
    }
    email_page(StatusCode::OK, "Approval request", &body_html)
}

/// `POST /api/approvals/email` with an `application/x-www-form-urlencoded` body
/// `a,p,e,s,decision=approve|reject`: verified exactly like the GET, then decided
/// through the same `approvals::decide` path as the UI button, with the SIGNED person as
/// the actor (`user:<p>`, `via = email`) — the session `Actor`, if the mounting app
/// inserted one, is ignored entirely. Outcomes are HTML pages, never JSON.
async fn email_approval_decide(
    State(state): State<ApiState>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let Some(cfg) = state.email_link.as_ref() else {
        return email_page(
            StatusCode::NOT_FOUND,
            "Email approvals are not enabled",
            "<p>Email approvals are not enabled on this Beacon installation.</p>",
        );
    };
    // The HMAC in the body is the authorization, not a cookie: a cross-site form can
    // only re-post a link its author already holds, so the JSON content-type guard is
    // deliberately NOT applied here (this is the one POST that takes a form). Anything
    // but the form encoding is still refused.
    let media_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if media_type != "application/x-www-form-urlencoded" {
        return email_page(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Unsupported media type",
            "<p>This route accepts application/x-www-form-urlencoded bodies only.</p>",
        );
    }
    // Body size: axum's default body limit guards this extraction.
    let form = parse_urlencoded(&String::from_utf8_lossy(&body));
    let Some((approval_id, person, exp_unix, _sig)) = verified_link_params(cfg, &form) else {
        return email_link_rejected();
    };
    if exp_unix <= Utc::now().timestamp() {
        return email_link_expired();
    }
    let decision = match form.get("decision").map(String::as_str) {
        Some("approve") => Decision::Approve,
        Some("reject") => Decision::Reject,
        _ => {
            return email_page(
                StatusCode::BAD_REQUEST,
                "Unknown decision",
                "<p>The decision must be approve or reject.</p>",
            );
        }
    };
    let actor = format!("user:{person}");
    match approvals::decide(
        &state.pool,
        state.alert_ops.as_ref(),
        approval_id,
        &actor,
        decision,
        Via::Email,
        None,
    )
    .await
    {
        Ok(DecideOutcome::Approved { execution_id }) => email_page(
            StatusCode::OK,
            "Approved",
            &format!(
                "<p>The request was approved.</p>\n<p>Execution: {}</p>\n",
                html_escape(&execution_id.to_string())
            ),
        ),
        Ok(DecideOutcome::Rejected) => email_page(
            StatusCode::OK,
            "Rejected",
            "<p>The request was rejected.</p>\n",
        ),
        Err(e) => {
            tracing::warn!(error = %e, approval_id = %approval_id, "email link decide failed");
            match e {
                DecideError::Forbidden(message) => email_page(
                    StatusCode::FORBIDDEN,
                    "Forbidden",
                    &format!("<p>{}</p>\n", html_escape(&message)),
                ),
                DecideError::NotFound => email_page(
                    StatusCode::NOT_FOUND,
                    "Approval not found",
                    "<p>This approval no longer exists.</p>",
                ),
                DecideError::AlreadyDecided(state_str) => email_page(
                    StatusCode::CONFLICT,
                    "Already decided",
                    &format!(
                        "<p>This approval was already decided: {}.</p>\n",
                        html_escape(&state_str)
                    ),
                ),
                DecideError::Expired => email_link_expired(),
                DecideError::InvalidTransition(message) => email_page(
                    StatusCode::CONFLICT,
                    "Cannot decide",
                    &format!("<p>{}</p>\n", html_escape(&message)),
                ),
                DecideError::Db(_) => email_page(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal error",
                    "<p>Something went wrong. Try the Beacon UI.</p>",
                ),
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// GET /api/runbooks, GET /api/runbooks/{runbookId}, POST /api/runbooks[/import]
// ---------------------------------------------------------------------------------------------

/// `GET /api/runbooks`: the effective library (a runbook imported at runtime is listed
/// with no restart). An imported runbook carries its row's revision, a file one its
/// content hash. Same access as before (any session).
async fn runbooks_handler(
    State(state): State<ApiState>,
    axum::Extension(effective): axum::Extension<Arc<overrides::EffectiveDirectory>>,
    actor: Option<axum::Extension<Actor>>,
) -> Result<Response, ApiError> {
    let _actor = actor.map(|a| a.0).ok_or_else(unauthorized)?;
    let dir = effective.get(&state.pool).await;
    let items = crate::runbook::list_items(&dir.runbooks);
    Ok((StatusCode::OK, Json(json!({ "items": items }))).into_response())
}

/// `GET /api/runbooks/{runbookId}`: the current effective version of one runbook with
/// its step templates ([`crate::runbook::detail_item`]); same access as the list (any
/// session), `404 not_found` for an unknown id.
async fn runbook_detail_handler(
    State(state): State<ApiState>,
    Path(runbook_id): Path<String>,
    axum::Extension(effective): axum::Extension<Arc<overrides::EffectiveDirectory>>,
    actor: Option<axum::Extension<Actor>>,
) -> Result<Response, ApiError> {
    let _actor = actor.map(|a| a.0).ok_or_else(unauthorized)?;
    let dir = effective.get(&state.pool).await;
    let item = crate::runbook::detail_item(&dir.runbooks, &runbook_id)
        .ok_or_else(|| not_found(format!("unknown runbook {runbook_id:?}")))?;
    Ok((StatusCode::OK, Json(item)).into_response())
}

/// The import routes' team authorization (the mock's rule): a runbook may be imported
/// for a team only by one of its members or its lead — an unknown team authorizes
/// nobody. On-call alone does not grant importing (it grants running steps).
fn member_or_lead(dir: &Directory, team_id: &str, actor: &str) -> bool {
    let Some(person) = actor.strip_prefix("user:") else {
        return false;
    };
    dir.teams
        .get(team_id)
        .is_some_and(|t| t.members.iter().any(|m| m == person) || t.lead.as_deref() == Some(person))
}

/// The checks both import routes share: the mock's team authorization BEFORE anything is
/// parsed (a named team must be the actor's own, so a foreign team is a 403 whatever
/// the content says), then the `RunbookImport` validation and the content parse.
async fn validated_import(
    state: &ApiState,
    effective: &Arc<overrides::EffectiveDirectory>,
    actor: &str,
    body: &Value,
) -> Result<crate::runbook::ImportInput, ApiError> {
    if let Some(team) = body.get("team").and_then(Value::as_str).map(str::trim)
        && !team.is_empty()
    {
        // The effective directory: a saved team change applies to this check too.
        let dir = effective.get(&state.pool).await;
        if !member_or_lead(&dir, team, actor) {
            return Err(forbidden(
                "only a member or the lead of the team may import a runbook for it",
            ));
        }
    }
    crate::runbook::validate_import(body).map_err(invalid)
}

/// `POST /api/runbooks/import` (contract `previewRunbookImport`): validate the
/// `RunbookImport`, parse the content with the same adapter a `config/runbooks` file of
/// that format takes, answer the derived preview `{id, title, steps}`. Nothing is
/// stored — the UI shows the checklist for editing, then publishes it.
async fn import_runbook_handler(
    State(state): State<ApiState>,
    headers: axum::http::HeaderMap,
    axum::Extension(effective): axum::Extension<Arc<overrides::EffectiveDirectory>>,
    actor: Option<axum::Extension<Actor>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let Some(axum::Extension(Actor(actor))) = actor else {
        return Err(unauthorized());
    };
    require_json_body(&headers)?;
    let body = parse_body(&body)?;
    let input = validated_import(&state, &effective, &actor, &body).await?;
    Ok((
        StatusCode::OK,
        Json(json!({
            "id": input.id,
            "title": input.title,
            "steps": crate::runbook::preview_steps(&input.runbook),
        })),
    )
        .into_response())
}

/// `POST /api/runbooks` (contract `publishRunbook`): publish a previewed import. The
/// submitted (possibly edited) `steps` are validated like a file's
/// (`runbook::imported_steps`) and stored with the import as a `kind = 'runbook'` row
/// of `directory_overrides` (revision 1, the actor in `updated_by`); the effective
/// directory rebuilds when the table's stamp moves, so the router, scheduler, actions
/// worker and this API see the new runbook with no restart. An id that exists in the
/// YAML base, or a row that was already published, is a `409 revision_conflict` (a
/// runbook is published once; there is no update route). `201` answers the contract's
/// `RunbookSummary`; the stored runbook is what the preview steps describe.
async fn publish_runbook_handler(
    State(state): State<ApiState>,
    headers: axum::http::HeaderMap,
    axum::Extension(effective): axum::Extension<Arc<overrides::EffectiveDirectory>>,
    actor: Option<axum::Extension<Actor>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let Some(axum::Extension(Actor(actor))) = actor else {
        return Err(unauthorized());
    };
    require_json_body(&headers)?;
    let body = parse_body(&body)?;
    let input = validated_import(&state, &effective, &actor, &body).await?;
    let steps = crate::runbook::imported_steps(body.get("steps"), input.format).map_err(invalid)?;

    if state.dir.runbooks.get(&input.id).is_some() {
        return Err(revision_conflict(format!(
            "runbook {:?} already exists in config/runbooks",
            input.id
        )));
    }
    let stored = json!({
        "format": input.format.as_str(),
        "team": input.team,
        "title": input.title,
        "content": input.content,
        "steps": body.get("steps").cloned().unwrap_or(Value::Null),
    });

    let mut tx = state.pool.begin().await.map_err(ApiError::internal)?;
    let inserted = sqlx::query(
        "INSERT INTO directory_overrides (kind, id, body, revision, updated_by) \
         VALUES ($1, $2, $3, 1, $4) \
         ON CONFLICT (kind, id) DO NOTHING",
    )
    .bind(overrides::KIND_RUNBOOK)
    .bind(&input.id)
    .bind(&stored)
    .bind(&actor)
    .execute(&mut *tx)
    .await
    .map_err(ApiError::internal)?;
    if inserted.rows_affected() == 0 {
        let _ = tx.rollback().await;
        return Err(revision_conflict(format!(
            "runbook {:?} is already published (revision 1)",
            input.id
        )));
    }
    audit::record(
        &mut tx,
        &actor,
        "ui",
        None,
        "runbook.published",
        &format!("published runbook {} (revision 1)", input.id),
        json!({
            "runbook_id": input.id,
            "team": input.team,
            "format": input.format.as_str(),
            "revision": 1,
        }),
    )
    .await
    .map_err(ApiError::internal)?;
    tx.commit().await.map_err(ApiError::internal)?;

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "id": input.id,
            "revision": 1,
            "title": input.title,
            "format": input.format.as_str(),
            "team": input.team,
            "steps": steps.len(),
        })),
    )
        .into_response())
}

// ---------------------------------------------------------------------------------------------
// GET /api/people/directory, /api/people, /api/teams (effective Directory views)
// ---------------------------------------------------------------------------------------------

/// The id title-cased (first char uppercased): `bob` -> `Bob`,
/// `counterparty-support` -> `Counterparty-support`.
fn title_cased(id: &str) -> String {
    let mut chars = id.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// The sorted ids of the teams where `person` is a member or the lead.
fn teams_of(dir: &Directory, person: &str) -> Vec<String> {
    let mut ids: Vec<String> = dir
        .teams
        .values()
        .filter(|t| t.members.iter().any(|m| m == person) || t.lead.as_deref() == Some(person))
        .map(|t| t.id.clone())
        .collect();
    ids.sort();
    ids
}

/// `PersonDirectoryEntry` (contract): `{id, name, teams}` — a minimal recipient-picker
/// entry, no `contact`, no `preferences` keys at all.
#[derive(Serialize)]
struct PersonDirectoryEntry {
    id: String,
    /// The configured name, else the title-cased id.
    name: String,
    teams: Vec<String>,
}

/// `PersonView.contact`: only the keys that are `Some` (never `null`); both absent
/// serializes to `{}` — also the redacted shape for other users.
#[derive(Serialize)]
struct ContactView {
    #[serde(skip_serializing_if = "Option::is_none")]
    email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    phone: Option<String>,
}

/// `PersonView` (contract).
#[derive(Serialize)]
struct PersonView {
    id: String,
    /// The configured name, else the title-cased id.
    name: String,
    contact: ContactView,
    teams: Vec<String>,
    /// The person's raw `preferences` value if it is a JSON array, else `[]` — `[]`
    /// unless `can_edit` (redacted like `contact`).
    preferences: Vec<Value>,
    revision: i64,
    can_edit: bool,
}

/// `TeamView` (contract).
#[derive(Serialize)]
struct TeamView {
    id: String,
    /// Always the title-cased id (team YAML has no name).
    name: String,
    members: Vec<String>,
    lead: Option<String>,
    /// The schedule -> rotation map as configured; `{}` when empty.
    oncall: HashMap<String, Vec<String>>,
    /// The configured list, or `[]` when unset.
    default_channels: Vec<String>,
    revision: i64,
    can_edit: bool,
}

/// `PersonView` (contract): how `me` (an admin or not) sees `p`. `revision` comes from
/// `ov` — the override's, else the YAML base's `1`.
fn person_view(
    dir: &Directory,
    ov: &Overrides,
    p: &Person,
    me: &str,
    is_admin: bool,
) -> PersonView {
    let can_edit = p.id == me || is_admin;
    PersonView {
        id: p.id.clone(),
        name: p.name.clone().unwrap_or_else(|| title_cased(&p.id)),
        contact: if can_edit {
            ContactView {
                email: p.contact.email.clone(),
                phone: p.contact.phone.clone(),
            }
        } else {
            // Redacted like the contract asks: no other user's email or phone may leak.
            ContactView {
                email: None,
                phone: None,
            }
        },
        teams: teams_of(dir, &p.id),
        preferences: match (&p.preferences, can_edit) {
            (Some(Value::Array(raw)), true) => raw.clone(),
            _ => Vec::new(),
        },
        revision: overrides::revision_of(ov, overrides::KIND_PERSON, &p.id),
        can_edit,
    }
}

/// `TeamView` (contract): how `me` (an admin or not) sees `t`.
fn team_view(ov: &Overrides, t: &Team, me: &str, is_admin: bool) -> TeamView {
    TeamView {
        id: t.id.clone(),
        name: title_cased(&t.id),
        members: t.members.clone(),
        lead: t.lead.clone(),
        oncall: t.oncall.clone(),
        default_channels: t.default_channels.clone().unwrap_or_default(),
        revision: overrides::revision_of(ov, overrides::KIND_TEAM, &t.id),
        can_edit: t.lead.as_deref() == Some(me) || is_admin,
    }
}

/// `GET /api/people/directory`: every person as `{id, name, teams}`, sorted by id.
async fn directory_handler(
    State(state): State<ApiState>,
    axum::Extension(effective): axum::Extension<Arc<overrides::EffectiveDirectory>>,
    actor: Option<axum::Extension<Actor>>,
) -> Result<Response, ApiError> {
    let _actor = actor.map(|a| a.0).ok_or_else(unauthorized)?;
    let dir = effective.get(&state.pool).await;
    let mut people: Vec<&Person> = dir.people.values().collect();
    people.sort_by(|a, b| a.id.cmp(&b.id));
    let items: Vec<PersonDirectoryEntry> = people
        .into_iter()
        .map(|p| PersonDirectoryEntry {
            id: p.id.clone(),
            name: p.name.clone().unwrap_or_else(|| title_cased(&p.id)),
            teams: teams_of(&dir, &p.id),
        })
        .collect();
    Ok((StatusCode::OK, Json(json!({ "items": items }))).into_response())
}

/// `GET /api/people`: every person as `PersonView`, sorted by id. The actor (and an
/// admin) sees their own contact details and raw preferences; everyone else is redacted
/// to `contact: {}` and `preferences: []` — no other user's email/phone may leak.
async fn people_handler(
    State(state): State<ApiState>,
    axum::Extension(effective): axum::Extension<Arc<overrides::EffectiveDirectory>>,
    actor: Option<axum::Extension<Actor>>,
    admin: Option<axum::Extension<Admin>>,
) -> Result<Response, ApiError> {
    let actor = actor
        .map(|axum::Extension(Actor(a))| a)
        .ok_or_else(unauthorized)?;
    let me = actor.strip_prefix("user:").unwrap_or(actor.as_str());
    let is_admin = admin.is_some_and(|axum::Extension(Admin(a))| a);
    let snap = effective.snapshot(&state.pool).await;
    let (dir, ov) = (&snap.dir, &snap.overrides);
    let mut people: Vec<&Person> = dir.people.values().collect();
    people.sort_by(|a, b| a.id.cmp(&b.id));
    let items: Vec<PersonView> = people
        .into_iter()
        .map(|p| person_view(dir, ov, p, me, is_admin))
        .collect();
    Ok((StatusCode::OK, Json(json!({ "items": items }))).into_response())
}

/// `GET /api/teams`: every team as `TeamView`, sorted by id, as the effective directory
/// has it — `lead` is `null` when unset, `oncall` the schedule -> rotation map (`{}` when
/// empty) and `default_channels` the configured list (`[]` when unset). `can_edit` is
/// the team's lead or an admin.
async fn teams_handler(
    State(state): State<ApiState>,
    axum::Extension(effective): axum::Extension<Arc<overrides::EffectiveDirectory>>,
    actor: Option<axum::Extension<Actor>>,
    admin: Option<axum::Extension<Admin>>,
) -> Result<Response, ApiError> {
    let actor = actor
        .map(|axum::Extension(Actor(a))| a)
        .ok_or_else(unauthorized)?;
    let me = actor.strip_prefix("user:").unwrap_or(actor.as_str());
    let is_admin = admin.is_some_and(|axum::Extension(Admin(a))| a);
    let snap = effective.snapshot(&state.pool).await;
    let (dir, ov) = (&snap.dir, &snap.overrides);
    let mut teams: Vec<&Team> = dir.teams.values().collect();
    teams.sort_by(|a, b| a.id.cmp(&b.id));
    let items: Vec<TeamView> = teams
        .into_iter()
        .map(|t| team_view(ov, t, me, is_admin))
        .collect();
    Ok((StatusCode::OK, Json(json!({ "items": items }))).into_response())
}

// ---------------------------------------------------------------------------------------------
// PUT /api/people/{personId}/preferences, PUT /api/teams/{teamId} (saved overrides)
// ---------------------------------------------------------------------------------------------

/// The compare-and-set write for one override row, in ONE statement. The insert is
/// attempted only when it could possibly win: `expected_revision == 1` (the YAML
/// base — the insert then writes revision 2 and `ON CONFLICT` makes it fail against a
/// row that already exists, since the stored revision is always >= 2), or a row
/// already at the expected revision (the `EXISTS` guard). Attempting the insert is what
/// makes the `DO UPDATE` branch fire; its `WHERE directory_overrides.revision = $4`
/// then requires the CURRENT revision to match. Zero rows returned means the expected
/// revision was stale -> 409.
const CAS_OVERRIDE_SQL: &str = "\
    INSERT INTO directory_overrides (kind, id, body, revision, updated_by) \
    SELECT $1, $2, $3, $4 + 1, $5 \
    WHERE $4 = 1 \
       OR EXISTS (SELECT 1 FROM directory_overrides \
                  WHERE kind = $1 AND id = $2 AND revision = $4) \
    ON CONFLICT (kind, id) DO UPDATE SET \
        body = $3, \
        revision = directory_overrides.revision + 1, \
        updated_by = $5, \
        updated_at = now() \
    WHERE directory_overrides.revision = $4 \
    RETURNING revision";

/// Run [`CAS_OVERRIDE_SQL`]; `None` = the expected revision is stale (-> 409).
async fn cas_override(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    kind: &str,
    id: &str,
    body: &Value,
    expected: i64,
    actor: &str,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar(CAS_OVERRIDE_SQL)
        .bind(kind)
        .bind(id)
        .bind(body)
        .bind(expected)
        .bind(actor)
        .fetch_optional(&mut **tx)
        .await
}

/// The body's `expected_revision`: required, an integer revision (`>= 1`, the YAML base
/// is 1; below `i64::MAX` so the CAS's `+ 1` cannot overflow). Anything else is a
/// `400 bad_request`, like the run route's shape checks.
fn expected_revision(body: &Value) -> Result<i64, ApiError> {
    body.get("expected_revision")
        .and_then(Value::as_i64)
        .filter(|r| (1..i64::MAX).contains(r))
        .ok_or_else(|| bad_request("expected_revision must be a positive integer revision"))
}

/// A JSON array of strings, else `422 invalid`.
fn string_list(v: &Value, what: &str) -> Result<Vec<String>, ApiError> {
    let list = v
        .as_array()
        .ok_or_else(|| invalid(format!("{what} must be an array of strings")))?;
    list.iter()
        .map(|item| {
            item.as_str()
                .map(str::to_string)
                .ok_or_else(|| invalid(format!("{what} entries must be strings")))
        })
        .collect()
}

/// `PUT /api/people/{personId}/preferences` `{preferences, expected_revision}`
/// (contract `savePreferences`): self or admin only; the preferences go through the same
/// parsing and validation a YAML import takes (`overrides::apply` ->
/// `Directory::from_parts`, SPEC §4.6 one validator); the row is written with a
/// compare-and-set on the revision; every save is audited. `200` answers with the
/// updated `PersonView` as the actor sees it.
async fn save_preferences_handler(
    State(state): State<ApiState>,
    Path(person_raw): Path<String>,
    headers: axum::http::HeaderMap,
    axum::Extension(effective): axum::Extension<Arc<overrides::EffectiveDirectory>>,
    actor: Option<axum::Extension<Actor>>,
    admin: Option<axum::Extension<Admin>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let Some(axum::Extension(Actor(actor))) = actor else {
        return Err(unauthorized());
    };
    require_json_body(&headers)?;
    let me = actor.strip_prefix("user:").unwrap_or(actor.as_str());
    let is_admin = admin.is_some_and(|axum::Extension(Admin(a))| a);

    let snap = effective.snapshot(&state.pool).await;
    let Some(person) = snap.dir.people.get(&person_raw) else {
        return Err(not_found(format!("unknown person {person_raw:?}")));
    };
    if me != person.id && !is_admin {
        return Err(forbidden(
            "only the person themself or an admin may save these preferences",
        ));
    }

    let body = parse_body(&body)?;
    let preferences = body.get("preferences").cloned().unwrap_or(Value::Null);
    let expected = expected_revision(&body)?;

    // The candidate directory after this save: the applied overrides plus this body,
    // over the base through the one YAML validator. A bad `preferences` value (not a
    // list, unknown channel, bad window, ...) is a 422 naming the person. Validating on
    // top of the APPLIED set means another stale override cannot block this save.
    let stored = json!({ "preferences": preferences });
    let mut candidate = (*snap.applied).clone();
    candidate.people.insert(
        person_raw.clone(),
        overrides::PersonOverride {
            body: stored.clone(),
            // On a successful save the new revision is always `expected + 1` (the
            // insert writes 2, the update bumps the matched row), so the response view
            // built from this candidate carries the new revision.
            revision: expected + 1,
        },
    );
    let new_dir = overrides::apply(&state.dir, &candidate).map_err(|e| invalid(e.to_string()))?;

    let mut tx = state.pool.begin().await.map_err(ApiError::internal)?;
    let Some(new_revision) = cas_override(
        &mut tx,
        overrides::KIND_PERSON,
        &person_raw,
        &stored,
        expected,
        &actor,
    )
    .await
    .map_err(ApiError::internal)?
    else {
        let _ = tx.rollback().await;
        let mut err = revision_conflict(format!(
            "person {person_raw} is not at revision {expected} any more"
        ));
        // `current` from a fresh snapshot: the concurrent save moved the stamp, so this
        // reloads the stored rows and carries the revision the database has now.
        let now = effective.snapshot(&state.pool).await;
        if let Some(p) = now.dir.people.get(&person_raw) {
            err.current = Some(
                serde_json::to_value(person_view(&now.dir, &now.overrides, p, me, is_admin))
                    .map_err(ApiError::internal)?,
            );
        }
        return Err(err);
    };
    audit::record(
        &mut tx,
        &actor,
        "ui",
        None,
        "person.preferences_updated",
        &format!("saved preferences for {person_raw} (revision {new_revision})"),
        json!({ "person_id": person_raw, "revision": new_revision }),
    )
    .await
    .map_err(ApiError::internal)?;
    tx.commit().await.map_err(ApiError::internal)?;

    let person = new_dir
        .people
        .get(&person_raw)
        .ok_or_else(|| ApiError::internal("the candidate directory lost the person"))?;
    let view = person_view(&new_dir, &candidate, person, me, is_admin);
    Ok((StatusCode::OK, Json(view)).into_response())
}

/// `PUT /api/teams/{teamId}` `{members?, lead?, oncall?, default_channels?,
/// expected_revision}` (contract `saveTeam`): the team's CURRENT lead (as the effective
/// directory has it) or an admin only — a plain member cannot promote themself.
/// Omitted fields keep their current effective value; `lead: null` clears the lead; the
/// stored body is the full effective team after the save, validated through the one
/// YAML validator (unknown people and channels are a 422). Every save is audited;
/// `200` answers with the updated `TeamView`.
async fn save_team_handler(
    State(state): State<ApiState>,
    Path(team_raw): Path<String>,
    headers: axum::http::HeaderMap,
    axum::Extension(effective): axum::Extension<Arc<overrides::EffectiveDirectory>>,
    actor: Option<axum::Extension<Actor>>,
    admin: Option<axum::Extension<Admin>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let Some(axum::Extension(Actor(actor))) = actor else {
        return Err(unauthorized());
    };
    require_json_body(&headers)?;
    let me = actor.strip_prefix("user:").unwrap_or(actor.as_str());
    let is_admin = admin.is_some_and(|axum::Extension(Admin(a))| a);

    let snap = effective.snapshot(&state.pool).await;
    let Some(team) = snap.dir.teams.get(&team_raw) else {
        return Err(not_found(format!("unknown team {team_raw:?}")));
    };
    if team.lead.as_deref() != Some(me) && !is_admin {
        return Err(forbidden("only the team's lead or an admin may save it"));
    }

    let body = parse_body(&body)?;
    let expected = expected_revision(&body)?;

    // Omitted fields keep the current effective value; `lead: null` clears it.
    let mut new_team = team.clone();
    if let Some(members) = body.get("members") {
        new_team.members = string_list(members, "members")?;
    }
    match body.get("lead") {
        None => {}
        Some(Value::Null) => new_team.lead = None,
        Some(Value::String(lead)) => new_team.lead = Some(lead.clone()),
        Some(_) => return Err(invalid("lead must be a person id or null")),
    }
    if let Some(oncall) = body.get("oncall") {
        let rotations = oncall
            .as_object()
            .ok_or_else(|| invalid("oncall must be an object"))?;
        let mut schedule = HashMap::new();
        for (id, rotation) in rotations {
            schedule.insert(id.clone(), string_list(rotation, "oncall rotation")?);
        }
        new_team.oncall = schedule;
    }
    if let Some(defaults) = body.get("default_channels") {
        let list = string_list(defaults, "default_channels")?;
        // Same rule as the YAML loader: an explicit default must name a channel.
        if list.is_empty() {
            return Err(invalid("default_channels must not be empty"));
        }
        new_team.default_channels = Some(list);
    }

    // The full effective team after the save is what gets stored and re-applied; a body
    // naming unknown people or channels is a 422 from the shared validator.
    let stored = json!({
        "members": new_team.members,
        "lead": new_team.lead,
        "oncall": new_team.oncall,
        "default_channels": new_team.default_channels.clone().unwrap_or_default(),
    });
    let mut candidate = (*snap.applied).clone();
    candidate.teams.insert(
        team_raw.clone(),
        overrides::TeamOverride {
            body: stored.clone(),
            // On a successful save the new revision is always `expected + 1`.
            revision: expected + 1,
        },
    );
    let new_dir = overrides::apply(&state.dir, &candidate).map_err(|e| invalid(e.to_string()))?;

    let mut tx = state.pool.begin().await.map_err(ApiError::internal)?;
    let Some(new_revision) = cas_override(
        &mut tx,
        overrides::KIND_TEAM,
        &team_raw,
        &stored,
        expected,
        &actor,
    )
    .await
    .map_err(ApiError::internal)?
    else {
        let _ = tx.rollback().await;
        let mut err = revision_conflict(format!(
            "team {team_raw} is not at revision {expected} any more"
        ));
        // `current` from a fresh snapshot (see the person save).
        let now = effective.snapshot(&state.pool).await;
        if let Some(t) = now.dir.teams.get(&team_raw) {
            err.current = Some(
                serde_json::to_value(team_view(&now.overrides, t, me, is_admin))
                    .map_err(ApiError::internal)?,
            );
        }
        return Err(err);
    };
    audit::record(
        &mut tx,
        &actor,
        "ui",
        None,
        "team.updated",
        &format!("saved team {team_raw} (revision {new_revision})"),
        json!({ "team_id": team_raw, "revision": new_revision }),
    )
    .await
    .map_err(ApiError::internal)?;
    tx.commit().await.map_err(ApiError::internal)?;

    let team = new_dir
        .teams
        .get(&team_raw)
        .ok_or_else(|| ApiError::internal("the candidate directory lost the team"))?;
    let view = team_view(&candidate, team, me, is_admin);
    Ok((StatusCode::OK, Json(view)).into_response())
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
    axum::Extension(effective): axum::Extension<Arc<overrides::EffectiveDirectory>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    // A cross-site form can never satisfy this (audit #11), and the signature below
    // authenticates the caller (the gateway), not a session: no Actor here.
    require_json_body(&headers)?;
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

    // The effective directory: a saved team change applies to the approval decisions a
    // reply can drive. A database that is away serves the last good directory.
    let dir = effective.get(&state.pool).await;
    let outcome = approvals::handle_reply(
        &state.pool,
        &dir,
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
