//! "Suggest change" proposals (SPEC §6.5, contract v0.2.0).
//!
//! ```text
//! GET  /api/proposals?team=&state=        newest first, scoped to visible teams
//! POST /api/proposals                     {rule_id, base_revision, kind, summary, note?, proposed_rule, alert_id?} → 201
//! GET  /api/proposals/count?team=         {pending}
//! POST /api/proposals/{id}/accept|reject  owning team (or admin); accept publishes
//!                                         proposed_rule against base_revision, and a
//!                                         moved rule makes it 409 + state=stale
//! ```

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use beacon_engine::config_store::{self, WriteError};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::Row;
use uuid::Uuid;

use crate::ApiState;
use crate::error::ApiError;

const KINDS: &[&str] = &["noisy", "reroute", "template", "other"];
const STATES: &[&str] = &["pending", "accepted", "rejected", "stale"];
const MAX_BODY: usize = 128 * 1024;

fn ts(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn param(raw: Option<&str>, name: &str) -> Option<String> {
    form_urlencoded::parse(raw.unwrap_or("").as_bytes()).find(|(k, _)| k == name).map(|(_, v)| v.into_owned())
}

const COLUMNS: &str = "id, rule_id, rule_name, team, kind, summary, note, base_revision, base_rule, proposed_rule,
    author, auto, alert_id, created_at, state, decided_by, decided_at";

fn view(state: &ApiState, r: &sqlx::postgres::PgRow) -> Result<Value, ApiError> {
    let team: String = r.try_get("team")?;
    Ok(json!({
        "id": r.try_get::<Uuid, _>("id")?.to_string(),
        "rule_id": r.try_get::<String, _>("rule_id")?,
        "rule_name": r.try_get::<String, _>("rule_name")?,
        "kind": r.try_get::<String, _>("kind")?,
        "summary": r.try_get::<String, _>("summary")?,
        "note": r.try_get::<String, _>("note")?,
        "base_revision": r.try_get::<i64, _>("base_revision")?,
        "base_rule": r.try_get::<Value, _>("base_rule")?,
        "proposed_rule": r.try_get::<Value, _>("proposed_rule")?,
        "author": r.try_get::<String, _>("author")?,
        "auto": r.try_get::<bool, _>("auto")?,
        "alert_id": r.try_get::<Option<Uuid>, _>("alert_id")?.map(|u| u.to_string()),
        "created_at": ts(r.try_get("created_at")?),
        "state": r.try_get::<String, _>("state")?,
        "decided_by": r.try_get::<Option<String>, _>("decided_by")?,
        "decided_at": r.try_get::<Option<DateTime<Utc>>, _>("decided_at")?.map(ts),
        "can_review": state.session.can_see(&team),
    }))
}

fn scope(state: &ApiState, raw: Option<&str>) -> Result<Vec<String>, ApiError> {
    let visible: Vec<String> = state.session.teams.iter().map(|t| t.id.clone()).collect();
    match param(raw, "team") {
        Some(t) if !state.session.can_see(&t) => Err(ApiError::forbidden(format!("team `{t}` is not visible to this session"))),
        Some(t) => Ok(vec![t]),
        None => Ok(visible),
    }
}

pub async fn list(State(state): State<ApiState>, RawQuery(raw): RawQuery) -> Result<Json<Value>, ApiError> {
    let teams = scope(&state, raw.as_deref())?;
    let st = param(raw.as_deref(), "state");
    if let Some(s) = &st
        && !STATES.contains(&s.as_str()) {
            return Err(ApiError::bad_request(format!("unknown state `{s}`")));
        }
    let rows = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM proposals WHERE team = ANY($1) AND ($2::text IS NULL OR state = $2)
         ORDER BY created_at DESC, id DESC LIMIT 500"
    ))
    .bind(&teams)
    .bind(&st)
    .fetch_all(&state.pool)
    .await?;
    let items = rows.iter().map(|r| view(&state, r)).collect::<Result<Vec<_>, _>>()?;
    Ok(Json(json!({"items": items})))
}

pub async fn count(State(state): State<ApiState>, RawQuery(raw): RawQuery) -> Result<Json<Value>, ApiError> {
    let teams = scope(&state, raw.as_deref())?;
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM proposals WHERE team = ANY($1) AND state = 'pending'")
        .bind(&teams)
        .fetch_one(&state.pool)
        .await?;
    Ok(Json(json!({"pending": n})))
}

#[derive(Deserialize)]
struct CreateBody {
    rule_id: String,
    base_revision: i64,
    kind: String,
    summary: String,
    #[serde(default)]
    note: Option<String>,
    proposed_rule: Value,
    #[serde(default)]
    alert_id: Option<String>,
}

fn invalid(path: &str, message: impl Into<String>) -> ApiError {
    let mut e = ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "invalid", "the proposal is not valid");
    e.details = Some(json!([{"path": path, "message": message.into()}]));
    e
}

pub async fn create(State(state): State<ApiState>, headers: HeaderMap, body: Body) -> Result<Response, ApiError> {
    state.session.check_csrf(&headers)?;
    let body = crate::rules::read_body(body, MAX_BODY).await?;
    let b: CreateBody = serde_json::from_slice(&body).map_err(|e| ApiError::bad_request(format!("not a valid proposal: {e}")))?;
    if !KINDS.contains(&b.kind.as_str()) {
        return Err(invalid("kind", format!("one of {KINDS:?}")));
    }
    if b.summary.trim().is_empty() || b.summary.chars().count() > 200 {
        return Err(invalid("summary", "1-200 characters"));
    }
    let note = b.note.unwrap_or_default();
    if note.chars().count() > 4000 {
        return Err(invalid("note", "at most 4000 characters"));
    }
    let alert_id = match b.alert_id.as_deref() {
        None | Some("") => None,
        Some(a) => Some(a.parse::<Uuid>().map_err(|_| invalid("alert_id", "not an alert id"))?),
    };
    let current = config_store::get(&state.pool, &b.rule_id)
        .await?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "not_found", "rule not found"))?;
    let team = current.spec.get("team").and_then(Value::as_str).unwrap_or("").to_string();
    if !state.session.can_see(&team) {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "not_found", "rule not found"));
    }
    if current.revision != b.base_revision {
        return Err(ApiError::conflict(
            "revision_conflict",
            "the rule changed since you loaded it",
            json!({"rule": current.spec, "revision": current.revision}),
        ));
    }
    // Same validator as publishing; the proposal must target the same rule
    // and may not move it to a team the author can't see.
    let spec = config_store::validate(&b.proposed_rule).map_err(|d| {
        let mut e = ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "invalid", "the proposed rule is not valid");
        e.details = Some(json!(d.into_iter().map(|i| json!({"path": format!("proposed_rule.{}", i.path).trim_end_matches('.').to_string(), "message": i.message})).collect::<Vec<_>>()));
        e
    })?;
    if spec.id != b.rule_id {
        return Err(invalid("proposed_rule.id", "must be the rule being changed"));
    }
    // Same team and routing rules as a direct edit.
    crate::rules::check_team(&state, &b.proposed_rule)?;
    let _ = &spec;
    let name = current.spec.get("name").and_then(Value::as_str).unwrap_or(&b.rule_id).to_string();
    let id = Uuid::now_v7();
    let row = sqlx::query(&format!(
        "INSERT INTO proposals (id, rule_id, rule_name, team, kind, summary, note, base_revision, base_rule, proposed_rule, author, alert_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) RETURNING {COLUMNS}"
    ))
    .bind(id)
    .bind(&b.rule_id)
    .bind(&name)
    .bind(&team)
    .bind(&b.kind)
    .bind(b.summary.trim())
    .bind(&note)
    .bind(b.base_revision)
    .bind(&current.spec)
    .bind(&b.proposed_rule)
    .bind(state.session.actor())
    .bind(alert_id)
    .fetch_one(&state.pool)
    .await?;
    Ok((StatusCode::CREATED, Json(view(&state, &row)?)).into_response())
}

pub async fn decide(State(state): State<ApiState>, headers: HeaderMap, Path((id, decision)): Path<(String, String)>) -> Result<Json<Value>, ApiError> {
    state.session.check_csrf(&headers)?;
    let not_found = || ApiError::new(StatusCode::NOT_FOUND, "not_found", "proposal not found");
    let id: Uuid = id.parse().map_err(|_| not_found())?;
    if decision != "accept" && decision != "reject" {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "not_found", "decision must be accept or reject"));
    }
    // One transaction: the proposal row is locked, so a concurrent accept and
    // reject can't both act, and an accepted rule write commits together with
    // the proposal's state.
    let mut tx = state.pool.begin().await?;
    let row = sqlx::query(&format!("SELECT {COLUMNS} FROM proposals WHERE id = $1 FOR UPDATE"))
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(not_found)?;
    let team: String = row.try_get("team")?;
    if !state.session.can_see(&team) {
        return Err(not_found());
    }
    let st: String = row.try_get("state")?;
    if st != "pending" {
        return Err(ApiError::conflict("invalid_state", format!("the proposal is already {st}"), view(&state, &row)?));
    }
    let actor = state.session.actor();
    let author: String = row.try_get("author")?;
    if author == actor && !state.session.admin {
        return Err(ApiError::forbidden("someone other than the author has to review this proposal"));
    }
    let new_state = if decision == "reject" {
        "rejected"
    } else {
        let proposed: Value = row.try_get("proposed_rule")?;
        let rule_id: String = row.try_get("rule_id")?;
        let base: i64 = row.try_get("base_revision")?;
        crate::rules::check_team(&state, &proposed)?;
        match config_store::update_in(&mut tx, &rule_id, &proposed, base, &actor, Some(&crate::rules::visible_teams(&state))).await? {
            Ok(_) => "accepted",
            Err(WriteError::Conflict(_) | WriteError::NotFound | WriteError::Forbidden) => {
                let stale = set_state(&state, &mut tx, id, "stale", &actor).await?;
                tx.commit().await?;
                return Err(ApiError::conflict("revision_conflict", "the rule changed after this proposal was made", stale));
            }
            Err(WriteError::Invalid(d)) => {
                let mut e = ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "invalid", "the proposed rule is no longer valid");
                e.details = Some(serde_json::to_value(d).unwrap_or_default());
                return Err(e);
            }
        }
    };
    let out = set_state(&state, &mut tx, id, new_state, &actor).await?;
    tx.commit().await?;
    Ok(Json(out))
}

async fn set_state(state: &ApiState, tx: &mut sqlx::PgConnection, id: Uuid, new_state: &str, actor: &str) -> Result<Value, ApiError> {
    let row = sqlx::query(&format!(
        "UPDATE proposals SET state = $2, decided_by = $3, decided_at = now() WHERE id = $1 AND state = 'pending' RETURNING {COLUMNS}"
    ))
    .bind(id)
    .bind(new_state)
    .bind(actor)
    .fetch_one(&mut *tx)
    .await?;
    view(state, &row)
}
