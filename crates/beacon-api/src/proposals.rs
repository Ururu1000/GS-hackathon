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
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Value, json};
use sqlx::Row;
use uuid::Uuid;

use crate::ApiState;
use crate::error::ApiError;

const KINDS: &[&str] = &["noisy", "reroute", "template", "other"];
const STATES: &[&str] = &["pending", "accepted", "rejected", "stale"];
const MAX_BODY: usize = 128 * 1024;
/// Newest proposals returned by a list call. With [`MAX_STORED_RULES`] per
/// row this bounds a list response to roughly 15 MB.
const LIST_LIMIT: i64 = 100;
/// Both rule bodies of a proposal, measured as Postgres stores and returns
/// them: jsonb prints numbers in full (`1e308` comes back as 309 digits), so
/// a rule that validates at 64 KiB can be far larger on the way out.
const MAX_STORED_RULES: i64 = 128 * 1024;
/// Pending proposals allowed per rule and per author, so the table (and the
/// list, which carries two rule bodies per row) can't be flooded.
const MAX_PENDING_PER_RULE: i64 = 10;
const MAX_PENDING_PER_AUTHOR: i64 = 50;

fn ts(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn param(raw: Option<&str>, name: &str) -> Option<String> {
    form_urlencoded::parse(raw.unwrap_or("").as_bytes()).find(|(k, _)| k == name).map(|(_, v)| v.into_owned())
}

const COLUMNS: &str = "id, rule_id, rule_name, team, kind, summary, note, base_revision, base_rule, proposed_rule,
    author, auto, alert_id, created_at, state, decided_by, decided_at";

/// A proposal as the API returns it. The two rule bodies pass through as raw
/// JSON text, so a list never decodes them into `Value` trees.
#[derive(Serialize)]
struct ProposalView {
    id: String,
    rule_id: String,
    rule_name: String,
    kind: String,
    summary: String,
    note: String,
    base_revision: i64,
    base_rule: Box<RawValue>,
    proposed_rule: Box<RawValue>,
    author: String,
    auto: bool,
    alert_id: Option<String>,
    created_at: String,
    state: String,
    decided_by: Option<String>,
    decided_at: Option<String>,
    can_review: bool,
}

fn raw_view(state: &ApiState, r: &sqlx::postgres::PgRow) -> Result<ProposalView, ApiError> {
    let team: String = r.try_get("team")?;
    let author: String = r.try_get("author")?;
    // Mirrors decide(): the team's members review, but not their own proposal.
    let can_review = state.session.can_see(&team) && (state.session.admin || author != state.session.actor());
    Ok(ProposalView {
        id: r.try_get::<Uuid, _>("id")?.to_string(),
        rule_id: r.try_get("rule_id")?,
        rule_name: r.try_get("rule_name")?,
        kind: r.try_get("kind")?,
        summary: r.try_get("summary")?,
        note: r.try_get("note")?,
        base_revision: r.try_get("base_revision")?,
        base_rule: r.try_get::<sqlx::types::Json<Box<RawValue>>, _>("base_rule")?.0,
        proposed_rule: r.try_get::<sqlx::types::Json<Box<RawValue>>, _>("proposed_rule")?.0,
        author,
        auto: r.try_get("auto")?,
        alert_id: r.try_get::<Option<Uuid>, _>("alert_id")?.map(|u| u.to_string()),
        created_at: ts(r.try_get("created_at")?),
        state: r.try_get("state")?,
        decided_by: r.try_get("decided_by")?,
        decided_at: r.try_get::<Option<DateTime<Utc>>, _>("decided_at")?.map(ts),
        can_review,
    })
}

fn view(state: &ApiState, r: &sqlx::postgres::PgRow) -> Result<Value, ApiError> {
    serde_json::to_value(raw_view(state, r)?).map_err(|e| ApiError::from(anyhow::anyhow!("proposal view: {e}")))
}

#[derive(Serialize)]
struct ListOut {
    items: Vec<ProposalView>,
}

fn scope(state: &ApiState, raw: Option<&str>) -> Result<Vec<String>, ApiError> {
    let visible: Vec<String> = state.session.teams.iter().map(|t| t.id.clone()).collect();
    match param(raw, "team") {
        Some(t) if !state.session.can_see(&t) => Err(ApiError::forbidden(format!("team `{t}` is not visible to this session"))),
        Some(t) => Ok(vec![t]),
        None => Ok(visible),
    }
}

pub async fn list(State(state): State<ApiState>, RawQuery(raw): RawQuery) -> Result<Response, ApiError> {
    let teams = scope(&state, raw.as_deref())?;
    let st = param(raw.as_deref(), "state");
    if let Some(s) = &st
        && !STATES.contains(&s.as_str()) {
            return Err(ApiError::bad_request(format!("unknown state `{s}`")));
        }
    let rows = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM proposals WHERE team = ANY($1) AND ($2::text IS NULL OR state = $2)
         ORDER BY created_at DESC, id DESC LIMIT $3"
    ))
    .bind(&teams)
    .bind(&st)
    .bind(LIST_LIMIT)
    .fetch_all(&state.pool)
    .await?;
    let items = rows.iter().map(|r| raw_view(&state, r)).collect::<Result<Vec<_>, _>>()?;
    Ok(Json(ListOut { items }).into_response())
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
    // Same team and routing rules as a direct edit, which can't move a rule
    // to another team either.
    if spec.team != team {
        return Err(invalid("proposed_rule.team", "team cannot be changed on an existing rule"));
    }
    crate::rules::check_team(&state, &b.proposed_rule)?;
    let stored: i64 = sqlx::query_scalar("SELECT (octet_length($1::jsonb::text) + octet_length($2::jsonb::text))::bigint")
        .bind(&current.spec)
        .bind(&b.proposed_rule)
        .fetch_one(&state.pool)
        .await?;
    if stored > MAX_STORED_RULES {
        return Err(invalid("proposed_rule", format!("the current and proposed rule together must be under {} KiB once stored", MAX_STORED_RULES / 1024)));
    }
    // Best effort (two concurrent creates can both pass): a bound, not an invariant.
    let actor = state.session.actor();
    let (per_rule, per_author): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE rule_id = $1), count(*) FILTER (WHERE author = $2)
         FROM proposals WHERE state = 'pending' AND (rule_id = $1 OR author = $2)",
    )
    .bind(&b.rule_id)
    .bind(&actor)
    .fetch_one(&state.pool)
    .await?;
    if per_rule >= MAX_PENDING_PER_RULE || per_author >= MAX_PENDING_PER_AUTHOR {
        return Err(ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "too_many_proposals",
            format!("at most {MAX_PENDING_PER_RULE} pending proposals per rule and {MAX_PENDING_PER_AUTHOR} per author; review some first"),
        ));
    }
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
    .bind(&actor)
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
