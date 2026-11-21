//! Rules, the rule test panel, recent events and schema discovery
//! (`ui/contract/openapi.yaml` v0.2.0).
//!
//! ```text
//! GET    /api/rules?team=              RuleItem[] with stats
//! POST   /api/rules                    {rule} → 201 | 409 | 422
//! GET    /api/rules/{id}
//! PUT    /api/rules/{id}               {rule, expected_revision} → 200 | 409 | 422
//! DELETE /api/rules/{id}?expected_revision=
//! POST   /api/rules/test               {rule, event, source?} → RuleTestResult (no writes)
//! GET    /api/events/recent?limit=     recent event samples
//! GET    /api/schemas                  configured schema mappings
//! GET    /api/schemas/discovered       unknown shapes seen recently, with a sample
//! GET    /api/sources                  sources seen recently (HTTP and Kafka)
//! ```
//! Writes go through `beacon_engine::config_store` (one validator); matchers
//! reload within seconds.

use std::collections::HashMap;

use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use beacon_core::rule::RuleSpec;
use beacon_core::{ConfigSnapshot, Event};
use beacon_engine::config_store::{self, Invalid, StoredRule, WriteError};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::Row;

use crate::ApiState;
use crate::error::ApiError;

/// Bodies above this are rejected (413) before parsing.
pub const MAX_TEST_BODY: usize = 256 * 1024;

fn ts(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn query_param(raw: Option<&str>, name: &str) -> Option<String> {
    form_urlencoded::parse(raw.unwrap_or("").as_bytes()).find(|(k, _)| k == name).map(|(_, v)| v.into_owned())
}

fn invalid(details: Vec<Invalid>) -> ApiError {
    let mut e = ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "invalid", "the rule is not valid");
    e.details = Some(serde_json::to_value(details).unwrap_or_default());
    e
}

fn team_of(spec: &Value) -> String {
    spec.get("team").and_then(Value::as_str).unwrap_or("").to_string()
}

/// Stats from the alerts table, scoped to visible teams (and optionally one
/// rule) so callers never aggregate hidden teams' alerts.
async fn stats(state: &ApiState, rule_id: Option<&str>) -> Result<HashMap<String, Value>, ApiError> {
    let visible: Vec<String> = state.session.teams.iter().map(|t| t.id.clone()).collect();
    let rows = match rule_id {
        Some(rid) => {
            sqlx::query(
                "SELECT rule_id,
                        COALESCE(sum(count) FILTER (WHERE last_seen > now() - interval '24 hours'), 0)::bigint AS fires_24h,
                        count(*) FILTER (WHERE status IN ('open', 'acked')) AS open_alerts,
                        max(last_seen) AS last_fired_at,
                        array_agg(count) FILTER (WHERE last_seen > now() - interval '24 hours') AS counts,
                        array_agg(floor(extract(epoch FROM (now() - last_seen)) / 3600)::int)
                            FILTER (WHERE last_seen > now() - interval '24 hours') AS hours_ago
                 FROM alerts WHERE team = ANY($1) AND rule_id = $2 GROUP BY rule_id",
            )
            .bind(&visible)
            .bind(rid)
            .fetch_all(&state.pool)
            .await?
        }
        None => {
            sqlx::query(
                "SELECT rule_id,
                        COALESCE(sum(count) FILTER (WHERE last_seen > now() - interval '24 hours'), 0)::bigint AS fires_24h,
                        count(*) FILTER (WHERE status IN ('open', 'acked')) AS open_alerts,
                        max(last_seen) AS last_fired_at,
                        array_agg(count) FILTER (WHERE last_seen > now() - interval '24 hours') AS counts,
                        array_agg(floor(extract(epoch FROM (now() - last_seen)) / 3600)::int)
                            FILTER (WHERE last_seen > now() - interval '24 hours') AS hours_ago
                 FROM alerts WHERE team = ANY($1) GROUP BY rule_id",
            )
            .bind(&visible)
            .fetch_all(&state.pool)
            .await?
        }
    };
    let mut out = HashMap::new();
    for r in rows {
        let mut hourly = [0i64; 24];
        let counts: Option<Vec<i64>> = r.try_get("counts")?;
        let hours: Option<Vec<i32>> = r.try_get("hours_ago")?;
        for (c, h) in counts.unwrap_or_default().into_iter().zip(hours.unwrap_or_default()) {
            let h = h.clamp(0, 23) as usize;
            hourly[23 - h] += c; // oldest first
        }
        let last: Option<DateTime<Utc>> = r.try_get("last_fired_at")?;
        out.insert(
            r.try_get::<String, _>("rule_id")?,
            json!({"fires_24h": r.try_get::<i64, _>("fires_24h")?, "open_alerts": r.try_get::<i64, _>("open_alerts")?,
                   "last_fired_at": last.map(ts), "hourly": hourly}),
        );
    }
    Ok(out)
}

fn empty_stats() -> Value {
    json!({"fires_24h": 0, "open_alerts": 0, "last_fired_at": null, "hourly": vec![0; 24]})
}

fn item(state: &ApiState, r: &StoredRule, stats: Option<&Value>) -> Value {
    json!({
        "rule": r.spec,
        "revision": r.revision,
        "updated_at": ts(r.updated_at),
        "updated_by": r.updated_by,
        "stats": stats.cloned().unwrap_or_else(empty_stats),
        "can_edit": state.session.can_see(&team_of(&r.spec)),
    })
}

pub async fn list(State(state): State<ApiState>, RawQuery(raw): RawQuery) -> Result<Json<Value>, ApiError> {
    let team = query_param(raw.as_deref(), "team");
    if let Some(t) = &team
        && !state.session.can_see(t) {
            return Err(ApiError::forbidden(format!("team `{t}` is not visible to this session")));
        }
    let stats = stats(&state, None).await?;
    let items: Vec<Value> = config_store::list(&state.pool)
        .await?
        .iter()
        .filter(|r| {
            let t = team_of(&r.spec);
            state.session.can_see(&t) && team.as_ref().is_none_or(|x| *x == t)
        })
        .map(|r| item(&state, r, stats.get(r.spec.get("id").and_then(Value::as_str).unwrap_or(""))))
        .collect();
    Ok(Json(json!({"items": items})))
}

pub async fn get(State(state): State<ApiState>, Path(id): Path<String>) -> Result<Json<Value>, ApiError> {
    let r = config_store::get(&state.pool, &id).await?.ok_or_else(rule_not_found)?;
    if !state.session.can_see(&team_of(&r.spec)) {
        return Err(rule_not_found());
    }
    let stats = stats(&state, Some(&id)).await?;
    Ok(Json(item(&state, &r, stats.get(&id))))
}

fn rule_not_found() -> ApiError {
    ApiError::new(StatusCode::NOT_FOUND, "not_found", "rule not found")
}

#[derive(Deserialize)]
struct WriteBody {
    rule: Value,
    #[serde(default)]
    expected_revision: Option<i64>,
}

fn parse_body<T: serde::de::DeserializeOwned>(body: &Bytes, what: &str) -> Result<T, ApiError> {
    serde_json::from_slice(body).map_err(|e| ApiError::bad_request(format!("body must be {what}: {e}")))
}

/// Writers must be able to see (own) the rule's team, before and after, and
/// may only route to teams/schedules they can see (or themselves).
pub(crate) fn check_team(state: &ApiState, spec: &Value) -> Result<(), ApiError> {
    let t = team_of(spec);
    if !(t.is_empty() || state.session.can_see(&t)) {
        return Err(ApiError::forbidden(format!("you can't publish rules for team `{t}`")));
    }
    if let Some(routes) = spec.get("route").and_then(Value::as_array) {
        for r in routes {
            let Some(to) = r.get("to").and_then(Value::as_str) else { continue };
            if let Some(target) = to.strip_prefix("team:")
                && !state.session.can_see(target)
            {
                return Err(ApiError::forbidden(format!("you can't route to team `{target}`")));
            }
            if let Some(target) = to.strip_prefix("oncall:")
                && !state.session.can_see_schedule(target)
            {
                return Err(ApiError::forbidden(format!("you can't route to on-call schedule `{target}`")));
            }
            if let Some(target) = to.strip_prefix("user:")
                && !state.session.admin
                && target != state.session.user.id
            {
                return Err(ApiError::forbidden(format!("you can't route directly to user `{target}`")));
            }
        }
    }
    Ok(())
}

/// Rule writes are small; anything bigger is refused during extraction.
const MAX_WRITE_BODY: usize = 128 * 1024;
/// Bound concurrent blocking rule-test evaluations per replica.
static TEST_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(8);

pub(crate) async fn read_body(body: Body, limit: usize) -> Result<Bytes, ApiError> {
    axum::body::to_bytes(body, limit)
        .await
        .map_err(|_| ApiError::new(StatusCode::PAYLOAD_TOO_LARGE, "too_large", format!("body exceeds {limit} bytes")))
}

pub(crate) fn require_admin(state: &ApiState, what: &str) -> Result<(), ApiError> {
    if state.session.admin {
        Ok(())
    } else {
        Err(ApiError::forbidden(format!("{what} is limited to admins")))
    }
}

async fn item_with_stats(state: &ApiState, r: &StoredRule) -> Result<Value, ApiError> {
    let id = r.spec.get("id").and_then(Value::as_str).unwrap_or("").to_string();
    let stats = stats(state, Some(&id)).await?;
    Ok(item(state, r, stats.get(&id)))
}

async fn map_write(state: &ApiState, e: WriteError) -> ApiError {
    match e {
        WriteError::Invalid(d) => invalid(d),
        WriteError::NotFound => rule_not_found(),
        WriteError::Forbidden => ApiError::forbidden("rule belongs to another team"),
        WriteError::Conflict(current) => {
            // Only show the current rule to someone allowed to see its team.
            let visible = current.filter(|c| state.session.can_see(&team_of(&c.spec)));
            let current = match visible {
                Some(c) => item_with_stats(state, &c).await.unwrap_or(Value::Null),
                None => Value::Null,
            };
            ApiError::conflict("revision_conflict", "the rule changed since you loaded it", current)
        }
    }
}

pub(crate) fn visible_teams(state: &ApiState) -> Vec<String> {
    state.session.teams.iter().map(|t| t.id.clone()).collect()
}

pub async fn create(State(state): State<ApiState>, headers: HeaderMap, body: Body) -> Result<Response, ApiError> {
    state.session.check_csrf(&headers)?;
    let body = read_body(body, MAX_WRITE_BODY).await?;
    let b: WriteBody = parse_body(&body, "{\"rule\": {...}}")?;
    check_team(&state, &b.rule)?;
    let visible = visible_teams(&state);
    match config_store::create_scoped(&state.pool, &b.rule, &state.session.actor(), Some(&visible)).await? {
        Ok(r) => Ok((StatusCode::CREATED, Json(item_with_stats(&state, &r).await?)).into_response()),
        Err(e) => Err(map_write(&state, e).await),
    }
}

pub async fn update(State(state): State<ApiState>, headers: HeaderMap, Path(id): Path<String>, body: Body) -> Result<Json<Value>, ApiError> {
    state.session.check_csrf(&headers)?;
    let body = read_body(body, MAX_WRITE_BODY).await?;
    let b: WriteBody = parse_body(&body, "{\"rule\": {...}, \"expected_revision\": <int>}")?;
    let expected = b.expected_revision.ok_or_else(|| ApiError::bad_request("expected_revision is required"))?;
    check_team(&state, &b.rule)?;
    let visible = visible_teams(&state);
    match config_store::update_scoped(&state.pool, &id, &b.rule, expected, &state.session.actor(), Some(&visible)).await? {
        Ok(r) => Ok(Json(item_with_stats(&state, &r).await?)),
        Err(e) => Err(map_write(&state, e).await),
    }
}

pub async fn delete(State(state): State<ApiState>, headers: HeaderMap, Path(id): Path<String>, RawQuery(raw): RawQuery) -> Result<Json<Value>, ApiError> {
    state.session.check_csrf(&headers)?;
    let expected: i64 = query_param(raw.as_deref(), "expected_revision")
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| ApiError::bad_request("expected_revision query parameter is required"))?;
    let visible = visible_teams(&state);
    match config_store::delete_scoped(&state.pool, &id, expected, &state.session.actor(), Some(&visible)).await? {
        Ok(()) => Ok(Json(json!({"deleted": id}))),
        Err(e) => Err(map_write(&state, e).await),
    }
}

#[derive(Deserialize)]
struct TestBody {
    rule: Value,
    event: Value,
    #[serde(default)]
    source: Option<String>,
}

/// Dry run: the same compiler and matcher as production, zero writes.
pub async fn test(State(state): State<ApiState>, headers: HeaderMap, body: Body) -> Result<Json<Value>, ApiError> {
    state.session.check_csrf(&headers)?;
    let body = read_body(body, MAX_TEST_BODY).await?;
    let b: TestBody = parse_body(&body, "{\"rule\": {...}, \"event\": {...}}")?;
    let Ok(slot) = TEST_SLOTS.try_acquire() else {
        return Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "too_many_tests", "too many rule tests in flight, retry"));
    };
    // Compiling and evaluating is CPU work: keep it off the async workers, and
    // hold the concurrency permit inside the blocking task even if the request cancels.
    // The live mappings (Postgres), so a just-published mapping is used.
    let stored = config_store::list_schemas(&state.pool).await?;
    let out = tokio::task::spawn_blocking(move || {
        let _slot = slot;
        let schemas = config_store::valid_schema_specs(stored);
        run_test(&schemas, b.rule, b.event, b.source)
    })
        .await
        .map_err(|e| ApiError::from(anyhow::anyhow!("rule test task failed: {e}")))?;
    Ok(Json(out))
}

pub fn run_test(schemas: &[beacon_core::mapping::SchemaMappingSpec], rule: Value, raw: Value, source: Option<String>) -> Value {
    let source = source.unwrap_or_else(|| "http:test".into());
    let fail = |errors: Value, warnings: Vec<String>| {
        json!({"matched": false, "schema": "", "fields": {}, "nodes": [], "card": null, "warnings": warnings, "errors": errors})
    };
    let spec: RuleSpec = match config_store::validate(&rule) {
        Ok(s) => s,
        Err(d) => return fail(serde_json::to_value(d).unwrap_or_default(), vec![]),
    };
    let snapshot = match ConfigSnapshot::build(0, schemas.to_vec(), vec![spec]) {
        Ok(s) => s,
        Err(e) => return fail(json!([{"path": "", "message": e.to_string()}]), vec![]),
    };
    let mut event = Event::new("test".into(), source, raw);
    let diags = snapshot.normalize(&mut event);
    let mut warnings: Vec<String> = diags.iter().map(|d| format!("{}: {}", d.field, d.message)).collect();
    if event.schema.starts_with("unknown:") {
        warnings.push("No schema mapping recognises this event; match on raw.* paths".into());
    }
    let rule = &snapshot.rules[0];
    let nodes = rule.when.test_nodes(&event);
    let matches = snapshot.match_event(&event);
    let fire = matches.iter().find(|m| m.kind == beacon_core::MatchKind::Fire);
    if matches.iter().any(|m| m.kind == beacon_core::MatchKind::Recover) {
        warnings.push("This event is a recovery signal for the rule (auto_resolve.recover_when)".into());
    }
    if !rule.spec.enabled {
        warnings.push("The rule is disabled; it would not fire".into());
    }
    let card = fire.map(|m| {
        warnings.extend(m.diagnostics.iter().cloned());
        json!({
            "title": m.title, "body": m.body, "severity": m.severity, "color": m.color,
            "fields": m.fields.iter().map(|f| json!({"path": f.path, "label": f.label, "value": render(&f.value)})).collect::<Vec<_>>(),
            "dedup_key": m.key.iter().map(render).collect::<Vec<_>>(),
        })
    });
    json!({
        "matched": fire.is_some(),
        "schema": event.schema,
        "fields": event.fields,
        "nodes": nodes,
        "card": card,
        "warnings": warnings,
        "errors": [],
    })
}

fn render(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

pub async fn recent_events(State(state): State<ApiState>, RawQuery(raw): RawQuery) -> Result<Json<Value>, ApiError> {
    require_admin(&state, "seeing events from every source")?;
    let limit: i64 = query_param(raw.as_deref(), "limit").and_then(|v| v.parse().ok()).unwrap_or(20).clamp(1, 50);
    let rows = sqlx::query(
        "SELECT event_id, source, schema, received_at, raw FROM event_samples ORDER BY id DESC LIMIT $1",
    )
    .bind(limit)
    .fetch_all(&state.pool)
    .await?;
    let items = rows
        .iter()
        .map(|r| {
            Ok(json!({
                "event_id": r.try_get::<String, _>("event_id")?,
                "source": r.try_get::<String, _>("source")?,
                "schema": r.try_get::<String, _>("schema")?,
                "received_at": ts(r.try_get("received_at")?),
                "raw": beacon_core::matcher::redact(r.try_get::<Value, _>("raw")?),
            }))
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    Ok(Json(json!({"items": items})))
}

pub async fn schemas(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    let items: Vec<Value> = config_store::list_schemas(&state.pool)
        .await?
        .iter()
        .map(|s| json!({"spec": s.spec, "revision": s.revision, "updated_at": ts(s.updated_at), "updated_by": s.updated_by}))
        .collect();
    Ok(Json(Value::Array(items)))
}

#[derive(Deserialize)]
struct SchemaWrite {
    spec: Value,
    #[serde(default)]
    expected_revision: Option<i64>,
    /// The mapping being edited; required with `expected_revision`, so an
    /// edited `spec.schema` can't overwrite a different mapping.
    #[serde(default)]
    schema: Option<String>,
}

/// Publish a schema mapping (create, or compare-and-set with expected_revision).
/// Mappings are global (they recognise shapes, not team data), so publishing
/// is limited to admins.
pub async fn save_schema(State(state): State<ApiState>, headers: HeaderMap, body: Body) -> Result<Response, ApiError> {
    state.session.check_csrf(&headers)?;
    require_admin(&state, "publishing schema mappings")?;
    let body = read_body(body, MAX_WRITE_BODY).await?;
    let b: SchemaWrite = parse_body(&body, "{\"spec\": {...}, \"expected_revision\"?: <int>, \"schema\"?: \"...\"}")?;
    if b.expected_revision.is_some() {
        let name = b.spec.get("schema").and_then(Value::as_str);
        match b.schema.as_deref() {
            None => return Err(ApiError::bad_request("schema (the mapping being edited) is required with expected_revision")),
            Some(target) if Some(target) != name => {
                return Err(invalid(vec![config_store::Invalid {
                    path: "schema".into(),
                    message: format!("a mapping can't be renamed; publish `{}` as a new mapping", name.unwrap_or("")),
                }]));
            }
            Some(_) => {}
        }
    }
    match config_store::save_schema(&state.pool, &b.spec, b.expected_revision, &state.session.actor()).await? {
        Ok((s, created)) => {
            let v = json!({"spec": s.spec, "revision": s.revision, "updated_at": ts(s.updated_at), "updated_by": s.updated_by});
            let status = if created { StatusCode::CREATED } else { StatusCode::OK };
            Ok((status, Json(v)).into_response())
        }
        Err(WriteError::Invalid(d)) => Err(invalid(d)),
        Err(WriteError::NotFound | WriteError::Forbidden) => Err(ApiError::new(StatusCode::NOT_FOUND, "not_found", "no mapping with that schema name to update")),
        Err(WriteError::Conflict(_)) => {
            let name = b.spec.get("schema").and_then(Value::as_str).unwrap_or("");
            let current = config_store::get_schema(&state.pool, name)
                .await?
                .map(|s| json!({"spec": s.spec, "revision": s.revision}))
                .unwrap_or(Value::Null);
            Err(ApiError::conflict("revision_conflict", "the mapping changed since you loaded it (or already exists)", current))
        }
    }
}

#[derive(Deserialize)]
struct SchemaPreview {
    spec: Value,
    event: Value,
    #[serde(default)]
    source: Option<String>,
}

/// Evaluate a mapping against one event, without changing config.
pub async fn preview_schema(State(state): State<ApiState>, headers: HeaderMap, body: Body) -> Result<Json<Value>, ApiError> {
    state.session.check_csrf(&headers)?;
    let body = read_body(body, MAX_TEST_BODY).await?;
    let b: SchemaPreview = parse_body(&body, "{\"spec\": {...}, \"event\": {...}, \"source\": \"...\"}")?;
    let Ok(slot) = TEST_SLOTS.try_acquire() else {
        return Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "too_many_tests", "too many previews in flight, retry"));
    };
    // Validating compiles the detector's regexes: CPU work, so it runs under
    // the permit on a blocking thread too.
    let out = tokio::task::spawn_blocking(move || {
        let _slot = slot;
        let spec = config_store::validate_schema(&b.spec)?;
        let m = beacon_core::mapping::SchemaMapping::compile(&spec).expect("validated above");
        let event = Event::new("preview".into(), b.source.unwrap_or_else(|| "http:preview".into()), b.event);
        let matched = m.detects(&event);
        let (fields, diags) = if matched { m.extract(&event) } else { (Default::default(), vec![]) };
        let mut warnings: Vec<String> = diags.iter().map(|d| format!("{}: {}", d.field, d.message)).collect();
        if !matched {
            warnings.push("The detector does not recognise this event".into());
        }
        Ok(json!({"matched": matched, "fields": fields, "warnings": warnings}))
    })
    .await
    .map_err(|e| ApiError::from(anyhow::anyhow!("preview task failed: {e}")))?
    .map_err(invalid)?;
    Ok(Json(out))
}

pub async fn discovered(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    require_admin(&state, "seeing events from every source")?;
    let rows = sqlx::query(
        "SELECT DISTINCT ON (schema) schema, source, raw,
                count(*) OVER (PARTITION BY schema) AS count,
                min(received_at) OVER (PARTITION BY schema) AS first_seen,
                max(received_at) OVER (PARTITION BY schema) AS last_seen
         FROM event_samples WHERE schema LIKE 'unknown:%'
         ORDER BY schema, id DESC",
    )
    .fetch_all(&state.pool)
    .await?;
    let mut items: Vec<Value> = rows
        .iter()
        .map(|r| {
            Ok(json!({
                "schema": r.try_get::<String, _>("schema")?,
                "source": r.try_get::<String, _>("source")?,
                "count": r.try_get::<i64, _>("count")?,
                "first_seen": ts(r.try_get("first_seen")?),
                "last_seen": ts(r.try_get("last_seen")?),
                "sample": beacon_core::matcher::redact(r.try_get::<Value, _>("raw")?),
            }))
        })
        .collect::<Result<_, sqlx::Error>>()?;
    items.sort_by(|a, b| b["last_seen"].as_str().cmp(&a["last_seen"].as_str()));
    items.truncate(50);
    Ok(Json(Value::Array(items)))
}

pub async fn sources(State(state): State<ApiState>) -> Result<Json<Value>, ApiError> {
    let rows = sqlx::query(
        "SELECT source, max(received_at) AS last_seen,
                count(*) FILTER (WHERE received_at > now() - interval '24 hours') AS samples_24h
         FROM event_samples GROUP BY source ORDER BY source",
    )
    .fetch_all(&state.pool)
    .await?;
    let mut items: Vec<Value> = Vec::new();
    for r in &rows {
        let id: String = r.try_get("source")?;
        let kind = if id.starts_with("http:") { "http" } else { "kafka" };
        items.push(json!({"id": id, "kind": kind, "type": kind, "topics": [],
            "last_seen": ts(r.try_get("last_seen")?), "events_24h": r.try_get::<i64, _>("samples_24h")?}));
    }
    // Configured Kafka sources that haven't produced a sample yet.
    for s in state.sources.iter() {
        if !items.iter().any(|i| i["id"] == json!(s.id)) {
            items.push(json!({"id": s.id, "kind": "kafka", "type": "kafka", "topics": s.topics, "last_seen": null, "events_24h": 0}));
        } else if let Some(i) = items.iter_mut().find(|i| i["id"] == json!(s.id)) {
            i["topics"] = json!(s.topics);
        }
    }
    Ok(Json(Value::Array(items)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_panel_matches_and_explains() {
        let schemas = config_store::load_schemas(&std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config")).unwrap();
        let rule = json!({"id": "rates-big", "team": "rates",
            "when": {"all": [{"path": "raw.payload.desk", "op": "eq", "value": "rates"}, {"path": "raw.payload.notional", "op": "gt", "value": 10000000}]},
            "route": [{"to": "team:rates"}], "present": {"title": "Rejected {{ raw.payload.notional | money }}", "fields": [{"path": "raw.payload.desk", "label": "Desk"}]},
            "dedup": {"key": ["raw.payload.tradeId"]}});
        let event = json!({"meta": {"schema": "trade.rejected.v1", "producer": "p", "ts": 1761210842000i64},
            "payload": {"tradeId": "T-1", "desk": "rates", "notional": 25000000}});
        let r = run_test(&schemas, rule.clone(), event.clone(), None);
        assert_eq!(r["matched"], json!(true));
        assert_eq!(r["schema"], json!("envelope.meta-payload"));
        assert_eq!(r["card"]["title"], json!("Rejected 25,000,000"));
        assert_eq!(r["card"]["dedup_key"], json!(["T-1"]));
        assert_eq!(r["nodes"][0]["node"], json!(""));
        assert_eq!(r["nodes"][2]["actual"], json!(25000000));

        let mut small = event;
        small["payload"]["notional"] = json!(5);
        let r = run_test(&schemas, rule.clone(), small, None);
        assert_eq!((r["matched"].clone(), r["card"].clone()), (json!(false), Value::Null));
        assert_eq!(r["nodes"][2]["result"], json!("no_match"));

        let mut bad = rule;
        bad["route"] = json!([]);
        let r = run_test(&schemas, bad, json!({}), None);
        assert_eq!(r["errors"][0]["path"], json!("route"));
    }
}
