//! Feed, detail and status transitions.

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, RawQuery, State};
use axum::http::HeaderMap;
use beacon_core::FeedAlert;
use beacon_engine::alerts as engine_alerts;
use beacon_engine::feed::{FEED_COLUMNS, row_to_feed};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use crate::ApiState;
use crate::error::ApiError;
use crate::session::Session;

pub const DEFAULT_LIMIT: i64 = 500;
pub const MAX_LIMIT: i64 = 5000;

/// The contract's `FeedAlert` JSON: field values are rendered strings,
/// secrets/contacts are masked on read (covering pre-existing rows), and
/// internal bookkeeping (`last_event_id`) is not exposed.
pub fn api_alert(a: &FeedAlert) -> Value {
    let mut v = serde_json::to_value(a).expect("FeedAlert serializes");
    if let Some(o) = v.as_object_mut() {
        o.remove("last_event_id");
        for k in ["title", "body"] {
            if let Some(Value::String(s)) = o.get_mut(k) {
                *s = beacon_core::matcher::redact_str(s);
            }
        }
        if let Some(Value::Array(fields)) = o.get_mut("fields") {
            for f in fields {
                let secret_path = f
                    .get("path")
                    .and_then(Value::as_str)
                    .is_some_and(beacon_core::matcher::is_secret_path_str);
                if let Some(val) = f.get_mut("value") {
                    *val = if secret_path {
                        Value::String("[redacted]".into())
                    } else {
                        let redacted = beacon_core::matcher::redact(val.take());
                        Value::String(render_value(&redacted))
                    };
                }
            }
        }
    }
    v
}

/// `anna@example.test` → `a***@example.test`, `+44 7700 900123` → `+44 *** 123`.
pub fn mask_address(a: &str) -> String {
    if let Some((user, domain)) = a.split_once('@') {
        let first: String = user.chars().take(1).collect();
        return format!("{first}***@{domain}");
    }
    let digits: Vec<char> = a.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() >= 6 {
        let head: String = a.chars().take_while(|c| *c != ' ').take(3).collect();
        let tail: String = digits[digits.len() - 3..].iter().collect();
        return format!("{head} *** {tail}");
    }
    "***".into()
}

/// Map free-form provider error strings to a safe status/category summary so
/// provider text (addresses, phone numbers, query-string credentials) never
/// leaves the database in API responses.
fn safe_error_summary(status: &str, class: Option<&str>, raw: &str) -> String {
    if status == "cancelled" {
        return "delivery cancelled".into();
    }
    if let Some(code) = raw.split_whitespace().find_map(|w| {
        let d = w.trim_matches(|c: char| !c.is_ascii_digit());
        (d.len() == 3)
            .then(|| d.parse::<u16>().ok())
            .flatten()
            .filter(|c| (400..=599).contains(c))
    }) {
        return format!("delivery failed ({code})");
    }
    if let Some(cls) = class {
        return format!("delivery failed ({cls})");
    }
    "delivery failed".into()
}

fn render_value(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn ts(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Feed filters, parsed from a query string with repeated parameters.
#[derive(Debug, Default, PartialEq)]
pub struct FeedQuery {
    pub team: Option<String>,
    pub status: Vec<String>,
    pub severity: Vec<String>,
    pub rule: Vec<String>,
    pub limit: i64,
    pub cursor: Option<(DateTime<Utc>, Uuid)>,
}

const STATUSES: &[&str] = &["open", "acked", "resolved"];
const SEVERITIES: &[&str] = &["critical", "error", "warning", "info"];
const MAX_RULE_FILTERS: usize = 64;
const MAX_TRANSITION_BODY: usize = 32 * 1024;

impl FeedQuery {
    pub fn parse(raw: Option<&str>) -> Result<FeedQuery, ApiError> {
        let mut q = FeedQuery { limit: DEFAULT_LIMIT, ..Default::default() };
        for (k, v) in form_urlencoded::parse(raw.unwrap_or("").as_bytes()) {
            let v = v.into_owned();
            match k.as_ref() {
                "team" if q.team.is_some() => return Err(ApiError::bad_request("team may be given once")),
                "team" if !v.is_empty() => q.team = Some(v),
                "status" if STATUSES.contains(&v.as_str()) => {
                    if q.status.len() >= STATUSES.len() * 2 {
                        return Err(ApiError::bad_request("too many status filters"));
                    }
                    q.status.push(v);
                }
                "status" => return Err(ApiError::bad_request(format!("unknown status `{v}`"))),
                "severity" if SEVERITIES.contains(&v.as_str()) => {
                    if q.severity.len() >= SEVERITIES.len() * 2 {
                        return Err(ApiError::bad_request("too many severity filters"));
                    }
                    q.severity.push(v);
                }
                "severity" => return Err(ApiError::bad_request(format!("unknown severity `{v}`"))),
                "rule" => {
                    if q.rule.len() >= MAX_RULE_FILTERS {
                        return Err(ApiError::bad_request(format!("at most {MAX_RULE_FILTERS} rule filters")));
                    }
                    q.rule.push(v);
                }
                "limit" => {
                    q.limit = v
                        .parse::<i64>()
                        .ok()
                        .filter(|l| (1..=MAX_LIMIT).contains(l))
                        .ok_or_else(|| ApiError::bad_request(format!("limit must be 1..={MAX_LIMIT}")))?
                }
                "cursor" => q.cursor = Some(decode_cursor(&v).ok_or_else(|| ApiError::bad_request("bad cursor"))?),
                _ => {}
            }
        }
        Ok(q)
    }
}

/// Opaque keyset cursor: hex of `<first_seen RFC 3339 µs>|<id>`.
pub fn encode_cursor(first_seen: DateTime<Utc>, id: Uuid) -> String {
    hex::encode(format!("{}|{id}", first_seen.to_rfc3339_opts(SecondsFormat::Micros, true)))
}

pub fn decode_cursor(s: &str) -> Option<(DateTime<Utc>, Uuid)> {
    let text = String::from_utf8(hex::decode(s).ok()?).ok()?;
    let (t, id) = text.split_once('|')?;
    Some((DateTime::parse_from_rfc3339(t).ok()?.with_timezone(&Utc), id.parse().ok()?))
}

fn build_feed_query(session: &Session, q: &FeedQuery) -> Result<QueryBuilder<'static, Postgres>, ApiError> {
    if let Some(t) = &q.team
        && !session.can_see(t)
    {
        return Err(ApiError::forbidden(format!("team `{t}` is not visible to this session")));
    }
    let visible: Vec<String> = session.teams.iter().map(|t| t.id.clone()).collect();
    let mut b: QueryBuilder<Postgres> = QueryBuilder::new(format!("SELECT {FEED_COLUMNS} FROM alerts WHERE team = ANY("));
    b.push_bind(visible).push(")");
    if let Some(t) = &q.team {
        b.push(" AND team = ").push_bind(t.clone());
    }
    if !q.status.is_empty() {
        b.push(" AND status = ANY(").push_bind(q.status.clone()).push(")");
    }
    if !q.severity.is_empty() {
        b.push(" AND severity = ANY(").push_bind(q.severity.clone()).push(")");
    }
    if !q.rule.is_empty() {
        b.push(" AND rule_id = ANY(").push_bind(q.rule.clone()).push(")");
    }
    if let Some((t, id)) = q.cursor {
        b.push(" AND (first_seen, id) < (").push_bind(t).push(", ").push_bind(id).push(")");
    }
    b.push(" ORDER BY first_seen DESC, id DESC LIMIT ").push_bind(q.limit + 1);
    Ok(b)
}

/// One feed page (`GET /api/alerts`).
pub async fn feed_page(pool: &PgPool, session: &Session, q: &FeedQuery) -> Result<(Vec<FeedAlert>, Option<String>), ApiError> {
    let mut b = build_feed_query(session, q)?;
    let rows = b.build().fetch_all(pool).await?;
    let mut items: Vec<FeedAlert> = rows.iter().map(row_to_feed).collect::<anyhow::Result<_>>()?;
    let next = if items.len() as i64 > q.limit {
        items.truncate(q.limit as usize);
        items.last().map(|a| encode_cursor(a.first_seen, a.id))
    } else {
        None
    };
    Ok((items, next))
}

/// Stream a feed page row-by-row, stopping once serialized alerts reach
/// `max_bytes` so SSE snapshots bound peak allocation during DB fetch.
pub async fn feed_page_budgeted(
    pool: &PgPool,
    session: &Session,
    q: &FeedQuery,
    max_bytes: usize,
) -> Result<(Vec<Value>, std::collections::HashMap<Uuid, i64>, Option<String>), ApiError> {
    use futures::TryStreamExt;
    let mut b = build_feed_query(session, q)?;
    let mut stream = b.build().fetch(pool);
    let mut items = Vec::new();
    let mut seen = std::collections::HashMap::new();
    let mut bytes = 0usize;
    let mut last_cursor = None;
    let mut has_more = false;
    while let Some(row) = stream.try_next().await? {
        if items.len() as i64 >= q.limit {
            has_more = true;
            break;
        }
        let a = row_to_feed(&row)?;
        let v = api_alert(&a);
        let sz = v.to_string().len();
        if !items.is_empty() && bytes + sz > max_bytes {
            has_more = true;
            break;
        }
        bytes += sz;
        seen.insert(a.id, a.version);
        last_cursor = Some((a.first_seen, a.id));
        items.push(v);
    }
    let next = has_more.then_some(last_cursor).flatten().map(|(t, id)| encode_cursor(t, id));
    Ok((items, seen, next))
}

pub async fn list(State(state): State<ApiState>, RawQuery(raw): RawQuery) -> Result<Json<Value>, ApiError> {
    let q = FeedQuery::parse(raw.as_deref())?;
    let (items, next) = feed_page(&state.pool, &state.session, &q).await?;
    Ok(Json(json!({"items": items.iter().map(api_alert).collect::<Vec<_>>(), "next_cursor": next})))
}

fn parse_id(id: &str) -> Result<Uuid, ApiError> {
    id.parse().map_err(|_| ApiError::not_found())
}

async fn load_alert(pool: &PgPool, session: &Session, id: Uuid) -> Result<FeedAlert, ApiError> {
    let row = sqlx::query(&format!("SELECT {FEED_COLUMNS} FROM alerts WHERE id = $1"))
        .bind(id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(ApiError::not_found)?;
    let a = row_to_feed(&row)?;
    if !session.can_see(&a.team) {
        // Same answer as a missing alert, so ids of other teams don't leak.
        return Err(ApiError::not_found());
    }
    Ok(a)
}

pub async fn detail(State(state): State<ApiState>, Path(id): Path<String>) -> Result<Json<Value>, ApiError> {
    let id = parse_id(&id)?;
    let a = load_alert(&state.pool, &state.session, id).await?;
    let jobs = sqlx::query(
        "SELECT id, recipient_id, channel, address, status, attempts, last_error, last_error_class, sent_at, updated_at
         FROM delivery_jobs WHERE alert_id = $1 ORDER BY created_at, id",
    )
    .bind(id)
    .fetch_all(&state.pool)
    .await?;
    let mut deliveries = Vec::with_capacity(jobs.len());
    let mut timeline = timeline(&a);
    for j in &jobs {
        let recipient = format!("user:{}", j.try_get::<String, _>("recipient_id")?);
        let channel: String = j.try_get("channel")?;
        let status: String = j.try_get("status")?;
        let error_class: Option<String> = j.try_get("last_error_class")?;
        let sent_at: Option<DateTime<Utc>> = j.try_get("sent_at")?;
        if let Some(at) = sent_at {
            timeline.push(json!({"at": ts(at), "actor": "system", "kind": "notified",
                "message": format!("Sent by {channel} to {}", recipient.trim_start_matches("user:"))}));
        }
        deliveries.push(json!({
            "id": j.try_get::<Uuid, _>("id")?.to_string(),
            "recipient": recipient,
            "channel": channel,
            "address": mask_address(&j.try_get::<String, _>("address")?),
            "state": status,
            "attempts": j.try_get::<i32, _>("attempts")?,
            "last_error": j.try_get::<Option<String>, _>("last_error")?.map(|e| safe_error_summary(&status, error_class.as_deref(), &e)),
            "sent_at": sent_at.map(ts),
            "updated_at": ts(j.try_get("updated_at")?),
        }));
    }
    // Delivery's audit trail (runbook steps, approvals, SMS acks, escalations,
    // digests). An ack/resolve it recorded replaces the one derived from the
    // alert row, so the timeline shows it once, with its channel.
    let actor = beacon_delivery::api::Actor(state.session.actor());
    let delivery = beacon_delivery::api::timeline_entries(&state.pool, id).await?;
    for kind in ["acked", "resolved"] {
        if delivery.iter().any(|e| e["kind"] == kind) {
            timeline.retain(|e| e["kind"] != kind);
        }
    }
    timeline.extend(delivery);
    timeline.sort_by(|x, y| x["at"].as_str().cmp(&y["at"].as_str()));
    let runbook = beacon_delivery::api::runbook_view(&state.pool, id, &actor).await?;
    let mut v = api_alert(&a);
    let o = v.as_object_mut().expect("alert is an object");
    o.insert("samples".into(), json!([]));
    o.insert("runbook".into(), runbook.unwrap_or(Value::Null));
    o.insert("deliveries".into(), Value::Array(deliveries));
    o.insert("timeline".into(), Value::Array(timeline));
    Ok(Json(v))
}

/// History derived from the alert row. Messages are plain sentences for
/// non-engineers; acked/resolved are phrased by the UI from actor and reason.
fn timeline(a: &FeedAlert) -> Vec<Value> {
    let mut t = vec![json!({"at": ts(a.first_seen), "actor": "system", "kind": "opened",
        "message": format!("Alert rule \u{201c}{}\u{201d} raised this alert", a.rule.name)})];
    if a.count > 1 {
        t.push(json!({"at": ts(a.last_seen), "actor": "system", "kind": "deduplicated",
            "message": format!("Happened again ({} times so far)", a.count)}));
    }
    if let (Some(by), Some(at)) = (&a.acked_by, a.acked_at) {
        t.push(json!({"at": ts(at), "actor": by, "kind": "acked", "message": "Acknowledged"}));
    }
    if let Some(at) = a.resolved_at {
        let by = a.resolved_by.clone().unwrap_or_else(|| "system".into());
        t.push(json!({"at": ts(at), "actor": by, "kind": "resolved", "message": "Resolved"}));
    }
    t
}

#[derive(Debug, Deserialize)]
pub struct TransitionRequest {
    pub expected_version: i64,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Transition {
    Ack,
    Resolve,
}

pub async fn ack(state: State<ApiState>, headers: HeaderMap, id: Path<String>, body: Body) -> Result<Json<Value>, ApiError> {
    transition(state, headers, id, body, Transition::Ack).await
}

pub async fn resolve(state: State<ApiState>, headers: HeaderMap, id: Path<String>, body: Body) -> Result<Json<Value>, ApiError> {
    transition(state, headers, id, body, Transition::Resolve).await
}

/// CSRF is checked before the body is extracted or parsed, and every body
/// problem is an `ApiError` (the contract's error shape), not a framework rejection.
async fn transition(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Body,
    t: Transition,
) -> Result<Json<Value>, ApiError> {
    state.session.check_csrf(&headers)?;
    let body = axum::body::to_bytes(body, MAX_TRANSITION_BODY)
        .await
        .map_err(|_| {
            ApiError::new(
                axum::http::StatusCode::PAYLOAD_TOO_LARGE,
                "too_large",
                format!("body exceeds {MAX_TRANSITION_BODY} bytes"),
            )
        })?;
    let req: TransitionRequest =
        serde_json::from_slice(&body).map_err(|e| ApiError::bad_request(format!("body must be {{\"expected_version\": <int>}}: {e}")))?;
    if req.note.as_ref().is_some_and(|n| n.chars().count() > 2000) {
        return Err(ApiError::bad_request("note is longer than 2000 characters"));
    }
    let id = parse_id(&id)?;
    let a = apply_transition(&state.pool, &state.session, id, req.expected_version, t).await?;
    Ok(Json(api_alert(&a)))
}

/// Ack or resolve via the shared engine transition (also used by delivery).
/// Team scoping is checked first; the transition itself takes the row lock.
pub async fn apply_transition(pool: &PgPool, session: &Session, id: Uuid, expected_version: i64, t: Transition) -> Result<FeedAlert, ApiError> {
    // Visibility first (team never changes), so other teams' ids don't leak.
    load_alert(pool, session, id).await?;
    let engine_t = match t {
        Transition::Ack => engine_alerts::Transition::Ack,
        Transition::Resolve => engine_alerts::Transition::Resolve { reason: "user".into() },
    };
    let mut tx = pool.begin().await?;
    match engine_alerts::transition(&mut tx, id, engine_t, &session.actor(), Some(expected_version)).await? {
        Ok(a) => {
            tx.commit().await?;
            Ok(a)
        }
        Err(engine_alerts::TransitionError::NotFound) => Err(ApiError::not_found()),
        Err(engine_alerts::TransitionError::InvalidTransition(current)) => {
            Err(ApiError::conflict("invalid_transition", "a resolved alert cannot be acknowledged", api_alert(&current)))
        }
        Err(engine_alerts::TransitionError::VersionConflict(current)) => Err(ApiError::conflict(
            "version_conflict",
            format!("status changed to {} after version {expected_version}", current.status),
            api_alert(&current),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_contact_details() {
        assert_eq!(mask_address("anna@example.test"), "a***@example.test");
        assert_eq!(mask_address("+44 7700 900123"), "+44 *** 123");
        assert_eq!(
            safe_error_summary("pending", Some("permanent"), "550 rejected <bob@example.test> ?token=xyz 07700900123"),
            "delivery failed (550)"
        );
        assert_eq!(
            safe_error_summary("pending", Some("transient"), "timeout dialing 07700900123"),
            "delivery failed (transient)"
        );
        assert_eq!(safe_error_summary("cancelled", None, "alert acked"), "delivery cancelled");
    }

    #[test]
    fn caps_repeated_filter_params() {
        let many: String = (0..65).map(|i| format!("rule=r{i}")).collect::<Vec<_>>().join("&");
        assert!(FeedQuery::parse(Some(&many)).is_err());
    }

    #[test]
    fn cursor_round_trip() {
        let t = DateTime::parse_from_rfc3339("2026-10-23T09:14:02.331456Z").unwrap().with_timezone(&Utc);
        let id = Uuid::now_v7();
        assert_eq!(decode_cursor(&encode_cursor(t, id)), Some((t, id)));
        assert_eq!(decode_cursor("zz"), None);
    }

    #[test]
    fn parses_repeated_filters() {
        let q = FeedQuery::parse(Some("team=rates&status=open&status=acked&severity=critical&rule=a&rule=b&limit=10")).unwrap();
        assert_eq!(q.team.as_deref(), Some("rates"));
        assert_eq!(q.status, vec!["open", "acked"]);
        assert_eq!(q.rule, vec!["a", "b"]);
        assert_eq!(q.limit, 10);
        assert!(FeedQuery::parse(Some("status=nope")).is_err());
        assert!(FeedQuery::parse(Some("limit=0")).is_err());
        assert!(FeedQuery::parse(Some("limit=5001")).is_err());
        assert_eq!(FeedQuery::parse(None).unwrap().limit, DEFAULT_LIMIT);
    }

    #[test]
    fn field_values_are_rendered_strings() {
        let a: FeedAlert = serde_json::from_value(json!({
            "id": Uuid::nil(), "team": "rates", "version": 1,
            "rule": {"id": "r", "revision": 1, "name": "R"}, "status": "open", "severity": "warning",
            "color": "amber", "title": "t", "body": "", "count": 1,
            "fields": [
                {"path": "raw.n", "label": "N", "value": 25000000},
                {"path": "raw.s", "label": "S", "value": "x"},
                {"path": "raw.meta", "label": "Meta", "value": {"password": "secret", "ok": 1}},
                {"path": "raw.legacy", "label": "Legacy", "value": "{\"authorization\":[\"raw-secret\"]…"}
            ],
            "first_seen": "2026-10-23T09:14:02.331Z", "last_seen": "2026-10-23T09:14:02.331Z",
            "source": "s", "schema": "x", "acked_by": null, "acked_at": null, "resolved_by": null,
            "resolved_at": null, "resolved_reason": null, "last_event_id": "e1"
        }))
        .unwrap();
        let v = api_alert(&a);
        assert_eq!(v["fields"][0]["value"], json!("25000000"));
        assert_eq!(v["fields"][1]["value"], json!("x"));
        assert!(!v["fields"][2]["value"].as_str().unwrap().contains("secret"), "legacy object field must be redacted before stringify");
        assert_eq!(v["fields"][3]["value"], json!("[truncated]"), "legacy truncated JSON previews must be suppressed");
        assert!(v.get("last_event_id").is_none());
        assert_eq!(v["first_seen"], json!("2026-10-23T09:14:02.331Z"));
    }
}
