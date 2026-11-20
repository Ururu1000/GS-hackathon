//! Fake SMS/phone provider (role `gateway`, default `:8070`). It stands in for an external
//! provider: it persists every accepted message in `gateway_receipts` (deduplicated by job id)
//! BEFORE replying, and can simulate outages. It also serves the device-simulator read API that
//! the UI uses (board #101/#102/#103); `router()` can be mounted by beacon-api later.
//!
//! Authentication (delivery review, "unauthenticated gateway on 0.0.0.0"): by default the
//! gateway runs without auth (dev mode; `serve` logs a `warn!` about it). Configure a token
//! with [`GatewayState::with_token`] and the control endpoints — `POST /provider/{sms|phone}`,
//! `POST /api/simulator/outage`, `DELETE /api/simulator/outage` — require
//! `Authorization: Bearer <token>` (compared in constant time) and answer `401 unauthorized`
//! otherwise. The read endpoints the UI's device simulator uses (`GET /api/simulator/messages`,
//! `GET /api/simulator/stream`, `GET /api/simulator/outage`) stay open.
//!
//! Request limits: the provider and outage POST routes cap bodies at 16 KiB
//! (`DefaultBodyLimit` → `413`), and `ProviderRequest` fields are validated (text <= 1600
//! chars, to <= 32 chars of `+`/digits/spaces, recipient <= 128, approval_challenge <= 32,
//! no NUL bytes → `400 bad_request`), which also bounds the SSE broadcast ring.
//!
//! Provider side (called by `channel::gateway_client::GatewayChannel`):
//! - `POST /provider/sms`, `POST /provider/phone`: body `ProviderRequest`, header
//!   `Idempotency-Key: <job_id>`. Insert `ON CONFLICT (job_id) DO UPDATE SET duplicates =
//!   duplicates + 1, last_seen_at = now()`; reply `200 ProviderResponse{receipt_id, duplicate}`.
//!   During an outage of that channel: mode `down` → `503` + `Retry-After: <OUTAGE_RETRY_AFTER_S>`
//!   without persisting; mode `lose_response` → persist as above, then reply `504`.
//!
//! Simulator side (UI, board #102):
//! - `GET /api/simulator/messages?to=&recipient=&channel=&limit=&cursor=` →
//!   `{items: [SimMessage], next_cursor: string|null}`, newest first, keyset on
//!   `(received_at, id)`, opaque base64 cursor, `limit` default 50, max 500, cursors longer
//!   than 256 chars → `400`.
//! - `GET /api/simulator/stream` (SSE): first `snapshot` `{items, next_cursor}`, then one
//!   `simulator.message` event (a `SimMessage`) per new or duplicated receipt; `: ping` every
//!   15 s; header `X-Accel-Buffering: no`.
//! - `POST /api/simulator/outage` `{channel: sms|phone, seconds, mode?: down|lose_response}`
//!   (default mode `down`) → `{outages}`; `DELETE /api/simulator/outage?channel=` ends it early;
//!   `GET /api/simulator/outage` → `{outages: [{channel, mode, until}]}` (expired ones omitted).
//! - Errors: `{error: {code, message}}` with 400/401/404/413. 500s are always the fixed
//!   `{code: "internal", message: "internal error"}` (the real error is logged, never leaked).

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::rejection::{BytesRejection, FailedToBufferBody, JsonRejection};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{delete, get, post};
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{error, warn};
use uuid::Uuid;

use crate::Shutdown;
use crate::channel::gateway_client::ProviderRequest;

/// `Retry-After` seconds returned while a channel is down.
pub const OUTAGE_RETRY_AFTER_S: u64 = 2;

/// Default page size for the simulator feed.
const DEFAULT_LIMIT: i64 = 50;
/// Max page size for the simulator feed.
const MAX_LIMIT: i64 = 500;
/// Snapshot page size for a new SSE stream.
const SNAPSHOT_LIMIT: i64 = 50;

/// Request-body cap on the provider and outage POST routes (`DefaultBodyLimit` → `413`).
const REQUEST_BODY_LIMIT: usize = 16 * 1024;
/// `ProviderRequest::text` bound in chars (SMS segment stack at most; also bounds every
/// persisted row, the SSE broadcast ring and the simulator UI payload).
const PROVIDER_TEXT_MAX_CHARS: usize = 1600;
/// `ProviderRequest::to` bound in chars.
const PROVIDER_TO_MAX_CHARS: usize = 32;
/// `ProviderRequest::recipient` bound in chars.
const PROVIDER_RECIPIENT_MAX_CHARS: usize = 128;
/// `ProviderRequest::approval_challenge` bound in chars.
const PROVIDER_CHALLENGE_MAX_CHARS: usize = 32;
/// Cursor strings longer than this are rejected before decoding.
const MAX_CURSOR_CHARS: usize = 256;

/// One `gateway_receipts` row as the UI sees it (board #102).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SimMessage {
    pub id: i64,
    pub receipt_id: Uuid,
    pub job_id: Uuid,
    pub alert_id: Uuid,
    /// `sms` | `phone`.
    pub channel: String,
    /// `user:<id>`.
    pub recipient: String,
    pub to: String,
    pub text: String,
    pub approval_challenge: Option<String>,
    pub duplicates: i32,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutageMode {
    Down,
    LoseResponse,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Outage {
    pub channel: String,
    pub mode: OutageMode,
    pub until: DateTime<Utc>,
}

/// Shared state: the pool, the in-memory outage table, a broadcast of new receipts for SSE,
/// and the optional bearer token for the control (provider + outage) endpoints.
#[derive(Clone)]
pub struct GatewayState {
    pool: PgPool,
    outages: Arc<RwLock<HashMap<String, Outage>>>,
    tx: tokio::sync::broadcast::Sender<SimMessage>,
    /// `Some` → the control endpoints require `Authorization: Bearer <token>`; `None` →
    /// no auth (dev; `serve` warns about it once).
    token: Option<String>,
    /// M5b (agent C): where `POST /api/simulator/reply` forwards replies (`None` → the
    /// endpoint answers `503 unavailable`).
    reply_forward_url: Option<String>,
    /// M5b (agent C): HMAC key for the forwarded replies' `X-Beacon-Signature`.
    reply_secret: Option<Vec<u8>>,
    /// M5b (agent B): mock action-target outcomes + call log (see the **Mock targets**
    /// section of `crate::actions`). In-memory only, shared by every handler.
    mocks: Arc<RwLock<MockState>>,
}

impl GatewayState {
    pub fn new(pool: PgPool) -> Self {
        let (tx, _rx) = tokio::sync::broadcast::channel(1024);
        Self {
            pool,
            outages: Arc::new(RwLock::new(HashMap::new())),
            tx,
            token: None,
            reply_forward_url: None,
            reply_secret: None,
            mocks: Arc::new(RwLock::new(MockState::default())),
        }
    }

    /// Require `Authorization: Bearer <token>` (constant-time compare) on
    /// `POST /provider/{sms|phone}`, `POST /api/simulator/outage` and
    /// `DELETE /api/simulator/outage`. `None` switches auth off again (dev). The read
    /// endpoints the UI's device simulator uses stay open either way.
    pub fn with_token(mut self, token: Option<String>) -> Self {
        self.token = token;
        self
    }

    /// Start an outage programmatically (tests, demo); same effect as `POST /api/simulator/outage`.
    pub fn set_outage(&self, channel: &str, mode: OutageMode, seconds: u64) {
        let until = Utc::now() + chrono::Duration::seconds(seconds as i64);
        self.outages.write().expect("outage lock").insert(
            channel.to_string(),
            Outage {
                channel: channel.to_string(),
                mode,
                until,
            },
        );
    }

    pub fn clear_outage(&self, channel: &str) {
        self.outages.write().expect("outage lock").remove(channel);
    }

    /// The still-active outage for a channel, if any. Expired entries are removed and ignored.
    fn active_outage(&self, channel: &str) -> Option<Outage> {
        let mut outages = self.outages.write().expect("outage lock");
        match outages.get(channel) {
            Some(o) if o.until > Utc::now() => Some(o.clone()),
            Some(_) => {
                outages.remove(channel);
                None
            }
            None => None,
        }
    }

    /// All still-active outages, oldest expiry first; prunes expired entries.
    fn active_outages(&self) -> Vec<Outage> {
        let mut outages = self.outages.write().expect("outage lock");
        let now = Utc::now();
        outages.retain(|_, o| o.until > now);
        let mut out: Vec<Outage> = outages.values().cloned().collect();
        out.sort_by(|a, b| a.until.cmp(&b.until).then(a.channel.cmp(&b.channel)));
        out
    }
}

/// M5b (agent C): the device-simulator routes only (`/api/simulator/{messages,stream,outage}`
/// and `POST /api/simulator/reply`), for beacon-api to mount on its origin (board #171). The
/// `/provider/*` and `/mock/*` routes stay on the gateway role.
pub fn simulator_router(state: GatewayState) -> axum::Router {
    axum::Router::new()
        .route("/api/simulator/messages", get(messages_handler))
        .route("/api/simulator/stream", get(stream_handler))
        .route(
            "/api/simulator/outage",
            post(outage_post_handler).layer(DefaultBodyLimit::max(REQUEST_BODY_LIMIT)),
        )
        .route("/api/simulator/outage", get(outage_get_handler))
        .route("/api/simulator/outage", delete(outage_delete_handler))
        .route(
            "/api/simulator/reply",
            post(simulator_reply_handler).layer(DefaultBodyLimit::max(REQUEST_BODY_LIMIT)),
        )
        .fallback(fallback_404)
        .with_state(state)
}

impl GatewayState {
    /// M5b (agent C): where `POST /api/simulator/reply {from, text}` forwards replies:
    /// `POST {url}/api/replies` with `X-Beacon-Signature: hex(hmac_sha256(secret, body))`.
    /// `None` → the reply endpoint answers `503 {error:{code:"unavailable"}}`.
    pub fn with_reply_forward(self, url: Option<String>, secret: Option<Vec<u8>>) -> Self {
        Self {
            reply_forward_url: url,
            reply_secret: secret,
            ..self
        }
    }

    /// M5b (agent B): reset the mock action targets (in-memory outcomes + call log);
    /// mock outcomes default to `ok`.
    pub fn with_mocks(self) -> Self {
        *self.mocks.write().expect("mock lock") = MockState::default();
        self
    }
}

/// All gateway routes (provider + simulator) on one router. The two body-consuming POST
/// routes are capped at [`REQUEST_BODY_LIMIT`] (a longer body is rejected with `413` before
/// any handler or field validation runs).
pub fn router(state: GatewayState) -> axum::Router {
    axum::Router::new()
        .route(
            "/provider/{channel}",
            post(provider_handler).layer(DefaultBodyLimit::max(REQUEST_BODY_LIMIT)),
        )
        .route("/api/simulator/messages", get(messages_handler))
        .route("/api/simulator/stream", get(stream_handler))
        .route(
            "/api/simulator/outage",
            post(outage_post_handler).layer(DefaultBodyLimit::max(REQUEST_BODY_LIMIT)),
        )
        .route("/api/simulator/outage", get(outage_get_handler))
        .route("/api/simulator/outage", delete(outage_delete_handler))
        // M5b (agent B): mock action targets + the mock control API. The health route
        // is a wildcard so both `/mock/health/<name>` and deeper alias-mapped paths
        // (e.g. `/mock/health/counterparty-service/health`) answer.
        .route("/mock/health/{*rest}", get(mock_health_handler))
        .route(
            "/mock/workflows/{id}/retry",
            post(mock_workflow_retry_handler),
        )
        .route(
            "/mock/services/{name}/restart",
            post(mock_service_restart_handler),
        )
        .route("/mock/tickets", post(mock_ticket_handler))
        .route("/api/simulator/mock", get(mock_status_handler))
        .route(
            "/api/simulator/mock",
            post(mock_set_handler).layer(DefaultBodyLimit::max(REQUEST_BODY_LIMIT)),
        )
        // M5b (agent C): the fake phone's reply endpoint, forwarded to the delivery API.
        .route(
            "/api/simulator/reply",
            post(simulator_reply_handler).layer(DefaultBodyLimit::max(REQUEST_BODY_LIMIT)),
        )
        .fallback(fallback_404)
        .with_state(state)
}

/// Bind `addr` and serve `router(state)` until shutdown. Returns the bound address (so tests can
/// pass port 0) through `on_bound` before serving.
pub async fn serve(
    state: GatewayState,
    addr: SocketAddr,
    mut shutdown: Shutdown,
    on_bound: impl FnOnce(SocketAddr) + Send,
) -> std::io::Result<()> {
    if state.token.is_none() {
        warn!(
            "gateway accepts UNAUTHENTICATED provider and outage requests; anyone reachable \
             can send messages and trigger outages. Configure GatewayState::with_token(Some(..)) \
             for anything exposed past localhost."
        );
    }
    let listener = tokio::net::TcpListener::bind(addr).await?;
    on_bound(listener.local_addr()?);
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move {
            let _ = shutdown.wait_for(|v| *v).await;
        })
        .await
}

// ---------------------------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------------------------

fn error_response(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    let body = serde_json::json!({
        "error": { "code": code, "message": message.into() }
    });
    (status, Json(body)).into_response()
}

/// Fixed body for every 500: the real error is logged with `error!` and never leaked to the
/// client (raw `sqlx::Error` text used to end up in HTTP bodies).
fn internal_error_response() -> Response {
    error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal",
        "internal error",
    )
}

/// Map a `JsonRejection` from a body-limited route: the body-limit rejection becomes
/// `413 bad_request`, every other malformed body stays `400 bad_request`.
fn json_rejection_response(e: JsonRejection) -> Response {
    if matches!(
        &e,
        JsonRejection::BytesRejection(BytesRejection::FailedToBufferBody(
            FailedToBufferBody::LengthLimitError(_)
        ))
    ) {
        error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "bad_request",
            format!("request body exceeds {REQUEST_BODY_LIMIT} bytes"),
        )
    } else {
        error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("invalid JSON body: {e}"),
        )
    }
}

async fn fallback_404() -> Response {
    error_response(StatusCode::NOT_FOUND, "not_found", "unknown route")
}

fn is_valid_channel(channel: &str) -> bool {
    channel == "sms" || channel == "phone"
}

// ---------------------------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------------------------

/// Constant-time byte comparison: no early exit, so the response time never reveals how
/// many leading bytes of the token matched. (A plain `==` would.)
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = (a.len() != b.len()) as u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// The token part of `Authorization: Bearer <token>` (scheme case-insensitive), or `None`
/// when the header is missing or malformed.
fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let (scheme, token) = value.split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then_some(token)
}

/// `None` when the request may proceed, `Some(401 response)` when a token is configured and
/// the request did not carry it (exactly). With no token configured (dev) everything passes.
fn require_bearer(state: &GatewayState, headers: &HeaderMap) -> Option<Response> {
    let expected = state.token.as_deref()?;
    match bearer_token(headers) {
        Some(supplied) if constant_time_eq(expected.as_bytes(), supplied.as_bytes()) => None,
        _ => Some(error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "missing or invalid Authorization: Bearer token",
        )),
    }
}

// ---------------------------------------------------------------------------------------------
// Provider side
// ---------------------------------------------------------------------------------------------

/// Field bounds for [`ProviderRequest`]: `text` <= 1600 chars, `to` <= 32 chars of
/// `+`/digits/spaces, `recipient` <= 128, `approval_challenge` <= 32, and no NUL bytes
/// anywhere (a NUL used to reach the persist query and turn into a 500). Violations are
/// `400 bad_request`. The bounds also keep one receipt — and the SSE broadcast that carries
/// it — small no matter what the caller sends.
fn validate_provider_request(req: &ProviderRequest) -> Option<&'static str> {
    // NUL bytes first, whatever the length: a NUL used to reach the persist query and turn
    // into a 500, so it is refused with its own message before any other rule.
    if req.text.contains('\0') {
        return Some("text must not contain NUL bytes");
    }
    if req.to.contains('\0') {
        return Some("to must not contain NUL bytes");
    }
    if req.recipient.contains('\0') {
        return Some("recipient must not contain NUL bytes");
    }
    if let Some(challenge) = req.approval_challenge.as_deref()
        && challenge.contains('\0')
    {
        return Some("approval_challenge must not contain NUL bytes");
    }
    if req.text.chars().count() > PROVIDER_TEXT_MAX_CHARS {
        return Some("text exceeds 1600 chars");
    }
    if req.to.chars().count() > PROVIDER_TO_MAX_CHARS
        || !req
            .to
            .chars()
            .all(|c| c.is_ascii_digit() || c == '+' || c == ' ')
    {
        return Some("to must be at most 32 chars of +, digits and spaces");
    }
    if req.recipient.chars().count() > PROVIDER_RECIPIENT_MAX_CHARS {
        return Some("recipient exceeds 128 chars");
    }
    if let Some(challenge) = req.approval_challenge.as_deref()
        && challenge.chars().count() > PROVIDER_CHALLENGE_MAX_CHARS
    {
        return Some("approval_challenge exceeds 32 chars");
    }
    None
}

async fn provider_handler(
    State(state): State<GatewayState>,
    Path(channel): Path<String>,
    headers: HeaderMap,
    body: Result<Json<ProviderRequest>, JsonRejection>,
) -> Response {
    // Auth first: no token, no parsing, no persisting.
    if let Some(resp) = require_bearer(&state, &headers) {
        return resp;
    }
    if !is_valid_channel(&channel) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("unknown channel {channel:?}; expected sms|phone"),
        );
    }
    let req = match body {
        Ok(Json(req)) => req,
        Err(e) => return json_rejection_response(e),
    };
    if let Some(key) = headers.get("idempotency-key") {
        let key = key.to_str().unwrap_or_default();
        if key != req.job_id.to_string() {
            return error_response(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "Idempotency-Key must equal job_id",
            );
        }
    }
    if let Some(problem) = validate_provider_request(&req) {
        return error_response(StatusCode::BAD_REQUEST, "bad_request", problem);
    }

    // Outage gate: `down` rejects without persisting; `lose_response` persists then drops.
    if let Some(outage) = state.active_outage(&channel) {
        if outage.mode == OutageMode::Down {
            let headers = [(
                axum::http::header::RETRY_AFTER,
                HeaderValue::from_str(&OUTAGE_RETRY_AFTER_S.to_string())
                    .expect("retry-after header"),
            )];
            let body = Json(serde_json::json!({
                "error": { "code": "outage", "message": format!("channel {channel} is down") }
            }));
            return (StatusCode::SERVICE_UNAVAILABLE, headers, body).into_response();
        }
        // LoseResponse: fall through to persist, then reply 504 below.
        match persist_receipt(&state.pool, &channel, &req).await {
            Ok((msg, _duplicate)) => {
                let _ = state.tx.send(msg);
                return error_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    "lose_response",
                    format!("channel {channel} accepted the message but the response was lost"),
                );
            }
            Err(e) => {
                error!(channel = %channel, job_id = %req.job_id, error = %e,
                       "cannot persist gateway receipt");
                return internal_error_response();
            }
        }
    }

    match persist_receipt(&state.pool, &channel, &req).await {
        Ok((msg, duplicate)) => {
            let receipt_id = msg.receipt_id;
            let _ = state.tx.send(msg.clone());
            tracing::info!(
                channel = %channel,
                to = %req.to,
                job_id = %req.job_id,
                duplicate,
                "gateway accepted message"
            );
            (
                StatusCode::OK,
                Json(crate::channel::gateway_client::ProviderResponse {
                    receipt_id,
                    duplicate,
                }),
            )
                .into_response()
        }
        Err(e) => {
            error!(channel = %channel, job_id = %req.job_id, error = %e,
                   "cannot persist gateway receipt");
            internal_error_response()
        }
    }
}

/// The single upsert statement owned by the provider side. Returns the row and whether this
/// `job_id` had already been accepted (`xmax <> 0` is true only for the `DO UPDATE` path).
async fn persist_receipt(
    pool: &PgPool,
    channel: &str,
    req: &ProviderRequest,
) -> Result<(SimMessage, bool), sqlx::Error> {
    let row: PgRow = sqlx::query(
        "INSERT INTO gateway_receipts \
            (job_id, alert_id, channel, recipient, to_address, text, approval_challenge) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         ON CONFLICT (job_id) DO UPDATE SET \
            duplicates = gateway_receipts.duplicates + 1, last_seen_at = now() \
         RETURNING id, receipt_id, job_id, alert_id, channel, recipient, to_address, text, \
            approval_challenge, duplicates, received_at, (xmax <> 0) AS duplicate",
    )
    .bind(req.job_id)
    .bind(req.alert_id)
    .bind(channel)
    .bind(&req.recipient)
    .bind(&req.to)
    .bind(&req.text)
    .bind(req.approval_challenge.as_deref())
    .fetch_one(pool)
    .await?;
    let duplicate: bool = row.try_get("duplicate")?;
    Ok((row_to_message(&row)?, duplicate))
}

fn row_to_message(row: &PgRow) -> Result<SimMessage, sqlx::Error> {
    Ok(SimMessage {
        id: row.try_get("id")?,
        receipt_id: row.try_get("receipt_id")?,
        job_id: row.try_get("job_id")?,
        alert_id: row.try_get("alert_id")?,
        channel: row.try_get("channel")?,
        recipient: row.try_get("recipient")?,
        to: row.try_get("to_address")?,
        text: row.try_get("text")?,
        approval_challenge: row.try_get("approval_challenge")?,
        duplicates: row.try_get("duplicates")?,
        received_at: row.try_get("received_at")?,
    })
}

// ---------------------------------------------------------------------------------------------
// Simulator: messages list (keyset pagination)
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct MessagesQuery {
    to: Option<String>,
    recipient: Option<String>,
    channel: Option<String>,
    limit: Option<i64>,
    cursor: Option<String>,
}

/// Keyset position: the last row of the previous page. Serialized as JSON, then base64.
#[derive(Debug, Serialize, Deserialize)]
struct FeedCursor {
    received_at: DateTime<Utc>,
    id: i64,
}

fn encode_cursor(cursor: &FeedCursor) -> String {
    let json = serde_json::to_string(cursor).expect("cursor JSON");
    base64_encode(json.as_bytes())
}

fn decode_cursor(s: &str) -> Option<FeedCursor> {
    let bytes = base64_decode(s)?;
    serde_json::from_slice(&bytes).ok()
}

/// Fetch the newest page after `cursor` (exclusive). Returns `(items, next_cursor)`.
async fn fetch_page(
    pool: &PgPool,
    to: Option<&str>,
    recipient: Option<&str>,
    channel: Option<&str>,
    limit: i64,
    cursor: Option<&FeedCursor>,
) -> Result<(Vec<SimMessage>, Option<String>), sqlx::Error> {
    use sqlx::QueryBuilder;
    let mut qb: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
        "SELECT id, receipt_id, job_id, alert_id, channel, recipient, to_address, text, \
            approval_challenge, duplicates, received_at FROM gateway_receipts WHERE 1 = 1",
    );
    if let Some(to) = to {
        qb.push(" AND to_address = ").push_bind(to);
    }
    if let Some(recipient) = recipient {
        qb.push(" AND recipient = ").push_bind(recipient);
    }
    if let Some(channel) = channel {
        qb.push(" AND channel = ").push_bind(channel);
    }
    if let Some(cur) = cursor {
        qb.push(" AND (received_at, id) < (")
            .push_bind(cur.received_at)
            .push(", ")
            .push_bind(cur.id)
            .push(")");
    }
    qb.push(" ORDER BY received_at DESC, id DESC LIMIT ")
        .push_bind(limit + 1);
    let rows: Vec<PgRow> = qb.build().fetch_all(pool).await?;
    let mut items: Vec<SimMessage> = rows.iter().map(row_to_message).collect::<Result<_, _>>()?;
    let next_cursor = if items.len() as i64 > limit {
        items.truncate(limit as usize);
        let last = items.last().expect("non-empty page");
        Some(encode_cursor(&FeedCursor {
            received_at: last.received_at,
            id: last.id,
        }))
    } else {
        None
    };
    Ok((items, next_cursor))
}

async fn messages_handler(
    State(state): State<GatewayState>,
    Query(q): Query<MessagesQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let cursor: Option<FeedCursor> = match q.cursor {
        Some(ref s) => {
            // Cap before decoding: a multi-megabyte cursor must not be base64-decoded or
            // JSON-parsed at all.
            if s.chars().count() > MAX_CURSOR_CHARS {
                return error_response(StatusCode::BAD_REQUEST, "bad_request", "cursor too long");
            }
            match decode_cursor(s) {
                Some(c) => Some(c),
                None => {
                    return error_response(
                        StatusCode::BAD_REQUEST,
                        "bad_request",
                        "invalid cursor",
                    );
                }
            }
        }
        None => None,
    };
    match fetch_page(
        &state.pool,
        q.to.as_deref(),
        q.recipient.as_deref(),
        q.channel.as_deref(),
        limit,
        cursor.as_ref(),
    )
    .await
    {
        Ok((items, next_cursor)) => (
            StatusCode::OK,
            Json(serde_json::json!({ "items": items, "next_cursor": next_cursor })),
        )
            .into_response(),
        Err(e) => {
            error!(error = %e, "cannot read simulator feed");
            internal_error_response()
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Simulator: SSE stream
// ---------------------------------------------------------------------------------------------

async fn stream_handler(State(state): State<GatewayState>) -> Response {
    let (items, next_cursor) =
        match fetch_page(&state.pool, None, None, None, SNAPSHOT_LIMIT, None).await {
            Ok(page) => page,
            Err(e) => {
                error!(error = %e, "cannot read simulator feed");
                return internal_error_response();
            }
        };
    let snapshot = serde_json::json!({ "items": items, "next_cursor": next_cursor }).to_string();

    let rx = state.tx.subscribe();
    // Skip broadcast-lag gaps; the stream only ends when the gateway state is dropped.
    let live = BroadcastStream::new(rx).filter_map(|res| async move {
        match res {
            Ok(msg) => {
                let data = serde_json::to_string(&msg).unwrap_or_default();
                Some(Ok::<Event, Infallible>(
                    Event::default().event("simulator.message").data(data),
                ))
            }
            Err(_) => None,
        }
    });
    let first = futures::stream::once(async move {
        Ok::<Event, Infallible>(Event::default().event("snapshot").data(snapshot))
    });
    let stream = first.chain(live);
    let sse = Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    );
    let mut resp = sse.into_response();
    resp.headers_mut().insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    resp
}

// ---------------------------------------------------------------------------------------------
// Simulator: outages
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct OutagePost {
    channel: String,
    seconds: u64,
    #[serde(default)]
    mode: Option<OutageMode>,
}

fn outages_body(outages: Vec<Outage>) -> serde_json::Value {
    serde_json::json!({ "outages": outages })
}

async fn outage_post_handler(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    body: Result<Json<OutagePost>, JsonRejection>,
) -> Response {
    // Auth first: with a token configured, outage control is operator-only.
    if let Some(resp) = require_bearer(&state, &headers) {
        return resp;
    }
    let req = match body {
        Ok(Json(req)) => req,
        Err(e) => return json_rejection_response(e),
    };
    if !is_valid_channel(&req.channel) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("unknown channel {:?}; expected sms|phone", req.channel),
        );
    }
    if req.seconds == 0 || req.seconds > 3600 {
        return error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "seconds must be in 1..=3600",
        );
    }
    state.set_outage(
        &req.channel,
        req.mode.unwrap_or(OutageMode::Down),
        req.seconds,
    );
    (StatusCode::OK, Json(outages_body(state.active_outages()))).into_response()
}

async fn outage_get_handler(State(state): State<GatewayState>) -> Response {
    (StatusCode::OK, Json(outages_body(state.active_outages()))).into_response()
}

#[derive(Debug, Deserialize)]
struct OutageDeleteQuery {
    channel: Option<String>,
}

async fn outage_delete_handler(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    Query(q): Query<OutageDeleteQuery>,
) -> Response {
    // Auth first (the matching GET stays open for the UI's simulator).
    if let Some(resp) = require_bearer(&state, &headers) {
        return resp;
    }
    match q.channel {
        Some(channel) if is_valid_channel(&channel) => {
            state.clear_outage(&channel);
            (StatusCode::OK, Json(outages_body(state.active_outages()))).into_response()
        }
        Some(channel) => error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("unknown channel {channel:?}; expected sms|phone"),
        ),
        None => error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "missing query parameter: channel",
        ),
    }
}

// ---------------------------------------------------------------------------------------------
// Simulator: SMS replies (agent C)
// ---------------------------------------------------------------------------------------------

/// `POST /api/simulator/reply {from, text}` — the fake phone's reply path. Requires the
/// gateway token like the other control endpoints; `from` is an E.164 phone, `text` at most
/// one SMS segment. Forwards the exact request bytes to `{reply_forward_url}/api/replies`
/// (signed with the reply secret) and relays that endpoint's status + JSON body.
#[derive(Debug, Deserialize)]
struct SimulatorReply {
    from: String,
    text: String,
}

async fn simulator_reply_handler(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    // Auth first, like the other simulator control endpoints.
    if let Some(resp) = require_bearer(&state, &headers) {
        return resp;
    }
    let reply: SimulatorReply = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "bad_request",
                format!("invalid JSON body: {e}"),
            );
        }
    };
    if reply.from.chars().count() > 32
        || !reply
            .from
            .chars()
            .all(|c| c.is_ascii_digit() || c == '+' || c == ' ')
        || !reply.from.chars().any(|c| c.is_ascii_digit())
    {
        return error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "from must be an E.164 phone number (digits, optional +)",
        );
    }
    if reply.text.contains('\0') {
        return error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "text must not contain NUL bytes",
        );
    }
    if reply.text.trim().is_empty() || reply.text.chars().count() > 160 {
        return error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "text must be 1..=160 chars",
        );
    }
    // The reply is recorded as a `simulator.message`-style log line (no new table: receipts
    // are for messages the provider accepts, a reply never becomes one).
    tracing::info!(
        from = %reply.from,
        text = %reply.text,
        "simulator.message: sms reply accepted"
    );

    let (Some(forward_url), Some(secret)) = (
        state.reply_forward_url.as_deref(),
        state.reply_secret.as_deref(),
    ) else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "no delivery API configured to receive SMS replies",
        );
    };

    // Sign the exact bytes we forward, so `/api/replies` verifies what it reads.
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(&body);
    let signature = hex::encode(mac.finalize().into_bytes());

    // No redirects, 5 s timeout: a broken delivery API must not park the simulator.
    let client = match reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "cannot build the reply forwarding http client");
            return internal_error_response();
        }
    };
    let url = format!("{}/api/replies", forward_url.trim_end_matches('/'));
    let forwarded = client
        .post(&url)
        .header("X-Beacon-Signature", signature)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(body.as_ref().to_vec())
        .send()
        .await;

    let mut resp = match forwarded {
        Ok(resp) => resp,
        Err(e) => {
            warn!(url = %url, error = %e, "cannot forward the simulator reply");
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "cannot reach the delivery API to forward this reply",
            );
        }
    };
    let status = match StatusCode::from_u16(resp.status().as_u16()) {
        Ok(s) => s,
        Err(_) => {
            return error_response(
                StatusCode::BAD_GATEWAY,
                "bad_gateway",
                "the delivery API answered with an invalid status",
            );
        }
    };
    // The relayed body is bounded (4 KiB): a hostile or broken upstream must not be buffered
    // whole.
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < FORWARD_BODY_MAX_BYTES {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                let room = FORWARD_BODY_MAX_BYTES - buf.len();
                if chunk.len() > room {
                    buf.extend_from_slice(&chunk[..room]);
                    break;
                }
                buf.extend_from_slice(&chunk);
            }
            Ok(None) | Err(_) => break,
        }
    }
    match serde_json::from_slice::<serde_json::Value>(&buf) {
        Ok(value) => (status, Json(value)).into_response(),
        Err(e) => {
            warn!(url = %url, error = %e, "the delivery API did not answer with JSON");
            error_response(
                StatusCode::BAD_GATEWAY,
                "bad_gateway",
                "the delivery API did not answer with JSON",
            )
        }
    }
}

// Cap on the bytes read from the delivery API's relayed response (its bodies are tiny).
const FORWARD_BODY_MAX_BYTES: usize = 4096;

// ---------------------------------------------------------------------------------------------
// Base64 (standard alphabet; decode also accepts URL-safe chars and missing padding so that
// cursors survive query-string round trips).
// ---------------------------------------------------------------------------------------------

const B64_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64_ALPHABET[((n >> 18) & 63) as usize] as char);
        out.push(B64_ALPHABET[((n >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(B64_ALPHABET[((n >> 6) & 63) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(B64_ALPHABET[(n & 63) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn base64_decode_value(c: u8) -> Option<u32> {
    match c {
        b'A'..=b'Z' => Some((c - b'A') as u32),
        b'a'..=b'z' => Some((c - b'a' + 26) as u32),
        b'0'..=b'9' => Some((c - b'0' + 52) as u32),
        b'+' | b'-' => Some(62),
        b'/' | b'_' => Some(63),
        _ => None,
    }
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let mut clean: Vec<u8> = s.bytes().filter(|&b| b != b'=').collect();
    // Tolerate missing padding.
    while !clean.len().is_multiple_of(4) {
        clean.push(b'A');
    }
    let mut out = Vec::with_capacity(clean.len() / 4 * 3);
    for chunk in clean.chunks(4) {
        let mut n = 0u32;
        for &c in chunk {
            n = (n << 6) | base64_decode_value(c)?;
        }
        out.push((n >> 16) as u8);
        out.push((n >> 8) as u8);
        out.push(n as u8);
    }
    // Strip the zero bytes implied by `=` padding (or by the padding we added):
    // output length = floor(unpadded_len * 6 / 8).
    let unpadded = s.bytes().filter(|&b| b != b'=').count();
    out.truncate(unpadded * 6 / 8);
    Some(out)
}

// ---------------------------------------------------------------------------------------------
// Mock action targets (M5b, agent B; owned by `crate::actions`)
// ---------------------------------------------------------------------------------------------

/// Outcome of one mock target. `Default` (and any unset target) is `ok`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MockMode {
    #[default]
    Ok,
    Fail,
}

impl MockMode {
    fn as_str(self) -> &'static str {
        match self {
            MockMode::Ok => "ok",
            MockMode::Fail => "fail",
        }
    }
}

/// One recorded mock-target call, oldest first in `GET /api/simulator/mock`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MockCall {
    /// `workflows` | `services` | `health` | `tickets`.
    target: String,
    method: String,
    path: String,
    at: DateTime<Utc>,
}

/// In-memory mock state: per-target outcomes + the call log. Modes default to `ok`.
#[derive(Debug, Clone, Default)]
struct MockState {
    modes: HashMap<String, MockMode>,
    calls: VecDeque<MockCall>,
}

/// The call log is bounded; the oldest entries drop first.
const MAX_MOCK_CALLS: usize = 5000;

fn is_mock_target(target: &str) -> bool {
    matches!(target, "workflows" | "services" | "health" | "tickets")
}

/// Record one mock call and return that target's current mode (`ok` when unset).
fn record_mock_call(state: &GatewayState, target: &str, method: &str, path: &str) -> MockMode {
    let mut mocks = state.mocks.write().expect("mock lock");
    while mocks.calls.len() >= MAX_MOCK_CALLS {
        mocks.calls.pop_front();
    }
    mocks.calls.push_back(MockCall {
        target: target.to_string(),
        method: method.to_string(),
        path: path.to_string(),
        at: Utc::now(),
    });
    mocks.modes.get(target).copied().unwrap_or_default()
}

/// Every mock failure is the same body: `500 {"error": "mock failure"}`.
fn mock_failure_response() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({ "error": "mock failure" })),
    )
        .into_response()
}

async fn mock_health_handler(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
) -> Response {
    let mode = record_mock_call(&state, "health", "GET", &format!("/mock/health/{name}"));
    match mode {
        MockMode::Ok => (
            StatusCode::OK,
            Json(serde_json::json!({ "status": "healthy" })),
        )
            .into_response(),
        MockMode::Fail => mock_failure_response(),
    }
}

async fn mock_workflow_retry_handler(
    State(state): State<GatewayState>,
    Path(id): Path<String>,
    _body: Bytes,
) -> Response {
    let mode = record_mock_call(
        &state,
        "workflows",
        "POST",
        &format!("/mock/workflows/{id}/retry"),
    );
    match mode {
        MockMode::Ok => (
            StatusCode::OK,
            Json(serde_json::json!({ "ok": true, "workflow": id })),
        )
            .into_response(),
        MockMode::Fail => mock_failure_response(),
    }
}

async fn mock_service_restart_handler(
    State(state): State<GatewayState>,
    Path(name): Path<String>,
    _body: Bytes,
) -> Response {
    let mode = record_mock_call(
        &state,
        "services",
        "POST",
        &format!("/mock/services/{name}/restart"),
    );
    match mode {
        MockMode::Ok => (
            StatusCode::OK,
            Json(serde_json::json!({ "ok": true, "service": name })),
        )
            .into_response(),
        MockMode::Fail => mock_failure_response(),
    }
}

async fn mock_ticket_handler(State(state): State<GatewayState>, _body: Bytes) -> Response {
    let mode = record_mock_call(&state, "tickets", "POST", "/mock/tickets");
    match mode {
        MockMode::Ok => (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response(),
        MockMode::Fail => mock_failure_response(),
    }
}

#[derive(Debug, Deserialize)]
struct MockSetRequest {
    target: String,
    mode: String,
}

/// Switch one mock target's outcome. Needs the gateway token exactly like the outage
/// control endpoints; unknown targets and modes are `400 bad_request`.
async fn mock_set_handler(
    State(state): State<GatewayState>,
    headers: HeaderMap,
    body: Result<Json<MockSetRequest>, JsonRejection>,
) -> Response {
    // Auth first, like the outage control endpoints.
    if let Some(resp) = require_bearer(&state, &headers) {
        return resp;
    }
    let req = match body {
        Ok(Json(req)) => req,
        Err(e) => return json_rejection_response(e),
    };
    if !is_mock_target(&req.target) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!(
                "unknown mock target {:?}; expected workflows|services|health|tickets",
                req.target
            ),
        );
    }
    let mode = match req.mode.as_str() {
        "ok" => MockMode::Ok,
        "fail" => MockMode::Fail,
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "bad_request",
                format!("unknown mock mode {:?}; expected ok|fail", req.mode),
            );
        }
    };
    state
        .mocks
        .write()
        .expect("mock lock")
        .modes
        .insert(req.target.clone(), mode);
    (
        StatusCode::OK,
        Json(serde_json::json!({ "target": req.target, "mode": mode.as_str() })),
    )
        .into_response()
}

/// `GET /api/simulator/mock` → `{modes, calls}`. Open (no auth), like the other
/// simulator read endpoints.
async fn mock_status_handler(State(state): State<GatewayState>) -> Response {
    let mocks = state.mocks.read().expect("mock lock");
    let mode_of = |target: &str| mocks.modes.get(target).copied().unwrap_or_default();
    let modes = serde_json::json!({
        "workflows": mode_of("workflows"),
        "services": mode_of("services"),
        "health": mode_of("health"),
        "tickets": mode_of("tickets"),
    });
    (
        StatusCode::OK,
        Json(serde_json::json!({ "modes": modes, "calls": mocks.calls })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider_req(text: &str, to: &str, recipient: &str) -> ProviderRequest {
        ProviderRequest {
            job_id: Uuid::new_v4(),
            alert_id: Uuid::new_v4(),
            recipient: recipient.to_string(),
            to: to.to_string(),
            text: text.to_string(),
            approval_challenge: None,
        }
    }

    #[test]
    fn bearer_token_parses_the_scheme_case_insensitively() {
        let mut headers = HeaderMap::new();
        assert_eq!(bearer_token(&headers), None, "no header at all");
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("Basic dXNlcjpwdw=="),
        );
        assert_eq!(bearer_token(&headers), None, "non-bearer scheme");
        for (raw, expected) in [
            ("Bearer sekrit", Some("sekrit")),
            ("bearer sekrit", Some("sekrit")),
            ("BEARER sekrit", Some("sekrit")),
            ("Bearer ", Some("")),
            ("Bearer", None),
        ] {
            headers.insert(
                axum::http::header::AUTHORIZATION,
                HeaderValue::from_static(raw),
            );
            assert_eq!(bearer_token(&headers), expected, "{raw:?}");
        }
    }

    #[test]
    fn constant_time_eq_matches_only_identical_values() {
        assert!(constant_time_eq(b"token", b"token"));
        assert!(constant_time_eq(b"", b""));
        assert!(!constant_time_eq(b"token", b"tokeN"));
        assert!(!constant_time_eq(b"token", b"tokenn"));
        assert!(!constant_time_eq(b"tokenn", b"token"));
        assert!(!constant_time_eq(b"", b"token"));
        // Same length, differs only in the last byte: the classic early-exit leak case.
        assert!(!constant_time_eq(b"aaaaaaaX", b"aaaaaaaY"));
    }

    #[test]
    fn provider_validation_accepts_in_bounds_requests() {
        let req = provider_req(&"x".repeat(1600), "+1 55 5000 1111", "user:anna");
        assert_eq!(validate_provider_request(&req), None);
        let mut with_challenge = provider_req("hi", "+1555", "user:anna");
        with_challenge.approval_challenge = Some("8-char".to_string());
        assert_eq!(validate_provider_request(&with_challenge), None);
    }

    #[test]
    fn provider_validation_rejects_oversize_fields() {
        let oversize_text = provider_req(&"x".repeat(1601), "+1555", "user:anna");
        assert_eq!(
            validate_provider_request(&oversize_text),
            Some("text exceeds 1600 chars")
        );
        let oversize_to = provider_req("hi", &"+".repeat(33), "user:anna");
        assert_eq!(
            validate_provider_request(&oversize_to),
            Some("to must be at most 32 chars of +, digits and spaces")
        );
        let oversize_recipient = provider_req("hi", "+1555", &"u".repeat(129));
        assert_eq!(
            validate_provider_request(&oversize_recipient),
            Some("recipient exceeds 128 chars")
        );
        let mut oversize_challenge = provider_req("hi", "+1555", "user:anna");
        oversize_challenge.approval_challenge = Some("c".repeat(33));
        assert_eq!(
            validate_provider_request(&oversize_challenge),
            Some("approval_challenge exceeds 32 chars")
        );
    }

    #[test]
    fn provider_validation_rejects_bad_to_and_nul_bytes() {
        for to in ["+155a5", "4412-3", "+44 111\t", "☎555"] {
            let req = provider_req("hi", to, "user:anna");
            assert_eq!(
                validate_provider_request(&req),
                Some("to must be at most 32 chars of +, digits and spaces"),
                "to={to:?}"
            );
        }
        // NUL bytes used to turn into a 500 deep inside the persist path.
        let nul_text = provider_req("a\0b", "+1555", "user:anna");
        assert_eq!(
            validate_provider_request(&nul_text),
            Some("text must not contain NUL bytes")
        );
        let nul_to = provider_req("hi", "+155\u{0}5", "user:anna");
        assert_eq!(
            validate_provider_request(&nul_to),
            Some("to must not contain NUL bytes")
        );
        let nul_recipient = provider_req("hi", "+1555", "user:\0anna");
        assert_eq!(
            validate_provider_request(&nul_recipient),
            Some("recipient must not contain NUL bytes")
        );
        let mut nul_challenge = provider_req("hi", "+1555", "user:anna");
        nul_challenge.approval_challenge = Some("c\0".to_string());
        assert_eq!(
            validate_provider_request(&nul_challenge),
            Some("approval_challenge must not contain NUL bytes")
        );
    }

    #[test]
    fn cursor_round_trip() {
        let cur = FeedCursor {
            received_at: Utc::now(),
            id: 42,
        };
        let s = encode_cursor(&cur);
        let back = decode_cursor(&s).expect("decode");
        assert_eq!(back.id, 42);
        assert_eq!(back.received_at, cur.received_at);
        assert!(decode_cursor("!!!not-base64!!!").is_none());
        // URL-safe variant decodes too.
        let urlsafe: String = s
            .chars()
            .map(|c| match c {
                '+' => '-',
                '/' => '_',
                c => c,
            })
            .filter(|&c| c != '=')
            .collect();
        let back = decode_cursor(&urlsafe).expect("url-safe decode");
        assert_eq!(back.id, 42);
    }

    #[test]
    fn base64_vectors() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        assert_eq!(base64_decode("Zg=="), Some(b"f".to_vec()));
        assert_eq!(base64_decode("Zm8="), Some(b"fo".to_vec()));
        assert_eq!(base64_decode("Zm9v"), Some(b"foo".to_vec()));
        assert_eq!(base64_decode("Zm9vYmFy"), Some(b"foobar".to_vec()));
        assert_eq!(base64_decode("not valid!!!"), None);
    }
}
