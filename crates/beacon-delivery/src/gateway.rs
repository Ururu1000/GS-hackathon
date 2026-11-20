//! Fake SMS/phone provider (role `gateway`, default `:8070`). It stands in for an external
//! provider: it persists every accepted message in `gateway_receipts` (deduplicated by job id)
//! BEFORE replying, and can simulate outages. It also serves the device-simulator read API that
//! the UI uses (board #101/#102/#103); `router()` can be mounted by beacon-api later.
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
//!   `(received_at, id)`, opaque base64 cursor, `limit` default 50, max 500.
//! - `GET /api/simulator/stream` (SSE): first `snapshot` `{items, next_cursor}`, then one
//!   `simulator.message` event (a `SimMessage`) per new or duplicated receipt; `: ping` every
//!   15 s; header `X-Accel-Buffering: no`.
//! - `POST /api/simulator/outage` `{channel: sms|phone, seconds, mode?: down|lose_response}`
//!   (default mode `down`) → `{outages}`; `DELETE /api/simulator/outage?channel=` ends it early;
//!   `GET /api/simulator/outage` → `{outages: [{channel, mode, until}]}` (expired ones omitted).
//! - Errors: `{error: {code, message}}` with 400/404.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{delete, get, post};
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use tokio_stream::wrappers::BroadcastStream;
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

/// Shared state: the pool, the in-memory outage table, and a broadcast of new receipts for SSE.
#[derive(Clone)]
pub struct GatewayState {
    pool: PgPool,
    outages: Arc<RwLock<HashMap<String, Outage>>>,
    tx: tokio::sync::broadcast::Sender<SimMessage>,
}

impl GatewayState {
    pub fn new(pool: PgPool) -> Self {
        let (tx, _rx) = tokio::sync::broadcast::channel(1024);
        Self {
            pool,
            outages: Arc::new(RwLock::new(HashMap::new())),
            tx,
        }
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

/// All gateway routes (provider + simulator) on one router.
pub fn router(state: GatewayState) -> axum::Router {
    axum::Router::new()
        .route("/provider/{channel}", post(provider_handler))
        .route("/api/simulator/messages", get(messages_handler))
        .route("/api/simulator/stream", get(stream_handler))
        .route("/api/simulator/outage", post(outage_post_handler))
        .route("/api/simulator/outage", get(outage_get_handler))
        .route("/api/simulator/outage", delete(outage_delete_handler))
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

async fn fallback_404() -> Response {
    error_response(StatusCode::NOT_FOUND, "not_found", "unknown route")
}

fn is_valid_channel(channel: &str) -> bool {
    channel == "sms" || channel == "phone"
}

// ---------------------------------------------------------------------------------------------
// Provider side
// ---------------------------------------------------------------------------------------------

async fn provider_handler(
    State(state): State<GatewayState>,
    Path(channel): Path<String>,
    headers: HeaderMap,
    body: Result<Json<ProviderRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if !is_valid_channel(&channel) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("unknown channel {channel:?}; expected sms|phone"),
        );
    }
    let req = match body {
        Ok(Json(req)) => req,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "bad_request",
                format!("invalid JSON body: {e}"),
            );
        }
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
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    format!("cannot persist receipt: {e}"),
                );
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
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            format!("cannot persist receipt: {e}"),
        ),
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
        Some(ref s) => match decode_cursor(s) {
            Some(c) => Some(c),
            None => {
                return error_response(StatusCode::BAD_REQUEST, "bad_request", "invalid cursor");
            }
        },
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
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            format!("cannot read simulator feed: {e}"),
        ),
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
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    format!("cannot read simulator feed: {e}"),
                );
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
    body: Result<Json<OutagePost>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let req = match body {
        Ok(Json(req)) => req,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "bad_request",
                format!("invalid JSON body: {e}"),
            );
        }
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
    Query(q): Query<OutageDeleteQuery>,
) -> Response {
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

#[cfg(test)]
mod tests {
    use super::*;

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
