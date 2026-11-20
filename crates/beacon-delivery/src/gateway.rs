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

use std::net::SocketAddr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::Shutdown;

/// `Retry-After` seconds returned while a channel is down.
pub const OUTAGE_RETRY_AFTER_S: u64 = 2;

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
    // agent 2: pool, Arc<RwLock<HashMap<String, Outage>>>, broadcast::Sender<SimMessage>.
}

impl GatewayState {
    pub fn new(pool: PgPool) -> Self {
        let _ = pool;
        todo!("agent 2")
    }

    /// Start an outage programmatically (tests, demo); same effect as `POST /api/simulator/outage`.
    pub fn set_outage(&self, channel: &str, mode: OutageMode, seconds: u64) {
        let _ = (channel, mode, seconds);
        todo!("agent 2")
    }

    pub fn clear_outage(&self, channel: &str) {
        let _ = channel;
        todo!("agent 2")
    }
}

/// All gateway routes (provider + simulator) on one router.
pub fn router(state: GatewayState) -> axum::Router {
    let _ = state;
    todo!("agent 2")
}

/// Bind `addr` and serve `router(state)` until shutdown. Returns the bound address (so tests can
/// pass port 0) through `on_bound` before serving.
pub async fn serve(
    state: GatewayState,
    addr: SocketAddr,
    shutdown: Shutdown,
    on_bound: impl FnOnce(SocketAddr) + Send,
) -> std::io::Result<()> {
    let _ = (state, addr, shutdown, on_bound);
    todo!("agent 2")
}
