//! Beacon's HTTP API for the UI (SPEC §8, `ui/contract/openapi.yaml`).
//!
//! ```text
//! GET  /api/session                     demo session: user, visible teams, CSRF token
//! GET  /api/alerts?team=&status=&...    feed page (keyset on first_seen, id; newest first)
//! GET  /api/alerts/{id}                 detail: deliveries + timeline
//! POST /api/alerts/{id}/ack|resolve     {expected_version} → 200 | 409
//! GET  /api/stream?team=                SSE: snapshot, then alert.upsert
//! GET  /ready                           200 once the alerts.updates consumer is assigned
//! ```
//!
//! Every API replica consumes `alerts.updates` in its own consumer group and
//! fans the records out to its SSE clients ([`feed`]).

pub mod alerts;
pub mod error;
pub mod feed;
pub mod session;
pub mod stream;

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use beacon_core::FeedAlert;
use sqlx::PgPool;
use tokio::sync::{broadcast, watch};

pub use session::Session;

#[derive(Clone)]
pub struct ApiState {
    pub pool: PgPool,
    /// Live alert updates from `alerts.updates`, fanned out to SSE clients.
    pub updates: broadcast::Sender<FeedAlert>,
    /// True once this replica's `alerts.updates` consumer has partitions.
    pub ready: watch::Receiver<bool>,
    pub session: Arc<Session>,
}

pub fn router(state: ApiState, ui_dir: Option<PathBuf>) -> Router {
    let api = Router::new()
        .route("/api/session", get(session::get_session))
        .route("/api/alerts", get(alerts::list))
        .route("/api/alerts/{id}", get(alerts::detail))
        .route("/api/alerts/{id}/ack", post(alerts::ack))
        .route("/api/alerts/{id}/resolve", post(alerts::resolve))
        .route("/api/stream", get(stream::stream))
        .route("/ready", get(ready))
        .with_state(state);
    match ui_dir {
        // The built UI (ui/dist) is served from the same origin, so the
        // session cookie and EventSource work without CORS.
        Some(dir) => {
            let index = dir.join("index.html");
            api.fallback_service(tower_http::services::ServeDir::new(dir).fallback(tower_http::services::ServeFile::new(index)))
        }
        None => api,
    }
}

async fn ready(axum::extract::State(state): axum::extract::State<ApiState>) -> axum::http::StatusCode {
    if *state.ready.borrow() {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    }
}
