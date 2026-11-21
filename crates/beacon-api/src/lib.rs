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
pub mod proposals;
pub mod rules;
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
    /// Configured Kafka sources (config/sources).
    pub sources: Arc<Vec<beacon_ingest::kafka_source::SourceSpec>>,
    /// Bounds concurrent SSE streams on this replica.
    pub streams: Arc<tokio::sync::Semaphore>,
}

/// Concurrent SSE streams per replica before new ones get 503.
pub const MAX_STREAMS: usize = 500;

pub fn router(state: ApiState, ui_dir: Option<PathBuf>) -> Router {
    router_with(state, ui_dir, None)
}

/// Like [`router`], optionally mounting the webhook ingest routes
/// (`/ingest/{source}[/batch]`) on the same origin for the UI's playground
/// and sample replay. There they require the session's `X-CSRF-Token`, so a
/// third-party page can't make a browser inject events.
pub fn router_with(state: ApiState, ui_dir: Option<PathBuf>, ingest: Option<Router>) -> Router {
    let session = state.session.clone();
    let mut api = Router::new()
        .route("/api/session", get(session::get_session))
        .route("/api/alerts", get(alerts::list))
        .route("/api/alerts/{id}", get(alerts::detail))
        .route("/api/alerts/{id}/ack", post(alerts::ack))
        .route("/api/alerts/{id}/resolve", post(alerts::resolve))
        .route("/api/stream", get(stream::stream))
        .route("/api/rules", get(rules::list).post(rules::create))
        .route("/api/rules/test", post(rules::test))
        .route("/api/rules/{id}", get(rules::get).put(rules::update).delete(rules::delete))
        .route("/api/events/recent", get(rules::recent_events))
        .route("/api/schemas", get(rules::schemas).post(rules::save_schema))
        .route("/api/schemas/preview", post(rules::preview_schema))
        .route("/api/proposals", get(proposals::list).post(proposals::create))
        .route("/api/proposals/count", get(proposals::count))
        .route("/api/proposals/{id}/{decision}", post(proposals::decide))
        .route("/api/schemas/discovered", get(rules::discovered))
        .route("/api/sources", get(rules::sources))
        .route("/ready", get(ready))
        // Unknown API paths are JSON 404s, never the UI's index.html.
        .route("/api/{*rest}", axum::routing::any(api_not_found))
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(state);
    if let Some(ingest) = ingest {
        let gated = ingest.layer(axum::middleware::from_fn(move |req: axum::extract::Request, next: axum::middleware::Next| {
            let session = session.clone();
            async move {
                if req.method() != axum::http::Method::GET
                    && let Err(e) = session.check_csrf(req.headers()) {
                        return axum::response::IntoResponse::into_response(e);
                    }
                next.run(req).await
            }
        }));
        api = api.merge(gated);
    }
    let app = match ui_dir {
        // The built UI (ui/dist) is served from the same origin, so the
        // session cookie and EventSource work without CORS.
        Some(dir) => {
            let index = dir.join("index.html");
            api.fallback_service(tower_http::services::ServeDir::new(dir).fallback(tower_http::services::ServeFile::new(index)))
        }
        None => api,
    };
    app.layer(axum::middleware::from_fn(security_headers))
}

/// Baseline security headers; API responses are never cached (they carry
/// session data and the CSRF token).
async fn security_headers(req: axum::extract::Request, next: axum::middleware::Next) -> axum::response::Response {
    use axum::http::{HeaderValue, header};
    let is_api = req.uri().path().starts_with("/api/");
    let mut res = next.run(req).await;
    let h = res.headers_mut();
    h.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    h.insert(header::REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    if is_api && !h.contains_key(header::CACHE_CONTROL) {
        h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
    res
}

async fn api_not_found() -> error::ApiError {
    error::ApiError::new(axum::http::StatusCode::NOT_FOUND, "not_found", "no such API endpoint")
}

async fn method_not_allowed() -> error::ApiError {
    error::ApiError::new(axum::http::StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed", "method not allowed")
}

async fn ready(axum::extract::State(state): axum::extract::State<ApiState>) -> axum::http::StatusCode {
    if *state.ready.borrow() {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    }
}
