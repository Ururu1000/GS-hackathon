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
//! With a [`DeliveryMount`] ([`router_full`]) the same origin also serves
//! delivery's runbook/approval/people/teams routes and the device simulator.
//!
//! Every API replica consumes `alerts.updates` in its own consumer group and
//! fans the records out to its SSE clients ([`feed`]).

pub mod alerts;
pub mod error;
pub mod feed;
pub mod host;
pub mod mapping_suggest;
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

pub use host::{AllowedHosts, host_guard};
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

/// Delivery's routes, mounted on this origin behind the session (part b,
/// `beacon_delivery::api` and `beacon_delivery::gateway::simulator_router`).
pub struct DeliveryMount {
    /// Runbook steps, approvals, runbooks, people/teams, `/api/replies`.
    pub api: beacon_delivery::api::ApiState,
    /// The device simulator (`/api/simulator/*`). Pass the SAME
    /// `GatewayState` the gateway role serves: outages and the SSE broadcast
    /// live in memory, so a second state would show nothing and switch nothing.
    pub simulator: Option<SimulatorMount>,
}

pub struct SimulatorMount {
    pub state: beacon_delivery::gateway::GatewayState,
    /// `GATEWAY_TOKEN`: injected as `Authorization: Bearer` after the session
    /// check, so the browser never holds it.
    pub token: Option<String>,
}

/// Like [`router`], optionally mounting the webhook ingest routes
/// (`/ingest/{source}[/batch]`) on the same origin for the UI's playground
/// and sample replay. There they require the session's `X-CSRF-Token`, so a
/// third-party page can't make a browser inject events.
pub fn router_with(state: ApiState, ui_dir: Option<PathBuf>, ingest: Option<Router>) -> Router {
    router_full(state, ui_dir, ingest, None)
}

/// Like [`router_with`], also mounting delivery's routes ([`DeliveryMount`]).
///
/// Delivery's handlers get `Extension(Actor("user:<id>"))` and
/// `Extension(Admin(session.admin))` from the session, and every non-GET
/// needs the session's `X-CSRF-Token` — except `POST /api/replies`, which the
/// gateway calls server-to-server and which authenticates by its HMAC
/// signature (`REPLY_SECRET`) instead.
pub fn router_full(state: ApiState, ui_dir: Option<PathBuf>, ingest: Option<Router>, delivery: Option<DeliveryMount>) -> Router {
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
        .route("/api/schemas/suggest", post(mapping_suggest::suggest))
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
        api = api.merge(csrf_gated(ingest, session.clone()));
    }
    if let Some(d) = delivery {
        let s = session.clone();
        let routes = beacon_delivery::api::router(d.api).layer(axum::middleware::from_fn(
            move |mut req: axum::extract::Request, next: axum::middleware::Next| {
                let s = s.clone();
                async move {
                    if req.method() != axum::http::Method::GET
                        && req.uri().path() != "/api/replies"
                        // Signed email approval links (M7): the HMAC in the form body is the
                        // authorization, the session Actor is ignored there (beacon_delivery::api).
                        && req.uri().path() != "/api/approvals/email"
                        && let Err(e) = s.check_csrf(req.headers())
                    {
                        return axum::response::IntoResponse::into_response(e);
                    }
                    req.extensions_mut().insert(beacon_delivery::api::Actor(s.actor()));
                    req.extensions_mut().insert(beacon_delivery::api::Admin(s.admin));
                    next.run(req).await
                }
            },
        ));
        api = api.merge(routes);
        if let Some(sim) = d.simulator {
            // Callers validate the token up front ([`bearer_header`]); one that
            // still can't be a header value is a startup bug, not a silent 401.
            let bearer = sim.token.as_deref().map(|t| bearer_header(t).expect("GATEWAY_TOKEN is not a valid header value"));
            let routes = beacon_delivery::gateway::simulator_router(sim.state)
                .layer(axum::middleware::from_fn(move |mut req: axum::extract::Request, next: axum::middleware::Next| {
                    let bearer = bearer.clone();
                    async move {
                        // Never let a browser-supplied credential through: the
                        // session (and CSRF, below) is what authorizes here.
                        req.headers_mut().remove(axum::http::header::AUTHORIZATION);
                        if let Some(b) = bearer {
                            req.headers_mut().insert(axum::http::header::AUTHORIZATION, b);
                        }
                        next.run(req).await
                    }
                }));
            api = api.merge(csrf_gated(routes, session.clone()));
        }
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

/// `Authorization: Bearer <token>` for the simulator mount; `Err` if the
/// token can't be sent as an HTTP header (e.g. contains a newline).
pub fn bearer_header(token: &str) -> Result<axum::http::HeaderValue, axum::http::header::InvalidHeaderValue> {
    axum::http::HeaderValue::try_from(format!("Bearer {token}"))
}

/// Non-GET requests to `routes` need the session's `X-CSRF-Token`.
fn csrf_gated(routes: Router, session: Arc<Session>) -> Router {
    routes.layer(axum::middleware::from_fn(move |req: axum::extract::Request, next: axum::middleware::Next| {
        let session = session.clone();
        async move {
            if req.method() != axum::http::Method::GET
                && let Err(e) = session.check_csrf(req.headers())
            {
                return axum::response::IntoResponse::into_response(e);
            }
            next.run(req).await
        }
    }))
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
