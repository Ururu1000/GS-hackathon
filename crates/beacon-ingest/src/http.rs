//! HTTP webhook ingest.
//!
//! ```text
//! POST /ingest/{source}         any JSON           → 202 {id} | 400 | 413 | 429 | 503
//! POST /ingest/{source}/batch   JSON array         → 202 {ids, accepted} | ...
//! GET  /ready
//! ```
//!
//! `Idempotency-Key` keeps the event id stable across client retries.
//!
//! Every POST must say `Content-Type: application/json` (or a `+json` type;
//! parameters such as `charset` are ignored), else `415`. A cross-site page
//! can only send `text/plain` or form bodies without a CORS preflight, which
//! this server never grants, so it can't make a visitor's browser inject
//! events. [`router_with_token`] additionally requires
//! `Authorization: Bearer <INGEST_TOKEN>` on the `/ingest` routes.

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use beacon_core::Event;
use futures::future::join_all;
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use crate::publisher::{EventPublisher, PublishError};

/// Below the broker's 1 MiB record limit, leaving room for the envelope.
pub const MAX_BODY: usize = 900 * 1024;
pub const MAX_BATCH: usize = 1000;
/// Bound how long a slow client may hold an in-flight permit while streaming a body.
pub const BODY_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Abstracts the broker so the handler can be tested without Kafka.
pub trait Sink: Send + Sync + 'static {
    fn publish(&self, event: Event) -> impl Future<Output = Result<(), PublishError>> + Send;
}

impl Sink for EventPublisher {
    async fn publish(&self, event: Event) -> Result<(), PublishError> {
        EventPublisher::publish(self, &event).await
    }
}

pub struct IngestState<S> {
    pub sink: S,
    /// Bounds in-flight events; when exhausted we answer 429.
    pub inflight: Semaphore,
}

impl<S: Sink> IngestState<S> {
    pub fn new(sink: S, max_inflight: usize) -> Arc<Self> {
        Arc::new(IngestState {
            sink,
            inflight: Semaphore::new(max_inflight),
        })
    }
}

pub fn router<S: Sink>(state: Arc<IngestState<S>>) -> Router {
    router_with_token(state, None)
}

/// [`router`], with `/ingest/...` requiring `Authorization: Bearer <token>`
/// when `token` is set (`/ready` stays open for health checks).
pub fn router_with_token<S: Sink>(state: Arc<IngestState<S>>, token: Option<String>) -> Router {
    let routes = ingest_routes(state);
    let routes = match token {
        Some(t) => token_gated(routes, Arc::from(t)),
        None => routes,
    };
    routes.route("/ready", get(|| async { "ok" }))
}

/// Requests to `routes` need `Authorization: Bearer <token>` (constant-time compare).
fn token_gated(routes: Router, token: Arc<str>) -> Router {
    routes.layer(axum::middleware::from_fn(move |req: axum::extract::Request, next: axum::middleware::Next| {
        let token = token.clone();
        async move {
            let supplied = req
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.split_once(' '))
                .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
                .map(|(_, t)| t.trim());
            match supplied {
                Some(t) if constant_time_eq(t.as_bytes(), token.as_bytes()) => next.run(req).await,
                _ => {
                    let mut r = error(StatusCode::UNAUTHORIZED, "unauthorized", "missing or invalid Authorization: Bearer token");
                    r.headers_mut().insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
                    r
                }
            }
        }
    }))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// `application/json` or `application/<anything>+json`, parameters ignored.
#[allow(clippy::result_large_err)] // the Err is a ready-made HTTP response
fn require_json(headers: &HeaderMap) -> Result<(), Response> {
    let media = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .map(|v| v.trim().to_ascii_lowercase())
        .unwrap_or_default();
    let json = media == "application/json" || (media.starts_with("application/") && media.ends_with("+json"));
    if json {
        Ok(())
    } else {
        Err(error(StatusCode::UNSUPPORTED_MEDIA_TYPE, "unsupported_media_type", "Content-Type must be application/json"))
    }
}

/// Only the `/ingest/...` routes, for mounting next to another app.
pub fn ingest_routes<S: Sink>(state: Arc<IngestState<S>>) -> Router {
    Router::new()
        .route("/ingest/{source}", post(ingest_one::<S>))
        .route("/ingest/{source}/batch", post(ingest_batch::<S>))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .with_state(state)
}

fn error(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    let mut r = (status, Json(json!({"error": {"code": code, "message": message.into()}}))).into_response();
    if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::SERVICE_UNAVAILABLE {
        r.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    }
    r
}

fn valid_source(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars().all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c))
}

#[allow(clippy::result_large_err)] // the Err is a ready-made HTTP response
fn event_id(headers: &HeaderMap, source: &str) -> Result<String, Response> {
    match headers.get("idempotency-key") {
        None => Ok(uuid::Uuid::now_v7().to_string()),
        Some(v) => {
            let k = v.to_str().unwrap_or("");
            if k.is_empty() || k.len() > 128 || !k.chars().all(|c| c.is_ascii_graphic()) {
                return Err(error(StatusCode::BAD_REQUEST, "bad_idempotency_key", "1-128 printable ASCII characters"));
            }
            Ok(format!("http:{source}:{k}"))
        }
    }
}

/// Read the body only after an in-flight permit is held, so saturation is
/// answered with 429 before any request body is buffered or parsed. Bounded by
/// `BODY_READ_TIMEOUT` so slow clients cannot hold permits indefinitely.
#[allow(clippy::result_large_err)] // the Err is a ready-made HTTP response
async fn read_body(body: Body) -> Result<Bytes, Response> {
    read_body_with_timeout(body, BODY_READ_TIMEOUT).await
}

#[allow(clippy::result_large_err)] // the Err is a ready-made HTTP response
async fn read_body_with_timeout(body: Body, timeout: Duration) -> Result<Bytes, Response> {
    match tokio::time::timeout(timeout, axum::body::to_bytes(body, MAX_BODY)).await {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(_)) => Err(error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "body_too_large",
            format!("body exceeds {MAX_BODY} bytes"),
        )),
        Err(_) => Err(error(
            StatusCode::REQUEST_TIMEOUT,
            "body_timeout",
            "timed out reading request body",
        )),
    }
}

fn map_publish_error(e: PublishError) -> Response {
    match e {
        PublishError::Saturated => error(StatusCode::TOO_MANY_REQUESTS, "saturated", "broker queue full, retry"),
        PublishError::TooLarge => error(StatusCode::PAYLOAD_TOO_LARGE, "event_too_large", "event exceeds the broker record limit"),
        PublishError::Broker(m) => {
            tracing::warn!(error = %m, "broker publish failed");
            error(StatusCode::SERVICE_UNAVAILABLE, "broker_unavailable", "broker unavailable, retry")
        }
    }
}

async fn ingest_one<S: Sink>(
    State(state): State<Arc<IngestState<S>>>,
    Path(source): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if let Err(r) = require_json(&headers) {
        return r;
    }
    if !valid_source(&source) {
        return error(StatusCode::BAD_REQUEST, "bad_source", "source must match [A-Za-z0-9-_.]{1,64}");
    }
    let Ok(_permit) = state.inflight.try_acquire() else {
        return error(StatusCode::TOO_MANY_REQUESTS, "saturated", "too many in-flight events");
    };
    let body = match read_body(body).await {
        Ok(b) => b,
        Err(r) => return r,
    };
    let raw: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return error(StatusCode::BAD_REQUEST, "invalid_json", e.to_string()),
    };
    let id = match event_id(&headers, &source) {
        Ok(id) => id,
        Err(r) => return r,
    };
    let event = Event::new(id.clone(), format!("http:{source}"), raw);
    match state.sink.publish(event).await {
        Ok(()) => (StatusCode::ACCEPTED, Json(json!({"id": id}))).into_response(),
        Err(e) => map_publish_error(e),
    }
}

/// Batch variant for high-volume producers and the load generator. Ids are
/// `<Idempotency-Key>-<index>` when the header is given.
async fn ingest_batch<S: Sink>(
    State(state): State<Arc<IngestState<S>>>,
    Path(source): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if let Err(r) = require_json(&headers) {
        return r;
    }
    if !valid_source(&source) {
        return error(StatusCode::BAD_REQUEST, "bad_source", "source must match [A-Za-z0-9-_.]{1,64}");
    }
    let Ok(_first) = state.inflight.try_acquire() else {
        return error(StatusCode::TOO_MANY_REQUESTS, "saturated", "too many in-flight events");
    };
    let body = match read_body(body).await {
        Ok(b) => b,
        Err(r) => return r,
    };
    let items: Vec<Value> = match serde_json::from_slice(&body) {
        Ok(Value::Array(items)) => items,
        Ok(_) => return error(StatusCode::BAD_REQUEST, "not_an_array", "batch body must be a JSON array"),
        Err(e) => return error(StatusCode::BAD_REQUEST, "invalid_json", e.to_string()),
    };
    if items.len() > MAX_BATCH {
        return error(StatusCode::PAYLOAD_TOO_LARGE, "batch_too_large", format!("at most {MAX_BATCH} events"));
    }
    let base = match headers.get("idempotency-key") {
        Some(_) => match event_id(&headers, &source) {
            Ok(id) => Some(id),
            Err(r) => return r,
        },
        None => None,
    };
    // One permit is already held for the body; take one more per extra event.
    let extra = (items.len() as u32).saturating_sub(1);
    let _rest = match extra {
        0 => None,
        n => match state.inflight.try_acquire_many(n) {
            Ok(p) => Some(p),
            Err(_) => return error(StatusCode::TOO_MANY_REQUESTS, "saturated", "too many in-flight events"),
        },
    };
    let src = format!("http:{source}");
    let events: Vec<Event> = items
        .into_iter()
        .enumerate()
        .map(|(i, raw)| {
            let id = match &base {
                Some(b) => format!("{b}-{i}"),
                None => uuid::Uuid::now_v7().to_string(),
            };
            Event::new(id, src.clone(), raw)
        })
        .collect();
    let ids: Vec<String> = events.iter().map(|e| e.id.clone()).collect();
    let results = tokio::time::timeout(
        Duration::from_secs(15),
        join_all(events.into_iter().map(|e| state.sink.publish(e))),
    )
    .await;
    let results = match results {
        Ok(r) => r,
        Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "timeout", "broker did not acknowledge in time"),
    };
    if let Some(e) = results.into_iter().find_map(Result::err) {
        // Some events may have been accepted; the client retries the batch with
        // the same Idempotency-Key, so ids stay stable.
        return map_publish_error(e);
    }
    (StatusCode::ACCEPTED, Json(json!({"ids": ids, "accepted": ids.len()}))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use std::sync::Mutex;
    use tower::ServiceExt;

    #[derive(Default)]
    struct Mem {
        events: Mutex<Vec<Event>>,
        fail: Option<fn() -> PublishError>,
    }

    impl Sink for Arc<Mem> {
        async fn publish(&self, event: Event) -> Result<(), PublishError> {
            if let Some(f) = self.fail {
                return Err(f());
            }
            self.events.lock().unwrap().push(event);
            Ok(())
        }
    }

    async fn call(app: Router, req: Request<Body>) -> (StatusCode, Value) {
        let res = app.oneshot(req).await.unwrap();
        let status = res.status();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    fn post(uri: &str, body: &str) -> Request<Body> {
        Request::post(uri).header("content-type", "application/json").body(Body::from(body.to_string())).unwrap()
    }

    #[tokio::test]
    async fn requires_a_json_content_type() {
        let mem = Arc::new(Mem::default());
        let app = router(IngestState::new(mem.clone(), 10));
        // What a cross-site page can send without a CORS preflight.
        for ct in [None, Some("text/plain"), Some("application/x-www-form-urlencoded"), Some("multipart/form-data; boundary=x")] {
            let mut req = Request::post("/ingest/x");
            if let Some(ct) = ct {
                req = req.header("content-type", ct);
            }
            let (s, v) = call(app.clone(), req.body(Body::from("{}")).unwrap()).await;
            assert_eq!(s, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{ct:?}");
            assert_eq!(v["error"]["code"], "unsupported_media_type");
            let batch = Request::post("/ingest/x/batch").header("content-type", ct.unwrap_or("text/plain")).body(Body::from("[{}]")).unwrap();
            assert_eq!(call(app.clone(), batch).await.0, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }
        for ct in ["application/json", "Application/JSON; charset=utf-8", "application/cloudevents+json"] {
            let req = Request::post("/ingest/x").header("content-type", ct).body(Body::from("{}")).unwrap();
            assert_eq!(call(app.clone(), req).await.0, StatusCode::ACCEPTED, "{ct}");
        }
        assert_eq!(mem.events.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn token_guards_ingest_but_not_ready() {
        let mem = Arc::new(Mem::default());
        let app = router_with_token(IngestState::new(mem.clone(), 10), Some("s3cret".into()));
        let (s, v) = call(app.clone(), post("/ingest/x", "{}")).await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
        assert_eq!(v["error"]["code"], "unauthorized");
        for auth in ["Bearer wrong", "Bearer s3cre", "Basic s3cret", "s3cret"] {
            let mut req = post("/ingest/x/batch", "[{}]");
            req.headers_mut().insert(header::AUTHORIZATION, auth.parse().unwrap());
            assert_eq!(call(app.clone(), req).await.0, StatusCode::UNAUTHORIZED, "{auth}");
        }
        let mut req = post("/ingest/x", "{}");
        req.headers_mut().insert(header::AUTHORIZATION, "bearer s3cret".parse().unwrap());
        assert_eq!(call(app.clone(), req).await.0, StatusCode::ACCEPTED);
        assert_eq!(mem.events.lock().unwrap().len(), 1);
        let ready = app.oneshot(Request::get("/ready").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(ready.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn accepts_any_json_and_returns_id() {
        let mem = Arc::new(Mem::default());
        let app = router(IngestState::new(mem.clone(), 10));
        let (s, v) = call(app, post("/ingest/alertmanager", r#"{"anything": [1, 2]}"#)).await;
        assert_eq!(s, StatusCode::ACCEPTED);
        let ev = &mem.events.lock().unwrap()[0];
        assert_eq!(v["id"], json!(ev.id));
        assert_eq!(ev.source, "http:alertmanager");
        assert_eq!(ev.raw, json!({"anything": [1, 2]}));
    }

    #[tokio::test]
    async fn idempotency_key_makes_id_stable() {
        let mem = Arc::new(Mem::default());
        let app = router(IngestState::new(mem.clone(), 10));
        let req = Request::post("/ingest/x").header("content-type", "application/json").header("Idempotency-Key", "abc-1").body(Body::from("{}")).unwrap();
        let (_, v) = call(app, req).await;
        assert_eq!(v["id"], json!("http:x:abc-1"));
    }

    #[tokio::test]
    async fn rejects_bad_input() {
        let mem = Arc::new(Mem::default());
        let app = router(IngestState::new(mem.clone(), 10));
        assert_eq!(call(app.clone(), post("/ingest/x", "{nope")).await.0, StatusCode::BAD_REQUEST);
        assert_eq!(call(app.clone(), post("/ingest/a%20b", "{}")).await.0, StatusCode::BAD_REQUEST);
        let big = format!("\"{}\"", "x".repeat(MAX_BODY + 10));
        assert_eq!(call(app, post("/ingest/x", &big)).await.0, StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn maps_broker_errors() {
        let sat = Arc::new(Mem { fail: Some(|| PublishError::Saturated), ..Default::default() });
        let (s, _) = call(router(IngestState::new(sat, 10)), post("/ingest/x", "{}")).await;
        assert_eq!(s, StatusCode::TOO_MANY_REQUESTS);
        let down = Arc::new(Mem { fail: Some(|| PublishError::Broker("secret-broker:19092".into())), ..Default::default() });
        let (s, v) = call(router(IngestState::new(down, 10)), post("/ingest/x", "{}")).await;
        assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            !v["error"]["message"].as_str().unwrap_or("").contains("secret-broker"),
            "broker internals must not leak in HTTP 503: {v}"
        );
    }

    #[tokio::test]
    async fn slow_body_times_out_with_408() {
        let pending_body = Body::from_stream(futures::stream::pending::<Result<Bytes, std::io::Error>>());
        let err = read_body_with_timeout(pending_body, Duration::from_millis(20))
            .await
            .expect_err("slow body must time out");
        assert_eq!(err.status(), StatusCode::REQUEST_TIMEOUT);
    }

    #[tokio::test]
    async fn saturation_is_checked_before_the_body() {
        let mem = Arc::new(Mem::default());
        let app = router(IngestState::new(mem, 0));
        // Invalid JSON would be a 400 if the body were read first.
        assert_eq!(call(app, post("/ingest/x", "{nope")).await.0, StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn saturation_returns_429() {
        let mem = Arc::new(Mem::default());
        let app = router(IngestState::new(mem, 0));
        assert_eq!(call(app, post("/ingest/x", "{}")).await.0, StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn batch() {
        let mem = Arc::new(Mem::default());
        let app = router(IngestState::new(mem.clone(), 100));
        let req = Request::post("/ingest/lg/batch").header("content-type", "application/json").header("Idempotency-Key", "b1").body(Body::from("[{}, {\"a\": 1}]")).unwrap();
        let (s, v) = call(app.clone(), req).await;
        assert_eq!(s, StatusCode::ACCEPTED);
        assert_eq!(v["ids"], json!(["http:lg:b1-0", "http:lg:b1-1"]));
        assert_eq!(mem.events.lock().unwrap().len(), 2);
        assert_eq!(call(app, post("/ingest/lg/batch", "{}")).await.0, StatusCode::BAD_REQUEST);
    }
}
