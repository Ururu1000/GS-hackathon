//! HTTP webhook ingest.
//!
//! ```text
//! POST /ingest/{source}         any JSON           → 202 {id} | 400 | 413 | 429 | 503
//! POST /ingest/{source}/batch   JSON array         → 202 {ids, accepted} | ...
//! GET  /ready
//! ```
//!
//! `Idempotency-Key` keeps the event id stable across client retries.

use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::body::Bytes;
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
    Router::new()
        .route("/ingest/{source}", post(ingest_one::<S>))
        .route("/ingest/{source}/batch", post(ingest_batch::<S>))
        .route("/ready", get(|| async { "ok" }))
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

fn map_publish_error(e: PublishError) -> Response {
    match e {
        PublishError::Saturated => error(StatusCode::TOO_MANY_REQUESTS, "saturated", "broker queue full, retry"),
        PublishError::TooLarge => error(StatusCode::PAYLOAD_TOO_LARGE, "event_too_large", "event exceeds the broker record limit"),
        PublishError::Broker(m) => error(StatusCode::SERVICE_UNAVAILABLE, "broker_unavailable", m),
    }
}

async fn ingest_one<S: Sink>(
    State(state): State<Arc<IngestState<S>>>,
    Path(source): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !valid_source(&source) {
        return error(StatusCode::BAD_REQUEST, "bad_source", "source must match [A-Za-z0-9-_.]{1,64}");
    }
    let raw: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return error(StatusCode::BAD_REQUEST, "invalid_json", e.to_string()),
    };
    let id = match event_id(&headers, &source) {
        Ok(id) => id,
        Err(r) => return r,
    };
    let Ok(_permit) = state.inflight.try_acquire() else {
        return error(StatusCode::TOO_MANY_REQUESTS, "saturated", "too many in-flight events");
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
    body: Bytes,
) -> Response {
    if !valid_source(&source) {
        return error(StatusCode::BAD_REQUEST, "bad_source", "source must match [A-Za-z0-9-_.]{1,64}");
    }
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
    let n = items.len() as u32;
    let Ok(_permit) = state.inflight.try_acquire_many(n.max(1)) else {
        return error(StatusCode::TOO_MANY_REQUESTS, "saturated", "too many in-flight events");
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
        Request::post(uri).body(Body::from(body.to_string())).unwrap()
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
        let req = Request::post("/ingest/x").header("Idempotency-Key", "abc-1").body(Body::from("{}")).unwrap();
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
        let down = Arc::new(Mem { fail: Some(|| PublishError::Broker("down".into())), ..Default::default() });
        let (s, _) = call(router(IngestState::new(down, 10)), post("/ingest/x", "{}")).await;
        assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
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
        let req = Request::post("/ingest/lg/batch").header("Idempotency-Key", "b1").body(Body::from("[{}, {\"a\": 1}]")).unwrap();
        let (s, v) = call(app.clone(), req).await;
        assert_eq!(s, StatusCode::ACCEPTED);
        assert_eq!(v["ids"], json!(["http:lg:b1-0", "http:lg:b1-1"]));
        assert_eq!(mem.events.lock().unwrap().len(), 2);
        assert_eq!(call(app, post("/ingest/lg/batch", "{}")).await.0, StatusCode::BAD_REQUEST);
    }
}
