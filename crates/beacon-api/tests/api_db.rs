//! beacon-api against a real Postgres (needs DATABASE_URL; sqlx::test creates
//! a fresh database per test). Alerts are created through the real matcher +
//! aggregator from the Appendix A samples.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use beacon_api::{ApiState, Session, router};
use beacon_core::{Event, FeedAlert, load_config_dir};
use beacon_engine::aggregator::apply_batch;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::sync::{broadcast, watch};
use tower::ServiceExt;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

async fn seed(pool: &PgPool, samples: &[&str]) {
    let cfg = load_config_dir(&root().join("config")).unwrap();
    let mut matches = Vec::new();
    for (i, s) in samples.iter().enumerate() {
        let raw: Value = serde_json::from_str(&std::fs::read_to_string(root().join("samples/events").join(s)).unwrap()).unwrap();
        let (_, m, _) = cfg.process(Event::new(format!("e{i}"), "http:test".into(), raw));
        matches.extend(m);
        // Distinct first_seen per alert so feed order is deterministic.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    apply_batch(pool, &matches).await.unwrap();
}

struct Harness {
    app: Router,
    updates: broadcast::Sender<FeedAlert>,
    csrf: String,
}

fn harness(pool: PgPool) -> Harness {
    let session = Arc::new(Session::demo(&root().join("config"), "anna").unwrap());
    let (updates, _) = broadcast::channel(64);
    let (_ready_tx, ready) = watch::channel(true);
    let csrf = session.csrf_token.clone();
    let state = ApiState { pool, updates: updates.clone(), ready, session };
    Harness { app: router(state, None), updates, csrf }
}

async fn get(app: &Router, uri: &str) -> (StatusCode, Value) {
    let res = app.clone().oneshot(Request::get(uri).body(Body::empty()).unwrap()).await.unwrap();
    let s = res.status();
    let b = res.into_body().collect().await.unwrap().to_bytes();
    (s, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

async fn post(app: &Router, uri: &str, csrf: Option<&str>, body: Value) -> (StatusCode, Value) {
    let mut r = Request::post(uri).header("content-type", "application/json");
    if let Some(c) = csrf {
        r = r.header("x-csrf-token", c);
    }
    let res = app.clone().oneshot(r.body(Body::from(body.to_string())).unwrap()).await.unwrap();
    let s = res.status();
    let b = res.into_body().collect().await.unwrap().to_bytes();
    (s, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

#[sqlx::test(migrations = "../../migrations")]
async fn session_lists_configured_teams(pool: PgPool) {
    let h = harness(pool);
    let (s, v) = get(&h.app, "/api/session").await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["user"]["id"], json!("anna"));
    let teams: Vec<&str> = v["teams"].as_array().unwrap().iter().map(|t| t["id"].as_str().unwrap()).collect();
    assert!(teams.contains(&"rates") && teams.contains(&"settlements"));
    assert_eq!(v["csrf_token"].as_str().unwrap().len(), 32);
}

#[sqlx::test(migrations = "../../migrations")]
async fn feed_filters_and_pages(pool: PgPool) {
    seed(&pool, &["workflow-failed.json", "trade-rejected.json", "high-error-rate.json", "pricing-log-warn.json"]).await;
    let h = harness(pool);

    let (s, v) = get(&h.app, "/api/alerts").await;
    assert_eq!(s, StatusCode::OK);
    let items = v["items"].as_array().unwrap();
    assert_eq!(items.len(), 4);
    let firsts: Vec<&str> = items.iter().map(|a| a["first_seen"].as_str().unwrap()).collect();
    assert!(firsts.windows(2).all(|w| w[0] >= w[1]), "newest first: {firsts:?}");
    assert!(items[0]["fields"].as_array().unwrap().iter().all(|f| f["value"].is_string()));
    assert!(v["next_cursor"].is_null());

    let (_, v) = get(&h.app, "/api/alerts?team=rates").await;
    assert_eq!(v["items"].as_array().unwrap().len(), 1);
    assert_eq!(v["items"][0]["title"], json!("Rejected EUR 25,000,000 trade T-99877"));

    // Repeated parameters mean OR: settlements + anna are critical.
    let (_, v) = get(&h.app, "/api/alerts?severity=critical").await;
    assert_eq!(v["items"].as_array().unwrap().len(), 2);
    let (_, v) = get(&h.app, "/api/alerts?severity=critical&severity=warning").await;
    assert_eq!(v["items"].as_array().unwrap().len(), 4);

    // Keyset paging: 2 + 2, no overlap.
    let (_, p1) = get(&h.app, "/api/alerts?limit=2").await;
    let cursor = p1["next_cursor"].as_str().unwrap().to_string();
    let (_, p2) = get(&h.app, &format!("/api/alerts?limit=2&cursor={cursor}")).await;
    let ids = |v: &Value| v["items"].as_array().unwrap().iter().map(|a| a["id"].as_str().unwrap().to_string()).collect::<Vec<_>>();
    let (a, b) = (ids(&p1), ids(&p2));
    assert_eq!((a.len(), b.len()), (2, 2));
    assert!(a.iter().all(|x| !b.contains(x)));
    assert!(p2["next_cursor"].is_null());

    assert_eq!(get(&h.app, "/api/alerts?status=bogus").await.0, StatusCode::BAD_REQUEST);
    assert_eq!(get(&h.app, "/api/alerts?team=nobody").await.0, StatusCode::FORBIDDEN);
}

#[sqlx::test(migrations = "../../migrations")]
async fn ack_and_resolve_rules(pool: PgPool) {
    seed(&pool, &["trade-rejected.json"]).await;
    let h = harness(pool.clone());
    let (_, v) = get(&h.app, "/api/alerts").await;
    let id = v["items"][0]["id"].as_str().unwrap().to_string();
    let ack = format!("/api/alerts/{id}/ack");
    let resolve = format!("/api/alerts/{id}/resolve");

    // CSRF is required on writes.
    assert_eq!(post(&h.app, &ack, None, json!({"expected_version": 1})).await.0, StatusCode::FORBIDDEN);

    // A count bump after the client's version does not conflict.
    sqlx::query("UPDATE alerts SET count = count + 1, version = version + 1").execute(&pool).await.unwrap();
    let (s, a) = post(&h.app, &ack, Some(&h.csrf), json!({"expected_version": 1})).await;
    assert_eq!(s, StatusCode::OK, "{a}");
    assert_eq!((a["status"].as_str(), a["acked_by"].as_str(), a["version"].as_i64()), (Some("acked"), Some("user:anna"), Some(3)));

    // Acking again is idempotent.
    let (s, again) = post(&h.app, &ack, Some(&h.csrf), json!({"expected_version": 1})).await;
    assert_eq!((s, again["version"].as_i64()), (StatusCode::OK, Some(3)));

    // A stale client that saw v1 tries to resolve after the status changed at v3.
    let (s, e) = post(&h.app, &resolve, Some(&h.csrf), json!({"expected_version": 2})).await;
    assert_eq!(s, StatusCode::CONFLICT);
    assert_eq!(e["error"]["code"], json!("version_conflict"));
    assert_eq!(e["error"]["current"]["status"], json!("acked"));

    let (s, r) = post(&h.app, &resolve, Some(&h.csrf), json!({"expected_version": 3})).await;
    assert_eq!((s, r["status"].as_str(), r["resolved_reason"].as_str()), (StatusCode::OK, Some("resolved"), Some("user")));

    let (s, e) = post(&h.app, &ack, Some(&h.csrf), json!({"expected_version": 4})).await;
    assert_eq!((s, e["error"]["code"].as_str()), (StatusCode::CONFLICT, Some("invalid_transition")));

    // Each transition queued one alert_update for the relay; resolve ends dedup.
    let updates: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox WHERE kind = 'alert_update'").fetch_one(&pool).await.unwrap();
    assert_eq!(updates, 1 + 2, "1 from the aggregator, 1 per transition");
    let active: bool = sqlx::query_scalar("SELECT dedup_active FROM alerts").fetch_one(&pool).await.unwrap();
    assert!(!active);

    assert_eq!(post(&h.app, "/api/alerts/not-a-uuid/ack", Some(&h.csrf), json!({"expected_version": 1})).await.0, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrations = "../../migrations")]
async fn detail_has_timeline_and_deliveries(pool: PgPool) {
    seed(&pool, &["trade-rejected.json", "trade-rejected.json"]).await;
    let h = harness(pool.clone());
    let (_, v) = get(&h.app, "/api/alerts").await;
    let id = v["items"][0]["id"].as_str().unwrap().to_string();
    sqlx::query(
        "INSERT INTO delivery_jobs (alert_id, notify_seq, recipient_id, channel, address, status, sent_at)
         VALUES ($1::uuid, 1, 'bob', 'email', 'bob@example.test', 'sent', now())",
    )
    .bind(&id)
    .execute(&pool)
    .await
    .unwrap();
    let (s, d) = get(&h.app, &format!("/api/alerts/{id}")).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(d["count"], json!(2));
    assert_eq!(d["deliveries"][0]["recipient"], json!("user:bob"));
    assert_eq!(d["deliveries"][0]["state"], json!("sent"));
    let kinds: Vec<&str> = d["timeline"].as_array().unwrap().iter().map(|t| t["kind"].as_str().unwrap()).collect();
    assert_eq!(kinds, vec!["opened", "deduplicated", "notified"]);
    assert_eq!(d["timeline"][1]["message"], json!("Happened again (2 times so far)"));
    assert!(d["samples"].as_array().unwrap().is_empty() && d["runbook"].is_null());
    assert_eq!(get(&h.app, &format!("/api/alerts/{}", uuid::Uuid::now_v7())).await.0, StatusCode::NOT_FOUND);
}

/// Read SSE frames until `n` events (not comments) have arrived.
async fn sse_events(body: &mut Body, n: usize) -> Vec<(String, Value)> {
    let mut buf = String::new();
    let mut out = Vec::new();
    while out.len() < n {
        let frame = tokio::time::timeout(Duration::from_secs(5), body.frame()).await.expect("sse frame").unwrap().unwrap();
        buf.push_str(std::str::from_utf8(&frame.into_data().unwrap()).unwrap());
        while let Some(end) = buf.find("\n\n") {
            let block: String = buf.drain(..end + 2).collect();
            let event = block.lines().find_map(|l| l.strip_prefix("event: ")).map(str::to_string);
            let data = block.lines().find_map(|l| l.strip_prefix("data: "));
            if let (Some(e), Some(d)) = (event, data) {
                out.push((e, serde_json::from_str(d).unwrap()));
            }
        }
    }
    out
}

#[sqlx::test(migrations = "../../migrations")]
async fn stream_snapshot_then_newer_upserts_only(pool: PgPool) {
    seed(&pool, &["trade-rejected.json", "workflow-failed.json"]).await;
    let h = harness(pool.clone());
    let res = h.app.clone().oneshot(Request::get("/api/stream?team=rates").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()["x-accel-buffering"], "no");
    let mut body = res.into_body();

    let ev = sse_events(&mut body, 1).await;
    assert_eq!(ev[0].0, "snapshot");
    let items = ev[0].1["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "team filter applies to the snapshot");
    let mut alert: FeedAlert = serde_json::from_value(items[0].clone()).unwrap();

    // Stale (same version), other team, then a newer version: only the last is sent.
    h.updates.send(alert.clone()).unwrap();
    let mut other = alert.clone();
    other.team = "settlements".into();
    other.version = 99;
    h.updates.send(other).unwrap();
    alert.version += 1;
    alert.count = 7;
    h.updates.send(alert.clone()).unwrap();

    let ev = sse_events(&mut body, 1).await;
    assert_eq!(ev[0].0, "alert.upsert");
    assert_eq!((ev[0].1["version"].as_i64(), ev[0].1["count"].as_i64()), (Some(alert.version), Some(7)));
}
