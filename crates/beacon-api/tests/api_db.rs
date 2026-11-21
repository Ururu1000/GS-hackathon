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
    ready: watch::Sender<bool>,
    csrf: String,
}

fn harness(pool: PgPool) -> Harness {
    let session = Arc::new(Session::demo(&root().join("config"), "anna").unwrap());
    let (updates, _) = broadcast::channel(64);
    let (ready_tx, ready) = watch::channel(true);
    let csrf = session.csrf_token.clone();
    let state = ApiState { pool, updates: updates.clone(), ready, session, sources: Arc::new(vec![]), streams: Arc::new(tokio::sync::Semaphore::new(8)) };
    Harness { app: router(state, None), updates, ready: ready_tx, csrf }
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

    // Body problems use the ApiError shape, and CSRF is checked before parsing.
    let (s, e) = post(&h.app, &ack, Some(&h.csrf), json!({"nope": 1})).await;
    assert_eq!((s, e["error"]["code"].as_str()), (StatusCode::BAD_REQUEST, Some("bad_request")));
    let (s, e) = post(&h.app, &ack, None, json!({"nope": 1})).await;
    assert_eq!((s, e["error"]["code"].as_str()), (StatusCode::FORBIDDEN, Some("csrf")));
}

#[sqlx::test(migrations = "../../migrations")]
async fn unknown_api_paths_and_methods_are_json_errors(pool: PgPool) {
    let h = harness(pool);
    let (s, e) = get(&h.app, "/api/nope").await;
    assert_eq!((s, e["error"]["code"].as_str()), (StatusCode::NOT_FOUND, Some("not_found")));
    let (s, e) = post(&h.app, "/api/alerts", None, json!({})).await;
    assert_eq!((s, e["error"]["code"].as_str()), (StatusCode::METHOD_NOT_ALLOWED, Some("method_not_allowed")));
    assert_eq!(get(&h.app, "/api/alerts?team=rates&team=pricing").await.0, StatusCode::BAD_REQUEST);
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

    // The feed consumer losing its partitions closes the stream for resync.
    h.ready.send(false).unwrap();
    let end = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match body.frame().await {
                None => return true,
                Some(Ok(_)) => continue,
                Some(Err(_)) => return true,
            }
        }
    })
    .await;
    assert_eq!(end, Ok(true), "stream must end when the feed is not ready");
}

async fn send(app: &Router, method: &str, uri: &str, csrf: &str, body: Option<Value>) -> (StatusCode, Value) {
    let r = Request::builder().method(method).uri(uri).header("content-type", "application/json").header("x-csrf-token", csrf);
    let res = app.clone().oneshot(r.body(body.map_or(Body::empty(), |b| Body::from(b.to_string()))).unwrap()).await.unwrap();
    let s = res.status();
    let b = res.into_body().collect().await.unwrap().to_bytes();
    (s, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

#[sqlx::test(migrations = "../../migrations")]
async fn rules_crud_with_revisions(pool: PgPool) {
    let seeded = beacon_engine::config_store::seed_from_yaml(&pool, &root().join("config")).await.unwrap();
    assert_eq!(seeded, 5);
    let h = harness(pool.clone());
    let (s, v) = get(&h.app, "/api/rules").await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["items"].as_array().unwrap().len(), 5);
    assert_eq!(v["items"][0]["stats"]["hourly"].as_array().unwrap().len(), 24);
    assert_eq!(v["items"][0]["updated_by"], json!("config"));

    let rule = json!({"id": "disk-full", "team": "settlements", "when": {"path": "raw.used_pct", "op": "gte", "value": 95},
        "route": [{"to": "team:settlements"}], "present": {"title": "Disk {{ raw.mount }} full"}});
    let (s, created) = send(&h.app, "POST", "/api/rules", &h.csrf, Some(json!({"rule": rule}))).await;
    assert_eq!((s, created["revision"].as_i64()), (StatusCode::CREATED, Some(1)));
    assert_eq!(created["updated_by"], json!("user:anna"));
    let (s, e) = send(&h.app, "POST", "/api/rules", &h.csrf, Some(json!({"rule": rule}))).await;
    assert_eq!((s, e["error"]["code"].as_str()), (StatusCode::CONFLICT, Some("revision_conflict")));

    let mut bad = rule.clone();
    bad["route"] = json!([]);
    let (s, e) = send(&h.app, "PUT", "/api/rules/disk-full", &h.csrf, Some(json!({"rule": bad, "expected_revision": 1}))).await;
    assert_eq!((s, e["error"]["code"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("invalid")));
    assert_eq!(e["error"]["details"][0]["path"], json!("route"));

    let mut v2 = rule.clone();
    v2["present"]["title"] = json!("Disk almost full");
    let (s, u) = send(&h.app, "PUT", "/api/rules/disk-full", &h.csrf, Some(json!({"rule": v2, "expected_revision": 1}))).await;
    assert_eq!((s, u["revision"].as_i64()), (StatusCode::OK, Some(2)));
    let (s, e) = send(&h.app, "PUT", "/api/rules/disk-full", &h.csrf, Some(json!({"rule": v2, "expected_revision": 1}))).await;
    assert_eq!((s, e["error"]["current"]["revision"].as_i64()), (StatusCode::CONFLICT, Some(2)));

    let (s, _) = send(&h.app, "DELETE", "/api/rules/disk-full?expected_revision=2", &h.csrf, None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(get(&h.app, "/api/rules/disk-full").await.0, StatusCode::NOT_FOUND);
    let (s, again) = send(&h.app, "POST", "/api/rules", &h.csrf, Some(json!({"rule": rule}))).await;
    assert_eq!((s, again["revision"].as_i64()), (StatusCode::CREATED, Some(3)), "recreated rule continues the revision");

    // CSRF and team ownership.
    assert_eq!(send(&h.app, "POST", "/api/rules", "wrong", Some(json!({"rule": rule}))).await.0, StatusCode::FORBIDDEN);
    let mut foreign = rule.clone();
    foreign["id"] = json!("foreign");
    foreign["team"] = json!("not-a-team");
    assert_eq!(send(&h.app, "POST", "/api/rules", &h.csrf, Some(json!({"rule": foreign}))).await.0, StatusCode::FORBIDDEN);

    let rev: i64 = sqlx::query_scalar("SELECT revision FROM config_state WHERE key = 'rules'").fetch_one(&pool).await.unwrap();
    assert_eq!(rev, 4 + 5 + 4, "4 schema seeds + 5 rule seeds + create, update, delete, recreate");
}

#[sqlx::test(migrations = "../../migrations")]
async fn rule_test_panel_and_discovery(pool: PgPool) {
    beacon_engine::config_store::seed_from_yaml(&pool, &root().join("config")).await.unwrap();
    let h = harness(pool.clone());
    let rule = json!({"id": "disk-full", "team": "settlements", "when": {"all": [{"path": "raw.event_kind", "op": "eq", "value": "disk_full"}, {"path": "raw.used_pct", "op": "gte", "value": 95}]},
        "route": [{"to": "team:settlements"}], "present": {"title": "Disk {{ raw.mount }} on {{ raw.host }}"}});
    let event: Value = serde_json::from_str(&std::fs::read_to_string(root().join("samples/events/unknown-shape.json")).unwrap()).unwrap();
    let (s, r) = send(&h.app, "POST", "/api/rules/test", &h.csrf, Some(json!({"rule": rule, "event": event}))).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(r["matched"], json!(true));
    assert!(r["schema"].as_str().unwrap().starts_with("unknown:"));
    assert_eq!(r["card"]["title"], json!("Disk /var/data on batch-07"));
    assert!(r["warnings"].as_array().unwrap().iter().any(|w| w.as_str().unwrap().contains("No schema mapping")));
    let big = json!({"rule": rule, "event": {"x": "y".repeat(300 * 1024)}});
    assert_eq!(send(&h.app, "POST", "/api/rules/test", &h.csrf, Some(big)).await.0, StatusCode::PAYLOAD_TOO_LARGE);

    sqlx::query("INSERT INTO event_samples (event_id, source, schema, received_at, raw) VALUES ('e1', 'http:judges', 'unknown:abc', now(), $1)")
        .bind(&event)
        .execute(&pool)
        .await
        .unwrap();
    let (_, d) = get(&h.app, "/api/schemas/discovered").await;
    assert_eq!((d[0]["schema"].as_str(), d[0]["count"].as_i64()), (Some("unknown:abc"), Some(1)));
    assert_eq!(d[0]["sample"]["host"], json!("batch-07"));
    let (_, r) = get(&h.app, "/api/events/recent?limit=5").await;
    assert_eq!(r["items"][0]["source"], json!("http:judges"));
    let (_, s) = get(&h.app, "/api/sources").await;
    assert_eq!(s[0]["kind"], json!("http"));
    let (_, sc) = get(&h.app, "/api/schemas").await;
    assert!(sc.as_array().unwrap().iter().any(|x| x["spec"]["schema"] == json!("prometheus.alert")));
}

#[sqlx::test(migrations = "../../migrations")]
async fn concurrent_creates_of_one_id_conflict(pool: PgPool) {
    let rule = json!({"id": "race", "team": "rates", "when": {"path": "raw.x", "op": "exists"},
        "route": [{"to": "team:rates"}], "present": {"title": "t"}});
    for _ in 0..10 {
        let (a, b) = tokio::join!(
            beacon_engine::config_store::create(&pool, &rule, "user:a"),
            beacon_engine::config_store::create(&pool, &rule, "user:b")
        );
        let ok = [a.unwrap().is_ok(), b.unwrap().is_ok()].iter().filter(|x| **x).count();
        assert_eq!(ok, 1, "exactly one create of the same id wins");
        let cur = beacon_engine::config_store::get(&pool, "race").await.unwrap().unwrap();
        beacon_engine::config_store::delete(&pool, "race", cur.revision, "user:a").await.unwrap().unwrap();
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn same_origin_ingest_needs_csrf(pool: PgPool) {
    let session = Arc::new(Session::demo(&root().join("config"), "anna").unwrap());
    let (updates, _) = broadcast::channel(8);
    let (_r, ready) = watch::channel(true);
    let csrf = session.csrf_token.clone();
    let state = ApiState { pool, updates, ready, session, sources: Arc::new(vec![]), streams: Arc::new(tokio::sync::Semaphore::new(8)) };
    let ingest = Router::new().route("/ingest/{source}", axum::routing::post(|| async { (StatusCode::ACCEPTED, "{\"id\":\"x\"}") }));
    let app = beacon_api::router_with(state, None, Some(ingest));
    assert_eq!(post(&app, "/ingest/demo", None, json!({})).await.0, StatusCode::FORBIDDEN);
    assert_eq!(post(&app, "/ingest/demo", Some(&csrf), json!({})).await.0, StatusCode::ACCEPTED);
    assert_eq!(get(&app, "/ready").await.0, StatusCode::OK, "no route clash with the API's /ready");
}

#[sqlx::test(migrations = "../../migrations")]
async fn hardening_headers_scoping_and_masking(pool: PgPool) {
    // Security headers and no caching of API responses.
    let h = harness(pool.clone());
    let res = h.app.clone().oneshot(Request::get("/api/session").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.headers()["cache-control"], "no-store");
    assert_eq!(res.headers()["x-content-type-options"], "nosniff");
    assert_eq!(res.headers()["x-frame-options"], "DENY");

    // Masked delivery address in alert detail.
    seed(&pool, &["trade-rejected.json"]).await;
    let (_, v) = get(&h.app, "/api/alerts").await;
    let id = v["items"][0]["id"].as_str().unwrap().to_string();
    sqlx::query("INSERT INTO delivery_jobs (alert_id, notify_seq, recipient_id, channel, address, status, last_error) VALUES ($1::uuid, 1, 'bob', 'email', 'bob@example.test', 'pending', '550 no such user <bob@example.test>')")
        .bind(&id).execute(&pool).await.unwrap();
    let (_, d) = get(&h.app, &format!("/api/alerts/{id}")).await;
    assert_eq!(d["deliveries"][0]["address"], json!("b***@example.test"));
    assert!(!d["deliveries"][0]["last_error"].as_str().unwrap().contains("bob@"));

    // A non-admin only sees their own teams and not the cross-team views.
    let session = Arc::new(Session::demo_with(&root().join("config"), "anna", false).unwrap());
    let teams: Vec<&str> = session.teams.iter().map(|t| t.id.as_str()).collect();
    assert_eq!(teams, vec!["payments"], "anna belongs to payments only");
    let csrf = session.csrf_token.clone();
    let (updates, _) = broadcast::channel(8);
    let (_r, ready) = watch::channel(true);
    let state = ApiState { pool: pool.clone(), updates, ready, session, sources: Arc::new(vec![]), streams: Arc::new(tokio::sync::Semaphore::new(8)) };
    let app = router(state, None);
    assert_eq!(get(&app, "/api/events/recent").await.0, StatusCode::FORBIDDEN);
    assert_eq!(get(&app, "/api/schemas/discovered").await.0, StatusCode::FORBIDDEN);
    let (_, feed) = get(&app, "/api/alerts").await;
    assert!(feed["items"].as_array().unwrap().is_empty(), "rates alert is invisible to anna");
    let rule = json!({"id": "sneaky", "team": "payments", "when": {"path": "raw.x", "op": "exists"},
        "route": [{"to": "team:rates"}], "present": {"title": "t"}});
    let (s, e) = send(&app, "POST", "/api/rules", &csrf, Some(json!({"rule": rule}))).await;
    assert_eq!((s, e["error"]["code"].as_str()), (StatusCode::FORBIDDEN, Some("forbidden")), "can't page another team");

    // Non-admins also cannot route to another team's on-call schedule or another user directly.
    let oncall_rule = json!({"id": "sneaky-oc", "team": "payments", "when": {"path": "raw.x", "op": "exists"},
        "route": [{"to": "oncall:settlements-primary"}], "present": {"title": "t"}});
    assert_eq!(send(&app, "POST", "/api/rules", &csrf, Some(json!({"rule": oncall_rule}))).await.0, StatusCode::FORBIDDEN);
    let user_rule = json!({"id": "sneaky-u", "team": "payments", "when": {"path": "raw.x", "op": "exists"},
        "route": [{"to": "user:bob"}], "present": {"title": "t"}});
    assert_eq!(send(&app, "POST", "/api/rules", &csrf, Some(json!({"rule": user_rule}))).await.0, StatusCode::FORBIDDEN);

    // A non-admin member of settlements CAN route to settlements' configured schedule (settlements-primary).
    let settlements_session = Session::demo_with(&root().join("config"), "bob", false).unwrap();
    assert!(settlements_session.can_see_schedule("settlements-primary"));
    assert!(!settlements_session.can_see_schedule("payments"));

    // Updating or deleting another team's rule returns 404 without leaking its team name.
    let rates_rule = json!({"id": "rates-secret", "team": "rates", "when": {"path": "raw.x", "op": "exists"},
        "route": [{"to": "team:rates"}], "present": {"title": "t"}});
    let created = beacon_engine::config_store::create(&pool, &rates_rule, "user:bob").await.unwrap().unwrap();
    let (s, e) = send(&app, "DELETE", &format!("/api/rules/rates-secret?expected_revision={}", created.revision), &csrf, None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert!(!e.to_string().contains("rates"), "must not leak hidden team name: {e}");
    let take_rule = json!({"id": "rates-secret", "team": "payments", "when": {"path": "raw.x", "op": "exists"},
        "route": [{"to": "team:payments"}], "present": {"title": "t"}});
    let (s, e) = send(&app, "PUT", "/api/rules/rates-secret", &csrf, Some(json!({"rule": take_rule, "expected_revision": created.revision}))).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert!(!e.to_string().contains("rates"));

    // Soft-deleted cross-team rule id cannot be recreated and does not leak its team name.
    beacon_engine::config_store::delete(&pool, "rates-secret", created.revision, "user:bob").await.unwrap().unwrap();
    let (s, e) = send(&app, "POST", "/api/rules", &csrf, Some(json!({"rule": take_rule}))).await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert!(!e.to_string().contains("rates"));

    // Even an admin cannot recreate a soft-deleted rule under a different team (keeps rule_id -> team invariant).
    let (s, e) = send(&h.app, "POST", "/api/rules", &h.csrf, Some(json!({"rule": take_rule}))).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(e["error"]["details"][0]["path"], json!("team"));

    // Existing DB seed rule with legacy oncall:settlements is migrated by seed_from_yaml
    // (only route[].to changes, revision increments, conditions stay intact).
    let legacy_seed = json!({"id": "settlements-workflow-failed", "team": "settlements",
        "when": {"path": "raw.x", "op": "eq", "value": "oncall:settlements"}, "route": [{"to": "oncall:settlements"}], "present": {"title": "t"}});
    beacon_engine::config_store::create(&pool, &legacy_seed, "config").await.unwrap().unwrap();
    beacon_engine::config_store::seed_from_yaml(&pool, &root().join("config")).await.unwrap();
    let migrated = beacon_engine::config_store::get(&pool, "settlements-workflow-failed").await.unwrap().unwrap();
    assert_eq!(migrated.revision, 2);
    assert_eq!(migrated.spec["route"][0]["to"], json!("oncall:settlements-primary"));
    assert_eq!(migrated.spec["when"]["value"], json!("oncall:settlements"));

    // Oversized transition body (> 32 KiB) is rejected during extraction.
    let big_ack = json!({"expected_version": 1, "note": "x".repeat(40_000)});
    assert_eq!(send(&app, "POST", &format!("/api/alerts/{id}/ack"), &csrf, Some(big_ack)).await.0, StatusCode::PAYLOAD_TOO_LARGE);

    // SSE stream carries no-store as well as no-cache.
    let sse_res = app.clone().oneshot(Request::get("/api/stream").body(Body::empty()).unwrap()).await.unwrap();
    let cc = sse_res.headers()["cache-control"].to_str().unwrap();
    assert!(cc.contains("no-cache") && cc.contains("no-store"), "got {cc}");
}

#[sqlx::test(migrations = "../../migrations")]
async fn schema_preview_and_publish(pool: PgPool) {
    beacon_engine::config_store::seed_from_yaml(&pool, &root().join("config")).await.unwrap();
    let h = harness(pool.clone());
    let event: Value = serde_json::from_str(&std::fs::read_to_string(root().join("samples/events/unknown-shape.json")).unwrap()).unwrap();
    let spec = json!({"schema": "infra.disk", "priority": 40,
        "detect": {"all": [{"path": "event_kind", "op": "exists"}, {"path": "host", "op": "exists"}]},
        "fields": {"type": {"path": "event_kind"}, "service": {"path": "host"}, "used": {"path": "used_pct", "as": "number"}}});
    let (s, p) = send(&h.app, "POST", "/api/schemas/preview", &h.csrf, Some(json!({"spec": spec, "event": event, "source": "http:judges"}))).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!((p["matched"].clone(), p["fields"]["service"].clone()), (json!(true), json!("batch-07")));
    let (s, e) = send(&h.app, "POST", "/api/schemas/preview", &h.csrf, Some(json!({"spec": {"schema": "x", "detect": {"path": "a", "op": "regex", "value": "("}}, "event": {}, "source": "s"}))).await;
    assert_eq!((s, e["error"]["details"][0]["path"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("detect")));

    let (s, created) = send(&h.app, "POST", "/api/schemas", &h.csrf, Some(json!({"spec": spec}))).await;
    assert_eq!((s, created["revision"].as_i64()), (StatusCode::CREATED, Some(1)));
    let (s, _) = send(&h.app, "POST", "/api/schemas", &h.csrf, Some(json!({"spec": spec}))).await;
    assert_eq!(s, StatusCode::CONFLICT, "exists without expected_revision");
    let (s, upd) = send(&h.app, "POST", "/api/schemas", &h.csrf, Some(json!({"spec": spec, "expected_revision": 1, "schema": "infra.disk"}))).await;
    assert_eq!((s, upd["revision"].as_i64()), (StatusCode::OK, Some(2)));
    // The rule test panel now sees the published mapping.
    let rule = json!({"id": "disk", "team": "settlements", "when": {"path": "fields.service", "op": "eq", "value": "batch-07"},
        "route": [{"to": "team:settlements"}], "present": {"title": "Disk on {{ fields.service }}"}});
    let (_, r) = send(&h.app, "POST", "/api/rules/test", &h.csrf, Some(json!({"rule": rule, "event": event}))).await;
    assert_eq!((r["schema"].clone(), r["matched"].clone()), (json!("infra.disk"), json!(true)));
    // And the live matcher snapshot picks it up.
    let snap = beacon_engine::config_store::load_snapshot(&pool).await.unwrap();
    assert!(snap.mappings.iter().any(|m| m.schema == "infra.disk"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn proposals_lifecycle(pool: PgPool) {
    beacon_engine::config_store::seed_from_yaml(&pool, &root().join("config")).await.unwrap();
    let h = harness(pool.clone());
    let (_, r) = get(&h.app, "/api/rules/rates-rejected-trades").await;
    let mut proposed = r["rule"].clone();
    proposed["dedup"] = json!({"key": ["raw.payload.tradeId"], "window": "30m"});
    let body = json!({"rule_id": "rates-rejected-trades", "base_revision": 1, "kind": "noisy", "summary": "Longer dedup window", "proposed_rule": proposed});
    let (s, p) = send(&h.app, "POST", "/api/proposals", &h.csrf, Some(body.clone())).await;
    assert_eq!((s, p["state"].as_str(), p["can_review"].as_bool()), (StatusCode::CREATED, Some("pending"), Some(true)));
    assert_eq!(get(&h.app, "/api/proposals/count").await.1["pending"], json!(1));
    let (_, l) = get(&h.app, "/api/proposals?state=pending").await;
    assert_eq!(l["items"].as_array().unwrap().len(), 1);

    // Accept publishes against base_revision.
    let pid = p["id"].as_str().unwrap().to_string();
    let (s, a) = send(&h.app, "POST", &format!("/api/proposals/{pid}/accept"), &h.csrf, None).await;
    assert_eq!((s, a["state"].as_str(), a["decided_by"].as_str()), (StatusCode::OK, Some("accepted"), Some("user:anna")));
    let (_, r2) = get(&h.app, "/api/rules/rates-rejected-trades").await;
    assert_eq!((r2["revision"].as_i64(), r2["rule"]["dedup"]["window"].as_str()), (Some(2), Some("30m")));
    assert_eq!(send(&h.app, "POST", &format!("/api/proposals/{pid}/accept"), &h.csrf, None).await.0, StatusCode::CONFLICT);

    // A proposal against an old revision can't be created; one that goes stale can't be accepted.
    let (s, _) = send(&h.app, "POST", "/api/proposals", &h.csrf, Some(body)).await;
    assert_eq!(s, StatusCode::CONFLICT);
    let mut p2 = r2["rule"].clone();
    p2["present"]["title"] = json!("Rejected trade {{ raw.payload.tradeId }}");
    let (_, created) = send(&h.app, "POST", "/api/proposals", &h.csrf, Some(json!({"rule_id": "rates-rejected-trades", "base_revision": 2, "kind": "template", "summary": "Shorter title", "proposed_rule": p2}))).await;
    let mut moved = r2["rule"].clone();
    moved["enabled"] = json!(false);
    send(&h.app, "PUT", "/api/rules/rates-rejected-trades", &h.csrf, Some(json!({"rule": moved, "expected_revision": 2}))).await;
    let (s, e) = send(&h.app, "POST", &format!("/api/proposals/{}/accept", created["id"].as_str().unwrap()), &h.csrf, None).await;
    assert_eq!((s, e["error"]["current"]["state"].as_str()), (StatusCode::CONFLICT, Some("stale")));

    // Validation.
    let (s, e) = send(&h.app, "POST", "/api/proposals", &h.csrf, Some(json!({"rule_id": "rates-rejected-trades", "base_revision": 3, "kind": "weird", "summary": "x", "proposed_rule": moved}))).await;
    assert_eq!((s, e["error"]["details"][0]["path"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("kind")));
    assert_eq!(get(&h.app, "/api/proposals?state=nope").await.0, StatusCode::BAD_REQUEST);
}

#[sqlx::test(migrations = "../../migrations")]
async fn proposal_decisions_are_atomic_and_routes_checked(pool: PgPool) {
    beacon_engine::config_store::seed_from_yaml(&pool, &root().join("config")).await.unwrap();
    let h = harness(pool.clone());
    let (_, r) = get(&h.app, "/api/rules/rates-rejected-trades").await;
    for round in 0..8 {
        let rev = r["revision"].as_i64().unwrap() + round;
        let (_, cur) = get(&h.app, "/api/rules/rates-rejected-trades").await;
        assert_eq!(cur["revision"].as_i64(), Some(rev));
        let mut proposed = cur["rule"].clone();
        proposed["dedup"] = json!({"key": ["raw.payload.tradeId"], "window": format!("{}m", 20 + round)});
        let (_, p) = send(&h.app, "POST", "/api/proposals", &h.csrf, Some(json!({"rule_id": "rates-rejected-trades", "base_revision": rev, "kind": "noisy", "summary": "s", "proposed_rule": proposed}))).await;
        let pid = p["id"].as_str().unwrap().to_string();
        let (acc, rej) = (format!("/api/proposals/{pid}/accept"), format!("/api/proposals/{pid}/reject"));
        let (a, b) = tokio::join!(send(&h.app, "POST", &acc, &h.csrf, None), send(&h.app, "POST", &rej, &h.csrf, None));
        let accepted = a.0 == StatusCode::OK;
        assert_ne!(a.0 == StatusCode::OK, b.0 == StatusCode::OK, "exactly one decision wins");
        let (_, after) = get(&h.app, "/api/rules/rates-rejected-trades").await;
        let applied = after["revision"].as_i64() == Some(rev + 1);
        assert_eq!(applied, accepted, "the rule changes if and only if the proposal was accepted");
        if !accepted {
            // Keep revisions moving so the loop's arithmetic holds.
            send(&h.app, "PUT", "/api/rules/rates-rejected-trades", &h.csrf, Some(json!({"rule": after["rule"], "expected_revision": rev}))).await;
        }
    }
    // Routing checks apply to proposals like to direct edits.
    let session = Arc::new(Session::demo_with(&root().join("config"), "erin", false).unwrap());
    let csrf = session.csrf_token.clone();
    let (updates, _) = broadcast::channel(8);
    let (_r, ready) = watch::channel(true);
    let state = ApiState { pool, updates, ready, session, sources: Arc::new(vec![]), streams: Arc::new(tokio::sync::Semaphore::new(8)) };
    let app = router(state, None);
    let (_, pr) = get(&app, "/api/rules/pricing-engine-warnings").await;
    let mut sneaky = pr["rule"].clone();
    sneaky["route"] = json!([{"to": "team:settlements"}]);
    let (s, _) = send(&app, "POST", "/api/proposals", &csrf, Some(json!({"rule_id": "pricing-engine-warnings", "base_revision": pr["revision"], "kind": "reroute", "summary": "s", "proposed_rule": sneaky}))).await;
    assert_eq!(s, StatusCode::FORBIDDEN, "a proposal can't page a team the author can't see");
}

#[sqlx::test(migrations = "../../migrations")]
async fn proposal_and_mapping_guards(pool: PgPool) {
    beacon_engine::config_store::seed_from_yaml(&pool, &root().join("config")).await.unwrap();
    let h = harness(pool.clone());

    // A proposal can't move the rule to another team (direct edits can't either).
    let (_, r) = get(&h.app, "/api/rules/rates-rejected-trades").await;
    let mut moved = r["rule"].clone();
    moved["team"] = json!("settlements");
    let (s, e) = send(&h.app, "POST", "/api/proposals", &h.csrf, Some(json!({"rule_id": "rates-rejected-trades", "base_revision": r["revision"], "kind": "reroute", "summary": "s", "proposed_rule": moved}))).await;
    assert_eq!((s, e["error"]["details"][0]["path"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("proposed_rule.team")));

    // Mappings: no renaming through an edit, no secret source paths, bounded field count.
    let spec = json!({"schema": "guard.a", "detect": {"path": "kind", "op": "exists"}, "fields": {"type": {"path": "kind"}}});
    assert_eq!(send(&h.app, "POST", "/api/schemas", &h.csrf, Some(json!({"spec": spec}))).await.0, StatusCode::CREATED);
    let mut renamed = spec.clone();
    renamed["schema"] = json!("guard.b");
    let (s, e) = send(&h.app, "POST", "/api/schemas", &h.csrf, Some(json!({"spec": renamed, "expected_revision": 1, "schema": "guard.a"}))).await;
    assert_eq!((s, e["error"]["details"][0]["path"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("schema")));
    assert_eq!(send(&h.app, "POST", "/api/schemas", &h.csrf, Some(json!({"spec": spec, "expected_revision": 1}))).await.0, StatusCode::BAD_REQUEST);
    let secret = json!({"schema": "guard.c", "detect": {"path": "kind", "op": "exists"}, "fields": {"note": {"path": "auth.password"}}});
    let (s, e) = send(&h.app, "POST", "/api/schemas/preview", &h.csrf, Some(json!({"spec": secret, "event": {"kind": 1, "auth": {"password": "hunter2"}}}))).await;
    assert_eq!((s, e["error"]["details"][0]["path"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("fields.note")));
    let fields: serde_json::Map<String, Value> = (0..65).map(|i| (format!("f{i}"), json!({"path": "kind"}))).collect();
    let wide = json!({"schema": "guard.d", "detect": {"path": "kind", "op": "exists"}, "fields": fields});
    let (s, e) = send(&h.app, "POST", "/api/schemas/preview", &h.csrf, Some(json!({"spec": wide, "event": {"kind": 1}}))).await;
    assert_eq!((s, e["error"]["details"][0]["path"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("fields")));

    // Rule bodies are measured as stored: jsonb prints 1e308 as 309 digits.
    let (_, r) = get(&h.app, "/api/rules/rates-rejected-trades").await;
    let mut bloated = r["rule"].clone();
    bloated["throttle"] = json!(vec![1e308; 500]);
    let (s, e) = send(&h.app, "POST", "/api/proposals", &h.csrf, Some(json!({"rule_id": "rates-rejected-trades", "base_revision": r["revision"], "kind": "other", "summary": "s", "proposed_rule": bloated}))).await;
    assert_eq!((s, e["error"]["details"][0]["path"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("proposed_rule")), "{e}");

    // Pending proposals are capped per rule; the list still carries both rule bodies.
    let (_, r) = get(&h.app, "/api/rules/rates-rejected-trades").await;
    let flood = json!({"rule_id": "rates-rejected-trades", "base_revision": r["revision"], "kind": "other", "summary": "s", "proposed_rule": r["rule"]});
    for _ in 0..10 {
        assert_eq!(send(&h.app, "POST", "/api/proposals", &h.csrf, Some(flood.clone())).await.0, StatusCode::CREATED);
    }
    let (s, e) = send(&h.app, "POST", "/api/proposals", &h.csrf, Some(flood)).await;
    assert_eq!((s, e["error"]["code"].as_str()), (StatusCode::TOO_MANY_REQUESTS, Some("too_many_proposals")));
    let (_, l) = get(&h.app, "/api/proposals?state=pending").await;
    assert_eq!(l["items"].as_array().unwrap().len(), 10);
    assert_eq!((l["items"][0]["base_rule"]["id"].as_str(), l["items"][0]["proposed_rule"]["team"].as_str()), (Some("rates-rejected-trades"), Some("rates")));

    // A non-admin author sees their own proposal as not reviewable, and can't review it.
    let session = Arc::new(Session::demo_with(&root().join("config"), "erin", false).unwrap());
    let csrf = session.csrf_token.clone();
    let (updates, _) = broadcast::channel(8);
    let (_r, ready) = watch::channel(true);
    let state = ApiState { pool, updates, ready, session, sources: Arc::new(vec![]), streams: Arc::new(tokio::sync::Semaphore::new(8)) };
    let app = router(state, None);
    let (_, pr) = get(&app, "/api/rules/pricing-engine-warnings").await;
    let mut proposed = pr["rule"].clone();
    proposed["present"]["title"] = json!("Pricing warning");
    let (s, p) = send(&app, "POST", "/api/proposals", &csrf, Some(json!({"rule_id": "pricing-engine-warnings", "base_revision": pr["revision"], "kind": "template", "summary": "s", "proposed_rule": proposed}))).await;
    assert_eq!((s, p["can_review"].as_bool()), (StatusCode::CREATED, Some(false)));
    let (s, _) = send(&app, "POST", &format!("/api/proposals/{}/accept", p["id"].as_str().unwrap()), &csrf, None).await;
    assert_eq!(s, StatusCode::FORBIDDEN);
}

/// The app with delivery's routes and the simulator mounted (gateway token "gw-token").
fn delivery_harness(pool: PgPool) -> Harness {
    let session = Arc::new(Session::demo(&root().join("config"), "anna").unwrap());
    let (updates, _) = broadcast::channel(64);
    let (ready_tx, ready) = watch::channel(true);
    let csrf = session.csrf_token.clone();
    let state = ApiState { pool: pool.clone(), updates: updates.clone(), ready, session, sources: Arc::new(vec![]), streams: Arc::new(tokio::sync::Semaphore::new(8)) };
    let config = root().join("config");
    let dir = beacon_delivery::config::Directory::load(&config).unwrap();
    let registry = beacon_delivery::actions::ActionRegistry::builtin(beacon_delivery::actions::ActionsConfig {
        gateway_url: "http://127.0.0.1:1".into(),
        http_targets: beacon_delivery::actions::load_http_targets(&config).unwrap(),
        timeout: Duration::from_secs(1),
    });
    let delivery = beacon_api::DeliveryMount {
        api: beacon_delivery::api::ApiState {
            pool: pool.clone(),
            dir: Arc::new(dir),
            registry,
            alert_ops: Arc::new(beacon_delivery::alert_ops::SqlAlertOps),
            reply_secret: Some(Arc::new(b"reply-secret".to_vec())),
        },
        simulator: Some(beacon_api::SimulatorMount {
            state: beacon_delivery::gateway::GatewayState::new(pool).with_token(Some("gw-token".into())),
            token: Some("gw-token".into()),
        }),
    };
    Harness { app: beacon_api::router_full(state, None, None, Some(delivery)), updates, ready: ready_tx, csrf }
}

#[sqlx::test(migrations = "../../migrations")]
async fn delivery_routes_are_mounted_behind_the_session(pool: PgPool) {
    seed(&pool, &["trade-rejected.json"]).await;
    let h = delivery_harness(pool);

    // Reads get the session's actor: anna sees her own contact, others are redacted.
    let (s, v) = get(&h.app, "/api/people").await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert!(v["items"].as_array().unwrap().iter().any(|p| p["id"] == "anna"));
    let (s, v) = get(&h.app, "/api/teams").await;
    assert_eq!(s, StatusCode::OK, "{v}");
    // Admin(session.admin) is passed through: the demo admin may edit every team.
    assert!(v["items"].as_array().unwrap().iter().all(|t| t["can_edit"] == json!(true)), "{v}");
    assert_eq!(get(&h.app, "/api/runbooks").await.0, StatusCode::OK);

    // Writes need the CSRF token.
    let (s, e) = post(&h.app, "/api/approvals/00000000-0000-0000-0000-000000000000/approve", None, json!({})).await;
    assert_eq!((s, e["error"]["code"].as_str()), (StatusCode::FORBIDDEN, Some("csrf")));
    let (s, _) = post(&h.app, "/api/approvals/00000000-0000-0000-0000-000000000000/approve", Some(&h.csrf), json!({})).await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    // /api/replies is the gateway's signed callback: no CSRF, but the signature is checked.
    let (s, e) = post(&h.app, "/api/replies", None, json!({"from": "+1", "text": "ACK"})).await;
    assert_eq!((s, e["error"]["code"].as_str()), (StatusCode::UNAUTHORIZED, Some("unauthorized")));

    // The alert detail carries delivery's runbook view (null when none was frozen).
    let (_, v) = get(&h.app, "/api/alerts").await;
    let id = v["items"][0]["id"].as_str().unwrap().to_string();
    let (s, d) = get(&h.app, &format!("/api/alerts/{id}")).await;
    assert_eq!(s, StatusCode::OK);
    assert!(d.get("runbook").is_some() && d["timeline"].as_array().is_some_and(|t| !t.is_empty()));
}

#[sqlx::test(migrations = "../../migrations")]
async fn simulator_is_mounted_with_the_gateway_token_injected(pool: PgPool) {
    let h = delivery_harness(pool);
    // The browser never holds GATEWAY_TOKEN: the API adds it.
    let (s, v) = get(&h.app, "/api/simulator/messages").await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert!(v["items"].is_array());
    // A browser-supplied Authorization header is replaced, not trusted or forwarded.
    let res = h.app.clone().oneshot(Request::get("/api/simulator/outage").header("authorization", "Bearer wrong").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    // Outage control is a write: CSRF first.
    let (s, e) = post(&h.app, "/api/simulator/outage", None, json!({"channel": "sms", "mode": "down", "seconds": 5})).await;
    assert_eq!((s, e["error"]["code"].as_str()), (StatusCode::FORBIDDEN, Some("csrf")));
    let (s, v) = post(&h.app, "/api/simulator/outage", Some(&h.csrf), json!({"channel": "sms", "mode": "down", "seconds": 5})).await;
    assert!(s.is_success(), "{s} {v}");
}

#[sqlx::test(migrations = "../../migrations")]
async fn mapping_suggestion_mock_is_validated_and_previewed(pool: PgPool) {
    beacon_api::mapping_suggest::configure(beacon_api::mapping_suggest::Provider::Mock);
    let h = harness(pool);
    assert_eq!(get(&h.app, "/api/session").await.1["capabilities"]["llm_suggest"], json!(true));
    let event = json!({"event": "REFUND_FAILED", "level": "ERROR", "app": "payments-api", "ts": "2026-11-21T06:00:00Z", "detail": {"msg": "card declined"}});
    // A write: CSRF first.
    let (s, _) = post(&h.app, "/api/schemas/suggest", None, json!({"event": event})).await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    let (s, v) = post(&h.app, "/api/schemas/suggest", Some(&h.csrf), json!({"event": event})).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!((v["provider"].as_str(), v["model"].is_null()), (Some("mock"), true));
    assert_eq!(v["preview"]["matched"], json!(true), "{v}");
    assert_eq!(v["preview"]["fields"]["severity"], json!("error"));
    assert_eq!(v["preview"]["fields"]["service"], json!("payments-api"));
    // The suggestion is publishable as-is through the normal path.
    let (s, p) = post(&h.app, "/api/schemas", Some(&h.csrf), json!({"spec": v["spec"]})).await;
    assert!(s.is_success(), "{s} {p}");
    // Nonsense is a 400, not a call.
    let (s, _) = post(&h.app, "/api/schemas/suggest", Some(&h.csrf), json!({"event": [1, 2]})).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
}
