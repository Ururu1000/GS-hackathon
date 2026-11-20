//! Fake SMS/phone gateway + gateway channels integration suite (agent 2, M4).
//!
//! Needs Postgres (`DATABASE_URL`, sqlx creates a throwaway database per test).
//!
//! Run: `cargo test -p beacon-delivery --test gateway`

use std::time::Duration;

use beacon_delivery::channel::gateway_client::{GatewayChannel, GatewayClientConfig, GatewayKind};
use beacon_delivery::channel::{Channel, OutboundMessage, SendError};
use beacon_delivery::gateway::{self, GatewayState, OutageMode, SimMessage};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

/// Start the gateway on an ephemeral port. Returns the state, its base URL, and the shutdown
/// sender (keep it alive for the whole test; dropping it stops the server).
async fn start_gateway(pool: PgPool) -> (GatewayState, String, tokio::sync::watch::Sender<bool>) {
    start_gateway_token(pool, None).await
}

/// Like [`start_gateway`], with an optional bearer token configured on the gateway.
async fn start_gateway_token(
    pool: PgPool,
    token: Option<&str>,
) -> (GatewayState, String, tokio::sync::watch::Sender<bool>) {
    let state = GatewayState::new(pool).with_token(token.map(str::to_string));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let serve_state = state.clone();
    tokio::spawn(async move {
        gateway::serve(
            serve_state,
            "127.0.0.1:0".parse().unwrap(),
            shutdown_rx,
            |addr| {
                let _ = bound_tx.send(addr);
            },
        )
        .await
        .expect("gateway serve failed");
    });
    let addr = tokio::time::timeout(Duration::from_secs(10), bound_rx)
        .await
        .expect("gateway bind timed out")
        .expect("gateway on_bound dropped");
    (state, format!("http://{addr}"), shutdown_tx)
}

fn channel_for(kind: GatewayKind, base_url: &str) -> GatewayChannel {
    GatewayChannel::new(
        kind,
        GatewayClientConfig {
            base_url: base_url.to_string(),
            public_url: "http://localhost:5173".to_string(),
            timeout: Duration::from_secs(5),
            token: None,
        },
    )
    .expect("build GatewayChannel")
}

fn sms_msg(job_id: Uuid, body: &str) -> OutboundMessage {
    OutboundMessage {
        job_id,
        alert_id: Uuid::new_v4(),
        recipient_id: "anna".to_string(),
        address: "+15550001111".to_string(),
        severity: "critical".to_string(),
        subject: String::new(),
        body: body.to_string(),
    }
}

/// One `gateway_receipts` row as raw JSON for assertions.
async fn receipt_row(pool: &PgPool, job_id: Uuid) -> Option<Value> {
    let row: Option<(Uuid, String, String, String, String, i32, Uuid)> = sqlx::query_as(
        "SELECT receipt_id, channel, recipient, to_address, text, duplicates, alert_id \
         FROM gateway_receipts WHERE job_id = $1",
    )
    .bind(job_id)
    .fetch_optional(pool)
    .await
    .expect("read gateway_receipts");
    row.map(
        |(receipt_id, channel, recipient, to_address, text, duplicates, alert_id)| {
            serde_json::json!({
                "receipt_id": receipt_id,
                "channel": channel,
                "recipient": recipient,
                "to_address": to_address,
                "text": text,
                "duplicates": duplicates,
                "alert_id": alert_id,
            })
        },
    )
}

// ---------------------------------------------------------------------------------------------
// 1. Send persists; resend is a duplicate with the same receipt id.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn gateway_send_persists_and_dedupes(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_gateway(pool.clone()).await;

    let sms = channel_for(GatewayKind::Sms, &base_url);
    let phone = channel_for(GatewayKind::Phone, &base_url);
    assert_eq!(sms.name(), "sms");
    assert_eq!(phone.name(), "phone");

    // SMS send.
    let job_id = Uuid::new_v4();
    let alert_id = Uuid::new_v4();
    let mut msg = sms_msg(job_id, "hello sms");
    msg.alert_id = alert_id;
    let receipt = sms.send(&msg).await.expect("sms send");
    assert_eq!(receipt.detail["duplicate"], Value::Bool(false));
    assert_eq!(receipt.detail["http_status"], Value::from(200));
    assert_eq!(receipt.detail["gateway"], Value::String(base_url.clone()));
    let row = receipt_row(&pool, job_id)
        .await
        .expect("sms receipt row persisted");
    assert_eq!(row["channel"], Value::String("sms".to_string()));
    assert_eq!(row["recipient"], Value::String("user:anna".to_string()));
    assert_eq!(row["to_address"], Value::String("+15550001111".to_string()));
    assert_eq!(row["text"], Value::String("hello sms".to_string()));
    assert_eq!(row["duplicates"], Value::from(0));
    assert_eq!(row["alert_id"], Value::String(alert_id.to_string()));
    assert_eq!(
        row["receipt_id"],
        Value::String(receipt.provider_message_id.clone())
    );

    // Phone send.
    let phone_job = Uuid::new_v4();
    let phone_msg = OutboundMessage {
        job_id: phone_job,
        alert_id: Uuid::new_v4(),
        recipient_id: "bob".to_string(),
        address: "+15550002222".to_string(),
        severity: "warning".to_string(),
        subject: String::new(),
        body: "tts script".to_string(),
    };
    let phone_receipt = phone.send(&phone_msg).await.expect("phone send");
    assert_eq!(phone_receipt.detail["duplicate"], Value::Bool(false));
    let phone_row = receipt_row(&pool, phone_job)
        .await
        .expect("phone receipt row persisted");
    assert_eq!(phone_row["channel"], Value::String("phone".to_string()));
    assert_eq!(
        phone_row["recipient"],
        Value::String("user:bob".to_string())
    );

    // Resend the same SMS job id: duplicate, same receipt id, duplicates = 1, still one row.
    let dup = sms.send(&msg).await.expect("sms resend");
    assert_eq!(dup.detail["duplicate"], Value::Bool(true));
    assert_eq!(dup.provider_message_id, receipt.provider_message_id);
    let row2 = receipt_row(&pool, job_id)
        .await
        .expect("receipt row still there");
    assert_eq!(row2["receipt_id"], row["receipt_id"]);
    assert_eq!(row2["duplicates"], Value::from(1));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM gateway_receipts")
        .fetch_one(&pool)
        .await
        .expect("count receipts");
    assert_eq!(
        count, 2,
        "one row per job id, duplicates only bump the counter"
    );
}

// ---------------------------------------------------------------------------------------------
// 2. `down` outage: 503 + Retry-After, nothing persisted; phone unaffected; clear recovers.
//    Also via the HTTP outage API.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn gateway_down_outage_blocks_sms_only(pool: PgPool) {
    let (state, base_url, _shutdown) = start_gateway(pool.clone()).await;
    let http = reqwest::Client::new();
    let sms = channel_for(GatewayKind::Sms, &base_url);
    let phone = channel_for(GatewayKind::Phone, &base_url);

    state.set_outage("sms", OutageMode::Down, 30);

    // SMS send fails with RetryAfter(2s) and persists nothing.
    let job_id = Uuid::new_v4();
    let err = sms
        .send(&sms_msg(job_id, "blocked"))
        .await
        .expect_err("sms must fail during a down outage");
    match err {
        SendError::RetryAfter(_, d) => assert_eq!(d, Duration::from_secs(2)),
        e => panic!("expected RetryAfter(2s), got {e:?}"),
    }
    assert!(
        receipt_row(&pool, job_id).await.is_none(),
        "down outage must persist nothing"
    );

    // Phone is unaffected.
    let phone_job = Uuid::new_v4();
    let mut phone_msg = sms_msg(phone_job, "tts ok");
    phone_msg.recipient_id = "bob".to_string();
    phone
        .send(&phone_msg)
        .await
        .expect("phone works during an sms outage");

    // The outage is visible over HTTP.
    let outages: Value = http
        .get(format!("{base_url}/api/simulator/outage"))
        .send()
        .await
        .expect("GET outage")
        .error_for_status()
        .expect("GET outage status")
        .json()
        .await
        .expect("GET outage JSON");
    let list = outages["outages"].as_array().expect("outages array");
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["channel"], Value::String("sms".to_string()));
    assert_eq!(list[0]["mode"], Value::String("down".to_string()));

    // Unknown channel is a 400 `{error: ...}`.
    let bad: reqwest::Response = http
        .post(format!("{base_url}/api/simulator/outage"))
        .json(&serde_json::json!({"channel": "pager", "seconds": 10}))
        .send()
        .await
        .expect("POST bad outage");
    assert_eq!(bad.status().as_u16(), 400);
    let bad_body: Value = bad.json().await.expect("bad outage JSON");
    assert_eq!(
        bad_body["error"]["code"],
        Value::String("bad_request".to_string())
    );

    // DELETE ends it early; SMS works again.
    http.delete(format!("{base_url}/api/simulator/outage?channel=sms"))
        .send()
        .await
        .expect("DELETE outage")
        .error_for_status()
        .expect("DELETE outage status");
    sms.send(&sms_msg(job_id, "recovered"))
        .await
        .expect("sms works after clear");
    assert!(
        receipt_row(&pool, job_id).await.is_some(),
        "row persisted after the outage cleared"
    );

    // POST outage over HTTP has the same effect as set_outage.
    http.post(format!("{base_url}/api/simulator/outage"))
        .json(&serde_json::json!({"channel": "sms", "seconds": 30, "mode": "down"}))
        .send()
        .await
        .expect("POST outage")
        .error_for_status()
        .expect("POST outage status");
    let err = sms
        .send(&sms_msg(Uuid::new_v4(), "blocked again"))
        .await
        .expect_err("sms must fail after HTTP outage POST");
    assert!(matches!(err, SendError::RetryAfter(_, _)), "got {err:?}");
    state.clear_outage("sms");
}

// ---------------------------------------------------------------------------------------------
// 3. `lose_response` outage: ambiguous send, but the row is persisted; resend dedupes.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn gateway_lose_response_is_ambiguous_but_persisted(pool: PgPool) {
    let (state, base_url, _shutdown) = start_gateway(pool.clone()).await;
    let sms = channel_for(GatewayKind::Sms, &base_url);

    state.set_outage("sms", OutageMode::LoseResponse, 30);
    let job_id = Uuid::new_v4();
    let msg = sms_msg(job_id, "maybe accepted");
    let err = sms
        .send(&msg)
        .await
        .expect_err("lose_response must surface as ambiguous");
    assert!(matches!(err, SendError::Ambiguous(_)), "got {err:?}");
    let row = receipt_row(&pool, job_id)
        .await
        .expect("lose_response persists the row");
    assert_eq!(row["text"], Value::String("maybe accepted".to_string()));

    // After the outage clears, resending is a duplicate of the SAME row.
    state.clear_outage("sms");
    let dup = sms.send(&msg).await.expect("resend after clear");
    assert_eq!(dup.detail["duplicate"], Value::Bool(true));
    assert_eq!(dup.provider_message_id, row["receipt_id"].as_str().unwrap());
    let row2 = receipt_row(&pool, job_id).await.expect("row");
    assert_eq!(row2["duplicates"], Value::from(1));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM gateway_receipts WHERE job_id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(count, 1, "still exactly one row for the job");
}

// ---------------------------------------------------------------------------------------------
// 4. Connection refused -> Transient.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn gateway_connection_refused_is_transient(pool: PgPool) {
    let _ = pool;
    // A port nothing listens on.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    let dead = GatewayChannel::new(
        GatewayKind::Sms,
        GatewayClientConfig {
            base_url: format!("http://127.0.0.1:{port}"),
            public_url: "http://localhost:5173".to_string(),
            timeout: Duration::from_secs(3),
            token: None,
        },
    )
    .expect("build channel");
    let err = dead
        .send(&sms_msg(Uuid::new_v4(), "nobody home"))
        .await
        .expect_err("refused connection must fail");
    assert!(matches!(err, SendError::Transient(_)), "got {err:?}");
}

// ---------------------------------------------------------------------------------------------
// 5. Simulator messages: filters + keyset pagination, newest first.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn gateway_simulator_messages_filters_and_pagination(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_gateway(pool.clone()).await;
    let http = reqwest::Client::new();
    let sms = channel_for(GatewayKind::Sms, &base_url);
    let phone = channel_for(GatewayKind::Phone, &base_url);

    // 5 SMS to +15550001111/user:anna plus 2 phone to +15550002222/user:bob, sent oldest first.
    let mut sms_jobs = Vec::new();
    for i in 0..5 {
        let job_id = Uuid::new_v4();
        let msg = OutboundMessage {
            job_id,
            alert_id: Uuid::new_v4(),
            recipient_id: "anna".to_string(),
            address: "+15550001111".to_string(),
            severity: "warning".to_string(),
            subject: String::new(),
            body: format!("sms {i}"),
        };
        sms.send(&msg).await.expect("sms send");
        sms_jobs.push(job_id);
    }
    for i in 0..2 {
        let msg = OutboundMessage {
            job_id: Uuid::new_v4(),
            alert_id: Uuid::new_v4(),
            recipient_id: "bob".to_string(),
            address: "+15550002222".to_string(),
            severity: "info".to_string(),
            subject: String::new(),
            body: format!("phone {i}"),
        };
        phone.send(&msg).await.expect("phone send");
    }

    async fn get_page(http: &reqwest::Client, base_url: &str, query: &[(&str, String)]) -> Value {
        http.get(format!("{base_url}/api/simulator/messages"))
            .query(query)
            .send()
            .await
            .expect("GET messages")
            .error_for_status()
            .expect("GET messages status")
            .json()
            .await
            .expect("GET messages JSON")
    }

    // Channel filter.
    let sms_only = get_page(
        &http,
        &base_url,
        &[("channel", "sms".to_string()), ("limit", "50".to_string())],
    )
    .await;
    assert_eq!(sms_only["items"].as_array().unwrap().len(), 5);
    assert!(sms_only["next_cursor"].is_null());
    // Recipient + to filters.
    let bob = get_page(&http, &base_url, &[("recipient", "user:bob".to_string())]).await;
    assert_eq!(bob["items"].as_array().unwrap().len(), 2);
    let to = get_page(&http, &base_url, &[("to", "+15550001111".to_string())]).await;
    assert_eq!(to["items"].as_array().unwrap().len(), 5);

    // Pagination with limit=2 over the 5 SMS rows: newest first, 3 pages, null at the end.
    let mut seen_texts: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut q = vec![("channel", "sms".to_string()), ("limit", "2".to_string())];
        if let Some(c) = cursor.take() {
            q.push(("cursor", c));
        }
        let page = get_page(&http, &base_url, &q).await;
        let items = page["items"].as_array().expect("items array").clone();
        assert!(items.len() <= 2, "page too big: {}", items.len());
        for item in &items {
            let item: &Value = item;
            seen_texts.push(item["text"].as_str().unwrap().to_string());
            // Shape check: a SimMessage.
            for key in [
                "id",
                "receipt_id",
                "job_id",
                "alert_id",
                "channel",
                "recipient",
                "to",
                "text",
                "duplicates",
                "received_at",
            ] {
                assert!(item.get(key).is_some(), "missing key {key} in {item}");
            }
        }
        pages += 1;
        match &page["next_cursor"] {
            Value::String(next) => cursor = Some(next.clone()),
            Value::Null => break,
            other => panic!("unexpected next_cursor: {other}"),
        }
        assert!(pages < 10, "pagination did not terminate");
    }
    assert_eq!(pages, 3, "5 rows at limit 2 -> 3 pages");
    assert_eq!(
        seen_texts,
        vec![
            "sms 4".to_string(),
            "sms 3".to_string(),
            "sms 2".to_string(),
            "sms 1".to_string(),
            "sms 0".to_string(),
        ],
        "newest first across pages"
    );
}

// ---------------------------------------------------------------------------------------------
// 6. SSE stream: snapshot first, then one `simulator.message` per accepted receipt.
// ---------------------------------------------------------------------------------------------

/// Read SSE events from a chunked response until `want` events are collected.
/// Returns Vec<(event_name, data)>.
async fn read_sse_events(
    resp: &mut reqwest::Response,
    want: usize,
    timeout: Duration,
) -> Vec<(String, String)> {
    let mut buf = String::new();
    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;
    while events.len() < want {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            panic!("timed out waiting for SSE events; got {events:?} from {buf:?}");
        }
        let chunk = tokio::time::timeout(remaining, resp.chunk())
            .await
            .expect("chunk timeout")
            .expect("chunk error");
        let chunk = chunk.expect("stream ended");
        buf.push_str(&String::from_utf8_lossy(&chunk));
        // Events are separated by a blank line.
        while let Some(pos) = buf.find("\n\n") {
            let block: String = buf.drain(..pos + 2).collect();
            let mut name: Option<String> = None;
            let mut data = String::new();
            for line in block.lines() {
                if let Some(rest) = line.strip_prefix("event:") {
                    name = Some(rest.trim().to_string());
                } else if let Some(rest) = line.strip_prefix("data:") {
                    let rest = rest.strip_prefix(' ').unwrap_or(rest);
                    if !data.is_empty() {
                        data.push('\n');
                    }
                    data.push_str(rest);
                }
            }
            if let Some(name) = name {
                events.push((name, data));
            }
            if events.len() >= want {
                break;
            }
        }
    }
    events
}

#[sqlx::test(migrations = "../../migrations")]
async fn gateway_sse_snapshot_then_live_message(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_gateway(pool.clone()).await;

    // Open the stream before sending anything.
    let mut resp = reqwest::Client::new()
        .get(format!("{base_url}/api/simulator/stream"))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .expect("GET stream");
    assert!(
        resp.status().is_success(),
        "stream status: {}",
        resp.status()
    );
    assert_eq!(
        resp.headers()
            .get("x-accel-buffering")
            .and_then(|v| v.to_str().ok()),
        Some("no")
    );

    // First event is the snapshot (empty feed).
    let events = read_sse_events(&mut resp, 1, Duration::from_secs(10)).await;
    assert_eq!(events[0].0, "snapshot");
    let snapshot: Value = serde_json::from_str(&events[0].1).expect("snapshot JSON");
    assert_eq!(
        snapshot["items"].as_array().unwrap().len(),
        0,
        "fresh DB has no receipts"
    );
    assert!(snapshot["next_cursor"].is_null());

    // Send one SMS; the next event is its simulator.message.
    let sms = channel_for(GatewayKind::Sms, &base_url);
    let job_id = Uuid::new_v4();
    sms.send(&sms_msg(job_id, "live via sse"))
        .await
        .expect("sms send");

    let events = read_sse_events(&mut resp, 1, Duration::from_secs(10)).await;
    assert_eq!(events[0].0, "simulator.message");
    let live: SimMessage = serde_json::from_str(&events[0].1).expect("simulator.message JSON");
    assert_eq!(live.job_id, job_id);
    assert_eq!(live.channel, "sms");
    assert_eq!(live.text, "live via sse");
    assert_eq!(live.to, "+15550001111");
}

// ---------------------------------------------------------------------------------------------
// 7. Auth: with a token configured, the control routes need `Authorization: Bearer`.
// ---------------------------------------------------------------------------------------------

/// Raw POST to the provider endpoint (bypasses `GatewayChannel` so a test controls headers).
async fn post_provider(
    http: &reqwest::Client,
    base_url: &str,
    job_id: Uuid,
    body: &Value,
    bearer: Option<&str>,
) -> reqwest::Response {
    let mut req = http
        .post(format!("{base_url}/provider/sms"))
        .header("Idempotency-Key", job_id.to_string())
        .json(body);
    if let Some(token) = bearer {
        req = req.bearer_auth(token);
    }
    req.send().await.expect("provider POST")
}

/// A minimal valid `ProviderRequest` body for one job id.
fn provider_body(job_id: Uuid) -> Value {
    serde_json::json!({
        "job_id": job_id,
        "alert_id": Uuid::new_v4(),
        "recipient": "user:anna",
        "to": "+15550001111",
        "text": "auth test",
    })
}

async fn receipt_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM gateway_receipts")
        .fetch_one(pool)
        .await
        .expect("count receipts")
}

async fn error_body(resp: reqwest::Response) -> (u16, Value) {
    let status = resp.status().as_u16();
    let body: Value = resp.json().await.expect("error JSON");
    (status, body)
}

#[sqlx::test(migrations = "../../migrations")]
async fn gateway_with_token_requires_bearer_auth_on_control_routes(pool: PgPool) {
    const TOKEN: &str = "sekrit-gateway-token";
    let (_state, base_url, _shutdown) = start_gateway_token(pool.clone(), Some(TOKEN)).await;
    let http = reqwest::Client::new();

    // Provider POST without a token, with a wrong token, and with a case-mangled token: all
    // 401 unauthorized, and nothing is persisted by any of them.
    for bearer in [
        None,
        Some("wrong-token"),
        Some("SEKRIT-GATEWAY-TOKEN"),
        Some(""),
    ] {
        let job_id = Uuid::new_v4();
        let resp = post_provider(&http, &base_url, job_id, &provider_body(job_id), bearer).await;
        let (status, body) = error_body(resp).await;
        assert_eq!(status, 401, "bearer={bearer:?}: {body}");
        assert_eq!(body["error"]["code"], Value::String("unauthorized".into()));
    }
    assert_eq!(
        receipt_count(&pool).await,
        0,
        "401s must not persist anything"
    );

    // With the right token the provider accepts and persists.
    let job_id = Uuid::new_v4();
    let resp = post_provider(
        &http,
        &base_url,
        job_id,
        &provider_body(job_id),
        Some(TOKEN),
    )
    .await;
    assert_eq!(resp.status().as_u16(), 200);
    let accepted: Value = resp.json().await.expect("ProviderResponse JSON");
    assert_eq!(accepted["duplicate"], Value::Bool(false));
    assert_eq!(receipt_count(&pool).await, 1);

    // The GatewayChannel sends the token from its config.
    let sms = GatewayChannel::new(
        GatewayKind::Sms,
        GatewayClientConfig {
            base_url: base_url.clone(),
            public_url: "http://localhost:5173".to_string(),
            timeout: Duration::from_secs(5),
            token: Some(TOKEN.to_string()),
        },
    )
    .expect("build channel");
    let receipt = sms.send(&sms_msg(Uuid::new_v4(), "via channel")).await;
    assert!(receipt.is_ok(), "{receipt:?}");
    assert_eq!(receipt_count(&pool).await, 2);

    // Outage POST/DELETE need the token; the read endpoints stay open for the UI.
    let resp = http
        .post(format!("{base_url}/api/simulator/outage"))
        .json(&serde_json::json!({"channel": "sms", "seconds": 30}))
        .send()
        .await
        .expect("outage POST without token");
    let (status, body) = error_body(resp).await;
    assert_eq!(status, 401, "{body}");
    assert_eq!(body["error"]["code"], Value::String("unauthorized".into()));

    let resp = http
        .post(format!("{base_url}/api/simulator/outage"))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({"channel": "sms", "seconds": 30}))
        .send()
        .await
        .expect("outage POST with token");
    assert_eq!(resp.status().as_u16(), 200);

    // GET outage is open and shows the outage started above.
    let outages: Value = http
        .get(format!("{base_url}/api/simulator/outage"))
        .send()
        .await
        .expect("GET outage")
        .error_for_status()
        .expect("GET outage must stay open")
        .json()
        .await
        .expect("outages JSON");
    assert_eq!(outages["outages"].as_array().unwrap().len(), 1);

    // DELETE without the token: 401, the outage survives.
    let resp = http
        .delete(format!("{base_url}/api/simulator/outage?channel=sms"))
        .send()
        .await
        .expect("DELETE outage without token");
    assert_eq!(resp.status().as_u16(), 401);
    let outages: Value = http
        .get(format!("{base_url}/api/simulator/outage"))
        .send()
        .await
        .expect("GET outage")
        .json()
        .await
        .expect("outages JSON");
    assert_eq!(outages["outages"].as_array().unwrap().len(), 1);

    // DELETE with the token ends it.
    let resp = http
        .delete(format!("{base_url}/api/simulator/outage?channel=sms"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .expect("DELETE outage with token");
    assert_eq!(resp.status().as_u16(), 200);
    let outages: Value = http
        .get(format!("{base_url}/api/simulator/outage"))
        .send()
        .await
        .expect("GET outage")
        .json()
        .await
        .expect("outages JSON");
    assert_eq!(outages["outages"].as_array().unwrap().len(), 0);

    // The other read endpoints stay open too.
    let resp = http
        .get(format!("{base_url}/api/simulator/messages"))
        .send()
        .await
        .expect("GET messages");
    assert_eq!(resp.status().as_u16(), 200);
    let resp = http
        .get(format!("{base_url}/api/simulator/stream"))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .expect("GET stream");
    assert_eq!(resp.status().as_u16(), 200);
}

#[sqlx::test(migrations = "../../migrations")]
async fn gateway_without_token_keeps_every_route_open(pool: PgPool) {
    // Dev mode: no token configured, no Authorization header sent, everything works
    // (this is what the earlier sections exercise; here it is pinned down explicitly).
    let (_state, base_url, _shutdown) = start_gateway(pool.clone()).await;
    let http = reqwest::Client::new();

    let job_id = Uuid::new_v4();
    let resp = post_provider(&http, &base_url, job_id, &provider_body(job_id), None).await;
    assert_eq!(resp.status().as_u16(), 200);

    let resp = http
        .post(format!("{base_url}/api/simulator/outage"))
        .json(&serde_json::json!({"channel": "sms", "seconds": 10}))
        .send()
        .await
        .expect("outage POST");
    assert_eq!(resp.status().as_u16(), 200);

    let resp = http
        .delete(format!("{base_url}/api/simulator/outage?channel=sms"))
        .send()
        .await
        .expect("outage DELETE");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(receipt_count(&pool).await, 1);
}

// ---------------------------------------------------------------------------------------------
// 8. Field validation: bounds and NUL bytes on ProviderRequest.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn gateway_rejects_oversize_and_invalid_provider_fields(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_gateway(pool.clone()).await;
    let http = reqwest::Client::new();

    async fn assert_bad_request(http: &reqwest::Client, base_url: &str, body: &Value) -> String {
        let job_id: Uuid = serde_json::from_value(body["job_id"].clone()).expect("job_id");
        let resp = post_provider(http, base_url, job_id, body, None).await;
        let (status, err) = error_body(resp).await;
        assert_eq!(status, 400, "{err}");
        assert_eq!(err["error"]["code"], Value::String("bad_request".into()));
        err["error"]["message"]
            .as_str()
            .expect("message")
            .to_string()
    }

    // Oversize fields.
    let mut big_text = provider_body(Uuid::new_v4());
    big_text["text"] = Value::String("x".repeat(1601));
    assert_eq!(
        assert_bad_request(&http, &base_url, &big_text).await,
        "text exceeds 1600 chars"
    );

    let mut bad_to = provider_body(Uuid::new_v4());
    bad_to["to"] = Value::String("+1a55-5000".into());
    assert_eq!(
        assert_bad_request(&http, &base_url, &bad_to).await,
        "to must be at most 32 chars of +, digits and spaces"
    );

    let mut long_to = provider_body(Uuid::new_v4());
    long_to["to"] = Value::String("+".repeat(33));
    assert_eq!(
        assert_bad_request(&http, &base_url, &long_to).await,
        "to must be at most 32 chars of +, digits and spaces"
    );

    let mut big_recipient = provider_body(Uuid::new_v4());
    big_recipient["recipient"] = Value::String("u".repeat(129));
    assert_eq!(
        assert_bad_request(&http, &base_url, &big_recipient).await,
        "recipient exceeds 128 chars"
    );

    let mut big_challenge = provider_body(Uuid::new_v4());
    big_challenge["approval_challenge"] = Value::String("c".repeat(33));
    assert_eq!(
        assert_bad_request(&http, &base_url, &big_challenge).await,
        "approval_challenge exceeds 32 chars"
    );

    // NUL bytes in every text-ish field (these used to become 500s inside the query).
    for field in ["text", "to", "recipient", "approval_challenge"] {
        let mut nul = provider_body(Uuid::new_v4());
        nul[field] = Value::String("a\0b".into());
        let message = assert_bad_request(&http, &base_url, &nul).await;
        assert!(
            message.contains("NUL"),
            "NUL in {field} must be a 400, got {message:?}"
        );
    }

    assert_eq!(
        receipt_count(&pool).await,
        0,
        "rejected requests must not persist anything"
    );

    // The boundary itself passes: exactly 1600 chars.
    let job_id = Uuid::new_v4();
    let mut boundary = provider_body(job_id);
    boundary["text"] = Value::String("x".repeat(1600));
    let resp = post_provider(&http, &base_url, job_id, &boundary, None).await;
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(receipt_count(&pool).await, 1);
}

// ---------------------------------------------------------------------------------------------
// 9. Body limit: > 16 KiB on the provider and outage POST routes is a 413.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn gateway_caps_provider_and_outage_bodies_at_16_kib(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_gateway(pool.clone()).await;
    let http = reqwest::Client::new();

    // A ~20 KiB provider body (giant text field): rejected by the route's body limit
    // before any field validation runs.
    let job_id = Uuid::new_v4();
    let mut big = provider_body(job_id);
    big["text"] = Value::String("x".repeat(20 * 1024));
    let resp = post_provider(&http, &base_url, job_id, &big, None).await;
    let (status, body) = error_body(resp).await;
    assert_eq!(status, 413, "{body}");
    assert_eq!(body["error"]["code"], Value::String("bad_request".into()));

    // The outage POST route carries the same cap; a padded body is refused and starts
    // nothing.
    let resp = http
        .post(format!("{base_url}/api/simulator/outage"))
        .json(&serde_json::json!({
            "channel": "sms",
            "seconds": 10,
            "padding": "p".repeat(20 * 1024),
        }))
        .send()
        .await
        .expect("outage POST");
    let (status, body) = error_body(resp).await;
    assert!(status == 413 || status == 400, "status {status}: {body}");
    assert_eq!(body["error"]["code"], Value::String("bad_request".into()));

    assert_eq!(receipt_count(&pool).await, 0);
    let outages: Value = http
        .get(format!("{base_url}/api/simulator/outage"))
        .send()
        .await
        .expect("GET outage")
        .json()
        .await
        .expect("outages JSON");
    assert_eq!(outages["outages"].as_array().unwrap().len(), 0);
}

// ---------------------------------------------------------------------------------------------
// 10. Cursor cap: longer than 256 chars is refused before decoding.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn gateway_rejects_overlong_cursors_before_decoding(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_gateway(pool.clone()).await;
    let http = reqwest::Client::new();

    // 300 chars of valid base64: over the cap, so it never reaches the decoder.
    let resp = http
        .get(format!("{base_url}/api/simulator/messages"))
        .query(&[("cursor", "A".repeat(300))])
        .send()
        .await
        .expect("GET messages");
    let (status, body) = error_body(resp).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body["error"]["message"],
        Value::String("cursor too long".into())
    );

    // A short undecodable cursor is still the plain invalid-cursor 400.
    let resp = http
        .get(format!("{base_url}/api/simulator/messages"))
        .query(&[("cursor", "!!!")])
        .send()
        .await
        .expect("GET messages");
    let (status, body) = error_body(resp).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body["error"]["message"],
        Value::String("invalid cursor".into())
    );
}

// ---------------------------------------------------------------------------------------------
// 11. 500s never leak sqlx details.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn gateway_internal_errors_have_a_fixed_generic_body(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_gateway(pool.clone()).await;
    let http = reqwest::Client::new();
    // Break the persist path: the throwaway test DB is the only casualty.
    sqlx::query("DROP TABLE gateway_receipts")
        .execute(&pool)
        .await
        .expect("drop gateway_receipts");

    let expected = serde_json::json!({
        "error": { "code": "internal", "message": "internal error" }
    });

    // Provider POST: the persist fails; the client sees the fixed body, not the sqlx text.
    let job_id = Uuid::new_v4();
    let resp = post_provider(&http, &base_url, job_id, &provider_body(job_id), None).await;
    let (status, body) = error_body(resp).await;
    assert_eq!(status, 500, "{body}");
    assert_eq!(body, expected);
    assert!(
        !body.to_string().to_lowercase().contains("relation"),
        "driver error text leaked: {body}"
    );

    // The read endpoints fail the same fixed way.
    let resp = http
        .get(format!("{base_url}/api/simulator/messages"))
        .send()
        .await
        .expect("GET messages");
    let (status, body) = error_body(resp).await;
    assert_eq!(status, 500, "{body}");
    assert_eq!(body, expected);

    let resp = http
        .get(format!("{base_url}/api/simulator/stream"))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .expect("GET stream");
    assert_eq!(resp.status().as_u16(), 500);
    let (_, body) = error_body(resp).await;
    assert_eq!(body, expected);
}

// ---------------------------------------------------------------------------------------------
// 12. Client hardening: redirects are never followed; error bodies are read bounded.
// ---------------------------------------------------------------------------------------------

/// Serve `router` on an ephemeral port for the rest of the test.
async fn serve_test_router(router: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let addr = listener.local_addr().expect("test server addr");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("test server");
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn gateway_channel_never_follows_redirects() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let moved_hits = Arc::new(AtomicUsize::new(0));
    let hits_for_target = moved_hits.clone();
    let router = axum::Router::new()
        .route(
            "/provider/sms",
            axum::routing::post(|| async {
                // A 302 pointing somewhere else on the same server.
                (
                    axum::http::StatusCode::FOUND,
                    [("location", "/moved")],
                    "the provider moved",
                )
            }),
        )
        .route(
            "/moved",
            axum::routing::get(move || {
                let hits = hits_for_target.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    "the redirect target must never be reached"
                }
            }),
        );
    let base_url = serve_test_router(router).await;

    let sms = channel_for(GatewayKind::Sms, &base_url);
    let err = sms
        .send(&sms_msg(Uuid::new_v4(), "redirect me"))
        .await
        .expect_err("a 302 must be an error, not a silent follow");
    match err {
        SendError::Permanent(m) => assert!(m.contains("302"), "message: {m}"),
        e => panic!("a redirect must be Permanent, got {e:?}"),
    }
    assert_eq!(
        moved_hits.load(Ordering::SeqCst),
        0,
        "the redirect target was followed"
    );
}

#[tokio::test]
async fn gateway_channel_stops_reading_huge_error_bodies_early() {
    use std::time::Instant;

    // 10 MiB of 'x' in 64 KiB chunks, every chunk after the first delayed by 25 ms: fully
    // reading the body would take >= 4 s and run into the 2 s channel timeout below (which
    // maps to Ambiguous); a client that stops after its 4 KiB cap returns as Transient in
    // milliseconds.
    const CHUNK: usize = 64 * 1024;
    const CHUNKS: u32 = 160; // 160 * 64 KiB = 10 MiB
    let router = axum::Router::new().route(
        "/provider/sms",
        axum::routing::post(|| async {
            let body =
                axum::body::Body::from_stream(futures::stream::unfold(0u32, |i| async move {
                    if i >= CHUNKS {
                        return None;
                    }
                    if i > 0 {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    Some((
                        Ok::<axum::body::Bytes, std::io::Error>(axum::body::Bytes::from(vec![
                            b'x';
                            CHUNK
                        ])),
                        i + 1,
                    ))
                }));
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, body)
        }),
    );
    let base_url = serve_test_router(router).await;

    let sms = GatewayChannel::new(
        GatewayKind::Sms,
        GatewayClientConfig {
            base_url,
            public_url: "http://localhost:5173".to_string(),
            timeout: Duration::from_secs(2),
            token: None,
        },
    )
    .expect("build channel");

    let started = Instant::now();
    let err = sms
        .send(&sms_msg(Uuid::new_v4(), "boom"))
        .await
        .expect_err("a 500 must fail");
    match err {
        SendError::Transient(m) => {
            assert!(m.contains("500"), "message: {m}");
            // The snippet is truncated to 300 chars, not the 4 KiB read, not the 10 MiB body.
            assert!(
                m.matches('x').count() <= 300,
                "snippet not truncated: {} x's",
                m.matches('x').count()
            );
        }
        e => panic!("a 500 must be Transient, got {e:?}"),
    }
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the client must stop reading the error body early, took {:?}",
        started.elapsed()
    );
}
