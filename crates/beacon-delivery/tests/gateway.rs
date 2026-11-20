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
    let state = GatewayState::new(pool);
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
