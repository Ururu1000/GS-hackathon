//! Security-audit regression suite (Agent A): gateway auth lockdown, signed SMS replies,
//! SSE stream cap, bounded provider bodies, PII masking, approval prompts that lost their
//! race (S11-9) and SMTP TLS decisions.
//!
//! Needs Postgres (`DATABASE_URL`, sqlx creates a throwaway database per test).
//!
//! Run: `cargo test -p beacon-delivery --test security_gateway`

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beacon_delivery::channel::email::{
    EmailChannel, SmtpConfig, SmtpTls, is_local_smtp_host, mask_email, mask_emails_in_text,
    smtp_security,
};
use beacon_delivery::channel::gateway_client::{GatewayChannel, GatewayClientConfig, GatewayKind};
use beacon_delivery::channel::{Channel, OutboundMessage, SendError};
use beacon_delivery::gateway::{self, GatewayState, mask_phone};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

/// Serve `state` on an ephemeral loopback port. Returns the base URL and the shutdown
/// sender (keep it alive for the whole test; dropping it stops the server).
async fn serve_state(state: GatewayState) -> (String, tokio::sync::watch::Sender<bool>) {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let serve_state = state.clone();
    tokio::spawn(async move {
        gateway::serve(
            serve_state,
            "127.0.0.1:0".parse().expect("loopback addr"),
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
    (format!("http://{addr}"), shutdown_tx)
}

fn sms_channel(base_url: &str, token: Option<&str>) -> GatewayChannel {
    GatewayChannel::new(
        GatewayKind::Sms,
        GatewayClientConfig {
            base_url: base_url.to_string(),
            public_url: "http://localhost:5173".to_string(),
            timeout: Duration::from_secs(5),
            token: token.map(str::to_string),
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

async fn error_body(resp: reqwest::Response) -> (u16, Value) {
    let status = resp.status().as_u16();
    let body: Value = resp.json().await.expect("error JSON");
    (status, body)
}

// ---------------------------------------------------------------------------------------------
// 1. Reads need the token when one is set (audit #11 Crit + #9 High).
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn simulator_reads_require_the_token_when_configured(pool: PgPool) {
    const TOKEN: &str = "sekrit-read-token";
    let (base_url, _shutdown) =
        serve_state(GatewayState::new(pool).with_token(Some(TOKEN.to_string()))).await;
    let http = reqwest::Client::new();

    for path in [
        "/api/simulator/messages",
        "/api/simulator/outage",
        "/api/simulator/mock",
    ] {
        let resp = http
            .get(format!("{base_url}{path}"))
            .send()
            .await
            .expect("GET without token");
        let (status, body) = error_body(resp).await;
        assert_eq!(status, 401, "{path}");
        assert_eq!(body["error"]["code"], Value::String("unauthorized".into()));
        let resp = http
            .get(format!("{base_url}{path}"))
            .bearer_auth(TOKEN)
            .send()
            .await
            .expect("GET with token");
        assert_eq!(resp.status().as_u16(), 200, "{path}");
    }

    // The SSE stream is gated the same way (401 is JSON, not an event stream).
    let resp = http
        .get(format!("{base_url}/api/simulator/stream"))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .expect("GET stream without token");
    let (status, body) = error_body(resp).await;
    assert_eq!(status, 401, "{body}");
    assert_eq!(body["error"]["code"], Value::String("unauthorized".into()));
    let resp = http
        .get(format!("{base_url}/api/simulator/stream"))
        .bearer_auth(TOKEN)
        .header("Accept", "text/event-stream")
        .send()
        .await
        .expect("GET stream with token");
    assert_eq!(resp.status().as_u16(), 200);

    // And the provider routes stay gated too.
    let job_id = Uuid::new_v4();
    let resp = http
        .post(format!("{base_url}/provider/sms"))
        .header("Idempotency-Key", job_id.to_string())
        .json(&json!({
            "job_id": job_id, "alert_id": Uuid::new_v4(),
            "recipient": "user:anna", "to": "+15550001111", "text": "no token",
        }))
        .send()
        .await
        .expect("provider POST without token");
    assert_eq!(resp.status().as_u16(), 401);
}

// ---------------------------------------------------------------------------------------------
// 1b. serve refuses a non-loopback bind without a token.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn serve_refuses_non_loopback_bind_without_token(pool: PgPool) {
    // No token + a non-loopback address: a clear io::Error instead of a bound socket.
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let err = gateway::serve(
        GatewayState::new(pool.clone()),
        "0.0.0.0:0".parse().expect("addr"),
        rx,
        |_| {},
    )
    .await
    .expect_err("unauthenticated gateway must not bind past loopback");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert!(
        err.to_string().contains("GATEWAY_TOKEN"),
        "must name the missing token: {err}"
    );

    // Loopback without a token still binds (dev mode).
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let loopback_pool = pool.clone();
    let task = tokio::spawn(async move {
        gateway::serve(
            GatewayState::new(loopback_pool),
            "127.0.0.1:0".parse().expect("addr"),
            shutdown_rx,
            |addr| {
                let _ = bound_tx.send(addr);
            },
        )
        .await
    });
    let addr = tokio::time::timeout(Duration::from_secs(10), bound_rx)
        .await
        .expect("loopback bind timed out")
        .expect("on_bound dropped");
    assert!(addr.ip().is_loopback());
    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("serve must stop")
        .expect("serve task panicked")
        .expect("loopback dev serve must succeed");

    // Non-loopback WITH a token binds fine.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        gateway::serve(
            GatewayState::new(pool).with_token(Some("t".to_string())),
            "0.0.0.0:0".parse().expect("addr"),
            shutdown_rx,
            |addr| {
                let _ = bound_tx.send(addr);
            },
        )
        .await
    });
    let addr = tokio::time::timeout(Duration::from_secs(10), bound_rx)
        .await
        .expect("token bind timed out")
        .expect("on_bound dropped");
    assert!(!addr.ip().is_loopback(), "{addr}");
    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("serve must stop")
        .expect("serve task panicked")
        .expect("token serve must succeed");
}

// ---------------------------------------------------------------------------------------------
// 2. The reply endpoint always needs the token; the forward carries the shared signature.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn reply_is_unavailable_without_a_token_and_signs_nothing(pool: PgPool) {
    // No token configured (but a forward URL set): 503 naming GATEWAY_TOKEN.
    let (base_url, _shutdown) = serve_state(
        GatewayState::new(pool.clone())
            .with_reply_forward(Some("http://127.0.0.1:9".to_string()), Some(b"s".to_vec())),
    )
    .await;
    let http = reqwest::Client::new();
    let resp = http
        .post(format!("{base_url}/api/simulator/reply"))
        .json(&json!({"from": "+15550001111", "text": "Y 0000"}))
        .send()
        .await
        .expect("POST reply without token configured");
    let (status, body) = error_body(resp).await;
    assert_eq!(status, 503, "{body}");
    assert_eq!(body["error"]["code"], Value::String("unavailable".into()));
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("GATEWAY_TOKEN"),
        "{body}"
    );

    // A token but nowhere to forward: 503 naming the missing delivery API.
    let (base_url, _shutdown) =
        serve_state(GatewayState::new(pool).with_token(Some("t".to_string()))).await;
    let resp = http
        .post(format!("{base_url}/api/simulator/reply"))
        .bearer_auth("t")
        .json(&json!({"from": "+15550001111", "text": "Y 0000"}))
        .send()
        .await
        .expect("POST reply with token, no forward");
    let (status, body) = error_body(resp).await;
    assert_eq!(status, 503, "{body}");
    assert_eq!(body["error"]["code"], Value::String("unavailable".into()));
}

/// The captured request at the fake delivery API.
#[derive(Debug, Default)]
struct CapturedReply {
    count: usize,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

async fn run_receiver(captured: Arc<Mutex<CapturedReply>>) -> String {
    use axum::body::Bytes;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    let router = axum::Router::new().route(
        "/api/replies",
        axum::routing::post(move |headers: axum::http::HeaderMap, body: Bytes| {
            let captured = captured.clone();
            async move {
                let mut lock = captured.lock().expect("capture lock");
                lock.count += 1;
                lock.headers = headers
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.as_str().to_string(),
                            v.to_str().unwrap_or_default().to_string(),
                        )
                    })
                    .collect();
                lock.body = body.to_vec();
                (
                    StatusCode::OK,
                    axum::Json(json!({"outcome": "already_decided"})),
                )
                    .into_response()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind receiver");
    let addr = listener.local_addr().expect("receiver addr");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("receiver serve");
    });
    format!("http://{addr}")
}

#[sqlx::test(migrations = "../../migrations")]
async fn reply_forward_carries_timestamp_and_hmac_signature(pool: PgPool) {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    let secret = b"shared-reply-secret".to_vec();
    let captured = Arc::new(Mutex::new(CapturedReply::default()));
    let api_base = run_receiver(captured.clone()).await;

    let (base_url, _shutdown) = serve_state(
        GatewayState::new(pool)
            .with_token(Some("gw-token".to_string()))
            .with_reply_forward(Some(api_base), Some(secret.clone())),
    )
    .await;
    let http = reqwest::Client::new();
    let payload = json!({"from": "+447700900125", "text": "Y 4821"}).to_string();
    let resp = http
        .post(format!("{base_url}/api/simulator/reply"))
        .bearer_auth("gw-token")
        .header("Content-Type", "application/json")
        .body(payload.clone())
        .send()
        .await
        .expect("POST reply");
    assert_eq!(resp.status().as_u16(), 200);
    let relayed: Value = resp.json().await.expect("relayed JSON");
    assert_eq!(relayed["outcome"], json!("already_decided"));

    // The delivery API saw exactly one request with both signature headers.
    let lock = captured.lock().expect("capture lock");
    assert_eq!(lock.count, 1);
    let ts = lock
        .headers
        .get("x-beacon-timestamp")
        .expect("X-Beacon-Timestamp header");
    let sig = lock
        .headers
        .get("x-beacon-signature")
        .expect("X-Beacon-Signature header");
    let ts_secs: i64 = ts.parse().expect("unix-seconds timestamp");
    let now = chrono::Utc::now().timestamp();
    assert!(
        (now - ts_secs).abs() <= 300,
        "timestamp must be fresh: ts={ts} now={now}"
    );
    // hex(hmac_sha256(secret, "<timestamp>." + raw_body)).
    let mut mac = Hmac::<Sha256>::new_from_slice(&secret).expect("hmac key");
    mac.update(ts.as_bytes());
    mac.update(b".");
    mac.update(payload.as_bytes());
    assert_eq!(*sig, hex::encode(mac.finalize().into_bytes()));
    // The forwarded bytes are exactly what the phone sent.
    assert_eq!(lock.body, payload.as_bytes());
}

// ---------------------------------------------------------------------------------------------
// 4. SSE stream cap (default 64): the 65th stream is refused, slots are released.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn sixty_fifth_stream_is_refused_and_slots_are_released(pool: PgPool) {
    const TOKEN: &str = "stream-token";
    let (base_url, _shutdown) =
        serve_state(GatewayState::new(pool).with_token(Some(TOKEN.to_string()))).await;
    let http = reqwest::Client::new();
    let stream_url = format!("{base_url}/api/simulator/stream");

    let mut streams = Vec::new();
    for i in 0..64 {
        let resp = http
            .get(&stream_url)
            .bearer_auth(TOKEN)
            .header("Accept", "text/event-stream")
            .send()
            .await
            .expect("open stream");
        assert_eq!(resp.status().as_u16(), 200, "stream {i}");
        streams.push(resp);
    }

    // One past the cap: 503 unavailable, `too many streams`.
    let resp = http
        .get(&stream_url)
        .bearer_auth(TOKEN)
        .header("Accept", "text/event-stream")
        .send()
        .await
        .expect("65th stream");
    assert_eq!(resp.status().as_u16(), 503);
    let (status, body) = error_body(resp).await;
    assert_eq!(status, 503, "{body}");
    assert_eq!(body["error"]["code"], Value::String("unavailable".into()));
    assert_eq!(
        body["error"]["message"],
        Value::String("too many streams".into())
    );

    // A smaller explicit cap behaves the same way (and quickly).
    // (Covered by `tiny_stream_cap` below.)

    // Release one slot: dropping the response closes the connection, and the next
    // stream succeeds again.
    streams.pop();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let resp = http
            .get(&stream_url)
            .bearer_auth(TOKEN)
            .header("Accept", "text/event-stream")
            .send()
            .await
            .expect("retry stream");
        if resp.status().as_u16() == 200 {
            streams.push(resp);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the released stream slot never came back"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn tiny_stream_cap_refuses_the_second_stream(pool: PgPool) {
    const TOKEN: &str = "stream-token";
    let (base_url, _shutdown) = serve_state(
        GatewayState::new(pool)
            .with_token(Some(TOKEN.to_string()))
            .with_max_streams(1),
    )
    .await;
    let http = reqwest::Client::new();
    let stream_url = format!("{base_url}/api/simulator/stream");

    let first = http
        .get(&stream_url)
        .bearer_auth(TOKEN)
        .header("Accept", "text/event-stream")
        .send()
        .await
        .expect("first stream");
    assert_eq!(first.status().as_u16(), 200);

    let resp = http
        .get(&stream_url)
        .bearer_auth(TOKEN)
        .header("Accept", "text/event-stream")
        .send()
        .await
        .expect("second stream");
    let (status, body) = error_body(resp).await;
    assert_eq!(status, 503, "{body}");
    assert_eq!(body["error"]["code"], Value::String("unavailable".into()));
    drop(first);
}

// ---------------------------------------------------------------------------------------------
// 5. An oversize 200 body is Ambiguous, a normal one still parses (audit #9 Med).
// ---------------------------------------------------------------------------------------------

/// Serve one provider route: request bodies containing `small` get a tiny 200, everything
/// else a ~70 KiB JSON 200.
async fn serve_oversize_provider() -> String {
    use axum::body::Bytes;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    let router = axum::Router::new().route(
        "/provider/sms",
        axum::routing::post(|body: Bytes| async move {
            if body.windows(5).any(|w| w == b"small") {
                (
                    StatusCode::OK,
                    axum::Json(json!({
                        "receipt_id": Uuid::new_v4(), "duplicate": false,
                    })),
                )
                    .into_response()
            } else {
                (
                    StatusCode::OK,
                    axum::Json(json!({
                        "receipt_id": Uuid::new_v4(),
                        "duplicate": false,
                        "pad": "x".repeat(70 * 1024),
                    })),
                )
                    .into_response()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind provider");
    let addr = listener.local_addr().expect("provider addr");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("provider serve");
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn oversize_success_body_is_ambiguous() {
    let base_url = serve_oversize_provider().await;
    let sms = sms_channel(&base_url, None);

    let err = sms
        .send(&sms_msg(Uuid::new_v4(), "this answer is huge"))
        .await
        .expect_err("a >64 KiB 200 body must fail");
    match err {
        SendError::Ambiguous(m) => assert!(m.contains("oversize"), "must say oversize, got: {m}"),
        e => panic!("oversize 200 must be Ambiguous, got {e:?}"),
    }

    // A small 200 on the same client still parses.
    let receipt = sms
        .send(&sms_msg(Uuid::new_v4(), "small body please"))
        .await
        .expect("small 200 must parse");
    assert_eq!(receipt.detail["duplicate"], json!(false));
}

// ---------------------------------------------------------------------------------------------
// 6. Masking helpers and the SMTP TLS decision (mirrors the src unit tests).
// ---------------------------------------------------------------------------------------------

#[test]
fn masking_helpers_mask_pii() {
    assert_eq!(mask_phone("+447700900125"), "+44*******125");
    assert_eq!(mask_phone("+15550001111"), "+15******111");
    assert_eq!(mask_email("anna@example.test"), "a***@example.test");
    assert_eq!(
        mask_emails_in_text("refused for anna@example.test: 550"),
        "refused for a***@example.test: 550"
    );
    // Recipient ids are fine as-is.
    assert_eq!(mask_phone("user:anna"), "use***nna");
}

#[tokio::test]
async fn smtp_tls_decision_refuses_remote_plaintext() {
    assert!(is_local_smtp_host("localhost"));
    assert!(is_local_smtp_host("mailpit"));
    assert!(!is_local_smtp_host("smtp.example.test"));

    let remote = SmtpConfig {
        host: "smtp.example.test".to_string(),
        ..SmtpConfig::default()
    };
    assert_eq!(
        smtp_security(&remote),
        Err("plaintext SMTP to a non-local host needs SmtpConfig::allow_plaintext".to_string())
    );
    let err = match EmailChannel::new(remote) {
        Ok(_) => panic!("must refuse remote plaintext"),
        Err(e) => e,
    };
    assert_eq!(
        err,
        "plaintext SMTP to a non-local host needs SmtpConfig::allow_plaintext"
    );
    let mut allowed = SmtpConfig {
        host: "smtp.example.test".to_string(),
        allow_plaintext: true,
        ..SmtpConfig::default()
    };
    assert_eq!(smtp_security(&allowed), Ok(SmtpTls::None));
    assert!(EmailChannel::new(allowed.clone()).is_ok());
    allowed.tls = SmtpTls::StartTls;
    assert_eq!(smtp_security(&allowed), Ok(SmtpTls::StartTls));
    // The rustls backend is compiled in: STARTTLS to a remote host builds a TLS transport
    // (the upgrade is mandatory at send time, never a plaintext fallback).
    assert!(
        EmailChannel::new(allowed).is_ok(),
        "STARTTLS transport must build"
    );
}

// ---------------------------------------------------------------------------------------------
// 7. A prompt accepted after its approval stopped being pending (S11-9): no code at rest.
// ---------------------------------------------------------------------------------------------

/// One alert, its prompt sms job, and (when `state` is `Some`) an approvals row in exactly
/// that state whose id the job's `route_info.approval.id` names. `None` seeds a job that
/// references an approval that does not exist. Returns `(alert, job)`.
async fn seed_prompt_with_approval_state(
    pool: &PgPool,
    state: Option<&str>,
    code: &str,
) -> (Uuid, Uuid) {
    let alert = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO alerts (id, rule_id, rule_revision, rule_name, team, fingerprint, \
                             dedup_window_ms, status, severity, color, title, body, \
                             first_seen, last_seen, source, schema, notify_seq) \
         VALUES ($1, 'rates-rejected-trades', 1, 'Rates rejected trades', 'rates', $2, \
                 600000, 'open', 'warning', 'amber', 'late prompt', '', \
                 now(), now(), 'test', 'trade.rejected.v1', 1)",
    )
    .bind(alert)
    .bind(format!("fp-{alert}"))
    .execute(pool)
    .await
    .expect("alert");

    let approval = Uuid::new_v4();
    if let Some(state) = state {
        // The table never stores the code itself, only its hash; a stand-in is fine here.
        sqlx::query(
            "INSERT INTO approvals \
                 (id, alert_id, step_id, action, params, eligible, state, requested_by, \
                  expires_at, sms_code_hash) \
             VALUES ($1, $2, 's3', 'restart-counterparty', '{}', '{user:anna}', $3, \
                     'user:bob', now() + interval '15 minutes', $4)",
        )
        .bind(approval)
        .bind(alert)
        .bind(state)
        .bind(format!("sha256({approval}:{code})"))
        .execute(pool)
        .await
        .expect("approval");
    }

    let job = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO delivery_jobs \
             (id, alert_id, notify_seq, recipient_id, channel, address, route_info, \
              escalation_step) \
         VALUES ($1, $2, 1, 'anna', 'sms', '+15550001111', $3, 200)",
    )
    .bind(job)
    .bind(alert)
    .bind(json!({
        "target": "approval",
        "approval": {
            "id": approval,
            "code": code,
            "step_id": "s3",
            "action": "restart-counterparty",
        },
    }))
    .execute(pool)
    .await
    .expect("prompt job");
    (alert, job)
}

/// POST the prompt sms the channel rendered before the race; a `200` is expected either
/// way (the provider still accepts the message, only the code is dropped).
async fn post_prompt(http: &reqwest::Client, base_url: &str, job: Uuid, alert: Uuid, code: &str) {
    let resp = http
        .post(format!("{base_url}/provider/sms"))
        .header("Idempotency-Key", job.to_string())
        .json(&json!({
            "job_id": job,
            "alert_id": alert,
            "recipient": "user:anna",
            "to": "+15550001111",
            "text": format!(
                "Approve restart-counterparty? Reply \"Y {code}\" to approve, \
                 \"N {code}\" to reject."
            ),
            "approval_challenge": format!("Y {code}"),
        }))
        .send()
        .await
        .expect("POST provider prompt");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "the sms is still accepted; only the code is dropped"
    );
}

/// The receipt for `job`: the challenge column is gone and no plaintext code remains.
async fn assert_receipt_masked(pool: &PgPool, job: Uuid, code: &str) {
    let (challenge, text): (Option<String>, String) =
        sqlx::query_as("SELECT approval_challenge, text FROM gateway_receipts WHERE job_id = $1")
            .bind(job)
            .fetch_one(pool)
            .await
            .expect("receipt");
    assert_eq!(challenge, None, "the challenge must not persist: {text:?}");
    assert!(text.contains("****"), "the code must be masked: {text:?}");
    assert!(!text.contains(code), "the code leaked: {text:?}");
}

#[sqlx::test(migrations = "../../migrations")]
async fn late_prompt_for_expired_or_unknown_approval_persists_no_code(pool: PgPool) {
    let (base_url, _shutdown) = serve_state(GatewayState::new(pool.clone())).await;
    let http = reqwest::Client::new();

    // (a) Expired: the code is dead by TTL, a late provider retry must not revive it.
    let (alert, job) = seed_prompt_with_approval_state(&pool, Some("expired"), "4821").await;
    post_prompt(&http, &base_url, job, alert, "4821").await;
    assert_receipt_masked(&pool, job, "4821").await;

    // (b) Unknown: the job names an approval that does not exist — nothing authorizes the
    // code either.
    let (alert, job) = seed_prompt_with_approval_state(&pool, None, "0953").await;
    post_prompt(&http, &base_url, job, alert, "0953").await;
    assert_receipt_masked(&pool, job, "0953").await;
}
