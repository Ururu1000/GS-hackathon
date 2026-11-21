//! Audit #284 security suite for delivery's API (agent B's findings):
//!
//! 2. team authorization on step run/check (403 for actors off the alert's team),
//! 3. self-approval is impossible (requester never eligible, never prompted, never decides;
//!    a sole-approver step answers 409 "no one else can approve this step"),
//! 4. the replay window on `/api/replies` (missing/old/future timestamp -> 401, good -> 200),
//! 5. non-JSON POSTs are 415 — empty bodies included, whatever their (missing) header —
//!    the standalone demo header acts only when allowed and a non-loopback bind is refused,
//! 6. an expired approval sharing a code never makes a reply ambiguous.
//!
//! Needs Postgres (`DATABASE_URL`; sqlx creates a throwaway database per test).
//!
//! Run: `cargo test -p beacon-delivery --test security_api`

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use beacon_delivery::actions::ActionRegistry;
use beacon_delivery::alert_ops::SqlAlertOps;
use beacon_delivery::api::{self, Actor, ApiState};
use beacon_delivery::config::{Contact, Directory, Person, Team};
use beacon_delivery::runbook::{RenderedStep, StepKind};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

// ---------------------------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------------------------

fn person(id: &str, email: Option<&str>, phone: Option<&str>) -> Person {
    Person {
        id: id.to_string(),
        name: None,
        contact: Contact {
            email: email.map(str::to_string),
            phone: phone.map(str::to_string),
        },
        preferences: None,
        rules: Vec::new(),
    }
}

/// Teams:
/// - `settlements`: members bob/carol/frank/anna, lead carol, on-call `settlements` → bob,
///   on-call `night` → gina (gina is NOT a member — rotations authorize non-members too).
///   anna is a plain member WITH a phone (the requester in the self-approval tests).
/// - `solo`: only dave (lead, member, sole on-call) — the sole-approver team.
/// - zoe belongs to no team at all.
fn sec_dir() -> Directory {
    Directory::from_parts(
        vec![
            person("carol", Some("carol@example.test"), Some("+44 7700 900125")),
            person("bob", Some("bob@example.test"), Some("+44 7700 900124")),
            person("frank", Some("frank@example.test"), None),
            person("anna", Some("anna@example.test"), Some("+44 7700 900123")),
            person("gina", Some("gina@example.test"), None),
            person("zoe", Some("zoe@example.test"), None),
            person("dave", Some("dave@example.test"), None),
        ],
        vec![
            Team {
                id: "settlements".into(),
                members: vec!["bob".into(), "carol".into(), "frank".into(), "anna".into()],
                lead: Some("carol".into()),
                oncall: HashMap::from([
                    ("settlements".to_string(), vec!["bob".to_string()]),
                    ("night".to_string(), vec!["gina".to_string()]),
                ]),
                default_channels: None,
            },
            Team {
                id: "solo".into(),
                members: vec!["dave".into()],
                lead: Some("dave".into()),
                oncall: HashMap::new(),
                default_channels: None,
            },
        ],
    )
    .expect("directory builds")
}

fn api_state(pool: &PgPool, reply_secret: Option<Vec<u8>>) -> ApiState {
    ApiState {
        pool: pool.clone(),
        dir: std::sync::Arc::new(sec_dir()),
        registry: ActionRegistry::default(),
        alert_ops: std::sync::Arc::new(SqlAlertOps),
        reply_secret: reply_secret.map(std::sync::Arc::new),
        email_link: None,
    }
}

fn action_step(id: &str, approval: &str) -> RenderedStep {
    RenderedStep {
        id: id.into(),
        kind: StepKind::Action,
        text: format!("Run step {id}"),
        hint: None,
        action: Some(json!({
            "name": "retry_workflow",
            "label": "Retry the workflow",
            "params": { "workflowId": "wf-9" },
            "risk": "low",
        })),
        approval: approval.into(),
        condition: None,
        resolves: false,
        render_error: None,
    }
}

fn manual_step(id: &str) -> RenderedStep {
    RenderedStep {
        id: id.into(),
        kind: StepKind::Manual,
        text: format!("Check {id} by hand"),
        hint: None,
        action: None,
        approval: "none".into(),
        condition: None,
        resolves: false,
        render_error: None,
    }
}

/// An open alert on `team` with a frozen runbook of `steps`.
async fn insert_team_alert(pool: &PgPool, team: &str, steps: &[RenderedStep]) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO alerts (id, rule_id, rule_revision, rule_name, team, fingerprint, \
                             dedup_window_ms, status, severity, color, title, body, \
                             first_seen, last_seen, source, schema, notify_seq) \
         VALUES ($1, 'settlements-workflow-failed', 1, 'Settlement workflow failed', \
                 $2, $3, 600000, 'open', 'critical', 'red', \
                 'Settlement workflow wf-9 failed', '', now(), now(), 'test', \
                 'workflow.failed.v1', 1)",
    )
    .bind(id)
    .bind(team)
    .bind(format!("fp-{id}"))
    .execute(pool)
    .await
    .expect("insert alert");
    sqlx::query(
        "INSERT INTO alert_runbooks (alert_id, runbook_id, revision, title, format, steps, \
                                     context) \
         VALUES ($1, 'settlement-timeout', 7, 'Settlement timeout', 'yaml', $2, '{}')",
    )
    .bind(id)
    .bind(serde_json::to_value(steps).expect("steps json"))
    .execute(pool)
    .await
    .expect("insert alert_runbooks");
    id
}

/// A pending approval row with a chosen code (bypassing the collision-safe draw — the only
/// way to plant states the API refuses to create), eligible for carol only.
async fn insert_pending(pool: &PgPool, alert: Uuid, step: &str, code: &str, ttl: &str) -> Uuid {
    let id = Uuid::new_v4();
    let hash = hex::encode(Sha256::digest(format!("{id}:{code}").as_bytes()));
    sqlx::query(
        "INSERT INTO approvals \
            (id, alert_id, step_id, action, params, eligible, state, requested_by, \
             expires_at, sms_code_hash) \
         VALUES ($1, $2, $3, 'escalate', '{}', ARRAY['user:carol'], 'pending', 'user:anna', \
                 now() + ($4)::interval, $5)",
    )
    .bind(id)
    .bind(alert)
    .bind(step)
    .bind(ttl)
    .bind(hash)
    .execute(pool)
    .await
    .expect("insert pending approval");
    id
}

// ---------------------------------------------------------------------------------------------
// HTTP plumbing
// ---------------------------------------------------------------------------------------------

async fn serve_router(router: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("test server");
    });
    format!("http://127.0.0.1:{}", addr.port())
}

/// `api::router` + a middleware layer that inserts `Extension(Actor(...))`, standing in for
/// beacon-api's session layer.
async fn serve_api_as(state: ApiState, actor: &str) -> String {
    let actor = actor.to_string();
    let router = api::router(state).layer(axum::middleware::from_fn(
        move |mut req: axum::extract::Request, next: axum::middleware::Next| {
            let actor = actor.clone();
            async move {
                req.extensions_mut().insert(Actor(actor));
                next.run(req).await
            }
        },
    ));
    serve_router(router).await
}

/// The delivery-api standalone server on a random port, plus its shutdown sender. It binds
/// its own listener, so the port is picked by bind-drop-rebind (races are retried).
async fn serve_standalone(
    state: ApiState,
    demo_actor: &str,
    allow_demo_header: bool,
) -> (
    String,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    for _ in 0..10 {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("pick a port");
        let port = probe.local_addr().expect("addr").port();
        drop(probe);
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
        let (tx, rx) = tokio::sync::watch::channel(false);
        let st = state.clone();
        let demo_actor = Actor(demo_actor.to_string());
        let handle = tokio::spawn(async move {
            let _ = api::serve_standalone(st, addr, demo_actor, rx, allow_demo_header).await;
        });
        if wait_listening(port).await {
            return (format!("http://127.0.0.1:{port}"), tx, handle);
        }
    }
    panic!("could not start the standalone delivery api");
}

/// True once something accepts TCP connections on `port` (10 s budget).
async fn wait_listening(port: u16) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}

async fn post(url: &str, body: &Value) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(url)
        .json(body)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .expect("post");
    read_response(resp).await
}

async fn post_raw(url: &str, body: String, headers: &[(&str, String)]) -> (u16, Value) {
    let mut req = reqwest::Client::new()
        .post(url)
        .header("content-type", "application/json")
        .body(body)
        .timeout(Duration::from_secs(15));
    for (name, value) in headers {
        req = req.header(*name, value);
    }
    let resp = req.send().await.expect("post raw");
    read_response(resp).await
}

/// A POST whose body is sent with an explicit `content_type` (the 415 tests).
async fn post_body(url: &str, body: &str, content_type: &str) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(url)
        .header("content-type", content_type)
        .body(body.to_string())
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .expect("post body");
    read_response(resp).await
}

/// A POST sent with NO content-type header at all — what a bare cross-site form or a
/// hand-rolled client produces for an empty body.
async fn post_no_content_type(url: &str) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(url)
        .body(String::new())
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .expect("post without a content type");
    read_response(resp).await
}

async fn get(url: &str) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .get(url)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .expect("get");
    read_response(resp).await
}

async fn read_response(resp: reqwest::Response) -> (u16, Value) {
    let status = resp.status().as_u16();
    let body: Value = resp.json().await.unwrap_or(Value::Null);
    (status, body)
}

fn error_code(body: &Value) -> &str {
    body["error"]["code"].as_str().unwrap_or("<missing code>")
}

fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs() as i64
}

/// The gateway's reply headers (shared contract): `X-Beacon-Timestamp: <unix seconds>` and
/// `X-Beacon-Signature: hex(hmac_sha256(secret, "<timestamp>." + raw_body))`.
fn signed_reply(secret: &[u8], ts: i64, body: &str) -> Vec<(&'static str, String)> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac key");
    mac.update(format!("{ts}.").as_bytes());
    mac.update(body.as_bytes());
    vec![
        ("x-beacon-timestamp", ts.to_string()),
        (
            "x-beacon-signature",
            hex::encode(mac.finalize().into_bytes()),
        ),
    ]
}

/// The body-only signature (the old scheme), kept to prove it no longer verifies.
fn body_signature(secret: &[u8], body: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac key");
    mac.update(body.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

// ---------------------------------------------------------------------------------------------
// 2. Team authorization on run/check
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn actors_off_the_alerts_team_get_403_on_run_and_check(pool: PgPool) {
    let state = api_state(&pool, None);
    let alert = insert_team_alert(
        &pool,
        "settlements",
        &[action_step("s2", "none"), manual_step("m1")],
    )
    .await;

    // zoe is in no team: neither run nor check is hers to call.
    let as_zoe = serve_api_as(state.clone(), "user:zoe").await;
    let (status, body) = post(
        &format!("{as_zoe}/api/alerts/{alert}/steps/s2/run"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(error_code(&body), "forbidden", "{body}");
    let (status, body) = post(
        &format!("{as_zoe}/api/alerts/{alert}/steps/m1/check"),
        &json!({ "done": true }),
    )
    .await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(error_code(&body), "forbidden", "{body}");

    // The outsider's calls left nothing behind.
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM action_executions WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("count executions");
    assert_eq!(executions, 0);
    let marks: i64 = sqlx::query_scalar("SELECT count(*) FROM step_marks WHERE alert_id = $1")
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("count marks");
    assert_eq!(marks, 0);

    // A plain member (frank — not lead, not on-call) may run...
    let as_frank = serve_api_as(state.clone(), "user:frank").await;
    let (status, body) = post(
        &format!("{as_frank}/api/alerts/{alert}/steps/s2/run"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 202, "{body}");
    // ...and so may anyone in one of the team's on-call rotations, member or not (gina).
    let as_gina = serve_api_as(state.clone(), "user:gina").await;
    let (status, body) = post(
        &format!("{as_gina}/api/alerts/{alert}/steps/m1/check"),
        &json!({ "done": true }),
    )
    .await;
    assert_eq!(status, 200, "{body}");

    // `GET /api/runbooks` stays open to any authenticated user.
    let (status, body) = get(&format!("{as_zoe}/api/runbooks")).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body, json!({ "items": [] }), "{body}");
}

// ---------------------------------------------------------------------------------------------
// 3. Self-approval
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn the_requester_cannot_approve_their_own_request(pool: PgPool) {
    let state = api_state(&pool, None);
    let as_anna = serve_api_as(state.clone(), "user:anna").await;
    let alert = insert_team_alert(&pool, "settlements", &[action_step("s3", "required")]).await;

    // anna (a plain member with a phone) requests: the approval exists but is not hers.
    let (status, body) = post(
        &format!("{as_anna}/api/alerts/{alert}/steps/s3/run"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(error_code(&body), "approval_required", "{body}");
    let approval: Uuid = body["error"]["approval_id"]
        .as_str()
        .expect("approval_id")
        .parse()
        .expect("uuid");

    // Eligible is lead + on-call MINUS the requester.
    let (eligible, requested_by): (Vec<String>, String) =
        sqlx::query_as("SELECT eligible, requested_by FROM approvals WHERE id = $1")
            .bind(approval)
            .fetch_one(&pool)
            .await
            .expect("approval row");
    assert_eq!(requested_by, "user:anna");
    assert!(!eligible.contains(&"user:anna".to_string()), "{eligible:?}");
    assert!(eligible.contains(&"user:carol".to_string()), "{eligible:?}");
    assert!(eligible.contains(&"user:bob".to_string()), "{eligible:?}");

    // She has a phone but is never prompted for her own request.
    let own_prompts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM delivery_jobs \
         WHERE alert_id = $1 AND channel = 'sms' \
           AND route_info->>'target' = 'approval' AND recipient_id = 'anna'",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .expect("count anna prompts");
    assert_eq!(own_prompts, 0, "the requester must not be prompted");

    // anna cannot decide her own request, whatever route she tries.
    for route in ["approve", "reject"] {
        let (status, body) = post(
            &format!("{as_anna}/api/approvals/{approval}/{route}"),
            &json!({}),
        )
        .await;
        assert_eq!(status, 403, "{route}: {body}");
        assert_eq!(error_code(&body), "forbidden", "{route}: {body}");
        assert_eq!(
            body["error"]["message"],
            json!("the requester cannot approve their own request"),
            "{route}: {body}"
        );
    }
    let (state_str,): (String,) = sqlx::query_as("SELECT state FROM approvals WHERE id = $1")
        .bind(approval)
        .fetch_one(&pool)
        .await
        .expect("approval row");
    assert_eq!(state_str, "pending");

    // The lead decides it without trouble.
    let as_carol = serve_api_as(state, "user:carol").await;
    let (status, body) = post(
        &format!("{as_carol}/api/approvals/{approval}/approve"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (state_str,): (String,) = sqlx::query_as("SELECT state FROM approvals WHERE id = $1")
        .bind(approval)
        .fetch_one(&pool)
        .await
        .expect("approval row");
    assert_eq!(state_str, "approved");
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_sole_approver_step_answers_no_one_else_can_approve_this_step(pool: PgPool) {
    let state = api_state(&pool, None);
    let as_dave = serve_api_as(state, "user:dave").await;
    // `solo`'s only possible decider is dave himself — and he may not self-approve.
    let alert = insert_team_alert(&pool, "solo", &[action_step("s3", "required")]).await;

    let (status, body) = post(
        &format!("{as_dave}/api/alerts/{alert}/steps/s3/run"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(error_code(&body), "invalid_transition", "{body}");
    assert_eq!(
        body["error"]["message"],
        json!("no one else can approve this step"),
        "{body}"
    );

    // No approval, no prompt, nothing queued.
    let approvals: i64 = sqlx::query_scalar("SELECT count(*) FROM approvals WHERE alert_id = $1")
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("count approvals");
    assert_eq!(approvals, 0);
    let prompts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM delivery_jobs WHERE alert_id = $1 \
           AND route_info->>'target' = 'approval'",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .expect("count prompts");
    assert_eq!(prompts, 0);
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM action_executions WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("count executions");
    assert_eq!(executions, 0);
}

// ---------------------------------------------------------------------------------------------
// 4. Replay protection on /api/replies
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn replies_outside_the_replay_window_are_unauthorized(pool: PgPool) {
    let secret = b"reply-secret".to_vec();
    let state = api_state(&pool, Some(secret.clone()));
    let base = serve_api_as(state, "user:bob").await;
    let url = format!("{base}/api/replies");
    let payload = json!({ "from": "+44 7700 900125", "text": "ACK" }).to_string();
    let now = now_ts();

    // Inside the window (250 s old): accepted.
    let (status, res) = post_raw(
        &url,
        payload.clone(),
        &signed_reply(&secret, now - 250, &payload),
    )
    .await;
    assert_eq!(status, 200, "{res}");

    // Outside it — old or future — even a perfectly signed reply is 401. The margin is
    // 30 s, not 1 s: `now` is read once, and as the test runs the server's clock moves
    // toward a future timestamp (a `now + 301` reply is inside the window a second later).
    for ts in [now - 330, now - 3_600, now + 330, now + 3_600] {
        let (status, res) =
            post_raw(&url, payload.clone(), &signed_reply(&secret, ts, &payload)).await;
        assert_eq!(status, 401, "ts {ts}: {res}");
        assert_eq!(error_code(&res), "unauthorized", "ts {ts}: {res}");
    }

    // A missing timestamp (a body-only signature, the old scheme) is 401...
    let (status, res) = post_raw(
        &url,
        payload.clone(),
        &[("x-beacon-signature", body_signature(&secret, &payload))],
    )
    .await;
    assert_eq!(status, 401, "{res}");
    // ...and so is a garbage one, even with well-formed headers otherwise.
    let (status, res) = post_raw(
        &url,
        payload.clone(),
        &[
            ("x-beacon-timestamp", "yesterday".into()),
            ("x-beacon-signature", hex::encode([0u8; 32])),
        ],
    )
    .await;
    assert_eq!(status, 401, "{res}");
}

// ---------------------------------------------------------------------------------------------
// 5. text/plain CSRF gate + standalone demo header / loopback bind
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn non_json_post_bodies_are_unsupported_media_type(pool: PgPool) {
    let state = api_state(&pool, Some(b"secret".to_vec()));
    let alert = insert_team_alert(
        &pool,
        "settlements",
        &[action_step("s2", "none"), manual_step("m1")],
    )
    .await;
    let base = serve_api_as(state, "user:bob").await;

    // A text/plain body (the cross-site form enctype) never reaches a handler.
    for (url, body) in [
        (
            format!("{base}/api/alerts/{alert}/steps/s2/run"),
            json!({}).to_string(),
        ),
        (
            format!("{base}/api/alerts/{alert}/steps/m1/check"),
            json!({ "done": true }).to_string(),
        ),
        (
            format!("{base}/api/approvals/{}/approve", Uuid::new_v4()),
            json!({}).to_string(),
        ),
        (
            format!("{base}/api/approvals/{}/reject", Uuid::new_v4()),
            json!({}).to_string(),
        ),
    ] {
        let (status, res) = post_body(&url, &body, "text/plain").await;
        assert_eq!(status, 415, "{url}: {res}");
        assert_eq!(error_code(&res), "unsupported_media_type", "{url}: {res}");
    }
    let payload = json!({ "from": "+44 7700 900125", "text": "ACK" }).to_string();
    let (status, res) = post_body(
        &format!("{base}/api/replies"),
        &payload,
        "application/x-www-form-urlencoded",
    )
    .await;
    assert_eq!(status, 415, "{res}");
    assert_eq!(error_code(&res), "unsupported_media_type", "{res}");

    // An empty body without the JSON content type is a 415 too (the dedicated test below
    // covers every route and the no-header case); a charset parameter does not spoil a
    // JSON body that parses.
    let (status, res) = post_body(
        &format!("{base}/api/alerts/{alert}/steps/s2/run"),
        "",
        "text/plain",
    )
    .await;
    assert_eq!(
        status, 415,
        "an empty cross-site form body passes no gate: {res}"
    );
    let (status, res) = post_body(
        &format!("{base}/api/alerts/{alert}/steps/s2/run"),
        &json!({}).to_string(),
        "application/json; charset=utf-8",
    )
    .await;
    assert_eq!(status, 202, "{res}");
}

/// The gate holds for EMPTY bodies too: every mutating route requires the JSON content
/// type even when the body is empty — a cross-site `<form>` posting an empty body (no
/// header, or `text/plain`) passes nothing. An empty body WITH the content type is still
/// "no body" wherever the contract allows one.
#[sqlx::test(migrations = "../../migrations")]
async fn empty_post_bodies_need_the_json_content_type(pool: PgPool) {
    let state = api_state(&pool, Some(b"secret".to_vec()));
    let alert = insert_team_alert(
        &pool,
        "settlements",
        &[action_step("s2", "none"), manual_step("m1")],
    )
    .await;
    let base = serve_api_as(state, "user:bob").await;

    for url in [
        format!("{base}/api/alerts/{alert}/steps/s2/run"),
        format!("{base}/api/alerts/{alert}/steps/m1/check"),
        format!("{base}/api/approvals/{}/approve", Uuid::new_v4()),
        format!("{base}/api/approvals/{}/reject", Uuid::new_v4()),
        format!("{base}/api/replies"),
    ] {
        // No content-type header at all.
        let (status, res) = post_no_content_type(&url).await;
        assert_eq!(status, 415, "no content type: {url}: {res}");
        assert_eq!(error_code(&res), "unsupported_media_type", "{url}: {res}");
        // The form enctype.
        let (status, res) = post_body(&url, "", "text/plain").await;
        assert_eq!(status, 415, "text/plain: {url}: {res}");
        assert_eq!(error_code(&res), "unsupported_media_type", "{url}: {res}");
    }

    // The same empty body WITH the JSON content type means "no body": run and check
    // accept it, the approval routes get past the gate to their own errors, and
    // /api/replies passes the gate to its signature check.
    let (status, res) = post_body(
        &format!("{base}/api/alerts/{alert}/steps/s2/run"),
        "",
        "application/json",
    )
    .await;
    assert_eq!(status, 202, "an empty JSON body runs the step: {res}");
    let (status, res) = post_body(
        &format!("{base}/api/alerts/{alert}/steps/m1/check"),
        "",
        "application/json",
    )
    .await;
    assert_eq!(status, 200, "an empty JSON body checks the step: {res}");
    let (status, res) = post_body(
        &format!("{base}/api/approvals/{}/approve", Uuid::new_v4()),
        "",
        "application/json",
    )
    .await;
    assert_eq!(
        status, 404,
        "the gate passed; the unknown approval 404s: {res}"
    );
    let (status, res) = post_body(&format!("{base}/api/replies"), "", "application/json").await;
    assert_eq!(
        status, 401,
        "the gate passed; the signature check answers: {res}"
    );
    assert_eq!(error_code(&res), "unauthorized", "{res}");
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_standalone_demo_header_acts_only_when_allowed(pool: PgPool) {
    let state = api_state(&pool, None);

    // allow_demo_header = false: the header is ignored outright; every request acts as the
    // demo actor — even one naming a person who could not call the route.
    let (base, shutdown, task) = serve_standalone(state.clone(), "user:frank", false).await;
    let alert = insert_team_alert(&pool, "settlements", &[action_step("s2", "none")]).await;
    let url = format!("{base}/api/alerts/{alert}/steps/s2/run");
    for header in ["user:zoe", "user:carol"] {
        let (status, res) = post_raw(
            &url,
            json!({}).to_string(),
            &[("x-beacon-demo-actor", header.into())],
        )
        .await;
        assert_eq!(
            status,
            if header == "user:zoe" { 202 } else { 409 },
            "{header}: {res}"
        );
        // The first run queued the step as the demo actor; the repeat conflicts — never a
        // 401, the header never swapped the actor.
        let requested_by: String =
            sqlx::query_scalar("SELECT requested_by FROM action_executions WHERE alert_id = $1")
                .bind(alert)
                .fetch_one(&pool)
                .await
                .expect("execution row");
        assert_eq!(requested_by, "user:frank", "the demo actor ran the step");
    }
    shutdown.send(true).expect("signal shutdown");
    tokio::time::timeout(Duration::from_secs(15), task)
        .await
        .expect("standalone must stop")
        .expect("standalone task must not panic");

    // allow_demo_header = true: the header picks the acting person (unknown names are 401).
    let (base, shutdown, task) = serve_standalone(state, "user:frank", true).await;
    let url = format!("{base}/api/alerts/{alert}/steps/s2/run");
    // A repeat run of the live step still conflicts (whoever the header names).
    let (status, res) = post_raw(
        &url,
        json!({}).to_string(),
        &[("x-beacon-demo-actor", "user:carol".into())],
    )
    .await;
    assert_eq!(status, 409, "{res}");
    let (status, res) = post_raw(
        &url,
        json!({}).to_string(),
        &[("x-beacon-demo-actor", "user:ghost".into())],
    )
    .await;
    assert_eq!(status, 401, "unknown names are rejected: {res}");
    let other = insert_team_alert(&pool, "settlements", &[action_step("s9", "none")]).await;
    let (status, res) = post_raw(
        &format!("{base}/api/alerts/{other}/steps/s9/run"),
        json!({}).to_string(),
        &[("x-beacon-demo-actor", "user:carol".into())],
    )
    .await;
    assert_eq!(status, 202, "carol (the lead) may run: {res}");
    let requested_by: String =
        sqlx::query_scalar("SELECT requested_by FROM action_executions WHERE alert_id = $1")
            .bind(other)
            .fetch_one(&pool)
            .await
            .expect("execution row");
    assert_eq!(requested_by, "user:carol", "the header named the actor");
    shutdown.send(true).expect("signal shutdown");
    tokio::time::timeout(Duration::from_secs(15), task)
        .await
        .expect("standalone must stop")
        .expect("standalone task must not panic");
}

#[sqlx::test(migrations = "../../migrations")]
async fn standalone_refuses_non_loopback_binds(pool: PgPool) {
    let state = api_state(&pool, None);
    for addr in ["0.0.0.0:8071", "192.168.1.10:8071"] {
        let addr: SocketAddr = addr.parse().expect("addr");
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let err = api::serve_standalone(state.clone(), addr, Actor("user:bob".into()), rx, true)
            .await
            .expect_err("non-loopback binds are refused");
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::InvalidInput,
            "{addr}: {err}"
        );
        assert!(err.to_string().contains("loopback"), "{addr}: {err}");
    }
}

// ---------------------------------------------------------------------------------------------
// 6. An expired approval never makes a reply ambiguous
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn an_expired_approval_sharing_the_code_never_ambiguates_a_reply(pool: PgPool) {
    let secret = b"reply-secret".to_vec();
    let state = api_state(&pool, Some(secret.clone()));
    let base = serve_api_as(state, "user:bob").await;

    // A live approval and a lapsed one (still `pending`, but past its expiry), both eligible
    // for carol and hashed for the SAME code — only direct inserts can force that.
    let alert = common::insert_alert(&pool, "open", "Ambiguity guard").await;
    let code = "4821";
    let live = insert_pending(&pool, alert, "s1", code, "15 minutes").await;
    let lapsed = insert_pending(&pool, alert, "s2", code, "-1 second").await;

    // The reply decides the live one, not "ambiguous".
    let payload = json!({ "from": "+44 7700 900125", "text": format!("Y {code}") }).to_string();
    let headers = signed_reply(&secret, now_ts(), &payload);
    let (status, res) = post_raw(&format!("{base}/api/replies"), payload, &headers).await;
    assert_eq!(status, 200, "{res}");
    assert_eq!(res["outcome"], json!("approved"), "{res}");
    assert_eq!(res["approval_id"], json!(live.to_string()), "{res}");
    let (live_state, lapsed_state): (String, String) = sqlx::query_as(
        "SELECT (SELECT state FROM approvals WHERE id = $1), \
                (SELECT state FROM approvals WHERE id = $2)",
    )
    .bind(live)
    .bind(lapsed)
    .fetch_one(&pool)
    .await
    .expect("approval states");
    assert_eq!(live_state, "approved");
    assert_eq!(lapsed_state, "pending", "the lapsed row awaits the sweeper");

    // Contrast: two LIVE approvals with the same code stay ambiguous (decide in the UI).
    let other = common::insert_alert(&pool, "open", "Ambiguity guard 2").await;
    let first = insert_pending(&pool, other, "s1", code, "15 minutes").await;
    let second = insert_pending(&pool, other, "s2", code, "15 minutes").await;
    let payload = json!({ "from": "+44 7700 900125", "text": format!("Y {code}") }).to_string();
    let headers = signed_reply(&secret, now_ts(), &payload);
    let (status, res) = post_raw(&format!("{base}/api/replies"), payload, &headers).await;
    assert_eq!(status, 200, "{res}");
    assert_eq!(res["outcome"], json!("ambiguous"), "{res}");
    for id in [first, second] {
        let (state_str, attempts): (String, i32) =
            sqlx::query_as("SELECT state, sms_attempts FROM approvals WHERE id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .expect("approval row");
        assert_eq!(state_str, "pending", "{id}");
        assert_eq!(attempts, 0, "no attempt burned on {id}");
    }
}
