//! M5b delivery API suite: every route and status code in `api.rs`'s table, exercised over
//! real HTTP against `api::router` (with a middleware layer that inserts the `Actor`, like
//! beacon-api's session layer does) and against the standalone dev server, plus the gateway's
//! `/api/simulator/reply` forwarding end to end.
//!
//! Needs Postgres (`DATABASE_URL`; sqlx creates a throwaway database per test).
//!
//! Run: `cargo test -p beacon-delivery --test api -- --nocapture`

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use beacon_delivery::actions::ActionRegistry;
use beacon_delivery::alert_ops::SqlAlertOps;
use beacon_delivery::api::{self, Actor, ApiState};
use beacon_delivery::approvals;
use beacon_delivery::config::{Contact, Directory, Person, Team};
use beacon_delivery::gateway::{self, GatewayState};
use beacon_delivery::runbook::{Condition, Format, RenderedStep, Runbook, StepDef, StepKind};
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

/// Team `settlements`: lead carol (phone), on-call bob (phone), member frank (no phone).
fn api_dir() -> Directory {
    let mut dir = Directory::from_parts(
        vec![
            person("carol", Some("carol@example.test"), Some("+44 7700 900125")),
            person("bob", Some("bob@example.test"), Some("+44 7700 900124")),
            person("frank", Some("frank@example.test"), None),
        ],
        vec![Team {
            id: "settlements".into(),
            members: vec!["bob".into(), "carol".into(), "frank".into()],
            lead: Some("carol".into()),
            oncall: HashMap::from([("settlements".to_string(), vec!["bob".to_string()])]),
            default_channels: None,
        }],
    )
    .expect("directory builds");
    // `GET /api/runbooks` reads the library; hand-build one entry (agent A owns the loader).
    dir.runbooks.runbooks = vec![Runbook {
        id: "settlement-timeout".into(),
        revision: 7,
        title: "Settlement timeout".into(),
        format: Format::Yaml,
        team: Some("settlements".into()),
        priority: 0,
        matches: Default::default(),
        url: None,
        text: None,
        steps: vec![library_step("s1"), library_step("s2"), library_step("s3")],
    }];
    dir
}

/// A minimal `StepDef` for the runbook *library* (`GET /api/runbooks` only counts them).
fn library_step(id: &str) -> StepDef {
    StepDef {
        id: id.into(),
        kind: StepKind::Action,
        text: format!("Run step {id}"),
        hint: None,
        action: Some("retry_workflow".into()),
        params: Default::default(),
        approval_required: false,
        condition: None,
        resolves: false,
    }
}

fn api_state(pool: &PgPool, reply_secret: Option<Vec<u8>>) -> ApiState {
    ApiState {
        pool: pool.clone(),
        dir: Arc::new(api_dir()),
        registry: ActionRegistry::default(),
        alert_ops: Arc::new(SqlAlertOps),
        reply_secret: reply_secret.map(Arc::new),
    }
}

fn action_step(id: &str, approval: &str, condition: Option<Condition>) -> RenderedStep {
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
        condition,
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

async fn insert_alert_with_runbook(pool: &PgPool, status: &str, steps: &[RenderedStep]) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO alerts (id, rule_id, rule_revision, rule_name, team, fingerprint, \
                             dedup_window_ms, status, severity, color, title, body, \
                             first_seen, last_seen, source, schema, notify_seq) \
         VALUES ($1, 'settlements-workflow-failed', 1, 'Settlement workflow failed', \
                 'settlements', $2, 600000, $3, 'critical', 'red', \
                 'Settlement workflow wf-9 failed', '', now(), now(), 'test', \
                 'workflow.failed.v1', 1)",
    )
    .bind(id)
    .bind(format!("fp-{id}"))
    .bind(status)
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

// ---------------------------------------------------------------------------------------------
// HTTP plumbing
// ---------------------------------------------------------------------------------------------

/// Serve one router on a random port; returns the base URL. The task lives as long as the
/// test (the pattern of `tests/gateway.rs`).
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
/// `allow_demo_header` is forwarded to `api::serve_standalone`.
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

fn hmac_signature(secret: &[u8], body: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac key");
    mac.update(body.as_bytes());
    hex::encode(mac.finalize().into_bytes())
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

fn error_code(body: &Value) -> &str {
    body["error"]["code"].as_str().unwrap_or("<missing code>")
}

// ---------------------------------------------------------------------------------------------
// Auth: no Actor -> 401 everywhere except /api/replies
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn without_an_actor_every_route_but_replies_is_401(pool: PgPool) {
    let state = api_state(&pool, Some(b"secret".to_vec()));
    let alert = insert_alert_with_runbook(&pool, "open", &[manual_step("s1")]).await;
    let base = serve_router(api::router(state)).await;

    let approval = Uuid::new_v4();
    for (url, body) in [
        (format!("{base}/api/alerts/{alert}/steps/s1/run"), json!({})),
        (
            format!("{base}/api/alerts/{alert}/steps/s1/check"),
            json!({ "done": true }),
        ),
        (
            format!("{base}/api/approvals/{approval}/approve"),
            json!({}),
        ),
        (format!("{base}/api/approvals/{approval}/reject"), json!({})),
    ] {
        let (status, body) = post(&url, &body).await;
        assert_eq!(status, 401, "{url}");
        assert_eq!(error_code(&body), "unauthorized", "{url}: {body}");
    }
    let (status, _) = get(&format!("{base}/api/runbooks")).await;
    assert_eq!(status, 401, "GET /api/runbooks needs the session layer too");
    let (status, _) = get(&format!("{base}/api/runbooks/settlement-timeout")).await;
    assert_eq!(
        status, 401,
        "GET /api/runbooks/{{id}} needs the session layer too"
    );

    // `/api/replies` authenticates by its timestamped signature instead.
    let body = json!({ "from": "+44 7700 900125", "text": "ACK" }).to_string();
    let headers = signed_reply(b"secret", now_ts(), &body);
    let (status, outcome) = post_raw(&format!("{base}/api/replies"), body, &headers).await;
    assert_eq!(status, 200, "{outcome}");
    assert_eq!(outcome["outcome"], json!("unknown"), "{outcome}");
}

// ---------------------------------------------------------------------------------------------
// POST /api/alerts/{id}/steps/{step}/run
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn run_404s_on_bad_ids(pool: PgPool) {
    let state = api_state(&pool, None);
    let base = serve_api_as(state, "user:bob").await;

    // Malformed alert uuid, unknown alert, alert without a runbook, unknown step.
    for url in [
        format!("{base}/api/alerts/not-a-uuid/steps/s2/run"),
        format!("{base}/api/alerts/{}/steps/s2/run", Uuid::new_v4()),
    ] {
        let (status, body) = post(&url, &json!({})).await;
        assert_eq!(status, 404, "{url}: {body}");
        assert_eq!(error_code(&body), "not_found", "{url}: {body}");
    }

    let alert = common::insert_alert(&pool, "open", "No runbook alert").await;
    let (status, _) = post(
        &format!("{base}/api/alerts/{alert}/steps/s2/run"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 404, "an alert without a runbook has no steps");

    let with_runbook =
        insert_alert_with_runbook(&pool, "open", &[action_step("s2", "none", None)]).await;
    let (status, _) = post(
        &format!("{base}/api/alerts/{with_runbook}/steps/unknown/run"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 404, "unknown step id");
}

#[sqlx::test(migrations = "../../migrations")]
async fn run_conflicts_for_manual_resolved_render_broken_and_condition_steps(pool: PgPool) {
    let state = api_state(&pool, None);
    let base = serve_api_as(state, "user:bob").await;

    // Not an action step.
    let alert = insert_alert_with_runbook(&pool, "open", &[manual_step("s1")]).await;
    let (status, body) = post(
        &format!("{base}/api/alerts/{alert}/steps/s1/run"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(error_code(&body), "invalid_transition", "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("not an action step"),
        "{body}"
    );

    // Resolved alert.
    let alert =
        insert_alert_with_runbook(&pool, "resolved", &[action_step("s2", "none", None)]).await;
    let (status, body) = post(
        &format!("{base}/api/alerts/{alert}/steps/s2/run"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(error_code(&body), "invalid_transition", "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("resolved"),
        "{body}"
    );

    // A step whose rendering failed is disabled with its reason.
    let mut broken = action_step("s9", "none", None);
    broken.render_error = Some("missing value for workflowId".into());
    let alert = insert_alert_with_runbook(&pool, "open", &[broken]).await;
    let (status, body) = post(
        &format!("{base}/api/alerts/{alert}/steps/s9/run"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(error_code(&body), "invalid_transition", "{body}");
    assert_eq!(
        body["error"]["message"],
        json!("missing value for workflowId"),
        "{body}"
    );

    // Unmet condition: 2 failed retries required, none happened yet.
    let gated = action_step(
        "s3",
        "none",
        Some(Condition::AfterFailures {
            action: "retry_workflow".into(),
            count: 2,
        }),
    );
    let alert = insert_alert_with_runbook(&pool, "open", &[gated]).await;
    let (status, body) = post(
        &format!("{base}/api/alerts/{alert}/steps/s3/run"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(error_code(&body), "invalid_transition", "{body}");
    assert_eq!(
        body["error"]["message"],
        json!("Available after 2 failed retries (0 so far)"),
        "{body}"
    );

    // A malformed body is a 400, `expected_version` of the wrong type too.
    let alert = insert_alert_with_runbook(&pool, "open", &[action_step("s2", "none", None)]).await;
    let (status, body) = post_raw(
        &format!("{base}/api/alerts/{alert}/steps/s2/run"),
        "{nope".to_string(),
        &[],
    )
    .await;
    assert_eq!(status, 400, "{body}");
    let (status, body) = post(
        &format!("{base}/api/alerts/{alert}/steps/s2/run"),
        &json!({ "expected_version": "one" }),
    )
    .await;
    assert_eq!(status, 400, "{body}");
}

#[sqlx::test(migrations = "../../migrations")]
async fn run_queues_a_step_and_conflicts_when_it_is_live(pool: PgPool) {
    let state = api_state(&pool, None);
    let base = serve_api_as(state, "user:bob").await;
    let alert = insert_alert_with_runbook(
        &pool,
        "open",
        &[
            action_step("s2", "none", None),
            action_step(
                "s3",
                "none",
                Some(Condition::AfterFailures {
                    action: "retry_workflow".into(),
                    count: 0,
                }),
            ),
        ],
    )
    .await;

    // The empty body — sent WITH the JSON content type — and `{}` both mean
    // "no expected_version"; the empty form queues the step here, and the `{}` form is
    // used for the repeat below.
    let (status, body) = post_raw(
        &format!("{base}/api/alerts/{alert}/steps/s2/run"),
        String::new(),
        &[],
    )
    .await;
    assert_eq!(status, 202, "{body}");
    let execution_id: Uuid = body["execution_id"]
        .as_str()
        .expect("execution_id in the 202 body")
        .parse()
        .expect("uuid");
    let (step_id, action, requested_by, approval_id, exec_state): (
        String,
        String,
        String,
        Option<Uuid>,
        String,
    ) = sqlx::query_as(
        "SELECT step_id, action, requested_by, approval_id, state \
         FROM action_executions WHERE id = $1",
    )
    .bind(execution_id)
    .fetch_one(&pool)
    .await
    .expect("execution row");
    assert_eq!(
        (step_id.as_str(), action.as_str(), requested_by.as_str()),
        ("s2", "retry_workflow", "user:bob")
    );
    assert_eq!(approval_id, None);
    assert_eq!(exec_state, "queued");
    // `queue` audited the step_run and the route touched the alert version.
    assert!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM audit_log WHERE alert_id = $1 AND kind = 'step_run'",
        )
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("audit count")
            > 0,
        "the queued execution is audited"
    );
    let version: i64 = sqlx::query_scalar("SELECT version FROM alerts WHERE id = $1")
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("version");
    assert_eq!(version, 2, "the run route bumped the version via touch");

    // A second run of the live step is a 409.
    let (status, body) = post(
        &format!("{base}/api/alerts/{alert}/steps/s2/run"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(error_code(&body), "invalid_transition", "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("already queued"),
        "{body}"
    );

    // A met condition lets the gated step run.
    let (status, body) = post(
        &format!("{base}/api/alerts/{alert}/steps/s3/run"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 202, "{body}");
}

#[sqlx::test(migrations = "../../migrations")]
async fn run_on_an_approval_step_creates_the_approval_and_409s(pool: PgPool) {
    let state = api_state(&pool, None);
    let base = serve_api_as(state, "user:frank").await;
    let alert =
        insert_alert_with_runbook(&pool, "open", &[action_step("s3", "required", None)]).await;

    let (status, body) = post(
        &format!("{base}/api/alerts/{alert}/steps/s3/run"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(error_code(&body), "approval_required", "{body}");
    let approval_id: Uuid = body["error"]["approval_id"]
        .as_str()
        .expect("approval_id on the conflict")
        .parse()
        .expect("uuid");

    // The pending approval exists, eligible = lead + on-call; the run's actor may not be.
    let (state_str, eligible): (String, Vec<String>) =
        sqlx::query_as("SELECT state, eligible FROM approvals WHERE id = $1")
            .bind(approval_id)
            .fetch_one(&pool)
            .await
            .expect("approval row");
    assert_eq!(state_str, "pending");
    assert_eq!(eligible, vec!["user:carol", "user:bob"]);

    // One prompt sms job per eligible person with a phone (carol, bob), carrying the code.
    let prompts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM delivery_jobs \
         WHERE alert_id = $1 AND channel = 'sms' AND route_info->>'target' = 'approval'",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .expect("prompt count");
    assert_eq!(prompts, 2);
    // The request is audited and the alert version bumped by the route's touch.
    assert!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM audit_log WHERE alert_id = $1 AND kind = 'approval'",
        )
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("audit count")
            > 0
    );
    let version: i64 = sqlx::query_scalar("SELECT version FROM alerts WHERE id = $1")
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("version");
    assert_eq!(version, 2);

    // Repeating the run returns the SAME approval (idempotent), no second row.
    let (status, body) = post(
        &format!("{base}/api/alerts/{alert}/steps/s3/run"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 409, "{body}");
    let again: Uuid = body["error"]["approval_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(again, approval_id);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM approvals WHERE alert_id = $1")
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("count approvals");
    assert_eq!(rows, 1);
}

/// Review fix (PR #11, S11-10): five concurrent `POST .../run` on an approval-required
/// step all answer 409 `approval_required` with the SAME `approval_id` — one approval,
/// one set of prompt jobs, and no 500 from the pending unique index.
#[sqlx::test(migrations = "../../migrations")]
async fn concurrent_runs_on_an_approval_step_share_one_approval(pool: PgPool) {
    let state = api_state(&pool, None);
    let base = serve_api_as(state, "user:frank").await;
    let alert =
        insert_alert_with_runbook(&pool, "open", &[action_step("s3", "required", None)]).await;
    let url = format!("{base}/api/alerts/{alert}/steps/s3/run");

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..5 {
        let url = url.clone();
        tasks.spawn(async move { post(&url, &json!({})).await });
    }
    let mut approval_ids = Vec::new();
    while let Some(res) = tasks.join_next().await {
        let (status, body) = res.expect("task");
        assert_eq!(status, 409, "{body}");
        assert_eq!(error_code(&body), "approval_required", "{body}");
        approval_ids.push(
            body["error"]["approval_id"]
                .as_str()
                .expect("approval_id on the conflict")
                .to_string(),
        );
    }
    assert_eq!(approval_ids.len(), 5);
    assert!(
        approval_ids.iter().all(|id| *id == approval_ids[0]),
        "every concurrent run names the same approval: {approval_ids:?}"
    );

    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM approvals WHERE alert_id = $1")
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("count approvals");
    assert_eq!(rows, 1);
    let prompts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM delivery_jobs \
         WHERE alert_id = $1 AND channel = 'sms' AND route_info->>'target' = 'approval'",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .expect("prompt count");
    // Eligible with phones in `api_dir`: carol and bob.
    assert_eq!(prompts, 2);
}

// ---------------------------------------------------------------------------------------------
// POST /api/alerts/{id}/steps/{step}/check
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn check_marks_and_clears_manual_steps(pool: PgPool) {
    let state = api_state(&pool, None);
    let base = serve_api_as(state, "user:bob").await;
    let alert = insert_alert_with_runbook(
        &pool,
        "open",
        &[manual_step("s1"), action_step("s2", "none", None)],
    )
    .await;

    let url = format!("{base}/api/alerts/{alert}/steps/s1/check");
    let (status, body) = post(&url, &json!({ "done": true })).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body, json!({}));
    let (done_by,): (String,) =
        sqlx::query_as("SELECT done_by FROM step_marks WHERE alert_id = $1 AND step_id = 's1'")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("step mark");
    assert_eq!(done_by, "user:bob");
    assert!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM audit_log WHERE alert_id = $1 AND kind = 'step_done'",
        )
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("audit count")
            > 0,
        "the check route audited the mark"
    );

    // Clearing it removes the row.
    let (status, body) = post(&url, &json!({ "done": false })).await;
    assert_eq!(status, 200, "{body}");
    let marks: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM step_marks WHERE alert_id = $1 AND step_id = 's1'",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .expect("count marks");
    assert_eq!(marks, 0);

    // Not a manual step: 400. Non-boolean `done`: 400. Unknown step: 404.
    let (status, body) = post(
        &format!("{base}/api/alerts/{alert}/steps/s2/check"),
        &json!({ "done": true }),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(error_code(&body), "bad_request", "{body}");
    let (status, body) = post(&url, &json!({ "done": "yes" })).await;
    assert_eq!(status, 400, "{body}");
    let (status, _) = post(
        &format!("{base}/api/alerts/{alert}/steps/nope/check"),
        &json!({ "done": true }),
    )
    .await;
    assert_eq!(status, 404);
}

// ---------------------------------------------------------------------------------------------
// POST /api/approvals/{id}/{approve|reject}
// ---------------------------------------------------------------------------------------------

/// An approval to decide, created through the API's own run route mounted as `base`.
async fn approval_via_run(base: &str, pool: &PgPool) -> (Uuid, Uuid) {
    let alert =
        insert_alert_with_runbook(pool, "open", &[action_step("s3", "required", None)]).await;
    let (_, body) = post(
        &format!("{base}/api/alerts/{alert}/steps/s3/run"),
        &json!({}),
    )
    .await;
    let approval: Uuid = body["error"]["approval_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    (alert, approval)
}

#[sqlx::test(migrations = "../../migrations")]
async fn approvals_decide_endpoints_cover_the_whole_error_table(pool: PgPool) {
    let state = api_state(&pool, None);
    let base = serve_api_as(state.clone(), "user:carol").await;
    // The run route mounted as frank: approvals he requests stay decidable by carol — the
    // requester can never decide their own request (audit #11).
    let as_frank = serve_api_as(state, "user:frank").await;

    // Approve: 200 {execution_id}, the execution queued with the approval linked.
    let (alert, approval) = approval_via_run(&as_frank, &pool).await;
    let url = format!("{base}/api/approvals/{approval}/approve");
    let (status, body) = post(&url, &json!({ "note": "runbook says go" })).await;
    assert_eq!(status, 200, "{body}");
    let execution: Uuid = body["execution_id"].as_str().unwrap().parse().unwrap();
    let (state_str, execution_id, note): (String, Option<Uuid>, Option<String>) =
        sqlx::query_as("SELECT state, execution_id, note FROM approvals WHERE id = $1")
            .bind(approval)
            .fetch_one(&pool)
            .await
            .expect("approval row");
    assert_eq!(state_str, "approved");
    assert_eq!(execution_id, Some(execution));
    assert_eq!(note.as_deref(), Some("runbook says go"));
    let (approval_of, requested_by): (Option<Uuid>, String) =
        sqlx::query_as("SELECT approval_id, requested_by FROM action_executions WHERE id = $1")
            .bind(execution)
            .fetch_one(&pool)
            .await
            .expect("execution");
    assert_eq!(approval_of, Some(approval));
    assert_eq!(requested_by, "user:carol");

    // Already decided: 409 invalid_transition.
    let (status, body) = post(&url, &json!({})).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(error_code(&body), "invalid_transition", "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("already approved"),
        "{body}"
    );

    // Reject on a fresh approval: 200 {}.
    let (_, approval) = approval_via_run(&as_frank, &pool).await;
    let (status, body) = post(
        &format!("{base}/api/approvals/{approval}/reject"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body, json!({}));
    let (state_str, execution_id): (String, Option<Uuid>) =
        sqlx::query_as("SELECT state, execution_id FROM approvals WHERE id = $1")
            .bind(approval)
            .fetch_one(&pool)
            .await
            .expect("approval row");
    assert_eq!(state_str, "rejected");
    assert_eq!(execution_id, None);

    // Not eligible: 403. (The approval is carol's own request, so frank — a plain member —
    // is simply not in the eligible set.)
    let (_, approval) = approval_via_run(&base, &pool).await;
    let (status, body) = post(
        &format!("{as_frank}/api/approvals/{approval}/approve"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(error_code(&body), "forbidden", "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("not eligible"),
        "{body}"
    );

    // Unknown approval: 404; malformed uuid: 404.
    let (status, body) = post(
        &format!("{base}/api/approvals/{}/approve", Uuid::new_v4()),
        &json!({}),
    )
    .await;
    assert_eq!(status, 404, "{body}");
    assert_eq!(error_code(&body), "not_found", "{body}");
    let (status, _) = post(&format!("{base}/api/approvals/xyz/approve"), &json!({})).await;
    assert_eq!(status, 404);

    // Expired: 409 invalid_transition, state 'expired'.
    let (_, approval) = approval_via_run(&as_frank, &pool).await;
    sqlx::query("UPDATE approvals SET expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(approval)
        .execute(&pool)
        .await
        .expect("backdate");
    let (status, body) = post(
        &format!("{base}/api/approvals/{approval}/approve"),
        &json!({}),
    )
    .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(error_code(&body), "invalid_transition", "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("expired"),
        "{body}"
    );

    // Note over 2000 chars: 400.
    let (_, approval) = approval_via_run(&as_frank, &pool).await;
    let (status, body) = post(
        &format!("{base}/api/approvals/{approval}/approve"),
        &json!({ "note": "x".repeat(2001) }),
    )
    .await;
    assert_eq!(status, 400, "{body}");

    let _ = alert;
}

// ---------------------------------------------------------------------------------------------
// GET /api/runbooks
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn runbooks_lists_the_library(pool: PgPool) {
    let state = api_state(&pool, None);
    let base = serve_api_as(state, "user:bob").await;

    let (status, body) = get(&format!("{base}/api/runbooks")).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body,
        json!({
            "items": [{
                "id": "settlement-timeout",
                "revision": 7,
                "title": "Settlement timeout",
                "format": "yaml",
                "team": "settlements",
                "steps": 3,
            }],
        }),
        "{body}"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn runbook_detail_returns_the_current_templates(pool: PgPool) {
    let mut dir = api_dir();
    // s2 restarts a service (risk high → approval required even without `approval:`),
    // s3 is a manual step without an action.
    dir.runbooks.runbooks[0].steps[1].action = Some("restart_service".into());
    dir.runbooks.runbooks[0].steps[2].kind = StepKind::Manual;
    dir.runbooks.runbooks[0].steps[2].action = None;
    dir.runbooks.runbooks[0].steps[0].text = "Retry {{workflowId}}".into();
    let mut state = api_state(&pool, None);
    state.dir = Arc::new(dir);
    let base = serve_api_as(state, "user:bob").await;

    let (status, body) = get(&format!("{base}/api/runbooks/settlement-timeout")).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body,
        json!({
            "id": "settlement-timeout",
            "revision": 7,
            "title": "Settlement timeout",
            "format": "yaml",
            "team": "settlements",
            "steps": [
                {"id": "s1", "kind": "action", "text": "Retry {{workflowId}}",
                 "action": {"name": "retry_workflow", "label": "Retry workflow"}, "approval": "none"},
                {"id": "s2", "kind": "action", "text": "Run step s2",
                 "action": {"name": "restart_service", "label": "Restart service"}, "approval": "required"},
                {"id": "s3", "kind": "manual", "text": "Run step s3", "action": null, "approval": "none"},
            ],
        }),
        "{body}"
    );

    let (status, body) = get(&format!("{base}/api/runbooks/no-such-runbook")).await;
    assert_eq!(status, 404, "{body}");
    assert_eq!(body["error"]["code"], "not_found", "{body}");
}

// ---------------------------------------------------------------------------------------------
// POST /api/replies (signature)
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn replies_reject_everything_but_a_valid_current_signature(pool: PgPool) {
    let secret = b"reply-secret".to_vec();
    let state = api_state(&pool, Some(secret.clone()));
    let base = serve_api_as(state, "user:bob").await;
    let url = format!("{base}/api/replies");
    let payload = json!({ "from": "+44 7700 900125", "text": "ACK" }).to_string();
    let good = signed_reply(&secret, now_ts(), &payload);

    // Missing both headers: 401 unauthorized.
    let (status, res) = post_raw(&url, payload.clone(), &[]).await;
    assert_eq!(status, 401, "{res}");
    assert_eq!(error_code(&res), "unauthorized", "{res}");
    // Invalid-hex signature: 401.
    let (status, _) = post_raw(
        &url,
        payload.clone(),
        &[
            ("x-beacon-timestamp", now_ts().to_string()),
            ("x-beacon-signature", "zz-not-hex".into()),
        ],
    )
    .await;
    assert_eq!(status, 401);
    // A current timestamp but a body-only signature (the old, unsigned-timestamp scheme):
    // still 401 — the signature must cover the timestamp too.
    let (status, _) = post_raw(
        &url,
        payload.clone(),
        &[
            ("x-beacon-timestamp", now_ts().to_string()),
            ("x-beacon-signature", hmac_signature(&secret, &payload)),
        ],
    )
    .await;
    assert_eq!(status, 401);
    // A signature with the wrong secret: 401.
    let (status, _) = post_raw(
        &url,
        payload.clone(),
        &signed_reply(b"other", now_ts(), &payload),
    )
    .await;
    assert_eq!(status, 401);
    // A signature over different bytes: 401.
    let (status, _) = post_raw(
        &url,
        payload.clone(),
        &signed_reply(&secret, now_ts(), "other bytes"),
    )
    .await;
    assert_eq!(status, 401);
    // A signature without its timestamp header: 401.
    let (status, _) = post_raw(
        &url,
        payload.clone(),
        &[("x-beacon-signature", hmac_signature(&secret, &payload))],
    )
    .await;
    assert_eq!(status, 401);
    // A non-numeric timestamp: 401.
    let (status, _) = post_raw(
        &url,
        payload.clone(),
        &signed_reply(&secret, now_ts(), &payload)
            .into_iter()
            .map(|(k, _)| (k, "not-a-number".to_string()))
            .collect::<Vec<_>>(),
    )
    .await;
    assert_eq!(status, 401);

    // No secret configured at all: every reply is rejected.
    let no_secret = api_state(&pool, None);
    let bare = serve_api_as(no_secret, "user:bob").await;
    let (status, _) = post_raw(
        &format!("{bare}/api/replies"),
        payload.clone(),
        &signed_reply(&secret, now_ts(), &payload),
    )
    .await;
    assert_eq!(status, 401);

    // Good, current signature: the reply is handled (an unknown number is a 200 outcome).
    let (status, res) = post_raw(&url, payload.clone(), &good).await;
    assert_eq!(status, 200, "{res}");
    assert_eq!(res["outcome"], json!("unknown"), "{res}");
    // A signature from within the window (200 s old) is still fine.
    let (status, res) = post_raw(
        &url,
        payload.clone(),
        &signed_reply(&secret, now_ts() - 200, &payload),
    )
    .await;
    assert_eq!(status, 200, "{res}");
    // Malformed or shape-invalid bodies are 400s (the signature was fine).
    let bad = json!({ "from": "+44 7700 900125" }).to_string();
    let bad_headers = signed_reply(&secret, now_ts(), &bad);
    let (status, _) = post_raw(&url, bad, &bad_headers).await;
    assert_eq!(status, 400, "missing text field");
}

/// `hex(sha256("<approval id>:<code>"))` — what the approvals table stores for a code.
fn expected_hash(id: Uuid, code: &str) -> String {
    hex::encode(Sha256::digest(format!("{id}:{code}").as_bytes()))
}

#[sqlx::test(migrations = "../../migrations")]
async fn replies_answer_an_ambiguous_code_with_decide_in_the_ui(pool: PgPool) {
    let secret = b"reply-secret".to_vec();
    let state = api_state(&pool, Some(secret.clone()));
    let base = serve_api_as(state, "user:bob").await;

    // Two pending approvals eligible for carol, hashed for the same code: forced by
    // direct inserts (`approvals::request` refuses to hand out a code another live approval
    // of the same person already uses).
    let alert = common::insert_alert(&pool, "open", "Ambiguous code alert").await;
    let code = "4821";
    let mut ambiguous = Vec::new();
    for step in ["s1", "s2"] {
        let id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO approvals \
                (id, alert_id, step_id, action, params, eligible, state, requested_by, \
                 expires_at, sms_code_hash) \
             VALUES ($1, $2, $3, 'escalate', '{}', ARRAY['user:carol'], 'pending', \
                     'user:anna', now() + interval '15 minutes', $4)",
        )
        .bind(id)
        .bind(alert)
        .bind(step)
        .bind(expected_hash(id, code))
        .execute(&pool)
        .await
        .expect("insert approval");
        ambiguous.push(id);
    }

    // The reply is a 200 that decides nothing and points at the UI.
    let payload = json!({ "from": "+44 7700 900125", "text": format!("Y {code}") }).to_string();
    let headers = signed_reply(&secret, now_ts(), &payload);
    let (status, body) = post_raw(&format!("{base}/api/replies"), payload, &headers).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["outcome"], json!("ambiguous"), "{body}");
    assert_eq!(body["message"], json!("decide in the UI"), "{body}");

    // Both requests are still pending and no attempt was burned on either.
    for id in ambiguous {
        let (state_str, attempts): (String, i32) =
            sqlx::query_as("SELECT state, sms_attempts FROM approvals WHERE id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .expect("approval row");
        assert_eq!(state_str, "pending", "{id}");
        assert_eq!(attempts, 0, "{id}");
    }
}

// ---------------------------------------------------------------------------------------------
// End to end: gateway /api/simulator/reply -> serve_standalone /api/replies -> approval
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn the_simulator_reply_forwarding_flow_approves_end_to_end(pool: PgPool) {
    let secret = b"shared-secret".to_vec();
    let state = api_state(&pool, Some(secret.clone()));
    let (api_base, shutdown, task) = serve_standalone(state, "user:bob", true).await;

    // The fake phone gateway: simulator routes only, forwarding replies to the api.
    let gw = GatewayState::new(pool.clone())
        .with_token(Some("gw-token".into()))
        .with_reply_forward(Some(api_base.clone()), Some(secret.clone()));
    let gw_base = serve_router(gateway::simulator_router(gw)).await;
    let reply_url = format!("{gw_base}/api/simulator/reply");

    // An approval to answer, created directly through `approvals::request`.
    let dir = api_dir();
    let alert =
        insert_alert_with_runbook(&pool, "open", &[action_step("s3", "required", None)]).await;
    let mut tx = pool.begin().await.expect("tx");
    let approval = approvals::request(
        &mut tx,
        &dir,
        alert,
        "s3",
        "escalate",
        &json!({ "team": "counterparty-support" }),
        "user:bob",
    )
    .await
    .expect("request approval");
    tx.commit().await.expect("commit");
    let code: String = sqlx::query_scalar(
        "SELECT route_info->'approval'->>'code' FROM delivery_jobs \
         WHERE alert_id = $1 AND channel = 'sms' AND route_info->>'target' = 'approval' \
         ORDER BY escalation_step LIMIT 1",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .expect("prompt code");

    // The demo-actor middleware: unknown names are 401, the fallback actor works.
    let run_url = format!("{api_base}/api/alerts/{alert}/steps/s3/run");
    let (status, body) = post_raw(
        &run_url,
        json!({}).to_string(),
        &[("x-beacon-demo-actor", "user:ghost".into())],
    )
    .await;
    assert_eq!(status, 401, "{body}");
    let (status, _) = post_raw(&run_url, json!({}).to_string(), &[]).await;
    assert_eq!(
        status, 409,
        "demo actor user:bob runs like any sessioned user"
    );

    // The phone replies "Y <code>" from carol's number.
    let payload = json!({ "from": "+44 7700 900125", "text": format!("Y {code}") }).to_string();
    let (status, res) = post_raw(
        &reply_url,
        payload.clone(),
        &[("authorization", "Bearer gw-token".into())],
    )
    .await;
    assert_eq!(status, 200, "{res}");
    assert_eq!(res["outcome"], json!("approved"), "{res}");
    assert_eq!(res["approval_id"], json!(approval.to_string()), "{res}");
    let execution: Uuid = res["execution_id"].as_str().unwrap().parse().unwrap();

    // The approval was consumed via the sms path and the execution is queued.
    let (state_str, decided_via): (String, Option<String>) =
        sqlx::query_as("SELECT state, decided_via FROM approvals WHERE id = $1")
            .bind(approval)
            .fetch_one(&pool)
            .await
            .expect("approval row");
    assert_eq!(state_str, "approved");
    assert_eq!(decided_via.as_deref(), Some("sms"));
    let (exec_state,): (String,) =
        sqlx::query_as("SELECT state FROM action_executions WHERE id = $1")
            .bind(execution)
            .fetch_one(&pool)
            .await
            .expect("execution row");
    assert_eq!(exec_state, "queued");

    // The gateway demands its token and rejects bad shapes before forwarding.
    let (status, res) = post_raw(&reply_url, payload, &[]).await;
    assert_eq!(status, 401, "{res}");
    let (status, res) = post_raw(
        &reply_url,
        json!({ "from": "carol", "text": "hi" }).to_string(),
        &[("authorization", "Bearer gw-token".into())],
    )
    .await;
    assert_eq!(status, 400, "from must be E.164: {res}");
    let (status, res) = post_raw(
        &reply_url,
        json!({ "from": "+44 7700 900125", "text": "x".repeat(161) }).to_string(),
        &[("authorization", "Bearer gw-token".into())],
    )
    .await;
    assert_eq!(status, 400, "text is one segment: {res}");

    // A gateway with nowhere to forward answers 503 unavailable.
    let dead = serve_router(gateway::simulator_router(
        GatewayState::new(pool.clone()).with_token(Some("gw-token".into())),
    ))
    .await;
    let (status, res) = post_raw(
        &format!("{dead}/api/simulator/reply"),
        json!({ "from": "+44 7700 900125", "text": "Y 0000" }).to_string(),
        &[("authorization", "Bearer gw-token".into())],
    )
    .await;
    assert_eq!(status, 503, "{res}");
    assert_eq!(error_code(&res), "unavailable", "{res}");

    // Graceful shutdown: the standalone server stops on the watch signal.
    shutdown.send(true).expect("signal shutdown");
    tokio::time::timeout(Duration::from_secs(15), task)
        .await
        .expect("serve_standalone must stop on shutdown")
        .expect("the standalone task must not panic");
}
