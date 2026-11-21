//! Agent B: action executions, built-in actions and the gateway mock targets.
//!
//! Needs Postgres (`DATABASE_URL`; sqlx creates a throwaway database per test).
//! The audit-row assertions exercise `crate::audit::record` (agent C); they pass once that
//! lands — the execution logic itself is asserted first in every test.
//!
//! Run: `cargo test -p beacon-delivery --test actions`

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use beacon_delivery::actions::{self, ActionRegistry, ActionsConfig, ActionsWorkerConfig};
use beacon_delivery::alert_ops::SqlAlertOps;
use beacon_delivery::config::{Contact, Directory, Person, Team};
use beacon_delivery::gateway::{self, GatewayState};
use beacon_delivery::runbook::Condition;
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

/// Start the real gateway with mock targets on an ephemeral port.
async fn start_mock_gateway(
    pool: PgPool,
) -> (GatewayState, String, tokio::sync::watch::Sender<bool>) {
    start_mock_gateway_token(pool, None).await
}

async fn start_mock_gateway_token(
    pool: PgPool,
    token: Option<&str>,
) -> (GatewayState, String, tokio::sync::watch::Sender<bool>) {
    let state = GatewayState::new(pool)
        .with_token(token.map(str::to_string))
        .with_mocks();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let serve_state = state.clone();
    tokio::spawn(async move {
        gateway::serve(
            serve_state,
            "127.0.0.1:0".parse().expect("addr"),
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

fn actions_config(base_url: &str) -> ActionsConfig {
    ActionsConfig {
        gateway_url: base_url.to_string(),
        http_targets: HashMap::from([(
            "counterparty-service".to_string(),
            format!("{base_url}/mock/health/counterparty-service"),
        )]),
        timeout: Duration::from_secs(5),
    }
}

fn worker_cfg() -> ActionsWorkerConfig {
    ActionsWorkerConfig {
        worker_id: format!("actions-test-{}", Uuid::new_v4()),
        poll_interval: Duration::from_millis(10),
        batch_size: 10,
        lease: Duration::from_secs(30),
        timeout: Duration::from_secs(10),
    }
}

fn empty_dir() -> Directory {
    Directory::from_parts(vec![], vec![]).expect("empty directory")
}

fn person_with_email(id: &str) -> Person {
    Person {
        id: id.to_string(),
        name: None,
        contact: Contact {
            email: Some(format!("{id}@example.test")),
            phone: None,
        },
        preferences: None,
        rules: Vec::new(),
    }
}

async fn queue_exec(
    pool: &PgPool,
    alert_id: Uuid,
    step: &str,
    action: &str,
    params: Value,
) -> Option<Uuid> {
    let mut tx = pool.begin().await.expect("tx");
    let id = actions::queue(&mut tx, alert_id, step, action, &params, "user:bob", None)
        .await
        .expect("queue");
    tx.commit().await.expect("commit");
    id
}

async fn run(pool: &PgPool, dir: &Directory, registry: &ActionRegistry) -> actions::ActionStats {
    actions::run_once(pool, dir, registry, &SqlAlertOps, &worker_cfg())
        .await
        .expect("run_once")
}

async fn exec_state(pool: &PgPool, id: Uuid) -> (String, Option<String>, i32) {
    sqlx::query_as("SELECT state, output, attempts FROM action_executions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("exec row")
}

async fn set_mock_mode(http: &reqwest::Client, base_url: &str, target: &str, mode: &str) {
    http.post(format!("{base_url}/api/simulator/mock"))
        .json(&json!({"target": target, "mode": mode}))
        .send()
        .await
        .expect("set mock mode")
        .error_for_status()
        .expect("mock mode status");
}

async fn audit_messages(pool: &PgPool, alert_id: Uuid, kind: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT message FROM audit_log WHERE alert_id = $1 AND kind = $2 ORDER BY id",
    )
    .bind(alert_id)
    .bind(kind)
    .fetch_all(pool)
    .await
    .expect("audit rows")
}

// ---------------------------------------------------------------------------------------------
// 1. queue twice for the same (alert, step) → second is None; after finish, queue again.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn queue_twice_second_is_none_until_finished(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_mock_gateway(pool.clone()).await;
    let registry = ActionRegistry::builtin(actions_config(&base_url));
    let dir = empty_dir();
    let alert = common::insert_alert(&pool, "open", "queue test").await;
    let params = json!({"url": "http://counterparty-service/health"});

    let mut tx = pool.begin().await.expect("tx");
    let first = actions::queue(&mut tx, alert, "s1", "http_get", &params, "user:bob", None)
        .await
        .expect("queue 1");
    assert!(first.is_some());
    let second = actions::queue(&mut tx, alert, "s1", "http_get", &params, "user:bob", None)
        .await
        .expect("queue 2");
    assert_eq!(second, None, "same (alert, step) while live queues nothing");
    tx.commit().await.expect("commit");

    assert!(
        audit_messages(&pool, alert, "step_run")
            .await
            .contains(&"queued".to_string()),
        "queue writes the step_run queued audit row"
    );

    let stats = run(&pool, &dir, &registry).await;
    assert_eq!((stats.claimed, stats.ok), (1, 1));
    let (state, _output, _attempts) = exec_state(&pool, first.unwrap()).await;
    assert_eq!(state, "ok");

    let third = queue_exec(&pool, alert, "s1", "http_get", params).await;
    assert!(
        third.is_some(),
        "after the execution finished the step can be queued again"
    );
}

// ---------------------------------------------------------------------------------------------
// 2. http_get: alias ok, unknown host refused without a request, redirect not followed.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn http_get_alias_ok_unknown_refused_redirect_not_followed(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_mock_gateway(pool.clone()).await;

    // Redirect server: /start answers 302 to /final; /final counts hits.
    let final_hits = Arc::new(AtomicUsize::new(0));
    let hits = final_hits.clone();
    let app = axum::Router::new()
        .route(
            "/start",
            axum::routing::get(|| async {
                (
                    axum::http::StatusCode::FOUND,
                    [("location", "/final")],
                    "go",
                )
            }),
        )
        .route(
            "/final",
            axum::routing::get(move || {
                let hits = hits.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    (axum::http::StatusCode::OK, axum::Json(json!({"ok": true})))
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind redirect server");
    let redir_base = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("redirect server");
    });

    // Silent listener: proves a refused host is never contacted.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind probe");
    let probe_port = probe.local_addr().expect("addr").port();
    let contacts = Arc::new(AtomicUsize::new(0));
    let contacts_task = contacts.clone();
    tokio::spawn(async move {
        while probe.accept().await.is_ok() {
            contacts_task.fetch_add(1, Ordering::SeqCst);
        }
    });

    let cfg = ActionsConfig {
        gateway_url: base_url.clone(),
        http_targets: HashMap::from([
            (
                "counterparty-service".to_string(),
                format!("{base_url}/mock/health/counterparty-service"),
            ),
            ("redir".to_string(), format!("{redir_base}/start")),
        ]),
        timeout: Duration::from_secs(5),
    };
    let registry = ActionRegistry::builtin(cfg);
    let dir = empty_dir();
    let alert = common::insert_alert(&pool, "open", "http get").await;

    // 1. The alias maps and the mock answers 200.
    let id = queue_exec(
        &pool,
        alert,
        "s1",
        "http_get",
        json!({"url": "http://counterparty-service/health"}),
    )
    .await
    .expect("queue s1");
    run(&pool, &dir, &registry).await;
    let (state, output, _) = exec_state(&pool, id).await;
    assert_eq!(state, "ok", "{output:?}");

    // 2. A host with no alias fails without any request.
    let id = queue_exec(
        &pool,
        alert,
        "s2",
        "http_get",
        json!({"url": format!("http://127.0.0.1:{probe_port}/x")}),
    )
    .await
    .expect("queue s2");
    run(&pool, &dir, &registry).await;
    let (state, output, _) = exec_state(&pool, id).await;
    assert_eq!(state, "failed");
    assert!(
        output
            .clone()
            .unwrap_or_default()
            .contains("target not allowed"),
        "unexpected output: {output:?}"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        contacts.load(Ordering::SeqCst),
        0,
        "a refused host must never be contacted"
    );

    // 3. A 302 from the target is a failure; the redirect target is never hit.
    let id = queue_exec(
        &pool,
        alert,
        "s3",
        "http_get",
        json!({"url": "http://redir/start"}),
    )
    .await
    .expect("queue s3");
    run(&pool, &dir, &registry).await;
    let (state, output, _) = exec_state(&pool, id).await;
    assert_eq!(state, "failed");
    assert!(
        output.clone().unwrap_or_default().contains("302"),
        "unexpected output: {output:?}"
    );
    assert_eq!(
        final_hits.load(Ordering::SeqCst),
        0,
        "the client must never follow redirects"
    );
}

// ---------------------------------------------------------------------------------------------
// 3. retry_workflow: fail mode → failed; ok mode → ok; condition counts failures.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn retry_workflow_fail_then_ok_and_condition_counts_failures(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_mock_gateway(pool.clone()).await;
    let http = reqwest::Client::new();
    let registry = ActionRegistry::builtin(actions_config(&base_url));
    let dir = empty_dir();
    let alert = common::insert_alert(&pool, "open", "retry test").await;
    let cond = || Condition::AfterFailures {
        action: "retry_workflow".to_string(),
        count: 2,
    };

    set_mock_mode(&http, &base_url, "workflows", "fail").await;

    let id1 = queue_exec(
        &pool,
        alert,
        "s2",
        "retry_workflow",
        json!({"workflowId": "WF-1"}),
    )
    .await
    .expect("queue 1");
    run(&pool, &dir, &registry).await;
    let (state, output, _) = exec_state(&pool, id1).await;
    assert_eq!(state, "failed", "{output:?}");
    assert!(!output.unwrap().is_empty(), "failures carry an output");

    let (met, reason) = actions::condition_status(&pool, alert, &cond())
        .await
        .expect("condition");
    assert!(!met);
    assert_eq!(
        reason.as_deref(),
        Some("Available after 2 failed retries (1 so far)")
    );

    let id2 = queue_exec(
        &pool,
        alert,
        "s2",
        "retry_workflow",
        json!({"workflowId": "WF-1"}),
    )
    .await
    .expect("queue 2");
    run(&pool, &dir, &registry).await;
    assert_eq!(exec_state(&pool, id2).await.0, "failed");
    let (met, reason) = actions::condition_status(&pool, alert, &cond())
        .await
        .expect("condition");
    assert!(met, "two failures meet the condition");
    assert_eq!(reason, None);

    set_mock_mode(&http, &base_url, "workflows", "ok").await;
    let id3 = queue_exec(
        &pool,
        alert,
        "s2",
        "retry_workflow",
        json!({"workflowId": "WF-1"}),
    )
    .await
    .expect("queue 3");
    run(&pool, &dir, &registry).await;
    let (state, output, _) = exec_state(&pool, id3).await;
    assert_eq!(state, "ok", "{output:?}");

    // Missing params fail instead of calling anything.
    let bad = queue_exec(&pool, alert, "s9", "retry_workflow", json!({}))
        .await
        .expect("queue bad");
    run(&pool, &dir, &registry).await;
    let (state, output, _) = exec_state(&pool, bad).await;
    assert_eq!(state, "failed");
    assert!(
        output.unwrap().contains("workflowId"),
        "the failure names the missing param"
    );

    assert!(
        audit_messages(&pool, alert, "step_run")
            .await
            .iter()
            .any(|m| m.starts_with("retry_workflow ok")),
        "ok executions write a step_run audit row"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn condition_reason_names_other_actions(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_mock_gateway(pool.clone()).await;
    let http = reqwest::Client::new();
    let registry = ActionRegistry::builtin(actions_config(&base_url));
    let dir = empty_dir();
    let alert = common::insert_alert(&pool, "open", "condition test").await;

    set_mock_mode(&http, &base_url, "health", "fail").await;
    let id = queue_exec(
        &pool,
        alert,
        "s1",
        "http_get",
        json!({"url": "http://counterparty-service/health"}),
    )
    .await
    .expect("queue");
    run(&pool, &dir, &registry).await;
    assert_eq!(exec_state(&pool, id).await.0, "failed");

    let (met, reason) = actions::condition_status(
        &pool,
        alert,
        &Condition::AfterFailures {
            action: "http_get".to_string(),
            count: 2,
        },
    )
    .await
    .expect("condition");
    assert!(!met);
    assert_eq!(
        reason.as_deref(),
        Some("Available after 2 failed http_get runs (1 so far)")
    );
}

// ---------------------------------------------------------------------------------------------
// 4. escalate pages every team member with route_info.target = "escalate:<team>".
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn escalate_pages_every_team_member(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_mock_gateway(pool.clone()).await;
    let registry = ActionRegistry::builtin(actions_config(&base_url));
    let dir = Directory::from_parts(
        vec![person_with_email("d1"), person_with_email("d2")],
        vec![Team {
            id: "counterparty-support".to_string(),
            members: vec!["d1".to_string(), "d2".to_string()],
            lead: None,
            oncall: HashMap::new(),
            default_channels: None,
        }],
    )
    .expect("directory");
    let alert = common::insert_alert(&pool, "open", "escalate test").await;

    let id = queue_exec(
        &pool,
        alert,
        "s3",
        "escalate",
        json!({"team": "counterparty-support"}),
    )
    .await
    .expect("queue");
    let stats = run(&pool, &dir, &registry).await;
    assert_eq!(stats.ok, 1);
    let (state, output, _) = exec_state(&pool, id).await;
    assert_eq!(state, "ok");
    assert_eq!(
        output.as_deref(),
        Some("escalated to counterparty-support: 2 job(s)")
    );

    let jobs: Vec<(String, String, Value, Option<i32>, i32)> = sqlx::query_as(
        "SELECT recipient_id, channel, route_info, escalation_step, notify_seq \
         FROM delivery_jobs WHERE alert_id = $1 ORDER BY recipient_id",
    )
    .bind(alert)
    .fetch_all(&pool)
    .await
    .expect("jobs");
    assert_eq!(jobs.len(), 2, "one job per team member");
    let seq: i32 = sqlx::query_scalar("SELECT notify_seq FROM alerts WHERE id = $1")
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("alert notify_seq");
    for (recipient, channel, route_info, step, notify_seq) in &jobs {
        assert!(
            ["d1", "d2"].contains(&recipient.as_str()),
            "unexpected recipient {recipient}"
        );
        assert_eq!(channel, "email", "UI-only resolves to email");
        assert_eq!(route_info["target"], json!("escalate:counterparty-support"));
        assert!(
            step.unwrap_or(0) >= 100,
            "escalation_step stays out of the scheduler range: {step:?}"
        );
        assert_eq!(*notify_seq, seq, "jobs reuse the alert's notify_seq");
    }

    assert!(
        audit_messages(&pool, alert, "escalated")
            .await
            .iter()
            .any(|m| m.contains("counterparty-support")),
        "escalate writes an escalated audit row"
    );
}

// ---------------------------------------------------------------------------------------------
// 4b. escalate still pages a member already paged for this notification: the idempotency
//     key's escalation_step column (100 + n) tells the two jobs apart.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn escalate_pages_a_member_already_paged_for_this_notification(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_mock_gateway(pool.clone()).await;
    let registry = ActionRegistry::builtin(actions_config(&base_url));
    let dir = Directory::from_parts(
        vec![person_with_email("d1"), person_with_email("d2")],
        vec![Team {
            id: "counterparty-support".to_string(),
            members: vec!["d1".to_string(), "d2".to_string()],
            lead: None,
            oncall: HashMap::new(),
            default_channels: None,
        }],
    )
    .expect("directory");
    let alert = common::insert_alert(&pool, "open", "escalate collision test").await;

    // The original notification already paged d1 at the alert's current notify_seq: a plain
    // job with escalation_step 0.
    sqlx::query(
        "INSERT INTO delivery_jobs (alert_id, notify_seq, recipient_id, channel, address) \
         VALUES ($1, 1, 'd1', 'email', 'd1@example.test')",
    )
    .bind(alert)
    .execute(&pool)
    .await
    .expect("d1's original page");

    let id = queue_exec(
        &pool,
        alert,
        "s3",
        "escalate",
        json!({"team": "counterparty-support"}),
    )
    .await
    .expect("queue");
    let stats = run(&pool, &dir, &registry).await;
    assert_eq!(stats.ok, 1);
    let (state, output, _) = exec_state(&pool, id).await;
    assert_eq!(state, "ok");
    assert_eq!(
        output.as_deref(),
        Some("escalated to counterparty-support: 2 job(s)")
    );

    // d1's escalate job was not swallowed by the original page: same (alert, recipient,
    // channel, notify_seq), different escalation_step.
    let d1_jobs: Vec<(Option<i32>, i32)> = sqlx::query_as(
        "SELECT escalation_step, notify_seq FROM delivery_jobs \
         WHERE alert_id = $1 AND recipient_id = 'd1' ORDER BY escalation_step",
    )
    .bind(alert)
    .fetch_all(&pool)
    .await
    .expect("d1 jobs");
    assert_eq!(
        d1_jobs,
        vec![(Some(0), 1), (Some(100), 1)],
        "the original page (step 0) plus the escalate job (step >= 100)"
    );
    let d2_step: Option<i32> = sqlx::query_scalar(
        "SELECT escalation_step FROM delivery_jobs \
         WHERE alert_id = $1 AND recipient_id = 'd2'",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .expect("d2's job");
    assert!(
        d2_step.unwrap_or(0) >= 100,
        "d2's escalate job exists too: {d2_step:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// 5. Leases: expired reclaimed, stale token fenced, 3 attempts → uncertain.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn expired_leases_reclaimed_stale_fenced_attempts_capped(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_mock_gateway(pool.clone()).await;
    let registry = ActionRegistry::builtin(actions_config(&base_url));
    let dir = empty_dir();
    let alert = common::insert_alert(&pool, "open", "lease test").await;

    // An expired running lease is reclaimed and run to completion.
    let id = queue_exec(
        &pool,
        alert,
        "s1",
        "http_get",
        json!({"url": "http://counterparty-service/health"}),
    )
    .await
    .expect("queue");
    let stale = Uuid::new_v4();
    sqlx::query(
        "UPDATE action_executions SET state = 'running', locked_by = 'dead', \
            locked_until = now() - interval '1 second', lease_token = $2, attempts = 1, \
            updated_at = now() WHERE id = $1",
    )
    .bind(id)
    .bind(stale)
    .execute(&pool)
    .await
    .expect("plant expired lease");
    run(&pool, &dir, &registry).await;
    let (state, _, attempts) = exec_state(&pool, id).await;
    assert_eq!(state, "ok", "the reclaimed execution runs");
    assert_eq!(attempts, 2, "the reclaim takes one attempt");

    // A stale token finalizes nothing.
    let fenced: u64 = sqlx::query(
        "UPDATE action_executions SET output = output WHERE id = $1 AND lease_token = $2",
    )
    .bind(id)
    .bind(stale)
    .execute(&pool)
    .await
    .expect("fence check")
    .rows_affected();
    assert_eq!(fenced, 0, "a stale token must finalize nothing");

    // Past 3 attempts an expired lease goes uncertain ("lease expired").
    let id2 = queue_exec(
        &pool,
        alert,
        "s2",
        "http_get",
        json!({"url": "http://counterparty-service/health"}),
    )
    .await
    .expect("queue 2");
    sqlx::query(
        "UPDATE action_executions SET state = 'running', \
            locked_until = now() - interval '1 second', lease_token = $2, attempts = 3, \
            updated_at = now() WHERE id = $1",
    )
    .bind(id2)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .expect("plant lease");
    let stats = run(&pool, &dir, &registry).await;
    assert_eq!(stats.uncertain, 1);
    assert_eq!(stats.claimed, 0, "the swept row is not claimed again");
    let (state, output, _) = exec_state(&pool, id2).await;
    assert_eq!(state, "uncertain");
    assert_eq!(output.as_deref(), Some("lease expired"));
}

// ---------------------------------------------------------------------------------------------
// 6. A step with resolves: true resolves the alert (SqlAlertOps) + alert_update outbox row.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn resolves_true_step_ok_resolves_alert(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_mock_gateway(pool.clone()).await;
    let registry = ActionRegistry::builtin(actions_config(&base_url));
    let dir = empty_dir();
    let alert = common::insert_alert(&pool, "open", "resolves test").await;
    sqlx::query(
        "INSERT INTO alert_runbooks (alert_id, runbook_id, revision, title, format, steps, context) \
         VALUES ($1, 'rb', 1, 't', 'yaml', $2, '{}')",
    )
    .bind(alert)
    .bind(json!([{"id": "s1", "resolves": true}]))
    .execute(&pool)
    .await
    .expect("freeze runbook");

    let id = queue_exec(
        &pool,
        alert,
        "s1",
        "http_get",
        json!({"url": "http://counterparty-service/health"}),
    )
    .await
    .expect("queue");
    run(&pool, &dir, &registry).await;
    assert_eq!(exec_state(&pool, id).await.0, "ok");

    let status: String = sqlx::query_scalar("SELECT status FROM alerts WHERE id = $1")
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("status");
    assert_eq!(status, "resolved");
    let updates: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM outbox WHERE kind = 'alert_update' AND alert_id = $1",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .expect("outbox");
    assert!(
        updates >= 1,
        "touch/transition write alert_update rows, found {updates}"
    );
}

// ---------------------------------------------------------------------------------------------
// 7. run_actions stops on shutdown (true and sender dropped).
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn run_actions_stops_on_shutdown(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_mock_gateway(pool.clone()).await;
    let registry = ActionRegistry::builtin(actions_config(&base_url));
    let dir = Arc::new(empty_dir());
    let cfg = worker_cfg();

    let (tx, rx) = tokio::sync::watch::channel(false);
    let handle = tokio::spawn(actions::run_actions(
        pool.clone(),
        dir.clone(),
        registry.clone(),
        Arc::new(SqlAlertOps),
        cfg.clone(),
        rx,
    ));
    tx.send(true).expect("signal shutdown");
    tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("run_actions must stop on shutdown(true)")
        .expect("worker task must not panic");

    // Dropping the sender stops the loop too.
    let (tx2, rx2) = tokio::sync::watch::channel(false);
    let handle2 = tokio::spawn(actions::run_actions(
        pool.clone(),
        dir,
        registry,
        Arc::new(SqlAlertOps),
        cfg,
        rx2,
    ));
    drop(tx2);
    tokio::time::timeout(Duration::from_secs(30), handle2)
        .await
        .expect("run_actions must stop when the sender is dropped")
        .expect("worker task must not panic");
}

// ---------------------------------------------------------------------------------------------
// 8. The mock control API: token-gated POST, open GET, recorded calls, switchable outcomes.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn mock_api_modes_calls_and_token(pool: PgPool) {
    let (_state, base_url, _shutdown) =
        start_mock_gateway_token(pool.clone(), Some("mock-token")).await;
    let http = reqwest::Client::new();

    // GET is token-gated like every simulator route; every target defaults to ok with an
    // empty call log.
    let status: Value = http
        .get(format!("{base_url}/api/simulator/mock"))
        .bearer_auth("mock-token")
        .send()
        .await
        .expect("GET mock status")
        .json()
        .await
        .expect("status JSON");
    assert_eq!(
        status["modes"],
        json!({"workflows": "ok", "services": "ok", "health": "ok", "tickets": "ok"})
    );
    assert_eq!(status["calls"], json!([]));

    // POST without the token is 401 and changes nothing.
    let resp = http
        .post(format!("{base_url}/api/simulator/mock"))
        .json(&json!({"target": "health", "mode": "fail"}))
        .send()
        .await
        .expect("POST without token");
    assert_eq!(resp.status().as_u16(), 401);

    // Bad targets and modes are 400.
    for bad in [
        json!({"target": "nope", "mode": "fail"}),
        json!({"target": "health", "mode": "nope"}),
    ] {
        let resp = http
            .post(format!("{base_url}/api/simulator/mock"))
            .bearer_auth("mock-token")
            .json(&bad)
            .send()
            .await
            .expect("POST bad");
        assert_eq!(resp.status().as_u16(), 400, "{bad}");
        let body: Value = resp.json().await.expect("error JSON");
        assert_eq!(body["error"]["code"], json!("bad_request"));
    }

    // Fail the health mock: the target answers 500 and records the call.
    set_mock_mode_authed(&http, &base_url, "health", "fail").await;
    let resp = http
        .get(format!("{base_url}/mock/health/counterparty-service"))
        .send()
        .await
        .expect("GET health");
    assert_eq!(resp.status().as_u16(), 500);
    assert_eq!(
        resp.json::<Value>().await.expect("fail JSON"),
        json!({"error": "mock failure"})
    );
    let status: Value = http
        .get(format!("{base_url}/api/simulator/mock"))
        .bearer_auth("mock-token")
        .send()
        .await
        .expect("GET status")
        .json()
        .await
        .expect("status JSON");
    assert_eq!(status["modes"]["health"], json!("fail"));
    let calls = status["calls"].as_array().expect("calls array");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["target"], json!("health"));
    assert_eq!(calls[0]["method"], json!("GET"));

    // Back to ok, plus the other three targets.
    set_mock_mode_authed(&http, &base_url, "health", "ok").await;
    let resp = http
        .get(format!("{base_url}/mock/health/counterparty-service"))
        .send()
        .await
        .expect("GET health");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.json::<Value>().await.expect("ok JSON"),
        json!({"status": "healthy"})
    );

    let resp = http
        .post(format!("{base_url}/mock/workflows/WF-1/retry"))
        .json(&json!({}))
        .send()
        .await
        .expect("POST retry");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.json::<Value>().await.expect("retry JSON"),
        json!({"ok": true, "workflow": "WF-1"})
    );

    let resp = http
        .post(format!("{base_url}/mock/services/svc/restart"))
        .json(&json!({}))
        .send()
        .await
        .expect("POST restart");
    assert_eq!(resp.status().as_u16(), 200);

    let resp = http
        .post(format!("{base_url}/mock/tickets"))
        .json(&json!({}))
        .send()
        .await
        .expect("POST ticket");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.json::<Value>().await.expect("ticket JSON"),
        json!({"ok": true})
    );
}

// ---------------------------------------------------------------------------------------------
// 9. Audit #11: path segments are validated; outputs lose queries and credentials.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn invalid_path_segments_fail_the_action_without_a_request(pool: PgPool) {
    let (_state, base_url, _shutdown) = start_mock_gateway(pool.clone()).await;
    let registry = ActionRegistry::builtin(actions_config(&base_url));
    let dir = empty_dir();
    let alert = common::insert_alert(&pool, "open", "segment validation").await;

    // `..`, `.`, empty, and anything with `/` or `\` can never become part of the mock path.
    for (step, action, params) in [
        ("s1", "retry_workflow", json!({"workflowId": ".."})),
        ("s2", "retry_workflow", json!({"workflowId": "a/b"})),
        ("s3", "restart_service", json!({"service": "a\\b"})),
        ("s4", "restart_service", json!({"service": "."})),
        ("s5", "restart_service", json!({"service": ""})),
    ] {
        let id = queue_exec(&pool, alert, step, action, params)
            .await
            .expect("queue");
        run(&pool, &dir, &registry).await;
        let (state, output, _) = exec_state(&pool, id).await;
        assert_eq!(state, "failed", "{step}");
        assert_eq!(
            output.as_deref(),
            Some("invalid path segment"),
            "{step}: {output:?}"
        );
    }

    // Nothing ever reached the gateway's mock targets.
    let status: Value = reqwest::Client::new()
        .get(format!("{base_url}/api/simulator/mock"))
        .bearer_auth("mock-token")
        .send()
        .await
        .expect("mock status")
        .json()
        .await
        .expect("status JSON");
    assert_eq!(status["calls"], json!([]), "{status}");
}

#[sqlx::test(migrations = "../../migrations")]
async fn outputs_strip_url_queries_and_credentials(pool: PgPool) {
    // A target that echoes credentials back in its error body.
    let leak = axum::Router::new().route(
        "/leak/x",
        axum::routing::get(|| async {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Authorization: Bearer hunter2 and token=hunter2 in the body",
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind leak server");
    let leak_base = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        axum::serve(listener, leak).await.expect("leak server");
    });

    let (_state, base_url, _shutdown) = start_mock_gateway(pool.clone()).await;
    let cfg = ActionsConfig {
        gateway_url: base_url.clone(),
        http_targets: HashMap::from([
            (
                "counterparty-service".to_string(),
                format!("{base_url}/mock/health/counterparty-service"),
            ),
            ("leaky".to_string(), format!("{leak_base}/leak")),
        ]),
        timeout: Duration::from_secs(5),
    };
    let registry = ActionRegistry::builtin(cfg);
    let dir = empty_dir();
    let alert = common::insert_alert(&pool, "open", "redaction test").await;

    // 1. The stored output drops the request URL's query string (the request keeps it).
    let id = queue_exec(
        &pool,
        alert,
        "s1",
        "http_get",
        json!({"url": "http://counterparty-service/health?token=hunter2&verbose=1"}),
    )
    .await
    .expect("queue");
    run(&pool, &dir, &registry).await;
    let (state, output, _) = exec_state(&pool, id).await;
    assert_eq!(state, "ok", "{output:?}");
    let output = output.unwrap();
    assert!(!output.contains('?'), "the query survived: {output:?}");
    assert!(!output.contains("hunter2"), "{output:?}");

    // 2. Credential-shaped snippets (bearer/basic headers, token= values) never reach the
    //    stored output or the audit trail either.
    let id = queue_exec(
        &pool,
        alert,
        "s2",
        "http_get",
        json!({"url": "http://leaky/x?sig=hunter2"}),
    )
    .await
    .expect("queue");
    run(&pool, &dir, &registry).await;
    let (state, output, _) = exec_state(&pool, id).await;
    assert_eq!(state, "failed", "{output:?}");
    let output = output.unwrap();
    assert!(
        !output.contains("hunter2"),
        "the credential survived: {output:?}"
    );
    assert!(output.contains("[REDACTED]"), "{output:?}");
    assert!(!output.contains('?'), "{output:?}");
    for message in audit_messages(&pool, alert, "step_run").await {
        assert!(!message.contains("hunter2"), "the audit leaked: {message}");
    }
}

async fn set_mock_mode_authed(http: &reqwest::Client, base_url: &str, target: &str, mode: &str) {
    http.post(format!("{base_url}/api/simulator/mock"))
        .bearer_auth("mock-token")
        .json(&json!({"target": target, "mode": mode}))
        .send()
        .await
        .expect("set mock mode")
        .error_for_status()
        .expect("mock mode status");
}

// ---------------------------------------------------------------------------------------------
// 10. Review fix (PR #11): the whole claimed batch shares one lease but actions run
//     sequentially — the lease is refreshed and re-verified (token match) immediately
//     before each execution, so a slow first action cannot get a later execution re-run by
//     another worker while it waits.
// ---------------------------------------------------------------------------------------------

/// A worker config with a controllable lease for the concurrency tests.
fn batch_cfg(worker_id: &str, lease: Duration) -> ActionsWorkerConfig {
    ActionsWorkerConfig {
        worker_id: worker_id.to_string(),
        poll_interval: Duration::from_millis(10),
        batch_size: 10,
        lease,
        timeout: Duration::from_secs(20),
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_slow_first_action_does_not_get_the_second_execution_run_twice(pool: PgPool) {
    // Two local targets: /slow takes 3 s (longer than the lease below), /count counts hits.
    let slow_hits = Arc::new(AtomicUsize::new(0));
    let count_hits = Arc::new(AtomicUsize::new(0));
    let slow_srv = slow_hits.clone();
    let count_srv = count_hits.clone();
    let app = axum::Router::new()
        .route(
            "/slow",
            axum::routing::get(move || {
                let slow_srv = slow_srv.clone();
                async move {
                    slow_srv.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    (axum::http::StatusCode::OK, axum::Json(json!({"ok": true})))
                }
            }),
        )
        .route(
            "/count",
            axum::routing::get(move || {
                let count_srv = count_srv.clone();
                async move {
                    count_srv.fetch_add(1, Ordering::SeqCst);
                    (axum::http::StatusCode::OK, axum::Json(json!({"ok": true})))
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind target server");
    let base = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("target server");
    });

    let registry = ActionRegistry::builtin(ActionsConfig {
        gateway_url: base.clone(),
        http_targets: HashMap::from([
            ("slow".to_string(), format!("{base}/slow")),
            ("count".to_string(), format!("{base}/count")),
        ]),
        timeout: Duration::from_secs(20),
    });
    let dir = empty_dir();

    // A short lease: the slow first action (3 s) outlasts it, so a claim-once batch lease
    // would let the second execution's lease expire while it waits behind the first.
    let lease = Duration::from_secs(2);
    let alert = common::insert_alert(&pool, "open", "batch lease test").await;
    let _slow_id = queue_exec(
        &pool,
        alert,
        "s1",
        "http_get",
        // A path the alias base ends with, so the mapped URL is exactly {base}/slow.
        json!({"url": "http://slow/slow"}),
    )
    .await
    .expect("queue the slow execution");
    let count_id = queue_exec(
        &pool,
        alert,
        "s2",
        "http_get",
        json!({"url": "http://count/count"}),
    )
    .await
    .expect("queue the counted execution");

    // Worker A claims the batch (slow first) and grinds on the slow action.
    let worker_a = {
        let (pool, registry, dir) = (pool.clone(), registry.clone(), dir.clone());
        let cfg = batch_cfg("w-batch-a", lease);
        tokio::spawn(
            async move { actions::run_once(&pool, &dir, &registry, &SqlAlertOps, &cfg).await },
        )
    };

    // Worker B polls concurrently, reclaiming anything whose lease lapsed — under a
    // claim-once batch lease it would re-run the execution still waiting behind A's slow
    // action, and A would run it too after the slow one finished.
    let worker_b = {
        let (pool, registry, dir) = (pool.clone(), registry, dir);
        let count_watch = count_hits.clone();
        let cfg = batch_cfg("w-batch-b", lease);
        tokio::spawn(async move {
            let deadline = std::time::Instant::now() + Duration::from_secs(45);
            while std::time::Instant::now() < deadline {
                let stats = actions::run_once(&pool, &dir, &registry, &SqlAlertOps, &cfg)
                    .await
                    .expect("worker B run_once");
                if count_watch.load(Ordering::SeqCst) >= 1 {
                    return stats;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            panic!("worker B never saw the counted execution finish");
        })
    };

    let stats_a = tokio::time::timeout(Duration::from_secs(60), worker_a)
        .await
        .expect("worker A timed out")
        .expect("worker A panicked")
        .expect("worker A db error");
    let stats_b = tokio::time::timeout(Duration::from_secs(60), worker_b)
        .await
        .expect("worker B timed out")
        .expect("worker B panicked");

    // The slow action may legitimately be re-run once its own lease lapses mid-flight
    // (that is the ordinary reclaim path, fenced at finalize); the SECOND execution must
    // not: it never runs lease-less behind the first any more.
    assert!(slow_hits.load(Ordering::SeqCst) >= 1, "the slow action ran");
    assert_eq!(
        count_hits.load(Ordering::SeqCst),
        1,
        "the second execution must not be run twice: a={stats_a:?} b={stats_b:?}"
    );
    let (state, _, _) = exec_state(&pool, count_id).await;
    assert_eq!(state, "ok", "finalized exactly once");
}
