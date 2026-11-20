//! M5b runbook integration suite (SPEC §6.1): the four file formats against the seed
//! `config/runbooks/`, selection, rendering, router freezing, the email section and the
//! `RunbookView` projection for the UI contract.
//!
//! Needs Postgres (`DATABASE_URL`; sqlx creates a throwaway database per test).
//!
//! Run: `cargo test -p beacon-delivery --test runbooks -- --nocapture`

mod common;

use std::path::Path;
use std::sync::Arc;

use beacon_delivery::RouterConfig;
use beacon_delivery::actions::ActionRegistry;
use beacon_delivery::channel::{BoxFuture, Channel, OutboundMessage, Receipt, SendError};
use beacon_delivery::config::Directory;
use beacon_delivery::router::route_once;
use beacon_delivery::runbook::{self, Format, RenderedStep, Runbook, StepDef, StepKind};
use beacon_delivery::worker;
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

fn seed_dir() -> Directory {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");
    Directory::load(&root).expect("seed config directory loads")
}

/// Insert an alert with full control over status / severity / team / rule
/// (`common::insert_alert` fixes team, rule and severity).
async fn insert_alert_as(
    pool: &PgPool,
    status: &str,
    severity: &str,
    team_id: &str,
    rule_id: &str,
) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO alerts (id, rule_id, rule_revision, rule_name, team, fingerprint, \
                             dedup_window_ms, status, severity, color, title, body, \
                             first_seen, last_seen, source, schema, notify_seq) \
         VALUES ($1, $2, 1, $3, $4, $5, 600000, $6, $7, 'red', 'Runbook test alert', '', \
                 now(), now(), 'test', 'workflow.v1', 1)",
    )
    .bind(id)
    .bind(rule_id)
    .bind(rule_id)
    .bind(team_id)
    .bind(format!("fp-{id}"))
    .bind(status)
    .bind(severity)
    .execute(pool)
    .await
    .expect("insert alert");
    id
}

/// Insert an outbox notify row with an optional `event` and rule `runbook` binding.
#[allow(clippy::too_many_arguments)]
async fn insert_notify_event(
    pool: &PgPool,
    alert: Uuid,
    seq: i32,
    rule_id: &str,
    team: &str,
    severity: &str,
    reason: &str,
    route_target: &str,
    event: Option<Value>,
    runbook: Option<&str>,
) {
    let mut payload = json!({
        "alert_id": alert,
        "notify_seq": seq,
        "rule_id": rule_id,
        "rule_revision": 1,
        "team": team,
        "severity": severity,
        "reason": reason,
        "route": [{ "to": route_target, "channels": ["email", "ui"] }],
        "created_at": chrono::Utc::now(),
    });
    if let Some(event) = event {
        payload["event"] = event;
    }
    if let Some(runbook_id) = runbook {
        payload["runbook"] = json!(runbook_id);
    }
    sqlx::query("INSERT INTO outbox (kind, alert_id, payload) VALUES ('notify', $1, $2)")
        .bind(alert)
        .bind(payload)
        .execute(pool)
        .await
        .expect("insert notify row");
}

/// The demo's settlements notification: the on-call rotation gets email + ui, and the payload
/// carries the demo event plus the rule's runbook binding.
async fn notify_settlements(
    pool: &PgPool,
    alert: Uuid,
    seq: i32,
    reason: &str,
    event: Option<Value>,
) {
    insert_notify_event(
        pool,
        alert,
        seq,
        "settlements-workflow-failed",
        "settlements",
        "critical",
        reason,
        "oncall:settlements",
        event,
        Some("settlement-timeout"),
    )
    .await;
}

/// The demo event (Appendix B of docs/PROBLEM_STATEMENT.md): `WORKFLOW_FAILED` on
/// `order-settlement` with `workflowId` in `event.raw`.
fn demo_event() -> Value {
    json!({
        "raw": {
            "workflowId": "WF-1",
            "workflowName": "order-settlement",
            "step": "settle",
            "error": { "code": "TIMEOUT", "message": "request timed out" },
        },
        "fields": { "type": "WORKFLOW_FAILED", "environment": "prod" },
    })
}

/// The alert row values the router freezes into the template context.
fn demo_alert(alert: Uuid) -> Value {
    json!({
        "id": alert,
        "team": "settlements",
        "rule_id": "settlements-workflow-failed",
        "severity": "critical",
    })
}

async fn frozen_steps(pool: &PgPool, alert: Uuid) -> Vec<RenderedStep> {
    let steps: Value = sqlx::query_scalar("SELECT steps FROM alert_runbooks WHERE alert_id = $1")
        .bind(alert)
        .fetch_one(pool)
        .await
        .expect("frozen steps");
    serde_json::from_value(steps).expect("steps deserialize")
}

async fn insert_execution(
    pool: &PgPool,
    alert: Uuid,
    step_id: &str,
    action: &str,
    state: &str,
    requested_by: &str,
    age_secs: f64,
) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO action_executions \
             (id, alert_id, step_id, action, params, state, requested_by, created_at) \
         VALUES ($1, $2, $3, $4, '{}'::jsonb, $5, $6, now() - make_interval(secs => $7::float8))",
    )
    .bind(id)
    .bind(alert)
    .bind(step_id)
    .bind(action)
    .bind(state)
    .bind(requested_by)
    .bind(age_secs)
    .execute(pool)
    .await
    .expect("insert execution");
    id
}

async fn insert_approval(
    pool: &PgPool,
    alert: Uuid,
    step_id: &str,
    action: &str,
    state: &str,
    eligible: &[&str],
    requested_by: &str,
) -> Uuid {
    let id = Uuid::new_v4();
    let decided_by = if state == "pending" {
        None
    } else {
        Some(requested_by.to_string())
    };
    sqlx::query(
        "INSERT INTO approvals \
             (id, alert_id, step_id, action, params, eligible, state, requested_by, \
              expires_at, sms_code_hash, decided_by, decided_at) \
         VALUES ($1, $2, $3, $4, '{}'::jsonb, $5, $6, $7, now() + interval '15 minutes', \
                 'not-the-real-hash', $8, CASE WHEN $8 IS NULL THEN NULL ELSE now() END)",
    )
    .bind(id)
    .bind(alert)
    .bind(step_id)
    .bind(action)
    .bind(
        eligible
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>(),
    )
    .bind(state)
    .bind(requested_by)
    .bind(&decided_by)
    .execute(pool)
    .await
    .expect("insert approval");
    id
}

async fn insert_mark(pool: &PgPool, alert: Uuid, step_id: &str, done_by: &str) {
    sqlx::query("INSERT INTO step_marks (alert_id, step_id, done_by) VALUES ($1, $2, $3)")
        .bind(alert)
        .bind(step_id)
        .bind(done_by)
        .execute(pool)
        .await
        .expect("insert step mark");
}

/// A runbook without conditions, so the view's state machine is exercised without
/// `actions::condition_status`.
fn toy_runbook() -> Runbook {
    let step =
        |id: &str, kind: StepKind, text: &str, action: Option<&str>, approval: bool| -> StepDef {
            StepDef {
                id: id.into(),
                kind,
                text: text.into(),
                hint: None,
                action: action.map(str::to_string),
                params: Default::default(),
                approval_required: approval,
                condition: None,
                resolves: false,
            }
        };
    Runbook {
        id: "toy".into(),
        revision: 7,
        title: "Toy runbook".into(),
        format: Format::Yaml,
        team: Some("settlements".into()),
        priority: 0,
        matches: Default::default(),
        url: None,
        text: None,
        steps: vec![
            step("s1", StepKind::Manual, "Check the dashboard", None, false),
            step(
                "s2",
                StepKind::Action,
                "Retry workflow",
                Some("retry_workflow"),
                false,
            ),
            step(
                "s3",
                StepKind::Action,
                "Escalate to another team",
                Some("escalate"),
                true,
            ),
        ],
    }
}

async fn attach_toy(pool: &PgPool, alert: Uuid) {
    let rb = toy_runbook();
    let tctx = runbook::template_context(&json!({}), &demo_alert(alert));
    let steps = runbook::render(&rb, &tctx, &ActionRegistry::risk_of);
    let mut tx = pool.begin().await.expect("tx");
    assert!(
        runbook::attach(&mut tx, alert, &rb, &steps, &tctx)
            .await
            .expect("attach")
    );
    tx.commit().await.expect("commit");
}

/// A channel that records the body of every rendered message (the worker's `render` output).
#[derive(Default)]
struct RecordingChannel {
    bodies: std::sync::Mutex<Vec<String>>,
}

impl Channel for RecordingChannel {
    fn name(&self) -> &'static str {
        "email"
    }

    fn send<'a>(&'a self, msg: &'a OutboundMessage) -> BoxFuture<'a, Result<Receipt, SendError>> {
        self.bodies.lock().unwrap().push(msg.body.clone());
        Box::pin(async move {
            Ok(Receipt {
                provider_message_id: format!("<{}@rec>", msg.job_id),
                detail: json!({ "recorded": true }),
            })
        })
    }
}

async fn job_row_status(pool: &PgPool, job: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM delivery_jobs WHERE id = $1")
        .bind(job)
        .fetch_one(pool)
        .await
        .expect("job row")
}

// ---------------------------------------------------------------------------------------------
// Seed library
// ---------------------------------------------------------------------------------------------

#[test]
fn seed_runbooks_load() {
    let d = seed_dir();
    let lib = &d.runbooks;

    for (id, format) in [
        ("settlement-timeout", Format::Yaml),
        ("payments-high-error-rate", Format::Markdown),
        ("pricing-latency", Format::Text),
        ("trade-rejected", Format::Link),
    ] {
        let rb = lib
            .get(id)
            .unwrap_or_else(|| panic!("seed runbook {id} missing"));
        assert_eq!(rb.format, format, "{id}");
        assert!(rb.revision > 0, "{id}");
    }

    let settlements = lib.get("settlement-timeout").unwrap();
    assert_eq!(settlements.team.as_deref(), Some("settlements"));
    assert_eq!(settlements.steps.len(), 3);
    assert_eq!(
        settlements.steps[2].condition,
        Some(runbook::Condition::AfterFailures {
            action: "retry_workflow".into(),
            count: 2
        })
    );

    let payments = lib.get("payments-high-error-rate").unwrap();
    assert_eq!(payments.team.as_deref(), Some("payments"));
    assert_eq!(payments.steps.len(), 4);
    assert_eq!(payments.title, "HighErrorRate on payments-api");

    let pricing = lib.get("pricing-latency").unwrap();
    assert_eq!(pricing.team.as_deref(), Some("pricing"));
    assert_eq!(pricing.steps.len(), 1);
    assert_eq!(pricing.steps[0].text, "Follow the instructions below");
    assert!(
        pricing
            .text
            .as_deref()
            .is_some_and(|t| t.starts_with("When pricing latency"))
    );

    let link = lib.get("trade-rejected").unwrap();
    assert_eq!(link.team.as_deref(), Some("rates"));
    assert_eq!(
        link.url.as_deref(),
        Some("https://wiki.example.test/rates/trade-rejected")
    );
    assert!(link.steps.is_empty());

    // The escalate target is a real team with a real member.
    let gina = d.expand("team:counterparty-support").expect("expand team");
    assert_eq!(gina.len(), 1);
    assert_eq!(gina[0].id, "gina");
    assert_eq!(gina[0].contact.email.as_deref(), Some("gina@example.test"));
    assert_eq!(gina[0].contact.phone.as_deref(), Some("+44 7700 900107"));

    // `GET /api/runbooks` items carry every contract key.
    let items = runbook::list_items(lib);
    assert_eq!(items.len(), 4);
    for item in &items {
        for key in ["id", "title", "format", "team", "steps"] {
            assert!(item.get(key).is_some(), "item missing {key}");
        }
    }
    assert!(items.iter().any(|i| i["id"] == json!("settlement-timeout")
        && i["steps"] == json!(3)
        && i["team"] == json!("settlements")));

    // Revisions are stable across loads.
    let again = seed_dir();
    assert_eq!(
        again.runbooks.get("settlement-timeout").unwrap().revision,
        settlements.revision
    );
}

#[test]
fn seed_selection_and_rendering() {
    let lib = &seed_dir().runbooks;

    // Team scope: the demo event matches `settlement-timeout` by `fields.type` + `raw.error.code`.
    let tctx = runbook::template_context(
        &json!({"event": demo_event(), "context": {"service": "order-settlement"}}),
        &demo_alert(Uuid::new_v4()),
    );
    let rb = lib.select(None, "settlements", &tctx).expect("selected");
    assert_eq!(rb.id, "settlement-timeout");

    // A different error code does not match it, and no other settlements runbook exists.
    let other = json!({
        "raw": {"workflowId": "WF-2", "error": {"code": "INTERNAL"}},
        "fields": {"type": "WORKFLOW_FAILED"},
    });
    let tctx = runbook::template_context(&json!({"event": other}), &demo_alert(Uuid::new_v4()));
    assert!(lib.select(None, "settlements", &tctx).is_none());

    // An explicit rule binding wins regardless of team or matches.
    let rb = lib
        .select(Some("trade-rejected"), "settlements", &json!({}))
        .expect("rule-bound");
    assert_eq!(rb.id, "trade-rejected");

    // Rendering the demo event fills `{{workflowId}}` from `event.raw`.
    let rb = lib.get("settlement-timeout").unwrap();
    let tctx = runbook::template_context(
        &json!({"event": demo_event(), "context": {"service": "order-settlement"}}),
        &demo_alert(Uuid::new_v4()),
    );
    let steps = runbook::render(rb, &tctx, &ActionRegistry::risk_of);
    assert_eq!(steps.len(), 3);
    assert!(steps.iter().all(|s| s.render_error.is_none()));
    assert_eq!(
        steps[1].action.as_ref().unwrap()["params"],
        json!({"workflowId": "WF-1"})
    );
    assert_eq!(steps[2].approval, "required"); // risk high + approval required
    assert_eq!(steps[0].approval, "none");
}

// ---------------------------------------------------------------------------------------------
// Router freezing
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn router_freezes_runbook_once_per_alert(pool: PgPool) {
    let dir = seed_dir();
    let alert = insert_alert_as(
        &pool,
        "open",
        "critical",
        "settlements",
        "settlements-workflow-failed",
    )
    .await;
    notify_settlements(&pool, alert, 1, "new", Some(demo_event())).await;

    let stats = route_once(&pool, &dir, &RouterConfig::default())
        .await
        .expect("route_once");
    assert_eq!(stats.runbooks, 1, "{stats:?}");

    let (runbook_id, revision, title, format, context): (String, i32, String, String, Value) =
        sqlx::query_as(
            "SELECT runbook_id, revision, title, format, context \
             FROM alert_runbooks WHERE alert_id = $1",
        )
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("runbook row");
    assert_eq!(runbook_id, "settlement-timeout");
    assert_eq!(title, "settlement-timeout");
    assert_eq!(format, "yaml");
    assert!(revision > 0);
    // The pinned context is the notify payload's event.
    assert_eq!(context["raw"]["workflowId"], "WF-1");
    assert_eq!(context["fields"]["type"], "WORKFLOW_FAILED");
    assert_eq!(context["alert"]["id"], alert.to_string());

    let steps = frozen_steps(&pool, alert).await;
    assert_eq!(steps.len(), 3);
    assert_eq!(
        steps[1].action.as_ref().unwrap()["params"],
        json!({"workflowId": "WF-1"})
    );
    assert!(steps[1].render_error.is_none());
    assert_eq!(steps[0].action.as_ref().unwrap()["name"], json!("http_get"));
    assert_eq!(steps[2].approval, "required");

    // A renotify does not freeze a second copy.
    notify_settlements(&pool, alert, 2, "renotify", Some(demo_event())).await;
    let stats = route_once(&pool, &dir, &RouterConfig::default())
        .await
        .expect("route renotify");
    assert_eq!(stats.runbooks, 0, "{stats:?}");

    // Even a duplicate `new` cannot: one frozen runbook per alert.
    notify_settlements(&pool, alert, 3, "new", Some(demo_event())).await;
    let stats = route_once(&pool, &dir, &RouterConfig::default())
        .await
        .expect("route duplicate new");
    assert_eq!(stats.runbooks, 0, "{stats:?}");
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM alert_runbooks WHERE alert_id = $1")
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("count rows");
    assert_eq!(rows, 1);
    let steps2 = frozen_steps(&pool, alert).await;
    assert_eq!(steps2, steps, "the frozen snapshot must not change");
}

#[sqlx::test(migrations = "../../migrations")]
async fn router_without_event_freezes_render_error(pool: PgPool) {
    let dir = seed_dir();
    let alert = insert_alert_as(
        &pool,
        "open",
        "critical",
        "settlements",
        "settlements-workflow-failed",
    )
    .await;
    // An older producer: no event in the payload.
    notify_settlements(&pool, alert, 1, "new", None).await;
    let stats = route_once(&pool, &dir, &RouterConfig::default())
        .await
        .expect("route_once");
    assert_eq!(stats.runbooks, 1, "{stats:?}");

    let steps = frozen_steps(&pool, alert).await;
    assert_eq!(steps.len(), 3);
    let s2 = &steps[1];
    assert_eq!(
        s2.render_error.as_deref(),
        Some("missing value for workflowId")
    );
    // The raw template text is kept, not dropped.
    assert_eq!(
        s2.action.as_ref().unwrap()["params"]["workflowId"],
        json!("{{workflowId}}")
    );
    // The other steps rendered fine.
    assert!(steps[0].render_error.is_none());
    assert!(steps[2].render_error.is_none());
}

#[sqlx::test(migrations = "../../migrations")]
async fn router_selects_team_scoped_markdown_runbook(pool: PgPool) {
    let dir = seed_dir();
    // A rule without a `runbook:` binding (not in config/rules): team scope must pick the
    // payments Markdown page via `matches: {type: HighErrorRate}`.
    let alert = insert_alert_as(&pool, "open", "critical", "payments", "payments-custom").await;
    insert_notify_event(
        &pool,
        alert,
        1,
        "payments-custom",
        "payments",
        "critical",
        "new",
        "team:payments",
        Some(json!({
            "raw": {"annotations": {"summary": "5xx rate is 12%"}},
            "fields": {"type": "HighErrorRate", "service": "payments-api", "region": "eu"},
        })),
        None,
    )
    .await;
    let stats = route_once(&pool, &dir, &RouterConfig::default())
        .await
        .expect("route_once");
    assert_eq!(stats.runbooks, 1, "{stats:?}");

    let (runbook_id, format, text): (String, String, Option<String>) =
        sqlx::query_as("SELECT runbook_id, format, text FROM alert_runbooks WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("runbook row");
    assert_eq!(runbook_id, "payments-high-error-rate");
    assert_eq!(format, "markdown");
    assert!(
        text.as_deref()
            .is_some_and(|t| t.starts_with("# HighErrorRate on payments-api")),
        "the Markdown page is kept verbatim: {text:?}"
    );
    let steps = frozen_steps(&pool, alert).await;
    assert_eq!(steps.len(), 4);
    assert_eq!(
        steps[1].hint.as_deref(),
        Some("Needs approval (shown as guidance, not enforced)")
    );
}

// ---------------------------------------------------------------------------------------------
// Email section
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn email_section_lists_steps_and_worker_sends_it(pool: PgPool) {
    let dir = seed_dir();
    let alert = insert_alert_as(
        &pool,
        "open",
        "critical",
        "settlements",
        "settlements-workflow-failed",
    )
    .await;
    let rb = dir.runbooks.get("settlement-timeout").expect("runbook");
    let tctx = runbook::template_context(
        &json!({"event": {"raw": {"workflowId": "WF-77"}}}),
        &demo_alert(alert),
    );
    let steps = runbook::render(rb, &tctx, &ActionRegistry::risk_of);
    let mut tx = pool.begin().await.expect("tx");
    assert!(
        runbook::attach(&mut tx, alert, rb, &steps, &tctx)
            .await
            .expect("attach")
    );
    tx.commit().await.expect("commit");

    let section = runbook::email_section(&pool, alert)
        .await
        .expect("email_section")
        .expect("the alert has a runbook");
    assert!(
        section.starts_with("Runbook: settlement-timeout"),
        "{section}"
    );
    assert!(
        section.contains("1. Is counterparty service healthy?"),
        "{section}"
    );
    assert!(section.contains("2. Retry workflow"), "{section}");
    assert!(
        section.contains("3. Escalate to another team [needs approval]"),
        "{section}"
    );

    // An alert without a runbook gets no section.
    let bare = insert_alert_as(&pool, "open", "warning", "rates", "rates-rejected-trades").await;
    assert!(
        runbook::email_section(&pool, bare)
            .await
            .expect("email_section")
            .is_none()
    );

    // The channel worker appends the section to the email body before rendering.
    let job = common::insert_job(&pool, alert, "carol", "carol@example.test").await;
    let recorder = Arc::new(RecordingChannel::default());
    let channel: Arc<dyn Channel> = recorder.clone();
    let cfg = common::worker_cfg("runbook-email-worker");
    worker::work_once(&pool, &channel, &cfg)
        .await
        .expect("work_once");
    assert_eq!(job_row_status(&pool, job).await, "sent");
    let bodies = recorder.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 1, "exactly one email was rendered");
    assert!(
        bodies[0].contains("Runbook: settlement-timeout"),
        "{}",
        bodies[0]
    );
    assert!(
        bodies[0].contains("1. Is counterparty service healthy?"),
        "{}",
        bodies[0]
    );

    // A failed render is visible in the email.
    let broken = insert_alert_as(
        &pool,
        "open",
        "critical",
        "settlements",
        "settlements-workflow-failed",
    )
    .await;
    let tctx = runbook::template_context(&json!({}), &demo_alert(broken));
    let steps = runbook::render(rb, &tctx, &ActionRegistry::risk_of);
    let mut tx = pool.begin().await.expect("tx");
    assert!(
        runbook::attach(&mut tx, broken, rb, &steps, &tctx)
            .await
            .expect("attach")
    );
    tx.commit().await.expect("commit");
    let section = runbook::email_section(&pool, broken)
        .await
        .expect("email_section")
        .expect("Some");
    assert!(
        section.contains("2. Retry workflow [disabled: missing value for workflowId]"),
        "{section}"
    );
}

// ---------------------------------------------------------------------------------------------
// RunbookView
// ---------------------------------------------------------------------------------------------

const STEP_KEYS: [&str; 12] = [
    "id",
    "kind",
    "text",
    "hint",
    "action",
    "approval",
    "state",
    "disabled_reason",
    "done_by",
    "done_at",
    "runs",
    "approval_request",
];

fn step_of<'a>(view: &'a Value, id: &str) -> &'a Value {
    view["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["id"] == json!(id))
        .unwrap_or_else(|| panic!("step {id} not in view"))
}

#[sqlx::test(migrations = "../../migrations")]
async fn runbook_view_has_every_contract_key(pool: PgPool) {
    let alert = insert_alert_as(
        &pool,
        "open",
        "critical",
        "settlements",
        "settlements-workflow-failed",
    )
    .await;
    attach_toy(&pool, alert).await;

    let view = runbook::runbook_view(&pool, alert, "user:carol")
        .await
        .expect("view")
        .expect("Some");
    for key in ["id", "revision", "title", "format", "url", "text", "steps"] {
        assert!(view.get(key).is_some(), "RunbookView missing {key}");
    }
    assert_eq!(view["id"], json!("toy"));
    assert_eq!(view["revision"], json!(7));
    assert_eq!(view["title"], json!("Toy runbook"));
    assert_eq!(view["format"], json!("yaml"));
    assert_eq!(view["url"], Value::Null);
    assert_eq!(view["text"], Value::Null);

    for step in view["steps"].as_array().unwrap() {
        for key in STEP_KEYS {
            assert!(step.get(key).is_some(), "RunbookStep missing {key}");
        }
    }
    // Nothing has happened yet: idle everywhere, no runs, no approvals, no marks.
    for id in ["s1", "s2", "s3"] {
        let s = step_of(&view, id);
        assert_eq!(s["state"], json!("idle"), "{id}");
        assert_eq!(s["runs"], json!([]), "{id}");
        assert_eq!(s["approval_request"], Value::Null, "{id}");
        assert_eq!(s["done_by"], Value::Null, "{id}");
        assert_eq!(s["done_at"], Value::Null, "{id}");
        assert_eq!(s["disabled_reason"], Value::Null, "{id}");
    }
    assert_eq!(step_of(&view, "s1")["kind"], json!("manual"));
    let s3 = step_of(&view, "s3");
    assert_eq!(s3["kind"], json!("action"));
    assert_eq!(s3["approval"], json!("required"));
    assert_eq!(s3["action"]["name"], json!("escalate"));
    assert_eq!(s3["action"]["risk"], json!("high"));
    assert_eq!(s3["action"]["label"], json!("Escalate to another team"));

    // A different alert without a runbook has no view at all.
    let other = insert_alert_as(&pool, "open", "warning", "rates", "rates-rejected-trades").await;
    assert!(
        runbook::runbook_view(&pool, other, "user:carol")
            .await
            .expect("view")
            .is_none()
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn runbook_view_states_from_executions_marks_and_approvals(pool: PgPool) {
    let alert = insert_alert_as(
        &pool,
        "open",
        "critical",
        "settlements",
        "settlements-workflow-failed",
    )
    .await;
    attach_toy(&pool, alert).await;

    // s1 ticked in the UI, s2 queued by a run, s3 waiting on an approval.
    insert_mark(&pool, alert, "s1", "user:carol").await;
    let exec = insert_execution(
        &pool,
        alert,
        "s2",
        "retry_workflow",
        "queued",
        "user:bob",
        10.0,
    )
    .await;
    let approval = insert_approval(
        &pool,
        alert,
        "s3",
        "escalate",
        "pending",
        &["user:carol", "user:bob"],
        "user:bob",
    )
    .await;

    let view = runbook::runbook_view(&pool, alert, "user:carol")
        .await
        .expect("view")
        .expect("Some");
    let s1 = step_of(&view, "s1");
    assert_eq!(s1["state"], json!("done"));
    assert_eq!(s1["done_by"], json!("user:carol"));
    assert!(s1["done_at"].as_str().is_some_and(|t| t.ends_with('Z')));

    let s2 = step_of(&view, "s2");
    assert_eq!(s2["state"], json!("queued"));
    assert_eq!(s2["runs"].as_array().unwrap().len(), 1);
    let run = &s2["runs"][0];
    for key in [
        "id",
        "state",
        "requested_by",
        "started_at",
        "finished_at",
        "output",
    ] {
        assert!(run.get(key).is_some(), "Execution missing {key}");
    }
    assert_eq!(run["id"], json!(exec.to_string()));
    assert_eq!(run["state"], json!("queued"));
    assert_eq!(run["requested_by"], json!("user:bob"));
    assert_eq!(run["started_at"], Value::Null);

    let s3 = step_of(&view, "s3");
    assert_eq!(s3["state"], json!("awaiting_approval"));
    let req = &s3["approval_request"];
    for key in [
        "id",
        "state",
        "requested_by",
        "requested_at",
        "expires_at",
        "eligible",
        "can_decide",
        "sms_code",
        "decided_by",
        "decided_at",
        "note",
    ] {
        assert!(req.get(key).is_some(), "ApprovalRequest missing {key}");
    }
    assert_eq!(req["id"], json!(approval.to_string()));
    assert_eq!(req["state"], json!("pending"));
    assert_eq!(req["eligible"], json!(["user:carol", "user:bob"]));
    // Carol is eligible and it is pending; the code is never exposed.
    assert_eq!(req["can_decide"], json!(true));
    assert_eq!(req["sms_code"], Value::Null);

    // Someone not eligible cannot decide.
    let view = runbook::runbook_view(&pool, alert, "user:anna")
        .await
        .expect("view")
        .expect("Some");
    assert_eq!(
        step_of(&view, "s3")["approval_request"]["can_decide"],
        json!(false)
    );

    // A rejected approval shows on the step.
    let rejected_alert = insert_alert_as(
        &pool,
        "open",
        "critical",
        "settlements",
        "settlements-workflow-failed",
    )
    .await;
    attach_toy(&pool, rejected_alert).await;
    insert_approval(
        &pool,
        rejected_alert,
        "s3",
        "escalate",
        "rejected",
        &["user:carol"],
        "user:bob",
    )
    .await;
    let view = runbook::runbook_view(&pool, rejected_alert, "user:carol")
        .await
        .expect("view")
        .expect("Some");
    let s3 = step_of(&view, "s3");
    assert_eq!(s3["state"], json!("rejected"));
    assert_eq!(s3["approval_request"]["state"], json!("rejected"));
    assert_eq!(s3["approval_request"]["decided_by"], json!("user:bob"));
    assert_eq!(s3["approval_request"]["can_decide"], json!(false));

    // The latest execution decides the state; runs are oldest first.
    let retry_alert = insert_alert_as(
        &pool,
        "open",
        "critical",
        "settlements",
        "settlements-workflow-failed",
    )
    .await;
    attach_toy(&pool, retry_alert).await;
    insert_execution(
        &pool,
        retry_alert,
        "s2",
        "retry_workflow",
        "failed",
        "user:bob",
        30.0,
    )
    .await;
    let ok = insert_execution(
        &pool,
        retry_alert,
        "s2",
        "retry_workflow",
        "ok",
        "user:bob",
        5.0,
    )
    .await;
    let view = runbook::runbook_view(&pool, retry_alert, "user:carol")
        .await
        .expect("view")
        .expect("Some");
    let s2 = step_of(&view, "s2");
    assert_eq!(s2["state"], json!("ok"), "the newest execution wins");
    let runs = s2["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0]["state"], json!("failed"), "oldest first");
    assert_eq!(runs[1]["state"], json!("ok"));
    assert_eq!(runs[1]["id"], json!(ok.to_string()));
    assert_eq!(runs[1]["finished_at"], Value::Null);
}

#[sqlx::test(migrations = "../../migrations")]
async fn runbook_view_disables_action_steps_when_resolved(pool: PgPool) {
    let alert = insert_alert_as(
        &pool,
        "open",
        "critical",
        "settlements",
        "settlements-workflow-failed",
    )
    .await;
    attach_toy(&pool, alert).await;
    common::update_alert_status(&pool, alert, "resolved").await;

    let view = runbook::runbook_view(&pool, alert, "user:carol")
        .await
        .expect("view")
        .expect("Some");
    for id in ["s2", "s3"] {
        assert_eq!(
            step_of(&view, id)["disabled_reason"],
            json!("Alert is resolved"),
            "{id}"
        );
    }
    // Manual steps are not disabled by resolution.
    assert_eq!(step_of(&view, "s1")["disabled_reason"], Value::Null);

    // A ticked manual step still reads done.
    insert_mark(&pool, alert, "s1", "user:carol").await;
    let view = runbook::runbook_view(&pool, alert, "user:carol")
        .await
        .expect("view")
        .expect("Some");
    assert_eq!(step_of(&view, "s1")["state"], json!("done"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn runbook_view_gates_condition_steps_on_failures(pool: PgPool) {
    // The full Appendix B runbook, frozen the way the router does it.
    let dir = seed_dir();
    let alert = insert_alert_as(
        &pool,
        "open",
        "critical",
        "settlements",
        "settlements-workflow-failed",
    )
    .await;
    notify_settlements(&pool, alert, 1, "new", Some(demo_event())).await;
    let stats = route_once(&pool, &dir, &RouterConfig::default())
        .await
        .expect("route_once");
    assert_eq!(stats.runbooks, 1, "{stats:?}");

    // No retry has failed yet: the escalate step is gated with a reason.
    let view = runbook::runbook_view(&pool, alert, "user:carol")
        .await
        .expect("view")
        .expect("Some");
    let reason = step_of(&view, "s3")["disabled_reason"]
        .as_str()
        .expect("gated")
        .to_string();
    assert!(
        reason.contains("failed retries") && reason.contains("(0 so far)"),
        "unexpected reason {reason:?}"
    );

    // One failure: "1 so far".
    insert_execution(
        &pool,
        alert,
        "s2",
        "retry_workflow",
        "failed",
        "user:bob",
        5.0,
    )
    .await;
    let view = runbook::runbook_view(&pool, alert, "user:carol")
        .await
        .expect("view")
        .expect("Some");
    assert_eq!(
        step_of(&view, "s3")["disabled_reason"],
        json!("Available after 2 failed retries (1 so far)")
    );

    // Two failures: the condition is met and the step becomes runnable.
    insert_execution(
        &pool,
        alert,
        "s2",
        "retry_workflow",
        "failed",
        "user:bob",
        1.0,
    )
    .await;
    let view = runbook::runbook_view(&pool, alert, "user:carol")
        .await
        .expect("view")
        .expect("Some");
    let s3 = step_of(&view, "s3");
    assert_eq!(s3["disabled_reason"], Value::Null);
    assert_eq!(s3["state"], json!("idle"));
}
