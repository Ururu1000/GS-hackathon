//! Runtime runbook import (the backend of the UI's Runbooks tab, contract
//! `previewRunbookImport` / `publishRunbook`): preview per format, validation and team
//! authorization, publish + read-back through the effective library, id conflicts, the
//! live pickup by the router (no restart), and the lenient apply of a stored row that
//! no longer validates.
//!
//! Needs Postgres (`DATABASE_URL`; sqlx creates a throwaway database per test). The
//! router is driven in-process with `tower::ServiceExt::oneshot`; the middleware layer
//! inserting the `Actor` extension stands in for beacon-api's session layer.
//!
//! Run: `cargo test -p beacon-delivery --test runbook_import -- --nocapture`

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::{Method, StatusCode, header};
use beacon_delivery::RouterConfig;
use beacon_delivery::actions::ActionRegistry;
use beacon_delivery::alert_ops::SqlAlertOps;
use beacon_delivery::api::{self, Actor, ApiState};
use beacon_delivery::config::{Contact, Directory, Person, Team};
use beacon_delivery::overrides::EffectiveDirectory;
use beacon_delivery::router::route_once;
use beacon_delivery::runbook::{Format, Runbook, StepDef, StepKind};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

const JSON: &str = "application/json";

// ---------------------------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------------------------

fn person(id: &str, email: &str) -> Person {
    Person {
        id: id.to_string(),
        name: None,
        contact: Contact {
            email: Some(email.to_string()),
            phone: None,
        },
        preferences: None,
        rules: Vec::new(),
    }
}

/// Two teams: `settlements` (bob a member, carol the lead) and `pricing` (dave only,
/// no lead). The base library has one YAML runbook, `settlement-timeout`, standing in
/// for the seed `config/runbooks` (its id may not be published over).
fn fixture_dir() -> Directory {
    let mut dir = Directory::from_parts(
        vec![
            person("bob", "bob@example.test"),
            person("carol", "carol@example.test"),
            person("dave", "dave@example.test"),
        ],
        vec![
            Team {
                id: "settlements".into(),
                members: vec!["bob".into(), "carol".into()],
                lead: Some("carol".into()),
                oncall: HashMap::new(),
                default_channels: None,
            },
            Team {
                id: "pricing".into(),
                members: vec!["dave".into()],
                lead: None,
                oncall: HashMap::new(),
                default_channels: None,
            },
        ],
    )
    .expect("fixture directory builds");
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
        steps: vec![StepDef {
            id: "s1".into(),
            kind: StepKind::Manual,
            text: "Base step".into(),
            hint: None,
            action: None,
            params: Default::default(),
            approval_required: false,
            condition: None,
            resolves: false,
        }],
    }];
    dir
}

fn api_state(pool: &PgPool) -> ApiState {
    ApiState {
        pool: pool.clone(),
        dir: Arc::new(fixture_dir()),
        registry: ActionRegistry::default(),
        alert_ops: Arc::new(SqlAlertOps),
        reply_secret: None,
        email_link: None,
    }
}

// ---------------------------------------------------------------------------------------------
// HTTP plumbing (in-process, the pattern of tests/people_api_db.rs' session layer)
// ---------------------------------------------------------------------------------------------

/// `api::router` + a middleware layer inserting `Extension(Actor)` (when `actor` is
/// given), standing in for beacon-api's session layer.
fn app_as(state: &ApiState, actor: Option<&str>) -> axum::Router {
    let actor = actor.map(str::to_string);
    api::router(state.clone()).layer(axum::middleware::from_fn(
        move |mut req: axum::extract::Request, next: axum::middleware::Next| {
            let actor = actor.clone();
            async move {
                if let Some(actor) = actor {
                    req.extensions_mut().insert(Actor(actor));
                }
                next.run(req).await
            }
        },
    ))
}

/// `method uri` with `body` as the given media type, in-process.
async fn send(
    app: &axum::Router,
    method: Method,
    uri: &str,
    body: &Value,
    content_type: &str,
) -> (StatusCode, Value) {
    let resp = tower::ServiceExt::oneshot(
        app.clone(),
        axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, content_type)
            .body(axum::body::Body::from(body.to_string()))
            .expect("request builds"),
    )
    .await
    .expect("router is infallible");
    read(resp).await
}

async fn post(app: &axum::Router, uri: &str, body: &Value) -> (StatusCode, Value) {
    send(app, Method::POST, uri, body, JSON).await
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let resp = tower::ServiceExt::oneshot(
        app.clone(),
        axum::http::Request::builder()
            .uri(uri)
            .body(axum::body::Body::empty())
            .expect("request builds"),
    )
    .await
    .expect("router is infallible");
    read(resp).await
}

async fn read(resp: axum::response::Response) -> (StatusCode, Value) {
    let status = resp.status();
    let bytes = http_body_util::BodyExt::collect(resp.into_body())
        .await
        .expect("body collects")
        .to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

/// The `{items}` entry with this id.
fn find<'a>(body: &'a Value, id: &str) -> &'a Value {
    body["items"]
        .as_array()
        .expect("items array")
        .iter()
        .find(|i| i["id"] == id)
        .unwrap_or_else(|| panic!("no item {id} in {body}"))
}

fn steps_of(body: &Value) -> &Vec<Value> {
    body["steps"].as_array().expect("steps array")
}

/// A minimal yaml import (steps with actions and a gated escalate), the shape the
/// UI's "Structured YAML" field submits.
fn yaml_draft(title: &str) -> Value {
    json!({
        "format": "yaml",
        "team": "settlements",
        "title": title,
        "content": "matches:\n  type: WORKFLOW_FAILED\nsteps:\n  \
             - check: Is the counterparty healthy?\n    action: http_get\n    \
             url: http://counterparty-service/health\n  - action: retry_workflow\n    \
             workflowId: \"{{workflowId}}\"\n  - if: \"retry fails twice\"\n    \
             action: escalate\n    team: counterparty-support\n    approval: required\n",
    })
}

/// The `{...draft, ...preview}` publish body (what `publishRunbook` posts).
async fn publish_body(app: &axum::Router, draft: &Value) -> Value {
    let (status, preview) = post(app, "/api/runbooks/import", draft).await;
    assert_eq!(status, StatusCode::OK, "{preview}");
    let mut publish = draft.clone();
    publish["id"] = preview["id"].clone();
    publish["title"] = preview["title"].clone();
    publish["steps"] = preview["steps"].clone();
    publish
}

// ---------------------------------------------------------------------------------------------
// a) Preview: each format parses through its file adapter
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn preview_parses_each_format(pool: PgPool) {
    let state = api_state(&pool);
    let app = app_as(&state, Some("user:bob"));

    // YAML: actions with params, a gated escalate, the id derived from the title.
    let (status, body) = post(
        &app,
        "/api/runbooks/import",
        &yaml_draft("Imported failure!"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["id"], json!("imported-failure"));
    assert_eq!(body["title"], json!("Imported failure!"));
    let steps = steps_of(&body);
    assert_eq!(steps.len(), 3);
    assert_eq!(steps[0]["kind"], json!("action"));
    assert_eq!(steps[0]["id"], json!("s1"));
    assert_eq!(steps[0]["action"]["name"], json!("http_get"));
    assert_eq!(steps[0]["action"]["label"], json!("HTTP GET request"));
    assert_eq!(steps[0]["action"]["risk"], json!("low"));
    assert_eq!(
        steps[0]["action"]["params"]["url"],
        json!("http://counterparty-service/health")
    );
    assert_eq!(steps[0]["approval"], json!("none"));
    assert_eq!(
        steps[1]["action"]["params"]["workflowId"],
        json!("{{workflowId}}")
    );
    // The escalate step: high risk + `approval: required`, gated on the retry step.
    assert_eq!(steps[2]["action"]["name"], json!("escalate"));
    assert_eq!(steps[2]["approval"], json!("required"));
    assert_eq!(
        steps[2]["after_failures"],
        json!({ "step": "s2", "count": 2 })
    );

    // Markdown checklist: every list item a manual step, "needs approval" hinted.
    let (status, body) = post(
        &app,
        "/api/runbooks/import",
        &json!({
            "format": "markdown", "team": "settlements", "title": "Imported checklist",
            "content": "# Page\n1. Open the dashboard\n2. Fail traffic over (needs approval)\nplain text is not a step\n",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["id"], json!("imported-checklist"));
    let steps = steps_of(&body);
    assert_eq!(steps.len(), 2);
    assert!(steps.iter().all(|s| s["kind"] == json!("manual")));
    assert_eq!(steps[0]["text"], json!("Open the dashboard"));
    assert_eq!(
        steps[1]["hint"],
        json!("Needs approval (shown as guidance, not enforced)")
    );

    // Text: the file adapter's single follow-the-instructions step.
    let (status, body) = post(
        &app,
        "/api/runbooks/import",
        &json!({
            "format": "text", "team": "settlements", "title": "Pricing latency import",
            "content": "When pricing latency is high, it is usually the cache.",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let steps = steps_of(&body);
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0]["kind"], json!("manual"));
    assert_eq!(steps[0]["text"], json!("Follow the instructions below"));

    // Link: no steps at all.
    let (status, body) = post(
        &app,
        "/api/runbooks/import",
        &json!({
            "format": "link", "team": "settlements", "title": "Wiki import",
            "content": "https://wiki.example.test/settlements/imported",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(steps_of(&body).len(), 0);

    // A preview stores nothing.
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM directory_overrides")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(rows, 0);
}

// ---------------------------------------------------------------------------------------------
// b) Invalid input: 422 `invalid` with a message
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn invalid_imports_are_422_with_a_message(pool: PgPool) {
    let state = api_state(&pool);
    let app = app_as(&state, Some("user:bob"));

    for (name, draft) in [
        (
            "bad format",
            json!({ "format": "docx", "team": "settlements", "title": "T", "content": "x" }),
        ),
        ("empty title", yaml_draft("   ")),
        ("long title", {
            let mut d = yaml_draft("T");
            d["title"] = json!("t".repeat(121));
            d
        }),
        ("unknown action", {
            let mut d = yaml_draft("T");
            d["content"] = json!("steps:\n  - action: detonate\n");
            d
        }),
        ("unparsable yaml", {
            let mut d = yaml_draft("T");
            d["content"] = json!("steps: [::");
            d
        }),
        ("empty content", {
            let mut d = yaml_draft("T");
            d["content"] = json!("   ");
            d
        }),
        ("missing team", {
            let mut d = yaml_draft("T");
            d.as_object_mut().unwrap().remove("team");
            d
        }),
        ("bad link url", {
            let mut d = yaml_draft("T");
            d["format"] = json!("link");
            d["content"] = json!("ftp://wiki.test/x");
            d
        }),
    ] {
        let (status, body) = post(&app, "/api/runbooks/import", &draft).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{name}: {body}");
        assert_eq!(body["error"]["code"], json!("invalid"), "{name}: {body}");
        assert!(
            body["error"]["message"]
                .as_str()
                .is_some_and(|m| !m.is_empty()),
            "{name}: {body}"
        );
    }

    // The publish route repeats the checks and adds the submitted steps' validation.
    let draft = yaml_draft("Invalid publish");
    let mut publish = publish_body(&app.clone(), &draft).await;
    for (name, broken) in [
        ("missing steps", {
            publish.as_object_mut().unwrap().remove("steps");
            publish.clone()
        }),
        ("empty steps", {
            let mut p = publish.clone();
            p["steps"] = json!([]);
            p
        }),
        ("unknown action in a submitted step", {
            let mut p = publish.clone();
            p["steps"] = json!([{
                "id": "s1", "kind": "action", "text": "Boom",
                "action": { "name": "detonate", "params": {} }
            }]);
            p
        }),
        ("duplicate step ids", {
            let mut p = publish.clone();
            p["steps"] = json!([
                { "id": "s1", "kind": "manual", "text": "a" },
                { "id": "s1", "kind": "manual", "text": "b" },
            ]);
            p
        }),
    ] {
        let (status, body) = post(&app, "/api/runbooks", &broken).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{name}: {body}");
        assert_eq!(body["error"]["code"], json!("invalid"), "{name}: {body}");
    }
    // A link import may not carry steps.
    let mut link_publish = publish_body(
        &app,
        &json!({
            "format": "link", "team": "settlements", "title": "Link with steps",
            "content": "https://wiki.example.test/x",
        }),
    )
    .await;
    link_publish["steps"] = json!([{ "id": "s1", "kind": "manual", "text": "x" }]);
    let (status, body) = post(&app, "/api/runbooks", &link_publish).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

// ---------------------------------------------------------------------------------------------
// c) Authorization: 401 without a session, 403 for another team (before parsing)
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn imports_need_the_actors_own_team(pool: PgPool) {
    let state = api_state(&pool);

    // No session Actor: 401, like every other write.
    let app = app_as(&state, None);
    let (status, body) = post(&app, "/api/runbooks/import", &yaml_draft("No session")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(body["error"]["code"], json!("unauthorized"));
    let (status, body) = post(&app, "/api/runbooks", &yaml_draft("No session")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");

    // Wrong content type: 415 (audit #11 — every write must say application/json).
    let app = app_as(&state, Some("user:bob"));
    let (status, body) = send(
        &app,
        Method::POST,
        "/api/runbooks/import",
        &yaml_draft("T"),
        "text/plain",
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{body}");
    assert_eq!(body["error"]["code"], json!("unsupported_media_type"));

    // A non-member gets 403 BEFORE the content is parsed (mock PR #14: this body is
    // not even valid YAML).
    let (status, body) = post(
        &app,
        "/api/runbooks/import",
        &json!({
            "format": "yaml", "team": "pricing", "title": "Foreign", "content": "::: not yaml"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["code"], json!("forbidden"));
    let (status, body) = post(
        &app,
        "/api/runbooks",
        &json!({
            "format": "yaml", "team": "pricing", "title": "Foreign", "content": "::: not yaml",
            "id": "foreign", "steps": [{ "id": "s1", "kind": "manual", "text": "x" }],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    // An unknown team authorizes nobody.
    let (status, body) = post(&app, "/api/runbooks/import", &{
        let mut d = yaml_draft("Ghost team");
        d["team"] = json!("ghosts");
        d
    })
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    // A member (bob) and the lead (carol) may both import for their team; dave for
    // his lead-less team.
    for (actor, team) in [
        ("user:bob", "settlements"),
        ("user:carol", "settlements"),
        ("user:dave", "pricing"),
    ] {
        let app = app_as(&state, Some(actor));
        let mut draft = yaml_draft(&format!("{actor} import"));
        draft["team"] = json!(team);
        let (status, body) = post(&app, "/api/runbooks/import", &draft).await;
        assert_eq!(status, StatusCode::OK, "{actor}: {body}");
    }
}

// ---------------------------------------------------------------------------------------------
// d) Publish: 201, then the list and the detail serve it (the effective library)
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn publish_stores_the_edited_checklist_and_readers_serve_it(pool: PgPool) {
    let state = api_state(&pool);
    let app = app_as(&state, Some("user:bob"));

    // The user previews, edits the first step's text and drops the escalate step.
    let draft = yaml_draft("Live imported runbook");
    let (status, preview) = post(&app, "/api/runbooks/import", &draft).await;
    assert_eq!(status, StatusCode::OK, "{preview}");
    let mut steps = preview["steps"].clone();
    steps[0]["text"] = json!("Check the counterparty service");
    steps.as_array_mut().unwrap().remove(2);
    let mut publish = draft.clone();
    publish["id"] = preview["id"].clone();
    publish["title"] = preview["title"].clone();
    publish["steps"] = steps;

    let (status, summary) = post(&app, "/api/runbooks", &publish).await;
    assert_eq!(status, StatusCode::CREATED, "{summary}");
    assert_eq!(
        summary,
        json!({
            "id": "live-imported-runbook",
            "revision": 1,
            "title": "Live imported runbook",
            "format": "yaml",
            "team": "settlements",
            "steps": 2,
        }),
        "{summary}"
    );

    // The stored row is what the effective directory rebuilds from.
    let (kind, body, revision, updated_by): (String, Value, i64, String) = sqlx::query_as(
        "SELECT kind, body, revision, updated_by FROM directory_overrides \
         WHERE id = 'live-imported-runbook'",
    )
    .fetch_one(&pool)
    .await
    .expect("the published row");
    assert_eq!(kind, "runbook");
    assert_eq!(revision, 1);
    assert_eq!(updated_by, "user:bob");
    assert_eq!(body["format"], json!("yaml"));
    assert_eq!(body["team"], json!("settlements"));
    assert_eq!(body["content"], draft["content"]);
    assert_eq!(steps_of(&body).len(), 2);
    assert_eq!(
        body["steps"][0]["text"],
        json!("Check the counterparty service")
    );

    // GET /api/runbooks lists it (the effective library) with the row's revision; the
    // base entry keeps its own.
    let (status, list) = get(&app, "/api/runbooks").await;
    assert_eq!(status, StatusCode::OK, "{list}");
    let imported = find(&list, "live-imported-runbook");
    assert_eq!(imported["revision"], json!(1));
    assert_eq!(imported["steps"], json!(2));
    assert_eq!(find(&list, "settlement-timeout")["revision"], json!(7));

    // GET /api/runbooks/{id} returns the stored steps, action and approval.
    let (status, detail) = get(&app, "/api/runbooks/live-imported-runbook").await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail["id"], json!("live-imported-runbook"));
    assert_eq!(detail["revision"], json!(1));
    let steps = steps_of(&detail);
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0]["text"], json!("Check the counterparty service"));
    assert_eq!(steps[0]["action"]["name"], json!("http_get"));
    assert_eq!(steps[0]["approval"], json!("none"));
    assert_eq!(steps[1]["action"]["name"], json!("retry_workflow"));

    // The escalate step is gone from the row's rebuild too: a dropped step stays
    // dropped everywhere.
    let (status, missing) = get(&app, "/api/runbooks/no-such-runbook").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{missing}");
    assert_eq!(missing["error"]["code"], json!("not_found"));

    // One audit row for the publish.
    let (count,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM audit_log \
         WHERE kind = 'runbook.published' AND actor = 'user:bob' AND alert_id IS NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("audit count");
    assert_eq!(count, 1);
}

// ---------------------------------------------------------------------------------------------
// e) Conflicts: the same id again, or a config/runbooks id
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn publishing_an_existing_id_conflicts(pool: PgPool) {
    let state = api_state(&pool);
    let app = app_as(&state, Some("user:bob"));

    // Publish once, then the same id again: 409 revision_conflict.
    let publish = publish_body(&app.clone(), &yaml_draft("Conflict runbook")).await;
    let (status, body) = post(&app, "/api/runbooks", &publish).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, err) = post(&app, "/api/runbooks", &publish).await;
    assert_eq!(status, StatusCode::CONFLICT, "{err}");
    assert_eq!(err["error"]["code"], json!("revision_conflict"));

    // An id that exists in the YAML base (config/runbooks) may not be published over —
    // previewing it is fine, publishing is a conflict.
    let draft = json!({
        "format": "text", "team": "settlements", "title": "Settlement Timeout",
        "content": "When a settlement times out, check the counterparty first.",
    });
    let (status, preview) = post(&app, "/api/runbooks/import", &draft).await;
    assert_eq!(status, StatusCode::OK, "{preview}");
    assert_eq!(preview["id"], json!("settlement-timeout"));
    let mut publish = draft.clone();
    publish["id"] = preview["id"].clone();
    publish["title"] = preview["title"].clone();
    publish["steps"] = preview["steps"].clone();
    let (status, err) = post(&app, "/api/runbooks", &publish).await;
    assert_eq!(status, StatusCode::CONFLICT, "{err}");
    assert_eq!(err["error"]["code"], json!("revision_conflict"));
}

// ---------------------------------------------------------------------------------------------
// f) Live pickup: the router freezes the imported runbook with no restart
// ---------------------------------------------------------------------------------------------

/// Insert an open alert with full control over team / rule.
async fn insert_alert(pool: &PgPool, team: &str, rule_id: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO alerts (id, rule_id, rule_revision, rule_name, team, fingerprint, \
                             dedup_window_ms, status, severity, color, title, body, \
                             first_seen, last_seen, source, schema, notify_seq) \
         VALUES ($1, $2, 1, $3, $4, $5, 600000, 'open', 'critical', 'red', \
                 'Runbook import test alert', '', now(), now(), 'test', 'workflow.v1', 1)",
    )
    .bind(id)
    .bind(rule_id)
    .bind(rule_id)
    .bind(team)
    .bind(format!("fp-{id}"))
    .execute(pool)
    .await
    .expect("insert alert");
    id
}

/// One `notify` outbox row naming `runbook` in the payload (payload keys win over the
/// rule file, `rules::for_payload`), with the opening event.
async fn insert_notify(pool: &PgPool, alert: Uuid, rule_id: &str, team: &str, runbook: &str) {
    let payload = json!({
        "alert_id": alert,
        "notify_seq": 1,
        "rule_id": rule_id,
        "rule_revision": 1,
        "team": team,
        "severity": "critical",
        "reason": "new",
        "route": [{ "to": "user:bob", "channels": ["ui"] }],
        "created_at": chrono::Utc::now(),
        "runbook": runbook,
        "event": {
            "raw": { "workflowId": "WF-LIVE-1", "workflowName": "order-settlement" },
            "fields": { "type": "WORKFLOW_FAILED" },
        },
    });
    sqlx::query("INSERT INTO outbox (kind, alert_id, payload) VALUES ('notify', $1, $2)")
        .bind(alert)
        .bind(payload)
        .execute(pool)
        .await
        .expect("insert notify row");
}

#[sqlx::test(migrations = "../../migrations")]
async fn published_runbook_is_frozen_by_the_router_without_a_restart(pool: PgPool) {
    let state = api_state(&pool);
    let app = app_as(&state, Some("user:bob"));

    // Publish a yaml runbook (the UI flow: preview, then publish the preview).
    let publish = publish_body(&app, &yaml_draft("Live pickup")).await;
    let (status, summary) = post(&app, "/api/runbooks", &publish).await;
    assert_eq!(status, StatusCode::CREATED, "{summary}");
    let id = summary["id"].as_str().unwrap().to_string();

    // The effective directory the router reads (its own `EffectiveDirectory`, like
    // run_router / the scheduler / the actions worker) now contains it — same process,
    // no restart: the inserted row moved the table's stamp.
    let effective = EffectiveDirectory::new(state.dir.clone());
    let dir = effective.get(&pool).await;
    assert!(
        dir.runbooks.get(&id).is_some(),
        "the published runbook must be in the effective library"
    );

    // A notify payload names the runbook (overriding whatever the rule file says).
    let alert = insert_alert(&pool, "settlements", "settlements-workflow-failed").await;
    insert_notify(
        &pool,
        alert,
        "settlements-workflow-failed",
        "settlements",
        &id,
    )
    .await;
    let stats = route_once(&pool, &dir, &RouterConfig::default())
        .await
        .expect("route_once");
    assert_eq!(stats.runbooks, 1, "{stats:?}");

    // The frozen copy on the alert is the imported runbook, revision from its row,
    // rendered from the pinned event.
    let (runbook_id, revision, title, steps): (String, i32, String, Value) = sqlx::query_as(
        "SELECT runbook_id, revision, title, steps FROM alert_runbooks WHERE alert_id = $1",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .expect("frozen runbook row");
    assert_eq!(runbook_id, id);
    assert_eq!(revision, 1);
    assert_eq!(title, "Live pickup");
    let steps = steps.as_array().expect("frozen steps");
    assert_eq!(steps.len(), 3);
    assert_eq!(
        steps[1]["action"]["params"]["workflowId"],
        json!("WF-LIVE-1")
    );
    assert_eq!(steps[2]["approval"], json!("required"));
    assert_eq!(
        steps[2]["condition"],
        json!({ "type": "after_failures", "action": "retry_workflow", "count": 2 })
    );
}

// ---------------------------------------------------------------------------------------------
// g) A stored row that no longer validates drops alone
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn a_bad_stored_runbook_row_drops_alone(pool: PgPool) {
    let state = api_state(&pool);

    // A good runbook, published through the API like the UI does.
    let app = app_as(&state, Some("user:bob"));
    let publish = publish_body(&app, &yaml_draft("Kept import")).await;
    let (status, body) = post(&app, "/api/runbooks", &publish).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // A broken row (an action no registry knows any more, as if the validator moved
    // on after the save) and a saved preference, written straight to the table.
    sqlx::query(
        "INSERT INTO directory_overrides (kind, id, body, revision, updated_by) VALUES \
         ('runbook', 'broken-import', $1, 1, 'user:bob'), \
         ('person', 'bob', $2, 2, 'user:bob')",
    )
    .bind(json!({
        "format": "yaml", "team": "settlements", "title": "Broken import",
        "content": "steps: []",
        "steps": [{
            "id": "s1", "kind": "action", "text": "Detonate",
            "action": { "name": "detonate", "params": {} }
        }],
    }))
    .bind(json!({ "preferences": [{ "default": ["sms"] }] }))
    .execute(&pool)
    .await
    .expect("insert rows");

    // Only the broken row drops: the published runbook and bob's preference apply.
    let snap = EffectiveDirectory::new(state.dir.clone())
        .snapshot(&pool)
        .await;
    assert!(snap.dir.runbooks.get("kept-import").is_some());
    assert!(snap.dir.runbooks.get("broken-import").is_none());
    assert!(snap.applied.runbooks.contains_key("kept-import"));
    assert!(!snap.applied.runbooks.contains_key("broken-import"));
    assert!(snap.applied.people.contains_key("bob"));
    // The stored row is still reported (a fresh publish of that id is a conflict, not
    // an overwrite), and the library route never lists it.
    assert!(snap.overrides.runbooks.contains_key("broken-import"));
    let app = app_as(&state, Some("user:bob"));
    let (status, list) = get(&app, "/api/runbooks").await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert!(
        list["items"]
            .as_array()
            .expect("items")
            .iter()
            .any(|i| i["id"] == json!("kept-import"))
    );
    assert!(
        list["items"]
            .as_array()
            .expect("items")
            .iter()
            .all(|i| i["id"] != json!("broken-import"))
    );
}
