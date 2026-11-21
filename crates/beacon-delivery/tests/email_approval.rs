//! M7 email approvals integration suite (SPEC §6.3): the signed link an approval prompt
//! email carries — prompt job creation, rendering, and the `GET`/`POST /api/approvals/
//! email` routes over the real router (in-process, `tower::ServiceExt::oneshot`).
//!
//! Needs Postgres (`DATABASE_URL`; sqlx creates a throwaway database per test).
//!
//! Run: `cargo test -p beacon-delivery --test email_approval -- --nocapture`

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::{Method, StatusCode, header};
use beacon_delivery::actions::ActionRegistry;
use beacon_delivery::alert_ops::SqlAlertOps;
use beacon_delivery::api::{self, Actor, ApiState};
use beacon_delivery::approvals;
use beacon_delivery::channel::AlertView;
use beacon_delivery::config::{Contact, Directory, Person, Team};
use beacon_delivery::email_link::{self, EmailLinkConfig};
use beacon_delivery::model::DeliveryJob;
use beacon_delivery::worker;
use chrono::{DateTime, Utc};
use serde_json::json;
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

/// Team `settlements`: lead carol (phone + email), on-call `settlements` → bob (phone +
/// email), on-call `night` → erin (EMAIL ONLY — she can never be sms-prompted but must
/// still get an email prompt with a free slot). frank is a plain member with an email:
/// never eligible, so a link signed for him must be refused by the decide path. The
/// requester is `user:anna` (not in the directory at all, like `tests/approvals.rs`).
fn email_dir() -> Directory {
    Directory::from_parts(
        vec![
            person("carol", Some("carol@example.test"), Some("+44 7700 900125")),
            person("bob", Some("bob@example.test"), Some("+44 7700 900124")),
            person("erin", Some("erin@example.test"), None),
            person("frank", Some("frank@example.test"), None),
        ],
        vec![Team {
            id: "settlements".into(),
            members: vec!["bob".into(), "carol".into(), "erin".into(), "frank".into()],
            lead: Some("carol".into()),
            oncall: HashMap::from([
                ("settlements".to_string(), vec!["bob".to_string()]),
                ("night".to_string(), vec!["erin".to_string()]),
            ]),
            default_channels: None,
        }],
    )
    .expect("directory builds")
}

async fn insert_alert(pool: &PgPool, status: &str) -> Uuid {
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
    id
}

async fn request_approval(pool: &PgPool, alert: Uuid, step: &str) -> Uuid {
    let mut tx = pool.begin().await.expect("tx");
    let id = approvals::request(
        &mut tx,
        &email_dir(),
        alert,
        step,
        "escalate",
        &json!({ "team": "counterparty-support" }),
        "user:anna",
    )
    .await
    .expect("request approval");
    tx.commit().await.expect("commit");
    id
}

/// The alert as the worker's `AlertView` has it (title/severity as inserted above).
fn alert_view(alert: Uuid) -> AlertView {
    AlertView {
        id: alert,
        status: "open".to_string(),
        severity: "critical".to_string(),
        title: "Settlement workflow wf-9 failed".to_string(),
        body: String::new(),
    }
}

/// The prompt jobs of an alert on one channel (`route_info.target = "approval"`).
async fn prompt_jobs(pool: &PgPool, alert: Uuid, channel: &str) -> Vec<DeliveryJob> {
    sqlx::query_as::<_, DeliveryJob>(
        "SELECT * FROM delivery_jobs \
         WHERE alert_id = $1 AND channel = $2 AND route_info->>'target' = 'approval'",
    )
    .bind(alert)
    .bind(channel)
    .fetch_all(pool)
    .await
    .expect("prompt jobs")
}

async fn job_of(jobs: &[DeliveryJob], recipient: &str) -> DeliveryJob {
    jobs.iter()
        .find(|j| j.recipient_id == recipient)
        .cloned()
        .unwrap_or_else(|| panic!("no prompt job for {recipient}: {jobs:?}"))
}

#[derive(sqlx::FromRow)]
struct ApprovalRow {
    state: String,
    decided_by: Option<String>,
    decided_via: Option<String>,
    execution_id: Option<Uuid>,
    sms_attempts: i32,
}

async fn approval_row(pool: &PgPool, id: Uuid) -> ApprovalRow {
    sqlx::query_as::<_, ApprovalRow>(
        "SELECT state, decided_by, decided_via, execution_id, sms_attempts \
         FROM approvals WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("read approval")
}

async fn expires_at(pool: &PgPool, id: Uuid) -> DateTime<Utc> {
    sqlx::query_scalar("SELECT expires_at FROM approvals WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read expiry")
}

async fn audit_rows(pool: &PgPool, alert: Uuid) -> Vec<(String, String, String, String)> {
    sqlx::query_as(
        "SELECT actor, via, kind, message FROM audit_log WHERE alert_id = $1 ORDER BY id",
    )
    .bind(alert)
    .fetch_all(pool)
    .await
    .expect("audit rows")
}

// ---------------------------------------------------------------------------------------------
// HTTP plumbing (in-process, the pattern of tests/people_api_db.rs' session layer)
// ---------------------------------------------------------------------------------------------

fn link_cfg() -> EmailLinkConfig {
    EmailLinkConfig {
        secret: Arc::new(b"email-link-secret".to_vec()),
        public_url: "http://beacon.test".into(),
    }
}

fn state_with(pool: &PgPool, link: Option<EmailLinkConfig>) -> ApiState {
    ApiState {
        pool: pool.clone(),
        dir: Arc::new(email_dir()),
        registry: ActionRegistry::default(),
        alert_ops: Arc::new(SqlAlertOps),
        reply_secret: None,
        email_link: link,
    }
}

/// `api::router` + a middleware layer inserting `Extension(Actor(...))`, standing in for
/// beacon-api's session layer. The email routes must ignore it: every test passes an
/// actor that is NOT the signed person (usually `user:anna`, the requester).
fn app_as(state: &ApiState, actor: &str) -> axum::Router {
    let actor = actor.to_string();
    api::router(state.clone()).layer(axum::middleware::from_fn(
        move |mut req: axum::extract::Request, next: axum::middleware::Next| {
            let actor = actor.clone();
            async move {
                req.extensions_mut().insert(Actor(actor));
                next.run(req).await
            }
        },
    ))
}

async fn read(resp: axum::response::Response) -> (StatusCode, Vec<(String, String)>, String) {
    let status = resp.status();
    let headers: Vec<(String, String)> = resp
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or_default().to_string()))
        .collect();
    let bytes = http_body_util::BodyExt::collect(resp.into_body())
        .await
        .expect("body collects")
        .to_bytes();
    (
        status,
        headers,
        String::from_utf8_lossy(&bytes).into_owned(),
    )
}

/// GET `uri` in-process.
async fn get(app: axum::Router, uri: &str) -> (StatusCode, Vec<(String, String)>, String) {
    let resp = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::builder()
            .uri(uri)
            .body(axum::body::Body::empty())
            .expect("request builds"),
    )
    .await
    .expect("router is infallible");
    read(resp).await
}

/// POST `uri` with an `application/x-www-form-urlencoded` body, in-process.
async fn post_form(
    app: axum::Router,
    uri: &str,
    body: &str,
    content_type: &str,
) -> (StatusCode, Vec<(String, String)>, String) {
    let resp = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header(header::CONTENT_TYPE, content_type)
            .body(axum::body::Body::from(body.to_string()))
            .expect("request builds"),
    )
    .await
    .expect("router is infallible");
    read(resp).await
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> &'a str {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
        .unwrap_or_default()
}

/// The route (query) part of a signed link: `a=..&p=..&e=..&s=..`.
fn query_of(cfg: &EmailLinkConfig, approval: Uuid, person: &str, exp: i64) -> String {
    let url = email_link::link(cfg, approval, person, exp);
    url.split_once('?').expect("link has a query").1.to_string()
}

/// `GET /api/approvals/email?...` for one signed link.
async fn get_link(
    app: axum::Router,
    cfg: &EmailLinkConfig,
    approval: Uuid,
    person: &str,
    exp: i64,
) -> (StatusCode, Vec<(String, String)>, String) {
    get(
        app,
        &format!(
            "/api/approvals/email?{}",
            query_of(cfg, approval, person, exp)
        ),
    )
    .await
}

/// The form body for deciding through one signed link.
fn decide_body(
    cfg: &EmailLinkConfig,
    approval: Uuid,
    person: &str,
    exp: i64,
    decision: &str,
) -> String {
    format!(
        "{}&decision={decision}",
        query_of(cfg, approval, person, exp)
    )
}

// ---------------------------------------------------------------------------------------------
// (a) request: email prompt jobs next to the sms ones
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn request_creates_email_prompts_next_to_the_sms_ones(pool: PgPool) {
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, alert, "s3").await;

    // The sms prompts are unchanged: carol and bob (erin has no phone), one each, slot
    // 200, code inside the job.
    let sms = prompt_jobs(&pool, alert, "sms").await;
    assert_eq!(sms.len(), 2, "{sms:?}");
    for job in &sms {
        assert_eq!(job.notify_seq, 1);
        assert_eq!(job.escalation_step, Some(200));
        assert!(job.route_info["approval"]["code"].is_string(), "{job:?}");
    }

    // The email prompts: one per eligible person WITH an email — carol, erin and bob
    // (frank is a plain member, never eligible; the requester is never eligible).
    let email = prompt_jobs(&pool, alert, "email").await;
    let mut recipients: Vec<&str> = email.iter().map(|j| j.recipient_id.as_str()).collect();
    recipients.sort_unstable();
    assert_eq!(recipients, vec!["bob", "carol", "erin"], "{email:?}");
    for job in &email {
        assert_eq!(job.address, format!("{}@example.test", job.recipient_id));
        assert_eq!(job.route_info["target"], json!("approval"));
        assert_eq!(job.route_info["approval"]["id"], json!(id));
        assert_eq!(job.route_info["approval"]["step_id"], json!("s3"));
        assert_eq!(job.route_info["approval"]["action"], json!("escalate"));
        // NO code key: the code must never enter an email.
        assert!(
            job.route_info["approval"].get("code").is_none(),
            "email prompt carries a code: {job:?}"
        );
    }
    // Both channels share the person's slot: carol's email prompt sits on the same slot
    // as her sms prompt, and erin — no phone — still gets a FREE slot for hers.
    let carol_email = job_of(&email, "carol").await;
    let carol_sms = job_of(&sms, "carol").await;
    assert_eq!(carol_email.escalation_step, carol_sms.escalation_step);
    assert_eq!(carol_email.escalation_step, Some(200));
    assert_eq!(job_of(&email, "erin").await.escalation_step, Some(200));

    // Two approvals on one alert each get a FRESH slot per person, on both channels.
    let second = request_approval(&pool, alert, "s4").await;
    assert_ne!(second, id);
    // On sms: the people with a phone.
    let jobs = prompt_jobs(&pool, alert, "sms").await;
    for who in ["carol", "bob"] {
        let mut slots: Vec<Option<i32>> = jobs
            .iter()
            .filter(|j| j.recipient_id == who)
            .map(|j| j.escalation_step)
            .collect();
        slots.sort();
        assert_eq!(slots, vec![Some(200), Some(201)], "{who} on sms");
    }
    // On email: everyone with an email — including erin, who has no phone at all, so
    // her email prompts must earn their own slots (200, 201) instead of colliding.
    let jobs = prompt_jobs(&pool, alert, "email").await;
    for who in ["carol", "bob", "erin"] {
        let mut slots: Vec<Option<i32>> = jobs
            .iter()
            .filter(|j| j.recipient_id == who)
            .map(|j| j.escalation_step)
            .collect();
        slots.sort();
        assert_eq!(slots, vec![Some(200), Some(201)], "{who} on email");
    }
}

// ---------------------------------------------------------------------------------------------
// (b) the rendered prompt email: a verifiable link, never a code
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn the_rendered_approval_email_carries_a_verifiable_link_and_no_code(pool: PgPool) {
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, alert, "s3").await;
    let cfg = link_cfg();

    // The render function the email worker itself uses (`process_job`).
    let job = job_of(&prompt_jobs(&pool, alert, "email").await, "carol").await;
    let msg = worker::render_approval_email(&pool, Some(&cfg), &alert_view(alert), &job)
        .await
        .expect("render")
        .expect("the approval is still pending");
    assert_eq!(
        msg.subject,
        "Approval needed: escalate on Settlement workflow wf-9 failed"
    );
    assert_eq!(msg.recipient_id, "carol");
    assert_eq!(msg.address, "carol@example.test");

    // The body names the action, step, alert, team and expiry.
    for fact in [
        "Action: escalate",
        "Step: s3",
        "Alert: Settlement workflow wf-9 failed",
        "Team: settlements",
        "This request expires at",
    ] {
        assert!(msg.body.contains(fact), "missing {fact:?} in: {}", msg.body);
    }

    // The link line, and a link that verifies for exactly this person and expiry.
    let line = msg
        .body
        .lines()
        .find(|l| l.starts_with("Review and decide: "))
        .expect("link line");
    let url = line.strip_prefix("Review and decide: ").expect("url");
    assert!(
        url.starts_with("http://beacon.test/api/approvals/email?"),
        "{url}"
    );
    let query = url.split_once('?').expect("query").1;
    let get_param = |key: &str| {
        query
            .split('&')
            .find(|p| p.starts_with(&format!("{key}=")))
            .and_then(|p| p.split_once('=').map(|(_, v)| v))
            .unwrap_or_else(|| panic!("no {key} in {query}"))
    };
    assert_eq!(get_param("a"), id.to_string());
    assert_eq!(get_param("p"), "carol");
    let exp: i64 = get_param("e").parse().expect("exp unix");
    assert_eq!(
        exp,
        expires_at(&pool, id).await.timestamp(),
        "link e = row expiry"
    );
    assert!(email_link::verify(
        &cfg.secret,
        id,
        "carol",
        exp,
        get_param("s")
    ));
    // The signature is the approval-specific one, domain-separated from the SMS reply.
    assert!(!email_link::verify(
        &cfg.secret,
        id,
        "bob",
        exp,
        get_param("s")
    ));

    // The once/expiry note rides along, and NO 4-digit code ever appears: the code lives
    // only in the sms prompt job of the same person.
    assert!(
        msg.body.contains("works once and expires at"),
        "{}",
        msg.body
    );
    let sms_code = job_of(&prompt_jobs(&pool, alert, "sms").await, "carol").await;
    let code = sms_code.route_info["approval"]["code"]
        .as_str()
        .expect("code");
    assert_eq!(code.len(), 4);
    assert!(!msg.body.contains(code), "the code leaked into the email");
    assert!(
        !msg.body.contains("Reply") && !msg.body.contains("Y "),
        "the email must not teach the SMS instruction: {}",
        msg.body
    );
}

// ---------------------------------------------------------------------------------------------
// (c) GET: the form page, and nothing decided
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn get_with_a_valid_link_shows_the_form_and_changes_nothing(pool: PgPool) {
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, alert, "s3").await;
    let cfg = link_cfg();
    let exp = expires_at(&pool, id).await.timestamp();
    let app = app_as(&state_with(&pool, Some(link_cfg())), "user:anna");

    let (status, headers, body) = get_link(app, &cfg, id, "carol", exp).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(header(&headers, "content-type"), "text/html; charset=utf-8");
    assert_eq!(header(&headers, "cache-control"), "no-store");
    assert_eq!(header(&headers, "referrer-policy"), "no-referrer");
    for fact in [
        "Approval request",
        "<strong>escalate</strong> (step s3)",
        "Settlement workflow wf-9 failed",
        "user:anna",
        "Current state: pending",
        "<form method=\"post\" action=\"/api/approvals/email\">",
        "name=\"decision\" value=\"approve\"",
        "name=\"decision\" value=\"reject\"",
        "Approve</button>",
        "Reject</button>",
    ] {
        assert!(body.contains(fact), "missing {fact:?} in: {body}");
    }
    // The hidden fields carry the link's own parameters back.
    for value in [id.to_string(), "carol".to_string(), exp.to_string()] {
        assert!(
            body.contains(&format!("value=\"{value}\"")),
            "hidden field {value}: {body}"
        );
    }

    // GET NEVER changes state (mail scanners prefetch links).
    let row = approval_row(&pool, id).await;
    assert_eq!(row.state, "pending");
    assert_eq!(row.sms_attempts, 0, "no attempt burned by a prefetch");
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM action_executions WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("count executions");
    assert_eq!(executions, 0);
    assert!(
        audit_rows(&pool, alert)
            .await
            .iter()
            .all(|(_, _, _, message)| message.starts_with("requested")),
        "GET must not audit anything"
    );
}

// ---------------------------------------------------------------------------------------------
// (d) POST approve: decided via email, actor = the signed person (session ignored)
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn post_approve_decides_via_email_and_a_replay_conflicts(pool: PgPool) {
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, alert, "s3").await;
    let cfg = link_cfg();
    let exp = expires_at(&pool, id).await.timestamp();

    // The session actor is the REQUESTER (anna): the signed person (carol) decides, so
    // the route ignoring the session actor is what makes this approve at all.
    let app = app_as(&state_with(&pool, Some(link_cfg())), "user:anna");
    let (status, headers, body) = post_form(
        app,
        "/api/approvals/email",
        &decide_body(&cfg, id, "carol", exp, "approve"),
        "application/x-www-form-urlencoded",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("The request was approved."), "{body}");
    assert_eq!(header(&headers, "cache-control"), "no-store");

    let row = approval_row(&pool, id).await;
    assert_eq!(row.state, "approved");
    assert_eq!(row.decided_by.as_deref(), Some("user:carol"));
    assert_eq!(row.decided_via.as_deref(), Some("email"));
    let execution_id = row.execution_id.expect("approved stores its execution");
    let (requested_by, approval_id): (String, Option<Uuid>) =
        sqlx::query_as("SELECT requested_by, approval_id FROM action_executions WHERE id = $1")
            .bind(execution_id)
            .fetch_one(&pool)
            .await
            .expect("execution row");
    assert_eq!(requested_by, "user:carol");
    assert_eq!(approval_id, Some(id));

    // Audit via "email".
    assert!(
        audit_rows(&pool, alert)
            .await
            .iter()
            .any(|(actor, via, kind, message)| actor == "user:carol"
                && via == "email"
                && kind == "approval"
                && message == "approved escalate (step s3)"),
        "missing the email approval audit row"
    );

    // A second POST with the same (once) link: already decided.
    let app = app_as(&state_with(&pool, Some(link_cfg())), "user:anna");
    let (status, _, body) = post_form(
        app,
        "/api/approvals/email",
        &decide_body(&cfg, id, "carol", exp, "reject"),
        "application/x-www-form-urlencoded",
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(body.contains("already decided: approved"), "{body}");
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM action_executions WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("count executions");
    assert_eq!(executions, 1, "no second execution");
}

// ---------------------------------------------------------------------------------------------
// (e) tampered links: 401, generic, nothing burned
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn tampered_links_are_401_and_burn_nothing(pool: PgPool) {
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, alert, "s3").await;
    let cfg = link_cfg();
    let exp = expires_at(&pool, id).await.timestamp();
    let good = query_of(&cfg, id, "carol", exp);

    // A tampered signature, another person reusing carol's signature, a moved expiry,
    // and a missing parameter: all 401, all the same generic page.
    let bad_sig = {
        let mut sig = email_link::sign(&cfg.secret, id, "carol", exp);
        sig.replace_range(0..1, if sig.starts_with('0') { "1" } else { "0" });
        format!("a={id}&p=carol&e={exp}&s={sig}")
    };
    let other_person = format!(
        "a={id}&p=bob&e={exp}&s={}",
        email_link::sign(&cfg.secret, id, "carol", exp)
    );
    let moved_exp = format!("a={id}&p=carol&e={}&s={}", exp + 60, {
        // signed for the ORIGINAL exp, presented with a moved one
        email_link::sign(&cfg.secret, id, "carol", exp)
    });
    let missing = format!("a={id}&p=carol&e={exp}");
    for query in [&bad_sig, &other_person, &moved_exp, &missing] {
        let app = app_as(&state_with(&pool, Some(link_cfg())), "user:anna");
        let (status, _, body) = get(app, &format!("/api/approvals/email?{query}")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{query}: {body}");
        assert!(body.contains("This approval link is not valid."), "{body}");

        let app = app_as(&state_with(&pool, Some(link_cfg())), "user:anna");
        let (status, _, body) = post_form(
            app,
            "/api/approvals/email",
            &format!("{query}&decision=approve"),
            "application/x-www-form-urlencoded",
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{query}: {body}");
        assert!(body.contains("This approval link is not valid."), "{body}");
    }

    // Nothing was burned and nothing decided.
    let row = approval_row(&pool, id).await;
    assert_eq!(row.state, "pending");
    assert_eq!(row.sms_attempts, 0);
    // The untampered link still works — nothing was invalidated either.
    let app = app_as(&state_with(&pool, Some(link_cfg())), "user:anna");
    let (status, _, body) = get(app, &format!("/api/approvals/email?{good}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

// ---------------------------------------------------------------------------------------------
// (f) expiry: the link's e in the past, or the row's expires_at backdated
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn expired_links_and_lapsed_approvals_are_410(pool: PgPool) {
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, alert, "s3").await;
    let cfg = link_cfg();
    let exp = expires_at(&pool, id).await.timestamp();

    // A link whose signed e is already in the past.
    let past = exp - 3600;
    for (status, _, body) in [
        get_link(
            app_as(&state_with(&pool, Some(link_cfg())), "user:anna"),
            &cfg,
            id,
            "carol",
            past,
        )
        .await,
        post_form(
            app_as(&state_with(&pool, Some(link_cfg())), "user:anna"),
            "/api/approvals/email",
            &decide_body(&cfg, id, "carol", past, "approve"),
            "application/x-www-form-urlencoded",
        )
        .await,
    ] {
        assert_eq!(status, StatusCode::GONE, "{body}");
        assert!(body.contains("expired"), "{body}");
    }
    assert_eq!(approval_row(&pool, id).await.state, "pending");

    // A link signed for the future, but the approval row lapsed underneath it: the GET
    // shows no form and the POST decides Expired (and commits the row's expiry).
    sqlx::query("UPDATE approvals SET expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .expect("backdate expiry");
    let (status, _, body) = get_link(
        app_as(&state_with(&pool, Some(link_cfg())), "user:anna"),
        &cfg,
        id,
        "carol",
        exp,
    )
    .await;
    assert_eq!(status, StatusCode::GONE, "{body}");
    let (status, _, body) = post_form(
        app_as(&state_with(&pool, Some(link_cfg())), "user:anna"),
        "/api/approvals/email",
        &decide_body(&cfg, id, "carol", exp, "approve"),
        "application/x-www-form-urlencoded",
    )
    .await;
    assert_eq!(status, StatusCode::GONE, "{body}");
    assert_eq!(approval_row(&pool, id).await.state, "expired");
}

// ---------------------------------------------------------------------------------------------
// (g) validly signed links for people who may not decide
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn requester_and_non_eligible_links_are_403(pool: PgPool) {
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, alert, "s3").await;
    let cfg = link_cfg();
    let exp = expires_at(&pool, id).await.timestamp();

    // The requester's own link (signed with the real secret): the decide path refuses.
    let (status, _, body) = post_form(
        app_as(&state_with(&pool, Some(link_cfg())), "user:anna"),
        "/api/approvals/email",
        &decide_body(&cfg, id, "anna", exp, "approve"),
        "application/x-www-form-urlencoded",
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(
        body.contains("the requester cannot approve their own request"),
        "{body}"
    );

    // A plain member with an email — never eligible, even with a valid signature.
    let (status, _, body) = post_form(
        app_as(&state_with(&pool, Some(link_cfg())), "user:anna"),
        "/api/approvals/email",
        &decide_body(&cfg, id, "frank", exp, "approve"),
        "application/x-www-form-urlencoded",
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(
        body.contains("not eligible to decide this approval"),
        "{body}"
    );

    let row = approval_row(&pool, id).await;
    assert_eq!(row.state, "pending");
    assert_eq!(row.sms_attempts, 0);
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM action_executions WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("count executions");
    assert_eq!(executions, 0);
}

// ---------------------------------------------------------------------------------------------
// (h) POST reject
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn post_reject_rejects_via_email(pool: PgPool) {
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, alert, "s3").await;
    let cfg = link_cfg();
    let exp = expires_at(&pool, id).await.timestamp();

    let (status, _, body) = post_form(
        app_as(&state_with(&pool, Some(link_cfg())), "user:anna"),
        "/api/approvals/email",
        &decide_body(&cfg, id, "bob", exp, "reject"),
        "application/x-www-form-urlencoded",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("The request was rejected."), "{body}");

    let row = approval_row(&pool, id).await;
    assert_eq!(row.state, "rejected");
    assert_eq!(row.decided_by.as_deref(), Some("user:bob"));
    assert_eq!(row.decided_via.as_deref(), Some("email"));
    assert_eq!(row.execution_id, None, "a rejection queues nothing");
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM action_executions WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("count executions");
    assert_eq!(executions, 0);
    assert!(
        audit_rows(&pool, alert)
            .await
            .iter()
            .any(|(actor, via, _, message)| actor == "user:bob"
                && via == "email"
                && message == "rejected escalate (step s3)"),
        "missing the email rejection audit row"
    );
}

// ---------------------------------------------------------------------------------------------
// (i) no EmailLinkConfig: routes 404, prompts still created, body says Decide in Beacon
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn without_link_config_the_routes_404_but_prompts_still_render(pool: PgPool) {
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, alert, "s3").await;

    // The prompt jobs exist exactly as before — the link config is a render/endpoint
    // concern, not a request one.
    let email = prompt_jobs(&pool, alert, "email").await;
    assert_eq!(email.len(), 3, "{email:?}");
    for job in &email {
        assert!(job.route_info["approval"].get("code").is_none(), "{job:?}");
    }

    // A link built with a config the server does not have: 404, not enabled.
    let cfg = link_cfg();
    let exp = expires_at(&pool, id).await.timestamp();
    for (status, _, body) in [
        get_link(
            app_as(&state_with(&pool, None), "user:anna"),
            &cfg,
            id,
            "carol",
            exp,
        )
        .await,
        post_form(
            app_as(&state_with(&pool, None), "user:anna"),
            "/api/approvals/email",
            &decide_body(&cfg, id, "carol", exp, "approve"),
            "application/x-www-form-urlencoded",
        )
        .await,
    ] {
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert!(body.contains("Email approvals are not enabled"), "{body}");
    }
    assert_eq!(approval_row(&pool, id).await.state, "pending");

    // The worker still renders a prompt — the link-less "Decide in Beacon" form.
    let job = job_of(&email, "carol").await;
    let msg = worker::render_approval_email(&pool, None, &alert_view(alert), &job)
        .await
        .expect("render")
        .expect("still a prompt while pending");
    assert!(msg.body.contains("Decide in Beacon"), "{}", msg.body);
    assert!(!msg.body.contains("Review and decide:"), "{}", msg.body);
    assert_eq!(
        msg.subject,
        "Approval needed: escalate on Settlement workflow wf-9 failed"
    );
}

// ---------------------------------------------------------------------------------------------
// A person id with reserved characters: the link encodes it, the routes decode it
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn a_person_id_with_reserved_characters_round_trips_the_link(pool: PgPool) {
    // A lead whose id contains a space, a `+` and a `:` — everything a query component
    // must percent-encode, and everything a form body re-encodes differently.
    let dir = Directory::from_parts(
        vec![person(
            "on call:night+shift",
            Some("oncall@example.test"),
            None,
        )],
        vec![Team {
            id: "settlements".into(),
            members: vec!["on call:night+shift".into()],
            lead: Some("on call:night+shift".into()),
            oncall: HashMap::new(),
            default_channels: None,
        }],
    )
    .expect("directory builds");
    let alert = insert_alert(&pool, "open").await;
    let mut tx = pool.begin().await.expect("tx");
    let id = approvals::request(
        &mut tx,
        &dir,
        alert,
        "s3",
        "escalate",
        &json!({ "team": "counterparty-support" }),
        "user:anna",
    )
    .await
    .expect("request approval");
    tx.commit().await.expect("commit");

    let cfg = link_cfg();
    let exp = expires_at(&pool, id).await.timestamp();
    let app = app_as(&state_with(&pool, Some(link_cfg())), "user:anna");
    let (status, _, body) = get_link(app, &cfg, id, "on call:night+shift", exp).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // The hidden field carries the DECODED id, HTML-escaped.
    assert!(
        body.contains("name=\"p\" value=\"on call:night+shift\""),
        "decoded person field: {body}"
    );

    // The form a browser would produce: spaces as `+`, the literal `+` as `%2B`.
    let browser_form = format!(
        "a={id}&p=on+call%3Anight%2Bshift&e={exp}&s={}&decision=approve",
        email_link::sign(&cfg.secret, id, "on call:night+shift", exp)
    );
    let (status, _, body) = post_form(
        app_as(&state_with(&pool, Some(link_cfg())), "user:anna"),
        "/api/approvals/email",
        &browser_form,
        "application/x-www-form-urlencoded",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let row = approval_row(&pool, id).await;
    assert_eq!(row.state, "approved");
    assert_eq!(
        row.decided_by.as_deref(),
        Some("user:on call:night+shift"),
        "the decoded id is the actor"
    );
}

// ---------------------------------------------------------------------------------------------
// Content types on the POST route
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn post_rejects_every_other_content_type_with_415(pool: PgPool) {
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, alert, "s3").await;
    let cfg = link_cfg();
    let exp = expires_at(&pool, id).await.timestamp();
    let body = decide_body(&cfg, id, "carol", exp, "approve");

    // JSON included on purpose: this route takes the form encoding, and the form encoding
    // only — the HMAC in the body is the authorization, so the JSON CSRF guard would
    // break the one legitimate client (the browser form).
    for content_type in ["application/json", "text/plain", "multipart/form-data"] {
        let (status, _, body) = post_form(
            app_as(&state_with(&pool, Some(link_cfg())), "user:anna"),
            "/api/approvals/email",
            &body,
            content_type,
        )
        .await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{content_type}");
        assert!(body.contains("x-www-form-urlencoded"), "{body}");
    }
    // An unknown decision on an otherwise valid link is a 400.
    let (status, _, body) = post_form(
        app_as(&state_with(&pool, Some(link_cfg())), "user:anna"),
        "/api/approvals/email",
        &decide_body(&cfg, id, "carol", exp, "maybe"),
        "application/x-www-form-urlencoded",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(approval_row(&pool, id).await.state, "pending");
}
