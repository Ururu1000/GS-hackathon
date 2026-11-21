//! M5b approvals integration suite (SPEC §6.3): request, decide (UI), SMS replies, attempt
//! limits, ACK and the audit timeline's derived entries.
//!
//! Needs Postgres (`DATABASE_URL`; sqlx creates a throwaway database per test).
//!
//! Run: `cargo test -p beacon-delivery --test approvals -- --nocapture`

mod common;

use std::collections::HashMap;

use beacon_delivery::RouterConfig;
use beacon_delivery::alert_ops::SqlAlertOps;
use beacon_delivery::approvals::{
    self, DecideError, DecideOutcome, Decision, MAX_SMS_ATTEMPTS, ReplyOutcome, RequestError, Via,
};
use beacon_delivery::audit;
use beacon_delivery::config::{Contact, Directory, Person, Team};
use beacon_delivery::model::DeliveryJob;
use beacon_delivery::router::route_once;
use common::job_rows;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

// ---------------------------------------------------------------------------------------------
// Helpers
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

/// Team `settlements`: lead carol (phone), on-call `settlements` → bob (phone), on-call
/// `night` → erin (no phone). Eligible for approvals: carol, erin, bob (lead first, then
/// schedules in id order); only carol and bob can be SMS-prompted.
fn approvals_dir() -> Directory {
    Directory::from_parts(
        vec![
            person("carol", Some("carol@example.test"), Some("+44 7700 900125")),
            person("bob", Some("bob@example.test"), Some("+44 7700 900124")),
            person("erin", Some("erin@example.test"), None),
            // A plain member: neither lead nor on-call, so never eligible to decide.
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
    insert_alert_on_team(pool, status, "settlements").await
}

async fn insert_alert_on_team(pool: &PgPool, status: &str, team: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO alerts (id, rule_id, rule_revision, rule_name, team, fingerprint, \
                             dedup_window_ms, status, severity, color, title, body, \
                             first_seen, last_seen, source, schema, notify_seq) \
         VALUES ($1, 'settlements-workflow-failed', 1, 'Settlement workflow failed', \
                 $2, $3, 600000, $4, 'critical', 'red', \
                 'Settlement workflow wf-9 failed', '', now(), now(), 'test', \
                 'workflow.failed.v1', 1)",
    )
    .bind(id)
    .bind(team)
    .bind(format!("fp-{id}"))
    .bind(status)
    .execute(pool)
    .await
    .expect("insert alert");
    id
}

async fn request_approval(pool: &PgPool, dir: &Directory, alert: Uuid, step: &str) -> Uuid {
    let mut tx = pool.begin().await.expect("tx");
    let id = approvals::request(
        &mut tx,
        dir,
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

/// The prompt sms jobs of an alert (`route_info.target = "approval"`).
async fn prompt_jobs(pool: &PgPool, alert: Uuid) -> Vec<DeliveryJob> {
    job_rows(pool, alert)
        .await
        .into_iter()
        .filter(|j| j.channel == "sms" && j.route_info.get("target") == Some(&json!("approval")))
        .collect()
}

/// The code of one person's prompt job (the only place the plaintext code exists).
async fn prompt_code(pool: &PgPool, alert: Uuid, recipient: &str) -> String {
    let jobs = prompt_jobs(pool, alert).await;
    let job = jobs
        .iter()
        .find(|j| j.recipient_id == recipient)
        .unwrap_or_else(|| panic!("no prompt job for {recipient}: {jobs:?}"));
    job.route_info["approval"]["code"]
        .as_str()
        .expect("code in the prompt job")
        .to_string()
}

/// The test-side hash of `<id>:<code>` — what the approvals table must store.
fn expected_hash(id: Uuid, code: &str) -> String {
    hex::encode(Sha256::digest(format!("{id}:{code}").as_bytes()))
}

#[derive(Debug, sqlx::FromRow)]
struct ApprovalRow {
    state: String,
    eligible: Vec<String>,
    execution_id: Option<Uuid>,
    decided_by: Option<String>,
    decided_via: Option<String>,
    note: Option<String>,
    sms_attempts: i32,
    sms_code_hash: String,
    expires_soon: bool,
}

async fn approval_row(pool: &PgPool, id: Uuid) -> ApprovalRow {
    sqlx::query_as::<_, ApprovalRow>(
        "SELECT state, eligible, execution_id, decided_by, decided_via, note, sms_attempts, \
                sms_code_hash, expires_at BETWEEN now() + interval '14 minutes' \
                    AND now() + interval '16 minutes' AS expires_soon \
         FROM approvals WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("read approval")
}

async fn alert_version(pool: &PgPool, alert: Uuid) -> i64 {
    sqlx::query_scalar("SELECT version FROM alerts WHERE id = $1")
        .bind(alert)
        .fetch_one(pool)
        .await
        .expect("alert version")
}

async fn audit_messages(pool: &PgPool, alert: Uuid) -> Vec<(String, String, String, String)> {
    sqlx::query_as(
        "SELECT actor, via, kind, message FROM audit_log WHERE alert_id = $1 ORDER BY id",
    )
    .bind(alert)
    .fetch_all(pool)
    .await
    .expect("audit rows")
}

/// A different 4-digit code than `code`.
fn wrong_code(code: &str) -> String {
    let n: u32 = code.parse().expect("4-digit code");
    format!("{:04}", (n + 1) % 10_000)
}

/// Plant the gateway receipt for one prompt job, as the gateway writes it when the provider
/// accepts the prompt (the rendered SMS text names the code twice, plus the challenge).
async fn plant_receipt(pool: &PgPool, job: &DeliveryJob, code: &str) {
    sqlx::query(
        "INSERT INTO gateway_receipts \
            (job_id, alert_id, channel, recipient, to_address, text, approval_challenge) \
         VALUES ($1, $2, 'sms', $3, $4, $5, $6)",
    )
    .bind(job.id)
    .bind(job.alert_id)
    .bind(format!("user:{}", job.recipient_id))
    .bind(&job.address)
    .bind(format!(
        "Approve escalate for the alert? Reply \"Y {code}\" to approve, \"N {code}\" to reject."
    ))
    .bind(format!("Y {code}"))
    .execute(pool)
    .await
    .expect("plant gateway receipt");
}

/// The receipts of an alert's prompt jobs: `(approval_challenge, text)`, oldest first.
async fn receipt_rows(pool: &PgPool, alert: Uuid) -> Vec<(Option<String>, String)> {
    sqlx::query_as(
        "SELECT r.approval_challenge, r.text FROM gateway_receipts r \
         JOIN delivery_jobs j ON j.id = r.job_id \
         WHERE j.alert_id = $1 AND j.route_info->>'target' = 'approval' \
         ORDER BY r.id",
    )
    .bind(alert)
    .fetch_all(pool)
    .await
    .expect("receipt rows")
}

/// True when `s` contains any run of 4 or more ASCII digits (the scrub masks every 4-digit
/// run, so a scrubbed text has none).
fn has_digit_run(s: &str) -> bool {
    s.as_bytes()
        .windows(4)
        .any(|w| w.iter().all(|b| b.is_ascii_digit()))
}

/// Assert the code is gone at rest for every prompt job of `alert` (audit #11): the jobs'
/// `route_info` lost `approval.code` but kept the rest of the approval object, and the
/// receipts lost their challenge and every 4-digit run in `text`.
async fn assert_scrubbed(pool: &PgPool, alert: Uuid, approval: Uuid, prompts: usize) {
    let jobs = prompt_jobs(pool, alert).await;
    assert_eq!(jobs.len(), prompts, "{jobs:?}");
    for job in &jobs {
        assert!(
            job.route_info["approval"].get("code").is_none(),
            "{} still carries the code: {:?}",
            job.recipient_id,
            job.route_info
        );
        assert_eq!(job.route_info["approval"]["id"], json!(approval));
        assert_eq!(job.route_info["approval"]["step_id"], json!("s3"));
    }
    let receipts = receipt_rows(pool, alert).await;
    assert_eq!(receipts.len(), prompts, "{receipts:?}");
    for (challenge, text) in &receipts {
        assert_eq!(challenge.as_deref(), None, "{text:?}");
        assert!(text.contains("****"), "the code was not masked: {text:?}");
        assert!(!has_digit_run(text), "a digit run survived: {text:?}");
    }
}

// ---------------------------------------------------------------------------------------------
// request
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn request_creates_one_pending_row_and_one_prompt_job_per_phone(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;

    let id = request_approval(&pool, &dir, alert, "s3").await;

    let row = approval_row(&pool, id).await;
    assert_eq!(row.state, "pending");
    // Lead first, then the on-call of every schedule the team defines, in schedule-id order.
    assert_eq!(
        row.eligible,
        vec!["user:carol", "user:erin", "user:bob"],
        "eligible set"
    );
    assert!(row.expires_soon, "expires_at must be ~now + 15 min");
    assert_eq!(row.decided_by, None);
    assert_eq!(row.execution_id, None);

    // One prompt sms job per eligible person WITH a phone: carol and bob, not erin.
    let jobs = prompt_jobs(&pool, alert).await;
    assert_eq!(jobs.len(), 2, "prompt jobs: {jobs:?}");
    // Each person's first prompt slot on this notification is 200.
    let steps: Vec<_> = jobs.iter().map(|j| j.escalation_step).collect();
    assert_eq!(steps, vec![Some(200), Some(200)], "{steps:?}");
    for job in &jobs {
        assert_eq!(job.channel, "sms");
        assert_eq!(job.route_info["target"], json!("approval"));
        assert_eq!(job.route_info["approval"]["id"], json!(id));
        assert_eq!(job.route_info["approval"]["step_id"], json!("s3"));
        assert_eq!(job.route_info["approval"]["action"], json!("escalate"));
        let code = job.route_info["approval"]["code"].as_str().expect("code");
        assert_eq!(code.len(), 4, "the code is 4 digits: {code}");
        assert!(code.chars().all(|c| c.is_ascii_digit()), "{code}");
        // The table keeps only the hash of `<id>:<code>`.
        assert_eq!(row.sms_code_hash, expected_hash(id, code), "hash mismatch");
    }
    // Both prompts carry the same code (one approval, one challenge).
    assert_eq!(
        prompt_code(&pool, alert, "carol").await,
        prompt_code(&pool, alert, "bob").await
    );

    // The audit trail records the request.
    let audit_rows = audit_messages(&pool, alert).await;
    assert!(
        audit_rows
            .iter()
            .any(|(actor, via, kind, message)| actor == "user:anna"
                && via == "ui"
                && kind == "approval"
                && message == "requested escalate (step s3)"),
        "{audit_rows:?}"
    );
    // `request` itself only audits; the version bump is the run route's `AlertOps::touch`.
    assert_eq!(alert_version(&pool, alert).await, 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_second_request_returns_the_same_id_and_sends_nothing(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;

    let first = request_approval(&pool, &dir, alert, "s3").await;
    let count_after_first: i64 =
        sqlx::query_scalar("SELECT count(*) FROM approvals WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("count");
    assert_eq!(count_after_first, 1);
    assert_eq!(prompt_jobs(&pool, alert).await.len(), 2);

    let second = request_approval(&pool, &dir, alert, "s3").await;
    assert_eq!(first, second, "the pending request is returned as-is");
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM approvals WHERE alert_id = $1")
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(count, 1, "no second approvals row");
    assert_eq!(
        prompt_jobs(&pool, alert).await.len(),
        2,
        "no second prompts"
    );
    // A different step gets its own approval.
    let other = request_approval(&pool, &dir, alert, "s4").await;
    assert_ne!(other, first);
}

#[sqlx::test(migrations = "../../migrations")]
async fn prompt_reuses_the_alerts_seq_and_a_renotify_still_pages(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    // The original notification already paged carol on sms at the alert's current seq (1).
    sqlx::query(
        "INSERT INTO delivery_jobs (alert_id, notify_seq, recipient_id, channel, address) \
         VALUES ($1, 1, 'carol', 'sms', '+44 7700 900125')",
    )
    .bind(alert)
    .execute(&pool)
    .await
    .expect("carol's original sms page");

    request_approval(&pool, &dir, alert, "s3").await;

    // Both prompts reuse the alert's current notify_seq: the idempotency key includes
    // escalation_step, so carol's prompt is a SECOND sms job next to her page (step 0).
    let jobs = prompt_jobs(&pool, alert).await;
    let carol = jobs
        .iter()
        .find(|j| j.recipient_id == "carol")
        .expect("carol prompt");
    assert_eq!(carol.notify_seq, 1, "the alert's current notify_seq");
    assert_eq!(carol.escalation_step, Some(200));
    let bob = jobs
        .iter()
        .find(|j| j.recipient_id == "bob")
        .expect("bob prompt");
    assert_eq!(bob.notify_seq, 1, "the alert's current notify_seq");
    assert_eq!(bob.escalation_step, Some(200));
    let carol_sms: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM delivery_jobs \
         WHERE alert_id = $1 AND recipient_id = 'carol' AND channel = 'sms'",
    )
    .bind(alert)
    .fetch_one(&pool)
    .await
    .expect("count carol sms");
    assert_eq!(carol_sms, 2, "the page (step 0) plus the prompt (step 200)");

    // A real renotify one seq later still creates carol's sms job: the prompt did not
    // swallow the seq.
    sqlx::query("UPDATE alerts SET notify_seq = 2 WHERE id = $1")
        .bind(alert)
        .execute(&pool)
        .await
        .expect("bump the alert's notify_seq");
    common::insert_outbox_raw(
        &pool,
        "notify",
        alert,
        json!({
            "alert_id": alert,
            "notify_seq": 2,
            "rule_id": "settlements-workflow-failed",
            "rule_revision": 1,
            "team": "settlements",
            "severity": "critical",
            "reason": "renotify",
            "route": [{ "to": "user:carol", "channels": ["sms"] }],
            "created_at": chrono::Utc::now(),
        }),
    )
    .await;
    let stats = route_once(&pool, &dir, &RouterConfig::default())
        .await
        .expect("route_once");
    assert_eq!(stats.jobs_inserted, 1, "{stats:?}");

    let renotify: Vec<DeliveryJob> = sqlx::query_as(
        "SELECT * FROM delivery_jobs \
         WHERE alert_id = $1 AND recipient_id = 'carol' AND channel = 'sms' AND notify_seq = 2",
    )
    .bind(alert)
    .fetch_all(&pool)
    .await
    .expect("carol's renotify jobs");
    assert_eq!(renotify.len(), 1, "{renotify:?}");
    assert_eq!(renotify[0].escalation_step, Some(0), "a plain job");
}

#[sqlx::test(migrations = "../../migrations")]
async fn many_pending_approvals_sharing_a_person_never_share_a_code(pool: PgPool) {
    let dir = approvals_dir();

    // 30 pending approvals, one per alert: every one shares carol, erin and bob as its
    // eligible set, so no two of them may page anyone with the same code. (One alert per
    // request: a second approval of the SAME alert reuses its current notify_seq, so the
    // router's unique job key would swallow the later prompts.)
    let mut seen: HashMap<String, Uuid> = HashMap::new();
    for step in 0..30 {
        let alert = insert_alert(&pool, "open").await;
        let approval = request_approval(&pool, &dir, alert, &format!("s{step}")).await;
        // The prompt job is the only place the plaintext code exists.
        let code = prompt_code(&pool, alert, "carol").await;
        if let Some(other) = seen.insert(code.clone(), approval) {
            panic!("approvals {other} and {approval} share the code {code} for carol");
        }
    }
    // An SMS reply "Y <code>" from carol names exactly one request.
    assert_eq!(
        seen.len(),
        30,
        "every approval paged carol with its own code"
    );
}

// ---------------------------------------------------------------------------------------------
// decide (UI)
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn decide_approve_queues_the_execution_and_audits(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, &dir, alert, "s3").await;
    let version_before = alert_version(&pool, alert).await;

    let outcome = approvals::decide(
        &pool,
        &SqlAlertOps,
        id,
        "user:carol",
        Decision::Approve,
        Via::Ui,
        Some("escalating per the runbook"),
    )
    .await
    .expect("approve");
    let DecideOutcome::Approved { execution_id } = outcome else {
        panic!("expected Approved, got {outcome:?}");
    };

    let row = approval_row(&pool, id).await;
    assert_eq!(row.state, "approved");
    assert_eq!(row.execution_id, Some(execution_id));
    assert_eq!(row.decided_by.as_deref(), Some("user:carol"));
    assert_eq!(row.decided_via.as_deref(), Some("ui"));
    assert_eq!(row.note.as_deref(), Some("escalating per the runbook"));

    // The execution is queued in the same tx, frozen to the approval.
    let (state, step_id, action, requested_by, approval_id, params): (
        String,
        String,
        String,
        String,
        Option<Uuid>,
        Value,
    ) = sqlx::query_as(
        "SELECT state, step_id, action, requested_by, approval_id, params \
         FROM action_executions WHERE id = $1",
    )
    .bind(execution_id)
    .fetch_one(&pool)
    .await
    .expect("execution row");
    assert_eq!(state, "queued");
    assert_eq!(step_id, "s3");
    assert_eq!(action, "escalate");
    assert_eq!(requested_by, "user:carol");
    assert_eq!(approval_id, Some(id));
    assert_eq!(params, json!({ "team": "counterparty-support" }));

    // Audit + version bump, and no second execution anywhere.
    assert!(
        audit_messages(&pool, alert)
            .await
            .iter()
            .any(|(actor, via, kind, message)| actor == "user:carol"
                && via == "ui"
                && kind == "approval"
                && message == "approved escalate (step s3)"),
        "missing the approval audit row"
    );
    assert!(alert_version(&pool, alert).await > version_before);
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM action_executions WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("count executions");
    assert_eq!(executions, 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn decide_reject_audits_and_queues_nothing(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, &dir, alert, "s3").await;

    let outcome = approvals::decide(
        &pool,
        &SqlAlertOps,
        id,
        "user:bob",
        Decision::Reject,
        Via::Ui,
        None,
    )
    .await
    .expect("reject");
    assert_eq!(outcome, DecideOutcome::Rejected);

    let row = approval_row(&pool, id).await;
    assert_eq!(row.state, "rejected");
    assert_eq!(row.decided_by.as_deref(), Some("user:bob"));
    assert_eq!(row.execution_id, None);
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM action_executions WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("count executions");
    assert_eq!(executions, 0);
    assert!(
        audit_messages(&pool, alert)
            .await
            .iter()
            .any(|(actor, _, kind, message)| actor == "user:bob"
                && kind == "approval"
                && message == "rejected escalate (step s3)"),
        "missing the rejection audit row"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn decide_by_someone_not_eligible_is_forbidden(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, &dir, alert, "s3").await;

    // erin is on call for `night`, so she IS eligible; frank is a plain member and is not.
    let err = approvals::decide(
        &pool,
        &SqlAlertOps,
        id,
        "user:frank",
        Decision::Approve,
        Via::Ui,
        None,
    )
    .await
    .expect_err("frank may not decide");
    assert!(
        matches!(err, DecideError::Forbidden(ref m) if m.contains("not eligible")),
        "{err:?}"
    );
    // A complete outsider neither.
    let err = approvals::decide(
        &pool,
        &SqlAlertOps,
        id,
        "user:ghost",
        Decision::Approve,
        Via::Sms,
        None,
    )
    .await
    .expect_err("ghost may not decide");
    assert!(
        matches!(err, DecideError::Forbidden(ref m) if m.contains("not eligible")),
        "{err:?}"
    );
    assert_eq!(approval_row(&pool, id).await.state, "pending");
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM action_executions WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("count executions");
    assert_eq!(executions, 0);
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_expired_approval_is_expired_and_frees_the_slot(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, &dir, alert, "s3").await;
    sqlx::query("UPDATE approvals SET expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .expect("backdate expiry");

    let err = approvals::decide(
        &pool,
        &SqlAlertOps,
        id,
        "user:carol",
        Decision::Approve,
        Via::Ui,
        None,
    )
    .await
    .expect_err("expired");
    assert_eq!(err, DecideError::Expired);
    assert_eq!(approval_row(&pool, id).await.state, "expired");

    // The expired request no longer blocks the step: a new request gets a fresh approval.
    let fresh = request_approval(&pool, &dir, alert, "s3").await;
    assert_ne!(fresh, id);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_repeated_decide_is_already_decided_and_idempotent(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, &dir, alert, "s3").await;

    approvals::decide(
        &pool,
        &SqlAlertOps,
        id,
        "user:carol",
        Decision::Approve,
        Via::Ui,
        None,
    )
    .await
    .expect("first approve");

    // Same decision again, or the opposite one: both are AlreadyDecided, no side effects.
    for decision in [Decision::Approve, Decision::Reject] {
        let err = approvals::decide(
            &pool,
            &SqlAlertOps,
            id,
            "user:carol",
            decision,
            Via::Ui,
            None,
        )
        .await
        .expect_err("repeat decide");
        assert_eq!(
            err,
            DecideError::AlreadyDecided("approved".into()),
            "{err:?}"
        );
    }
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM action_executions WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("count executions");
    assert_eq!(executions, 1, "no second execution");
    assert_eq!(approval_row(&pool, id).await.state, "approved");
}

#[sqlx::test(migrations = "../../migrations")]
async fn deciding_an_approval_of_a_resolved_alert_is_invalid(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, &dir, alert, "s3").await;
    common::update_alert_status(&pool, alert, "resolved").await;

    let err = approvals::decide(
        &pool,
        &SqlAlertOps,
        id,
        "user:carol",
        Decision::Approve,
        Via::Ui,
        None,
    )
    .await
    .expect_err("resolved alert");
    assert!(
        matches!(err, DecideError::InvalidTransition(ref m) if m.contains("resolved")),
        "{err:?}"
    );
    assert_eq!(approval_row(&pool, id).await.state, "pending");
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM action_executions WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("count executions");
    assert_eq!(executions, 0);
}

// ---------------------------------------------------------------------------------------------
// SMS replies
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn a_reply_with_the_right_code_approves_via_sms(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, &dir, alert, "s3").await;
    let code = prompt_code(&pool, alert, "carol").await;

    // Padded, lower-case form of the same instruction.
    let outcome = approvals::handle_reply(
        &pool,
        &dir,
        &SqlAlertOps,
        " +44 7700 900125 ",
        &format!(" y {code} "),
    )
    .await
    .expect("reply");
    assert_eq!(
        outcome,
        ReplyOutcome::Approved {
            approval_id: id,
            execution_id: outcome_id(&pool, id).await,
        }
    );

    let row = approval_row(&pool, id).await;
    assert_eq!(row.state, "approved");
    assert_eq!(row.decided_by.as_deref(), Some("user:carol"));
    assert_eq!(row.decided_via.as_deref(), Some("sms"));
    assert!(
        audit_messages(&pool, alert)
            .await
            .iter()
            .any(|(actor, via, kind, _)| actor == "user:carol"
                && via == "sms"
                && kind == "approval"),
        "the sms decide is audited like the ui one"
    );
    // A reject from the other eligible person is impossible now (already decided), and a
    // replay of the same code changes nothing.
    let outcome = approvals::handle_reply(
        &pool,
        &dir,
        &SqlAlertOps,
        "+44 7700 900124",
        &format!("NO {code}"),
    )
    .await
    .expect("replay");
    assert_eq!(outcome, ReplyOutcome::AlreadyDecided);
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM action_executions WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("count executions");
    assert_eq!(executions, 1, "no second execution from the replay");
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_reject_reply_rejects_without_an_execution(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, &dir, alert, "s3").await;
    let code = prompt_code(&pool, alert, "bob").await;

    let outcome = approvals::handle_reply(
        &pool,
        &dir,
        &SqlAlertOps,
        "+44 7700 900124",
        &format!("REJECT {code}"),
    )
    .await
    .expect("reply");
    assert_eq!(outcome, ReplyOutcome::Rejected { approval_id: id });
    assert_eq!(approval_row(&pool, id).await.state, "rejected");
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM action_executions WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("count executions");
    assert_eq!(executions, 0);
}

async fn outcome_id(pool: &PgPool, approval: Uuid) -> Uuid {
    sqlx::query_scalar::<_, Option<Uuid>>("SELECT execution_id FROM approvals WHERE id = $1")
        .bind(approval)
        .fetch_one(pool)
        .await
        .expect("read execution id")
        .expect("approved approvals store their execution")
}

#[sqlx::test(migrations = "../../migrations")]
async fn wrong_codes_burn_attempts_and_the_fifth_expires(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, &dir, alert, "s3").await;
    let code = prompt_code(&pool, alert, "carol").await;
    let wrong = wrong_code(&code);

    for attempt in 1..MAX_SMS_ATTEMPTS {
        let outcome = approvals::handle_reply(
            &pool,
            &dir,
            &SqlAlertOps,
            "+44 7700 900125",
            &format!("Y {wrong}"),
        )
        .await
        .expect("wrong reply");
        assert_eq!(outcome, ReplyOutcome::WrongCode, "attempt {attempt}");
        assert_eq!(approval_row(&pool, id).await.sms_attempts, attempt);
    }

    // The 5th wrong reply expires the request (and frees the step for a new one).
    let outcome = approvals::handle_reply(
        &pool,
        &dir,
        &SqlAlertOps,
        "+44 7700 900125",
        &format!("Y {wrong}"),
    )
    .await
    .expect("fifth wrong reply");
    assert_eq!(outcome, ReplyOutcome::TooManyAttempts);
    let row = approval_row(&pool, id).await;
    assert_eq!(row.state, "expired");
    assert_eq!(row.sms_attempts, MAX_SMS_ATTEMPTS);

    // A reply that is not an instruction burns nothing.
    let outcome = approvals::handle_reply(
        &pool,
        &dir,
        &SqlAlertOps,
        "+44 7700 900124",
        "what is this about?",
    )
    .await
    .expect("gibberish reply");
    assert_eq!(outcome, ReplyOutcome::WrongCode);
}

/// A pending approval row inserted directly with a chosen code (and a non-zero attempt
/// count), bypassing `request`'s collision-safe draw — the only way to force the states
/// `request` refuses to create.
async fn insert_pending_with_code(pool: &PgPool, alert: Uuid, step: &str, code: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO approvals \
            (id, alert_id, step_id, action, params, eligible, state, requested_by, \
             expires_at, sms_code_hash, sms_attempts) \
         VALUES ($1, $2, $3, 'escalate', '{}', \
                 ARRAY['user:carol','user:erin','user:bob'], 'pending', 'user:anna', \
                 now() + interval '15 minutes', $4, 2)",
    )
    .bind(id)
    .bind(alert)
    .bind(step)
    .bind(expected_hash(id, code))
    .execute(pool)
    .await
    .expect("insert pending approval");
    id
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_code_matching_two_pending_approvals_is_ambiguous_and_decides_nothing(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    // Two pending approvals eligible for carol, hashed for the SAME code: only direct
    // inserts can force this — `request` refuses to draw a taken code.
    let code = "4821";
    let first = insert_pending_with_code(&pool, alert, "s1", code).await;
    let second = insert_pending_with_code(&pool, alert, "s2", code).await;

    let outcome = approvals::handle_reply(
        &pool,
        &dir,
        &SqlAlertOps,
        "+44 7700 900125",
        &format!("Y {code}"),
    )
    .await
    .expect("reply");
    assert_eq!(outcome, ReplyOutcome::Ambiguous);

    // Neither request was decided, no attempt was burned, nothing was executed or audited.
    for id in [first, second] {
        let row = approval_row(&pool, id).await;
        assert_eq!(row.state, "pending", "approval {id}");
        assert_eq!(row.sms_attempts, 2, "no attempt burned on {id}");
    }
    let executions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM action_executions WHERE alert_id = $1")
            .bind(alert)
            .fetch_one(&pool)
            .await
            .expect("count executions");
    assert_eq!(executions, 0);
    assert!(
        audit_messages(&pool, alert).await.is_empty(),
        "no decision was audited"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_reply_from_an_unknown_number_is_unknown(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    request_approval(&pool, &dir, alert, "s3").await;
    let code = prompt_code(&pool, alert, "carol").await;

    let outcome = approvals::handle_reply(
        &pool,
        &dir,
        &SqlAlertOps,
        "+44 7700 999999",
        &format!("Y {code}"),
    )
    .await
    .expect("reply");
    assert!(
        matches!(outcome, ReplyOutcome::Unknown(ref m) if m.contains("phone")),
        "{outcome:?}"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn ack_acks_the_latest_open_alert_paged_on_the_number(pool: PgPool) {
    let dir = approvals_dir();
    let older = insert_alert(&pool, "open").await;
    let newer = insert_alert(&pool, "open").await;
    let acked = insert_alert(&pool, "acked").await;

    let mut tx = pool.begin().await.expect("tx");
    for (alert, recipient, offset) in [
        (older, "carol", "10 seconds"),
        (acked, "bob", "5 seconds"),
        (newer, "carol", "0 seconds"),
    ] {
        sqlx::query(
            "INSERT INTO delivery_jobs \
                (alert_id, notify_seq, recipient_id, channel, address, created_at, updated_at) \
             VALUES ($1, 1, $2, 'sms', '+44 7700 900125', \
                     now() - ($3)::interval, now() - ($3)::interval)",
        )
        .bind(alert)
        .bind(recipient)
        .bind(offset)
        .execute(&mut *tx)
        .await
        .expect("paging job");
    }
    tx.commit().await.expect("commit");

    let outcome = approvals::handle_reply(&pool, &dir, &SqlAlertOps, "+447700900125", "ack")
        .await
        .expect("ack reply");
    assert_eq!(outcome, ReplyOutcome::Acked { alert_id: newer });

    // The newest OPEN alert on that number was acked; the older and the already-acked
    // ones are untouched.
    let (status, acked_by): (String, Option<String>) =
        sqlx::query_as("SELECT status, acked_by FROM alerts WHERE id = $1")
            .bind(newer)
            .fetch_one(&pool)
            .await
            .expect("newer alert");
    assert_eq!(status, "acked");
    assert_eq!(acked_by.as_deref(), Some("user:carol"));
    for untouched in [older, acked] {
        let (status,): (String,) = sqlx::query_as("SELECT status FROM alerts WHERE id = $1")
            .bind(untouched)
            .fetch_one(&pool)
            .await
            .expect("untouched alert");
        assert_eq!(status, if untouched == acked { "acked" } else { "open" });
    }
    // `acked` via sms lands in the audit trail.
    assert!(
        audit_messages(&pool, newer)
            .await
            .iter()
            .any(|(actor, via, kind, _)| actor == "user:carol" && via == "sms" && kind == "acked"),
        "missing the acked audit row"
    );

    // The remaining open alert on that number is the older one: the next ACK takes it...
    let outcome = approvals::handle_reply(&pool, &dir, &SqlAlertOps, "+44 7700 900125", "ACK")
        .await
        .expect("second ack");
    assert_eq!(outcome, ReplyOutcome::Acked { alert_id: older });
    // ...and with nothing open left, the third has nothing to ack.
    let outcome = approvals::handle_reply(&pool, &dir, &SqlAlertOps, "+44 7700 900125", "ACK")
        .await
        .expect("third ack");
    assert!(
        matches!(outcome, ReplyOutcome::Unknown(ref m) if m.contains("no open alert")),
        "{outcome:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// The audit timeline (audit_log + derived escalation / digest entries)
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn timeline_entries_merge_audit_rows_escalations_and_digests(pool: PgPool) {
    let alert = insert_alert(&pool, "open").await;
    let mut tx = pool.begin().await.expect("tx");

    // One direct audit row, backdated so the derived entries sort after it.
    audit::record(
        &mut tx,
        "user:carol",
        "ui",
        Some(alert),
        "approval",
        "requested escalate (step s3)",
        json!({}),
    )
    .await
    .expect("audit row");
    sqlx::query(
        "UPDATE audit_log SET at = now() - interval '10 seconds' \
                 WHERE alert_id = $1 AND kind = 'approval'",
    )
    .bind(alert)
    .execute(&mut *tx)
    .await
    .expect("backdate audit row");

    // An escalation-chain step: phone + sms for carol in ONE tx (they share `now()`, so the
    // timeline collapses them into one entry).
    for channel in ["phone", "sms"] {
        sqlx::query(
            "INSERT INTO delivery_jobs \
                (alert_id, notify_seq, recipient_id, channel, address, escalation_step, \
                 route_info, created_at, updated_at) \
             VALUES ($1, 1, 'carol', $2, '+44 7700 900125', 1, \
                     $3, now() - interval '5 seconds', now() - interval '5 seconds')",
        )
        .bind(alert)
        .bind(channel)
        .bind(json!({ "target": "escalation", "via": "oncall:settlements-primary", "step": 0 }))
        .execute(&mut *tx)
        .await
        .expect("escalation jobs");
    }
    // An `escalate` action's job for dave.
    sqlx::query(
        "INSERT INTO delivery_jobs \
            (alert_id, notify_seq, recipient_id, channel, address, escalation_step, \
             route_info, created_at, updated_at) \
         VALUES ($1, 1, 'dave', 'email', 'dave@example.test', 100, \
                 $2, now() - interval '3 seconds', now() - interval '3 seconds')",
    )
    .bind(alert)
    .bind(json!({ "target": "escalate:counterparty-support" }))
    .execute(&mut *tx)
    .await
    .expect("escalate job");

    // A throttle digest job filed under this alert.
    sqlx::query(
        "INSERT INTO delivery_jobs \
            (alert_id, notify_seq, recipient_id, channel, address, route_info, \
             created_at, updated_at) \
         VALUES ($1, 1, 'bob', 'email', 'bob@example.test', $2, \
                 now() - interval '1 second', now() - interval '1 second')",
    )
    .bind(alert)
    .bind(json!({
        "target": "digest",
        "digest": { "count": 3, "rule_id": "settlements-workflow-failed",
                    "alert_ids": [alert.to_string()] },
    }))
    .execute(&mut *tx)
    .await
    .expect("digest job");
    tx.commit().await.expect("commit");

    let entries = audit::timeline_entries(&pool, alert)
        .await
        .expect("timeline");
    let rendered: Vec<String> = entries
        .iter()
        .map(|e| {
            format!(
                "{}|{}|{}|{}",
                e["actor"].as_str().unwrap_or_default(),
                e["kind"].as_str().unwrap_or_default(),
                e["message"].as_str().unwrap_or_default(),
                e["at"].as_str().unwrap_or_default(),
            )
        })
        .collect();

    // Oldest first: the audit row, then carol's collapsed escalation, dave's escalate, the
    // digest.
    let kinds: Vec<&str> = entries
        .iter()
        .map(|e| e["kind"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(
        kinds,
        ["approval", "escalated", "escalated", "digest"],
        "{rendered:?}"
    );

    let escalated_carol = &entries[1];
    assert_eq!(escalated_carol["actor"], json!("system"));
    assert_eq!(
        escalated_carol["message"],
        json!("escalated to carol via oncall:settlements-primary"),
        "{rendered:?}"
    );
    assert_eq!(
        entries[2]["message"],
        json!("escalated to dave (escalate:counterparty-support)"),
        "{rendered:?}"
    );
    assert!(
        entries[3]["message"]
            .as_str()
            .expect("digest message")
            .contains("digest delivered to bob: 3 suppressed notifications"),
        "{rendered:?}"
    );
    // `at` is RFC 3339 UTC, millisecond precision, `Z`.
    for entry in &entries {
        let at = entry["at"].as_str().expect("at");
        assert!(at.ends_with('Z') && at.contains('.'), "at format: {at}");
    }
    // Strictly ordered, oldest first.
    let ats: Vec<&str> = entries.iter().map(|e| e["at"].as_str().unwrap()).collect();
    let mut sorted = ats.clone();
    sorted.sort();
    assert_eq!(ats, sorted, "{rendered:?}");

    // Other alerts' entries never leak in.
    let other = insert_alert(&pool, "open").await;
    assert!(
        audit::timeline_entries(&pool, other)
            .await
            .expect("empty")
            .is_empty(),
        "an alert with no history has an empty timeline"
    );
}

/// Two approvals on ONE alert (different steps) both reach the same people: each person gets
/// the next free prompt slot (200, 201), so the second prompt is never swallowed by the
/// delivery-job idempotency key.
#[sqlx::test(migrations = "../../migrations")]
async fn two_approvals_on_one_alert_both_prompt_the_same_people(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    let first = request_approval(&pool, &dir, alert, "s3").await;
    let second = request_approval(&pool, &dir, alert, "s4").await;
    assert_ne!(first, second);

    let jobs = prompt_jobs(&pool, alert).await;
    for who in ["carol", "bob"] {
        let mut slots: Vec<_> = jobs
            .iter()
            .filter(|j| j.recipient_id == who)
            .map(|j| j.escalation_step)
            .collect();
        slots.sort();
        assert_eq!(slots, vec![Some(200), Some(201)], "{who}: {jobs:?}");
    }
    let ids: std::collections::HashSet<_> = jobs
        .iter()
        .map(|j| j.route_info["approval"]["id"].clone())
        .collect();
    assert_eq!(ids.len(), 2, "both approvals prompted: {ids:?}");
}

// ---------------------------------------------------------------------------------------------
// Self-approval (audit #11): the requester never decides, is never eligible, is never prompted.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn the_requester_is_never_eligible_and_never_prompted(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;

    // carol — the lead, and she has a phone — requests the approval herself.
    let mut tx = pool.begin().await.expect("tx");
    let id = approvals::request(
        &mut tx,
        &dir,
        alert,
        "s3",
        "escalate",
        &json!({ "team": "counterparty-support" }),
        "user:carol",
    )
    .await
    .expect("request");
    tx.commit().await.expect("commit");

    // Eligible is lead + on-call MINUS the requester.
    let row = approval_row(&pool, id).await;
    assert_eq!(
        row.eligible,
        vec!["user:erin", "user:bob"],
        "{:?}",
        row.eligible
    );

    // carol has a phone but is never prompted for her own request: only bob is (erin has none).
    let jobs = prompt_jobs(&pool, alert).await;
    assert_eq!(jobs.len(), 1, "{jobs:?}");
    assert_eq!(jobs[0].recipient_id, "bob");

    // And even a hand-planted eligible entry cannot let the requester decide: the decide
    // state machine refuses them before eligibility is even consulted.
    let planted = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO approvals \
            (id, alert_id, step_id, action, params, eligible, state, requested_by, \
             expires_at, sms_code_hash) \
         VALUES ($1, $2, 's9', 'escalate', '{}', ARRAY['user:carol'], 'pending', 'user:carol', \
                 now() + interval '15 minutes', $3)",
    )
    .bind(planted)
    .bind(alert)
    .bind(expected_hash(planted, "4821"))
    .execute(&pool)
    .await
    .expect("plant self-eligible approval");
    let err = approvals::decide(
        &pool,
        &SqlAlertOps,
        planted,
        "user:carol",
        Decision::Approve,
        Via::Ui,
        None,
    )
    .await
    .expect_err("self-approval");
    assert!(
        matches!(
            err,
            DecideError::Forbidden(ref m) if m == "the requester cannot approve their own request"
        ),
        "{err:?}"
    );
    assert_eq!(approval_row(&pool, planted).await.state, "pending");
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_sole_approver_request_fails_with_no_eligible_approver(pool: PgPool) {
    // A team whose only decider is its lead: when the lead requests, nobody is left.
    let dir = Directory::from_parts(
        vec![person(
            "solo",
            Some("solo@example.test"),
            Some("+44 7700 900999"),
        )],
        vec![Team {
            id: "solo-team".into(),
            members: vec!["solo".into()],
            lead: Some("solo".into()),
            oncall: HashMap::new(),
            default_channels: None,
        }],
    )
    .expect("directory builds");
    let alert = insert_alert_on_team(&pool, "open", "solo-team").await;

    let mut tx = pool.begin().await.expect("tx");
    let err = approvals::request(
        &mut tx,
        &dir,
        alert,
        "s3",
        "escalate",
        &json!({ "team": "counterparty-support" }),
        "user:solo",
    )
    .await
    .expect_err("nobody else can decide");
    assert!(matches!(err, RequestError::NoEligibleApprover), "{err:?}");
    tx.rollback().await.expect("rollback");
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM approvals WHERE alert_id = $1")
        .bind(alert)
        .fetch_one(&pool)
        .await
        .expect("count approvals");
    assert_eq!(rows, 0, "no approvals row was written");
    assert_eq!(
        prompt_jobs(&pool, alert).await.len(),
        0,
        "no prompt was sent"
    );
}

// ---------------------------------------------------------------------------------------------
// Scrubbing the code at rest (audit #11): the code leaves the DB with the decision.
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn approving_scrubs_the_code_from_jobs_and_receipts(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, &dir, alert, "s3").await;
    for job in prompt_jobs(&pool, alert).await {
        let code = job.route_info["approval"]["code"].as_str().expect("code");
        plant_receipt(&pool, &job, code).await;
    }

    approvals::decide(
        &pool,
        &SqlAlertOps,
        id,
        "user:carol",
        Decision::Approve,
        Via::Ui,
        None,
    )
    .await
    .expect("approve");

    assert_scrubbed(&pool, alert, id, 2).await;
    assert_eq!(approval_row(&pool, id).await.state, "approved");
}

#[sqlx::test(migrations = "../../migrations")]
async fn rejecting_scrubs_the_code_from_jobs_and_receipts(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, &dir, alert, "s3").await;
    for job in prompt_jobs(&pool, alert).await {
        let code = job.route_info["approval"]["code"].as_str().expect("code");
        plant_receipt(&pool, &job, code).await;
    }

    approvals::decide(
        &pool,
        &SqlAlertOps,
        id,
        "user:bob",
        Decision::Reject,
        Via::Sms,
        None,
    )
    .await
    .expect("reject");

    assert_scrubbed(&pool, alert, id, 2).await;
    assert_eq!(approval_row(&pool, id).await.state, "rejected");
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_fifth_wrong_attempt_expires_and_scrubs_the_code(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, &dir, alert, "s3").await;
    let code = prompt_code(&pool, alert, "carol").await;
    for job in prompt_jobs(&pool, alert).await {
        let code = job.route_info["approval"]["code"].as_str().expect("code");
        plant_receipt(&pool, &job, code).await;
    }
    let wrong = wrong_code(&code);

    for _ in 1..MAX_SMS_ATTEMPTS {
        let outcome = approvals::handle_reply(
            &pool,
            &dir,
            &SqlAlertOps,
            "+44 7700 900125",
            &format!("Y {wrong}"),
        )
        .await
        .expect("wrong reply");
        assert_eq!(outcome, ReplyOutcome::WrongCode);
    }
    // The first four wrong replies scrub nothing; the fifth expires and scrubs.
    assert!(
        prompt_jobs(&pool, alert).await[0].route_info["approval"]
            .get("code")
            .is_some(),
        "the code lives while the approval is pending"
    );
    let outcome = approvals::handle_reply(
        &pool,
        &dir,
        &SqlAlertOps,
        "+44 7700 900125",
        &format!("Y {wrong}"),
    )
    .await
    .expect("fifth wrong reply");
    assert_eq!(outcome, ReplyOutcome::TooManyAttempts);

    assert_scrubbed(&pool, alert, id, 2).await;
    assert_eq!(approval_row(&pool, id).await.state, "expired");
}

#[sqlx::test(migrations = "../../migrations")]
async fn expire_stale_expires_and_scrubs_only_lapsed_approvals(pool: PgPool) {
    let dir = approvals_dir();
    let alert = insert_alert(&pool, "open").await;
    let id = request_approval(&pool, &dir, alert, "s3").await;
    for job in prompt_jobs(&pool, alert).await {
        let code = job.route_info["approval"]["code"].as_str().expect("code");
        plant_receipt(&pool, &job, code).await;
    }
    // A second, live approval on another alert: the sweeper must not touch it.
    let other_alert = insert_alert(&pool, "open").await;
    let fresh = request_approval(&pool, &dir, other_alert, "s5").await;

    sqlx::query("UPDATE approvals SET expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .expect("backdate expiry");

    let expired = approvals::expire_stale(&pool).await.expect("expire_stale");
    assert_eq!(expired, 1);

    assert_eq!(approval_row(&pool, id).await.state, "expired");
    assert_scrubbed(&pool, alert, id, 2).await;
    // The live one is untouched: still pending, code still in its prompt jobs.
    let fresh_row = approval_row(&pool, fresh).await;
    assert_eq!(fresh_row.state, "pending");
    let fresh_jobs = prompt_jobs(&pool, other_alert).await;
    assert_eq!(fresh_jobs.len(), 2);
    assert!(
        fresh_jobs
            .iter()
            .all(|j| j.route_info["approval"].get("code").is_some()),
        "{fresh_jobs:?}"
    );

    // Nothing left to sweep.
    assert_eq!(
        approvals::expire_stale(&pool).await.expect("second sweep"),
        0
    );
}
