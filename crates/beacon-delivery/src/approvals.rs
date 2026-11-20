//! Approvals (SPEC §6.3): one state machine for every channel (UI button and mock SMS reply;
//! the signed email link is M7). Table `approvals`.
//!
//! - **Request** (`request`): called by the run-step API when the step needs approval
//!   (`approval: required`, or the action's risk is `high`). Inside the caller's tx: if a pending
//!   request exists for (alert, step), return it (no new SMS); else insert one with frozen
//!   action + params, `eligible` = the alert team's lead + the current on-call person of every
//!   schedule that team defines (`user:<id>`, deduplicated; the requester may approve only if
//!   eligible), `expires_at = now() + 15 min`, a random 4-digit code (`0000`–`9999`) stored only
//!   as `sms_code_hash = hex(sha256(id || ":" || code))`. Then send the prompt: one `sms` job
//!   per eligible person with a phone, via `router::insert_jobs`, `route_info = {"target":
//!   "approval", "approval": {"id", "code", "step_id", "action"}}` (the code is in the job so
//!   the SMS can carry it; `GatewayChannel::render` turns it into `... Reply "Y 4821" to
//!   approve, "N 4821" to reject.` and sets `ProviderRequest.approval_challenge = "Y 4821"`),
//!   `notify_seq` = the alert's current `notify_seq`, `escalation_step = 200 + n` (unique key).
//!   Audit `approval` "requested <action> (step <id>)".
//!
//!   The old schema-forced deviation is gone: the unique key now includes `escalation_step`,
//!   so a prompt for someone this notification already paged on sms (the demo's on-call bob)
//!   simply reuses the alert's current `notify_seq` — the step column keeps it a second job
//!   next to the original page (step 0), and a later real renotify is never swallowed.
//! - **Decide** (`decide`): ONE tx: `SELECT ... FOR UPDATE`; not found → `NotFound`; actor not in
//!   `eligible` → `Forbidden`; state not `pending` → `AlreadyDecided(state)`; `expires_at <=
//!   now()` → set `expired`, `Expired`; alert not `open` (resolved) → `InvalidTransition`.
//!   Approve: `state='approved'`, `decided_*`, queue the execution (`actions::queue` with
//!   `approval_id`), store `execution_id`, audit, `AlertOps::touch`. Reject: `state='rejected'`,
//!   audit, touch.
//! - **SMS reply** (`handle_reply`): `from` (E.164, compare digits only) → the person in the
//!   Directory with that phone; unknown → `Unknown`. Text, trimmed, case-insensitive:
//!   `Y <code>` | `YES <code>` | `APPROVE <code>` → approve; `N <code>` | `NO <code>` |
//!   `REJECT <code>` → reject; `ACK` → ack the most recent `open` alert that sent an sms/phone job
//!   to that number (`AlertOps::transition(Ack)`, audit `acked` via sms). For a code: find the
//!   pending approvals where that person is eligible; match `sha256(id:code)` against each; on a
//!   match decide via the same path as the UI (`via = sms`); on no match increment
//!   `sms_attempts` of those pending approvals and expire any that reach 5 (`TooManyAttempts`).
//!   A repeated reply for an already-decided approval returns `AlreadyDecided` (idempotent, no
//!   side effects); so does a code that matches an approval that is no longer pending
//!   (approved/rejected/expired).

use chrono::{DateTime, Utc};
use rand::Rng;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::alert_ops::{AlertOps, Transition, TransitionOutcome};
use crate::audit;
use crate::config::Directory;
use crate::throttle::PlannedJob;

pub const APPROVAL_TTL: std::time::Duration = std::time::Duration::from_secs(15 * 60);
pub const MAX_SMS_ATTEMPTS: i32 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Approve,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Via {
    Ui,
    Sms,
}

impl Via {
    pub fn as_str(self) -> &'static str {
        match self {
            Via::Ui => "ui",
            Via::Sms => "sms",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecideOutcome {
    Approved { execution_id: Uuid },
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecideError {
    #[error("approval not found")]
    NotFound,
    #[error("not eligible to decide this approval")]
    Forbidden,
    #[error("approval already {0}")]
    AlreadyDecided(String),
    #[error("approval expired")]
    Expired,
    #[error("{0}")]
    InvalidTransition(String),
    #[error("database error: {0}")]
    Db(String),
}

/// The outcome of the decide state machine before it is mapped to the public
/// [`DecideError`]/[`DecideOutcome`] types. Shared by the UI and SMS paths so both channels
/// run exactly the same checks and writes inside one transaction.
enum Decided {
    NotFound,
    Forbidden,
    /// The request was already approved/rejected; carries that state.
    NotPending(String),
    /// `state = 'expired'` was written in the tx: commit it.
    Expired,
    Invalid(String),
    Applied(DecideOutcome),
}

/// `user:<id>` of everyone who may decide this approval: the alert team's lead plus the
/// current on-call person of every schedule that team defines, deduplicated, lead first and
/// schedules in id order so the array is stable.
fn eligible_ids(dir: &Directory, team_id: &str) -> Vec<String> {
    fn push_unique(out: &mut Vec<String>, id: &str) {
        let entry = format!("user:{id}");
        if !out.contains(&entry) {
            out.push(entry);
        }
    }
    let mut out = Vec::new();
    let Some(team) = dir.teams.get(team_id) else {
        return out;
    };
    if let Some(lead) = &team.lead {
        push_unique(&mut out, lead);
    }
    let mut schedules: Vec<&String> = team.oncall.keys().collect();
    schedules.sort();
    for schedule in schedules {
        if let Some(on_call) = team.oncall.get(schedule).and_then(|r| r.first()) {
            push_unique(&mut out, on_call);
        }
    }
    out
}

/// A random 4-digit code, `0000`–`9999`.
fn generate_code() -> String {
    let n: u32 = rand::rng().random_range(0..10_000);
    format!("{n:04}")
}

/// `hex(sha256("<approval id>:<code>"))` — the only form of the code the database ever sees.
fn code_hash(id: Uuid, code: &str) -> String {
    hex::encode(Sha256::digest(format!("{id}:{code}").as_bytes()))
}

/// Create (or return the pending) approval request; returns its id. See module doc.
pub async fn request(
    tx: &mut Transaction<'static, Postgres>,
    dir: &Directory,
    alert_id: Uuid,
    step_id: &str,
    action: &str,
    params: &Value,
    requested_by: &str,
) -> Result<Uuid, sqlx::Error> {
    // Idempotent per (alert, step): the pending request is returned as-is, no new SMS.
    if let Some((existing,)) = sqlx::query_as(
        "SELECT id FROM approvals WHERE alert_id = $1 AND step_id = $2 AND state = 'pending'",
    )
    .bind(alert_id)
    .bind(step_id)
    .fetch_optional(&mut **tx)
    .await?
    {
        return Ok(existing);
    }

    let row: Option<(String, i32)> =
        sqlx::query_as("SELECT team, notify_seq FROM alerts WHERE id = $1")
            .bind(alert_id)
            .fetch_optional(&mut **tx)
            .await?;
    let Some((team, notify_seq)) = row else {
        return Err(sqlx::Error::RowNotFound);
    };

    let eligible = eligible_ids(dir, &team);
    let id = Uuid::new_v4();
    let code = generate_code();
    let hash = code_hash(id, &code);

    sqlx::query(
        "INSERT INTO approvals \
            (id, alert_id, step_id, action, params, eligible, state, requested_by, \
             expires_at, sms_code_hash) \
         VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, \
                 now() + make_interval(secs => $8), $9)",
    )
    .bind(id)
    .bind(alert_id)
    .bind(step_id)
    .bind(action)
    .bind(params)
    .bind(&eligible)
    .bind(requested_by)
    .bind(APPROVAL_TTL.as_secs() as i64)
    .bind(&hash)
    .execute(&mut **tx)
    .await?;

    // The SMS prompt: one sms job per eligible person with a phone. The code travels in the
    // job (the SMS must carry it); the table keeps only its hash.
    let mut n = 0i32;
    for who in &eligible {
        let Some(person_id) = who.strip_prefix("user:") else {
            continue;
        };
        let Some(person) = dir.people.get(person_id) else {
            continue;
        };
        let Some(phone) = person.contact.phone.clone() else {
            continue;
        };
        let route_info = json!({
            "target": "approval",
            "approval": {
                "id": id,
                "code": code,
                "step_id": step_id,
                "action": action,
            },
        });
        crate::router::insert_jobs(
            tx,
            alert_id,
            notify_seq,
            person_id,
            &[PlannedJob {
                channel: "sms".to_string(),
                address: phone,
                route_info,
            }],
            Some(200 + n),
        )
        .await?;
        n += 1;
    }

    audit::record(
        tx,
        requested_by,
        "ui",
        Some(alert_id),
        "approval",
        &format!("requested {action} (step {step_id})"),
        json!({ "approval_id": id }),
    )
    .await?;
    Ok(id)
}

/// One locked `approvals` row as `decide_in_tx` reads it.
#[derive(sqlx::FromRow)]
struct ApprovalRow {
    alert_id: Uuid,
    step_id: String,
    action: String,
    params: Value,
    eligible: Vec<String>,
    state: String,
    expires_at: DateTime<Utc>,
}

/// The decide state machine inside the caller's transaction: lock the approval row, run the
/// checks in the module doc's order, then write the decision, the execution intent, the audit
/// row and the alert version bump — all or nothing.
async fn decide_in_tx(
    tx: &mut Transaction<'static, Postgres>,
    alert_ops: &dyn AlertOps,
    approval_id: Uuid,
    actor: &str,
    decision: Decision,
    via: Via,
    note: Option<&str>,
) -> Result<Decided, sqlx::Error> {
    let row = sqlx::query_as::<_, ApprovalRow>(
        "SELECT alert_id, step_id, action, params, eligible, state, expires_at \
         FROM approvals WHERE id = $1 FOR UPDATE",
    )
    .bind(approval_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(ApprovalRow {
        alert_id,
        step_id,
        action,
        params,
        eligible,
        state,
        expires_at,
    }) = row
    else {
        return Ok(Decided::NotFound);
    };
    if !eligible.iter().any(|e| e == actor) {
        return Ok(Decided::Forbidden);
    }
    if state != "pending" {
        return Ok(Decided::NotPending(state));
    }
    if expires_at <= Utc::now() {
        // The request lapsed without a decision; free the (alert, step) slot for a new one.
        sqlx::query("UPDATE approvals SET state = 'expired', updated_at = now() WHERE id = $1")
            .bind(approval_id)
            .execute(&mut **tx)
            .await?;
        return Ok(Decided::Expired);
    }
    // Revalidate the alert before claiming the execution (SPEC §6.3). Resolved alerts are
    // closed; acked alerts are being worked on and stay decidable.
    let status: Option<(String,)> = sqlx::query_as("SELECT status FROM alerts WHERE id = $1")
        .bind(alert_id)
        .fetch_optional(&mut **tx)
        .await?;
    match status {
        Some((s,)) if s != "resolved" => {}
        Some(_) => return Ok(Decided::Invalid("alert is resolved".into())),
        None => return Ok(Decided::Invalid("alert not found".into())),
    }

    let via_str = via.as_str();
    let (new_state, message, execution_id) = match decision {
        Decision::Approve => {
            let Some(execution_id) = crate::actions::queue(
                tx,
                alert_id,
                &step_id,
                &action,
                &params,
                actor,
                Some(approval_id),
            )
            .await?
            else {
                return Ok(Decided::Invalid(
                    "execution already queued or running".into(),
                ));
            };
            (
                "approved",
                format!("approved {action} (step {step_id})"),
                Some(execution_id),
            )
        }
        Decision::Reject => (
            "rejected",
            format!("rejected {action} (step {step_id})"),
            None,
        ),
    };

    sqlx::query(
        "UPDATE approvals \
         SET state = $2, decided_by = $3, decided_via = $4, decided_at = now(), \
             note = $5, execution_id = $6, updated_at = now() \
         WHERE id = $1",
    )
    .bind(approval_id)
    .bind(new_state)
    .bind(actor)
    .bind(via_str)
    .bind(note)
    .bind(execution_id)
    .execute(&mut **tx)
    .await?;
    audit::record(
        tx,
        actor,
        via_str,
        Some(alert_id),
        "approval",
        &message,
        json!({ "approval_id": approval_id }),
    )
    .await?;
    alert_ops.touch(tx, alert_id).await?;
    Ok(Decided::Applied(match decision {
        Decision::Approve => DecideOutcome::Approved {
            execution_id: execution_id.unwrap_or_default(),
        },
        Decision::Reject => DecideOutcome::Rejected,
    }))
}

pub async fn decide(
    pool: &PgPool,
    alert_ops: &dyn AlertOps,
    approval_id: Uuid,
    actor: &str,
    decision: Decision,
    via: Via,
    note: Option<&str>,
) -> Result<DecideOutcome, DecideError> {
    let db = |e: sqlx::Error| DecideError::Db(e.to_string());
    let mut tx = pool.begin().await.map_err(db)?;
    match decide_in_tx(&mut tx, alert_ops, approval_id, actor, decision, via, note).await {
        // The expired path wrote `state = 'expired'`: commit so it sticks.
        Ok(Decided::Expired) => {
            tx.commit().await.map_err(db)?;
            Err(DecideError::Expired)
        }
        Ok(Decided::Applied(outcome)) => {
            tx.commit().await.map_err(db)?;
            Ok(outcome)
        }
        Ok(Decided::NotFound) => {
            let _ = tx.rollback().await;
            Err(DecideError::NotFound)
        }
        Ok(Decided::Forbidden) => {
            let _ = tx.rollback().await;
            Err(DecideError::Forbidden)
        }
        Ok(Decided::NotPending(state)) => {
            let _ = tx.rollback().await;
            Err(DecideError::AlreadyDecided(state))
        }
        Ok(Decided::Invalid(m)) => {
            let _ = tx.rollback().await;
            Err(DecideError::InvalidTransition(m))
        }
        Err(e) => {
            let _ = tx.rollback().await;
            Err(DecideError::Db(e.to_string()))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyOutcome {
    Approved {
        approval_id: Uuid,
        execution_id: Uuid,
    },
    Rejected {
        approval_id: Uuid,
    },
    Acked {
        alert_id: Uuid,
    },
    AlreadyDecided,
    WrongCode,
    TooManyAttempts,
    Unknown(String),
}

pub async fn handle_reply(
    pool: &PgPool,
    dir: &Directory,
    alert_ops: &dyn AlertOps,
    from: &str,
    text: &str,
) -> Result<ReplyOutcome, sqlx::Error> {
    let Some(person) = person_by_phone(dir, from) else {
        return Ok(ReplyOutcome::Unknown(
            "no person in the directory has this phone number".into(),
        ));
    };
    let actor = format!("user:{}", person.id);
    let trimmed = text.trim();

    // `ACK` acks the most recent open alert this number was paged on.
    if trimmed.eq_ignore_ascii_case("ack") {
        return ack_latest(pool, alert_ops, &actor, from).await;
    }

    // `Y 4821` / `NO 4821` / ... decides the approval the code belongs to.
    if let Some((decision, code)) = parse_decision(trimmed) {
        return code_decision(pool, alert_ops, &actor, decision, &code).await;
    }

    // Anything else is not an instruction; no approval burns an attempt for it.
    Ok(ReplyOutcome::WrongCode)
}

/// `Y <code>` | `YES <code>` | `APPROVE <code>` → approve, `N`/`NO`/`REJECT` → reject.
/// Case-insensitive; words after the code are ignored (SMS drafts get messy).
fn parse_decision(text: &str) -> Option<(Decision, String)> {
    let mut words = text.split_whitespace();
    let first = words.next()?.to_ascii_lowercase();
    let decision = match first.as_str() {
        "y" | "yes" | "approve" => Decision::Approve,
        "n" | "no" | "reject" => Decision::Reject,
        _ => return None,
    };
    let code = words.next()?.to_string();
    Some((decision, code))
}

fn digits_only(s: &str) -> String {
    s.chars().filter(|c| c.is_ascii_digit()).collect()
}

/// The person in the Directory whose phone matches `from`, comparing digits only (a `+`, a
/// leading `00` country-code form or stray spaces must not stop a reply from being recognized).
/// Shared numbers resolve to the smallest user id, deterministically.
fn person_by_phone<'a>(dir: &'a Directory, from: &str) -> Option<&'a crate::config::Person> {
    let digits = digits_only(from);
    if digits.is_empty() {
        return None;
    }
    let mut matches: Vec<_> = dir
        .people
        .values()
        .filter(|p| {
            p.contact
                .phone
                .as_deref()
                .is_some_and(|ph| digits_only(ph) == digits)
        })
        .collect();
    matches.sort_by(|a, b| a.id.cmp(&b.id));
    matches.into_iter().next()
}

/// Decide one approval from an SMS code reply: the same tx as the UI decide, with the
/// attempt accounting of the module doc around it.
async fn code_decision(
    pool: &PgPool,
    alert_ops: &dyn AlertOps,
    actor: &str,
    decision: Decision,
    code: &str,
) -> Result<ReplyOutcome, sqlx::Error> {
    let mut tx = pool.begin().await?;
    // Lock every pending approval this person may decide; the reply decides exactly one.
    let pendings: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT id, sms_code_hash FROM approvals \
         WHERE state = 'pending' AND $1 = ANY(eligible) FOR UPDATE",
    )
    .bind(actor)
    .fetch_all(&mut *tx)
    .await?;

    for (id, hash) in &pendings {
        if code_hash(*id, code) == *hash {
            // The same decide path as the UI (`via = sms`), in this transaction.
            return match decide_in_tx(&mut tx, alert_ops, *id, actor, decision, Via::Sms, None)
                .await
            {
                Ok(Decided::Applied(DecideOutcome::Approved { execution_id })) => {
                    tx.commit().await?;
                    Ok(ReplyOutcome::Approved {
                        approval_id: *id,
                        execution_id,
                    })
                }
                Ok(Decided::Applied(DecideOutcome::Rejected)) => {
                    tx.commit().await?;
                    Ok(ReplyOutcome::Rejected { approval_id: *id })
                }
                // Right code, no longer actionable: keep the expired state, report
                // idempotently (the outcome set has no expired variant).
                Ok(Decided::Expired) => {
                    tx.commit().await?;
                    Ok(ReplyOutcome::AlreadyDecided)
                }
                Ok(Decided::NotPending(_)) => {
                    let _ = tx.rollback().await;
                    Ok(ReplyOutcome::AlreadyDecided)
                }
                Ok(Decided::Invalid(m)) => {
                    let _ = tx.rollback().await;
                    Ok(ReplyOutcome::Unknown(m))
                }
                // Unreachable: the row was selected as this person's pending approval.
                Ok(Decided::NotFound | Decided::Forbidden) => {
                    let _ = tx.rollback().await;
                    Ok(ReplyOutcome::WrongCode)
                }
                Err(e) => {
                    let _ = tx.rollback().await;
                    Err(e)
                }
            };
        }
    }

    // No pending match: a code belonging to an already-decided (or expired) approval is
    // idempotently "already decided", not an attempt.
    let decided: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT id, sms_code_hash FROM approvals \
         WHERE state <> 'pending' AND $1 = ANY(eligible)",
    )
    .bind(actor)
    .fetch_all(&mut *tx)
    .await?;
    if decided
        .iter()
        .any(|(id, hash)| code_hash(*id, code) == *hash)
    {
        let _ = tx.rollback().await;
        return Ok(ReplyOutcome::AlreadyDecided);
    }

    // A wrong code burns one SMS attempt on every pending approval of this person; the one
    // that reaches [`MAX_SMS_ATTEMPTS`] expires (and frees its (alert, step) slot).
    if pendings.is_empty() {
        let _ = tx.rollback().await;
        return Ok(ReplyOutcome::WrongCode);
    }
    let ids: Vec<Uuid> = pendings.iter().map(|(id, _)| *id).collect();
    sqlx::query(
        "UPDATE approvals SET sms_attempts = sms_attempts + 1, updated_at = now() \
         WHERE id = ANY($1)",
    )
    .bind(&ids)
    .execute(&mut *tx)
    .await?;
    let expired: Vec<Uuid> = sqlx::query_scalar(
        "UPDATE approvals SET state = 'expired', updated_at = now() \
         WHERE id = ANY($1) AND sms_attempts >= $2 RETURNING id",
    )
    .bind(&ids)
    .bind(MAX_SMS_ATTEMPTS)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    if expired.is_empty() {
        Ok(ReplyOutcome::WrongCode)
    } else {
        Ok(ReplyOutcome::TooManyAttempts)
    }
}

/// `ACK` from a known number: ack the most recent `open` alert that sent an sms/phone job to
/// that number, audit `acked` via sms.
async fn ack_latest(
    pool: &PgPool,
    alert_ops: &dyn AlertOps,
    actor: &str,
    from: &str,
) -> Result<ReplyOutcome, sqlx::Error> {
    let digits = digits_only(from);
    let latest: Option<(Uuid,)> = sqlx::query_as(
        "SELECT a.id FROM alerts a \
         JOIN delivery_jobs j ON j.alert_id = a.id \
         WHERE a.status = 'open' AND j.channel IN ('sms', 'phone') \
           AND regexp_replace(j.address, '[^0-9]', '', 'g') = $1 \
         ORDER BY j.created_at DESC, j.id DESC LIMIT 1",
    )
    .bind(&digits)
    .fetch_optional(pool)
    .await?;
    let Some((alert_id,)) = latest else {
        return Ok(ReplyOutcome::Unknown(
            "no open alert has paged this number".into(),
        ));
    };

    let mut tx = pool.begin().await?;
    match alert_ops
        .transition(&mut tx, alert_id, Transition::Ack, actor)
        .await?
    {
        TransitionOutcome::Applied | TransitionOutcome::Unchanged => {
            audit::record(
                &mut tx,
                actor,
                "sms",
                Some(alert_id),
                "acked",
                "acked by SMS reply",
                json!({}),
            )
            .await?;
            tx.commit().await?;
            Ok(ReplyOutcome::Acked { alert_id })
        }
        TransitionOutcome::Invalid(m) => {
            let _ = tx.rollback().await;
            Ok(ReplyOutcome::Unknown(m))
        }
    }
}
