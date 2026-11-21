//! Approvals (SPEC §6.3): one state machine for every channel (UI button and mock SMS reply;
//! the signed email link is M7). Table `approvals`.
//!
//! - **Request** (`request`): called by the run-step API when the step needs approval
//!   (`approval: required`, or the action's risk is `high`). Inside the caller's tx: if a pending
//!   request exists for (alert, step), return it (no new SMS); else insert one with frozen
//!   action + params, `eligible` = the alert team's lead + the current on-call person of every
//!   schedule that team defines (`user:<id>`, deduplicated) **minus the requester** — nobody may
//!   decide their own request (audit #11 self-approval), so the requester is never prompted and
//!   never eligible; if that leaves nobody at all, `request` fails with
//!   `RequestError::NoEligibleApprover`. `expires_at = now() + 15 min`, a random 4-digit code
//!   (`0000`–`9999`) stored only
//!   as `sms_code_hash = hex(sha256(id || ":" || code))`. Race-safe per (alert, step):
//!   inside the same tx, first take `pg_advisory_xact_lock` on the request key
//!   (`'approval-req:' || alert || ':' || step`), then re-check for a pending row — the
//!   loser returns the winner's id and sends no prompt SMS (one global lock order:
//!   request lock, then the person locks below, so concurrent requests cannot deadlock).
//!   The draw is collision-safe: inside the
//!   same tx, take `pg_advisory_xact_lock(hashtext('approval-code:' || person))` per eligible
//!   person (persons sorted, so concurrent requests lock in one order and cannot deadlock),
//!   load the pending, unexpired approvals whose `eligible` overlaps this one's
//!   (`eligible && $1::text[]`), and redraw while the candidate hashes equal any of their
//!   `sms_code_hash` (up to 50 tries, then `RequestError::NoFreeCode`): two pending approvals
//!   that share an eligible person never share a code, so an SMS reply can never decide the
//!   wrong request. Then send the prompt: one `sms` job
//!   per eligible person with a phone, via `router::insert_jobs`, `route_info = {"target":
//!   "approval", "approval": {"id", "code", "step_id", "action"}}` (the code is in the job so
//!   the SMS can carry it; `GatewayChannel::render` turns it into `... Reply "Y 4821" to
//!   approve, "N 4821" to reject.` and sets `ProviderRequest.approval_challenge = "Y 4821"`),
//!   `notify_seq` = the alert's current `notify_seq`, `escalation_step` = the person's next free slot >= 200 on this notification (unique key; several approvals on one alert each reach the same person).
//!   Audit `approval` "requested <action> (step <id>)".
//!
//!   The old schema-forced deviation is gone: the unique key now includes `escalation_step`,
//!   so a prompt for someone this notification already paged on sms (the demo's on-call bob)
//!   simply reuses the alert's current `notify_seq` — the step column keeps it a second job
//!   next to the original page (step 0), and a later real renotify is never swallowed.
//! - **Decide** (`decide`): ONE tx: `SELECT ... FOR UPDATE`; not found → `NotFound`; the actor
//!   is the requester → `Forbidden("the requester cannot approve their own request")` (audit
//!   #11: even when a foreign row lists the requester as eligible, they may not decide it);
//!   actor not in `eligible` → `Forbidden`; state not `pending` → `AlreadyDecided(state)`;
//!   `expires_at <= now()` → set `expired`, `Expired`; alert not `open` (resolved) →
//!   `InvalidTransition`.
//!   Approve: `state='approved'`, `decided_*`, queue the execution (`actions::queue` with
//!   `approval_id`), store `execution_id`, audit, `AlertOps::touch`. Reject: `state='rejected'`,
//!   audit, touch. Whichever way an approval leaves `pending` here (approved, rejected or
//!   expired), the same tx scrubs its plaintext code at rest (below).
//! - **SMS reply** (`handle_reply`): `from` (E.164, compare digits only) → the person in the
//!   Directory with that phone; unknown → `Unknown`. Text, trimmed, case-insensitive:
//!   `Y <code>` | `YES <code>` | `APPROVE <code>` → approve; `N <code>` | `NO <code>` |
//!   `REJECT <code>` → reject; `ACK` → ack the alert of the newest sms/phone job `sent` to
//!   that number — an unsent (pending/cancelled/dead) page never counts, the phone never
//!   rang (`AlertOps::transition(Ack)`, audit `acked` via sms). A repeated `ACK` is stable:
//!   when the newest paged alert is already acked it answers `AlreadyDecided` instead of
//!   advancing to an older open alert. For a code: find the
//!   pending approvals where that person is eligible; match `sha256(id:code)` against each;
//!   exactly one match decides via the same path as the UI (`via = sms`); more than one match
//!   (only rows inserted by hand can force it — the collision-safe draw above refuses to
//!   create it) is `Ambiguous`: nothing is decided, no attempt is burned, the sender picks
//!   the request in the UI; on no match increment
//!   `sms_attempts` of those pending approvals and expire any that reach 5 (`TooManyAttempts`).
//!   A repeated reply for an already-decided approval returns `AlreadyDecided` (idempotent, no
//!   side effects); so does a code that matches an approval that is no longer actionable
//!   (approved/rejected/expired, or pending past its expiry). Only pending approvals that have
//!   not expired are candidates for a code (audit #11: an expired approval sharing the code must
//!   not make the reply ambiguous); the candidate scan is bounded (`LIMIT 200`) and locks rows in
//!   a fixed order (`ORDER BY id FOR UPDATE`) so concurrent replies cannot deadlock, and hashes
//!   are compared in constant time.
//!
//! **Codes never survive at rest after the decision (audit #11).** The plaintext code exists in
//! exactly two places outside the `approvals` row's hash: the prompt jobs'
//! `delivery_jobs.route_info.approval.code` (the SMS must carry it) and the gateway's
//! `gateway_receipts.text`/`approval_challenge` once the provider accepted the prompt. The
//! moment an approval stops being `pending` — approved or rejected in `decide_in_tx`, expired
//! on a decide, expired by the 5-wrong-attempt rule, or swept by [`expire_stale`] — the same
//! transaction runs [`scrub_code`]: the prompt jobs' `route_info` drops `approval.code`
//! (`route_info #- '{approval,code}'`, `updated_at = now()`) and the matching `gateway_receipts`
//! rows lose `approval_challenge` and have every 4-digit run in `text` masked to `****`. A
//! prompt job still pending at that point simply renders as a plain alert SMS
//! (`channel::gateway_client` treats a codeless approval object as a non-prompt).

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
    /// Why the actor may not decide: not eligible, or the requester (self-approval, audit #11).
    #[error("{0}")]
    Forbidden(String),
    #[error("approval already {0}")]
    AlreadyDecided(String),
    #[error("approval expired")]
    Expired,
    #[error("{0}")]
    InvalidTransition(String),
    #[error("database error: {0}")]
    Db(String),
}

/// `request` fails when the database does, when no free code is left: with 10,000 codes,
/// 50 redraws without a hit mean ~9,990 live codes already share an eligible person with
/// this request — or when the requester is the only person who could decide (audit #11:
/// nobody may approve their own request).
#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    #[error("no free approval code after 50 tries")]
    NoFreeCode,
    #[error("no one else can approve this step")]
    NoEligibleApprover,
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

/// The outcome of the decide state machine before it is mapped to the public
/// [`DecideError`]/[`DecideOutcome`] types. Shared by the UI and SMS paths so both channels
/// run exactly the same checks and writes inside one transaction.
enum Decided {
    NotFound,
    /// Why the actor may not decide (not eligible / the requester).
    Forbidden(String),
    /// The request was already approved/rejected; carries that state.
    NotPending(String),
    /// `state = 'expired'` was written in the tx: commit it.
    Expired,
    Invalid(String),
    Applied(DecideOutcome),
}

/// `user:<id>` of everyone who may decide an approval of `team_id`: the alert team's lead plus
/// the current on-call person of every schedule that team defines, deduplicated, lead first and
/// schedules in id order so the array is stable. The requester is removed by `request` (audit
/// #11: they may never decide their own request).
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

/// Constant-time byte equality for comparing code hashes (audit #11): the accumulate-then-
/// compare loop takes the same path for every input, so a mismatch never reveals how many
/// bytes matched. Both sides are fixed-length hex digests, so the length check is not a
/// useful signal.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |diff, (x, y)| diff | (x ^ y)) == 0
}

/// Scrub one approval's plaintext code from everywhere it lives at rest (audit #11), inside
/// the caller's transaction: the prompt jobs' `route_info` loses `approval.code` (the rest of
/// the approval object stays, so receipts can still be matched later), and the gateway
/// receipts of those jobs lose `approval_challenge` and have every 4-digit run in `text`
/// masked to `****` (the rendered prompt names the code twice, `Y <code>`/`N <code>`). Called
/// whenever an approval leaves `pending`: `decide_in_tx` (approved/rejected/expired), the
/// 5-wrong-attempt expiry in `code_decision`, and `expire_stale`.
async fn scrub_code(
    tx: &mut Transaction<'_, Postgres>,
    approval_id: Uuid,
) -> Result<(), sqlx::Error> {
    let id = approval_id.to_string();
    // The prompt jobs of THIS approval; #- deletes only the code key.
    sqlx::query(
        "UPDATE delivery_jobs \
         SET route_info = route_info #- '{approval,code}', updated_at = now() \
         WHERE route_info->>'target' = 'approval' \
           AND route_info->'approval'->>'id' = $1",
    )
    .bind(&id)
    .execute(&mut **tx)
    .await?;
    // The receipts the gateway wrote for exactly those jobs (it keys them by job id).
    sqlx::query(
        "UPDATE gateway_receipts r \
         SET approval_challenge = NULL, \
             text = regexp_replace(text, '\\d{4}', '****', 'g') \
         WHERE r.job_id IN (SELECT id FROM delivery_jobs \
                            WHERE route_info->>'target' = 'approval' \
                              AND route_info->'approval'->>'id' = $1)",
    )
    .bind(&id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Expire every pending approval past `expires_at` and scrub its code at rest, in one tx
/// (audit #11). Returns how many approvals expired. The actions worker calls this once per
/// loop iteration — a single indexed UPDATE, cheap when there is nothing to do — so a request
/// nobody decided never leaves its code in `delivery_jobs`/`gateway_receipts` forever.
pub async fn expire_stale(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let expired: Vec<Uuid> = sqlx::query_scalar(
        "UPDATE approvals SET state = 'expired', updated_at = now() \
         WHERE state = 'pending' AND expires_at <= now() \
         RETURNING id",
    )
    .fetch_all(&mut *tx)
    .await?;
    for id in &expired {
        scrub_code(&mut tx, *id).await?;
    }
    tx.commit().await?;
    Ok(expired.len() as u64)
}

/// How many times `request` redraws before giving up on a code that no live approval of the
/// same persons already hashes to.
const MAX_CODE_DRAWS: usize = 50;

/// A random 4-digit code that hashes differently from every `(id, sms_code_hash)` in
/// `taken` — at most [`MAX_CODE_DRAWS`] draws, then `None` (the caller errors out).
fn draw_unique_code(taken: &[(Uuid, String)]) -> Option<String> {
    (0..MAX_CODE_DRAWS)
        .map(|_| generate_code())
        .find(|candidate| {
            !taken
                .iter()
                .any(|(id, hash)| code_hash(*id, candidate) == *hash)
        })
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
) -> Result<Uuid, RequestError> {
    // Race-safe per (alert, step): serialize concurrent requests for the same slot FIRST,
    // before the per-person code locks below (global lock order: request lock, then the
    // person locks sorted) so two callers can never both decide "no pending approval
    // exists" and race on the insert. The check below is the re-check under the lock; the
    // loser returns the winner's id and sends no prompt SMS of its own.
    let lock_key = format!("approval-req:{alert_id}:{step_id}");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
        .bind(&lock_key)
        .execute(&mut **tx)
        .await?;

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
        return Err(sqlx::Error::RowNotFound.into());
    };

    let eligible = eligible_ids(dir, &team);
    // Self-approval is impossible (audit #11): the requester is never eligible for their own
    // request and never gets a prompt SMS for it. If that leaves nobody who may decide, the
    // step cannot be run at all.
    let eligible: Vec<String> = eligible
        .into_iter()
        .filter(|who| who != requested_by)
        .collect();
    if eligible.is_empty() {
        return Err(RequestError::NoEligibleApprover);
    }
    let id = Uuid::new_v4();

    // One live code per eligible person: the transaction-scoped advisory lock per person
    // (persons sorted, so concurrent requests always take them in the same order and
    // cannot deadlock) serializes the collision check below.
    let mut lock_order = eligible.clone();
    lock_order.sort_unstable();
    for person in &lock_order {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('approval-code:' || $1))")
            .bind(person)
            .execute(&mut **tx)
            .await?;
    }

    // The codes those persons may already be holding: every pending, unexpired approval
    // whose eligible set overlaps this one's.
    let taken: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT id, sms_code_hash FROM approvals \
         WHERE state = 'pending' AND expires_at > now() AND eligible && $1::text[]",
    )
    .bind(&eligible)
    .fetch_all(&mut **tx)
    .await?;

    let code = draw_unique_code(&taken).ok_or(RequestError::NoFreeCode)?;
    let hash = code_hash(id, &code);

    // The per-(alert, step) advisory lock above makes this insert race-free; the
    // `ON CONFLICT DO NOTHING` belt-and-braces below covers any path that ever elides
    // the lock (the partial unique index only fires on `pending` rows, so a conflict
    // can only be the winner's row -- return it instead of failing the call).
    let inserted: Option<(Uuid,)> = sqlx::query_as(
        "INSERT INTO approvals \
            (id, alert_id, step_id, action, params, eligible, state, requested_by, \
             expires_at, sms_code_hash) \
         VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, \
                 now() + make_interval(secs => $8), $9) \
         ON CONFLICT (alert_id, step_id) WHERE state = 'pending' DO NOTHING RETURNING id",
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
    .fetch_optional(&mut **tx)
    .await?;
    if inserted.is_none() {
        // Lost the race after all: return the winner's id, send no prompt SMS.
        if let Some((existing,)) = sqlx::query_as(
            "SELECT id FROM approvals \
             WHERE alert_id = $1 AND step_id = $2 AND state = 'pending'",
        )
        .bind(alert_id)
        .bind(step_id)
        .fetch_optional(&mut **tx)
        .await?
        {
            return Ok(existing);
        }
        return Err(sqlx::Error::RowNotFound.into());
    }

    // The SMS prompt: one sms job per eligible person with a phone (the requester is not in
    // `eligible`, so they are never prompted for their own request). The code travels in the
    // job (the SMS must carry it); the table keeps only its hash.
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
        // Next free prompt slot for this person on this notification: several approvals on one
        // alert (different steps) each page the same person, so the slot is per person, not
        // per request. The per-person advisory lock taken above makes the count race-free.
        let (taken,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM delivery_jobs \
             WHERE alert_id = $1 AND recipient_id = $2 AND channel = 'sms' \
               AND notify_seq = $3 AND escalation_step >= 200",
        )
        .bind(alert_id)
        .bind(person_id)
        .bind(notify_seq)
        .fetch_one(&mut **tx)
        .await?;
        let slot = 200 + i32::try_from(taken).unwrap_or(i32::MAX - 200);
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
            Some(slot),
        )
        .await?;
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
    requested_by: String,
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
        "SELECT alert_id, step_id, action, params, eligible, state, requested_by, expires_at \
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
        requested_by,
        expires_at,
    }) = row
    else {
        return Ok(Decided::NotFound);
    };
    // Self-approval is impossible, whatever the row says (audit #11): the requester may never
    // decide their own request.
    if actor == requested_by {
        return Ok(Decided::Forbidden(
            "the requester cannot approve their own request".into(),
        ));
    }
    if !eligible.iter().any(|e| e == actor) {
        return Ok(Decided::Forbidden(
            "not eligible to decide this approval".into(),
        ));
    }
    if state != "pending" {
        return Ok(Decided::NotPending(state));
    }
    if expires_at <= Utc::now() {
        // The request lapsed without a decision; free the (alert, step) slot for a new one and
        // scrub the code at rest in the same tx.
        sqlx::query("UPDATE approvals SET state = 'expired', updated_at = now() WHERE id = $1")
            .bind(approval_id)
            .execute(&mut **tx)
            .await?;
        scrub_code(tx, approval_id).await?;
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
    // The approval has left `pending`: the plaintext code stops existing at rest in the same
    // tx (audit #11).
    scrub_code(tx, approval_id).await?;
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
        Ok(Decided::Forbidden(message)) => {
            let _ = tx.rollback().await;
            Err(DecideError::Forbidden(message))
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
    /// The code matches more than one pending approval of this sender: nothing is decided
    /// and no attempt is burned — the request has to be picked in the UI instead.
    Ambiguous,
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
///
/// Audit #11 hardening: only pending approvals that have NOT expired are candidates (an
/// expired one sharing the code must not make the reply ambiguous), the scan is bounded
/// (`LIMIT 200`), rows lock in a fixed order (`ORDER BY id FOR UPDATE`, so concurrent replies
/// cannot deadlock) and the hash comparison is constant time.
async fn code_decision(
    pool: &PgPool,
    alert_ops: &dyn AlertOps,
    actor: &str,
    decision: Decision,
    code: &str,
) -> Result<ReplyOutcome, sqlx::Error> {
    let mut tx = pool.begin().await?;
    // Lock every pending, unexpired approval this person may decide, in id order; the reply
    // decides exactly one — or, when the code matches several of them, none (`Ambiguous`).
    let pendings: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT id, sms_code_hash FROM approvals \
         WHERE state = 'pending' AND expires_at > now() AND $1 = ANY(eligible) \
         ORDER BY id LIMIT 200 FOR UPDATE",
    )
    .bind(actor)
    .fetch_all(&mut *tx)
    .await?;

    // The pending approvals this code unlocks for the sender.
    let matching: Vec<Uuid> = pendings
        .iter()
        .filter(|(id, hash)| ct_eq(code_hash(*id, code).as_bytes(), hash.as_bytes()))
        .map(|(id, _)| *id)
        .collect();

    // A code that matches more than one pending approval of this sender would decide an
    // arbitrary request: decide nothing, burn no attempt, tell the sender to pick in the
    // UI. (`request`'s collision-safe draw makes this unreachable for rows it creates;
    // direct inserts can still force it.)
    if matching.len() > 1 {
        let _ = tx.rollback().await;
        return Ok(ReplyOutcome::Ambiguous);
    }

    if let Some(id) = matching.first() {
        // The same decide path as the UI (`via = sms`), in this transaction.
        return match decide_in_tx(&mut tx, alert_ops, *id, actor, decision, Via::Sms, None).await {
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
            Ok(Decided::NotFound | Decided::Forbidden(_)) => {
                let _ = tx.rollback().await;
                Ok(ReplyOutcome::WrongCode)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        };
    }

    // No pending match: a code belonging to an already-decided approval, or to one still
    // `pending` past its expiry, is idempotently "already decided" (nothing actionable is left
    // to decide, and no attempt burns for it).
    let decided: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT id, sms_code_hash FROM approvals \
         WHERE (state <> 'pending' OR expires_at <= now()) AND $1 = ANY(eligible)",
    )
    .bind(actor)
    .fetch_all(&mut *tx)
    .await?;
    if decided
        .iter()
        .any(|(id, hash)| ct_eq(code_hash(*id, code).as_bytes(), hash.as_bytes()))
    {
        let _ = tx.rollback().await;
        return Ok(ReplyOutcome::AlreadyDecided);
    }

    // A wrong code burns one SMS attempt on every pending approval of this person; the one
    // that reaches [`MAX_SMS_ATTEMPTS`] expires (frees its (alert, step) slot) and has its
    // code scrubbed at rest in the same tx (audit #11).
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
    for id in &expired {
        scrub_code(&mut tx, *id).await?;
    }
    tx.commit().await?;
    if expired.is_empty() {
        Ok(ReplyOutcome::WrongCode)
    } else {
        Ok(ReplyOutcome::TooManyAttempts)
    }
}

/// `ACK` from a known number: ack the alert of the newest sms/phone job actually `sent` to
/// that number (an unsent page — pending, cancelled or dead — never rang the phone, so it
/// never makes an alert ackable), newest `sent_at` first; audit `acked` via sms.
///
/// Repeated callbacks are stable: the newest sent page decides the answer, so a repeat
/// whose newest paged alert is already acked answers [`ReplyOutcome::AlreadyDecided`]
/// (idempotent, no side effects) instead of advancing to an older open alert the sender
/// may not even have seen. A newest page whose alert is no longer open at all (resolved,
/// or a status this version does not know) has nothing left to ack and is reported as
/// `Unknown`.
async fn ack_latest(
    pool: &PgPool,
    alert_ops: &dyn AlertOps,
    actor: &str,
    from: &str,
) -> Result<ReplyOutcome, sqlx::Error> {
    let digits = digits_only(from);
    let latest: Option<(Uuid, String)> = sqlx::query_as(
        "SELECT a.id, a.status FROM alerts a \
         JOIN delivery_jobs j ON j.alert_id = a.id \
         WHERE j.channel IN ('sms', 'phone') AND j.status = 'sent' \
           AND regexp_replace(j.address, '[^0-9]', '', 'g') = $1 \
         ORDER BY j.sent_at DESC NULLS LAST, j.id DESC LIMIT 1",
    )
    .bind(&digits)
    .fetch_optional(pool)
    .await?;
    let Some((alert_id, status)) = latest else {
        return Ok(ReplyOutcome::Unknown(
            "no open alert has paged this number".into(),
        ));
    };
    if status == "acked" {
        return Ok(ReplyOutcome::AlreadyDecided);
    }
    if status != "open" {
        return Ok(ReplyOutcome::Unknown(format!(
            "the newest alert paged on this number is {status}"
        )));
    }

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
