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
//!   side effects).

use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::alert_ops::AlertOps;
use crate::config::Directory;

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
    let _ = (tx, dir, alert_id, step_id, action, params, requested_by);
    todo!("agent C")
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
    let _ = (pool, alert_ops, approval_id, actor, decision, via, note);
    todo!("agent C")
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
    let _ = (pool, dir, alert_ops, from, text);
    todo!("agent C")
}
