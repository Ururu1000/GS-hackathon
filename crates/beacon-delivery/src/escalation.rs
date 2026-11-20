//! Escalation (SPEC §4.4), table `escalation_timers` (migration
//! `20261120240000_delivery_escalation_timers.sql`), fired by the `scheduler` role.
//!
//! - **Start:** the router calls `schedule` in the same tx as an alert's FIRST notification
//!   (`reason = new`) when the rule has `escalate_after`; `due_at = now() + interval`. One timer
//!   per alert (`ON CONFLICT (alert_id) DO NOTHING`): renotify / severity_up don't restart it.
//!   Suppressed or failed sends still count as "notified" (the clock runs regardless).
//! - **Chain** (frozen, `plan_chain`, at most `MAX_CHAIN` steps), per route entry in order:
//!   `oncall:<s>` → the rest of that rotation after the person on call, then that team's lead;
//!   `team:<id>` → that team's lead; `user:<id>` → the lead of the alert's team. People already
//!   notified by this notification and repeats are skipped. Steps reached via an entry with
//!   explicit `channels:` carry them (`channels: Some`); others resolve preferences when fired.
//!   Empty chain → no timer.
//! - **Fire** (`escalate_once`): claim due `pending` timers with a lease (`FOR UPDATE SKIP
//!   LOCKED`, `locked_until = now() + lease`, fresh `lease_token`; expired leases are claimable).
//!   Per timer, ONE tx: lock the alert row `FOR SHARE`; status not `open` → `state='cancelled'`,
//!   `last_note = 'alert <status>'`. Else take `chain[step]`: unknown person → skip it (note).
//!   Channels = step.channels, else `prefs::resolve_channels` (context from the alert row:
//!   severity, team, rule_id); if that yields no job channel (UI only), use the distinct
//!   channels of this alert's existing jobs for `notify_seq`. Insert jobs (`notify_seq` = the
//!   timer's, `escalation_step = step + 1`, route_info `{"target": "escalation", "via",
//!   "step", "rule_id", "decision", "decided_at", "fallback"?}`, `ON CONFLICT DO NOTHING`).
//!   Non-critical alerts with a rule throttle go through `throttle::take`/`suppress`; critical
//!   ones bypass it. Then `step += 1`; past the end → `state='done'`; else
//!   `due_at = now() + interval`. Lease cleared, `updated_at = now()`, finalize fenced by
//!   `lease_token`.
//! - Ack/resolve needs no hook: the status check at fire time stops the chain.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::Shutdown;
use crate::config::Directory;
use crate::model::RouteEntry;

pub const MAX_CHAIN: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainStep {
    pub recipient_id: String,
    /// `oncall:<schedule>` or `lead:<team>`.
    pub via: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channels: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub worker_id: String,
    pub poll_interval: Duration,
    pub batch_size: i64,
    pub lease: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            worker_id: format!("scheduler-{}", Uuid::new_v4()),
            poll_interval: Duration::from_millis(500),
            batch_size: 50,
            lease: Duration::from_secs(30),
        }
    }
}

/// Pure: the frozen escalation chain for one notification (rules in the module doc).
pub fn plan_chain(
    dir: &Directory,
    route: &[RouteEntry],
    alert_team: &str,
    already_notified: &std::collections::HashSet<String>,
) -> Vec<ChainStep> {
    let _ = (dir, route, alert_team, already_notified);
    todo!("agent A")
}

/// Insert the timer (no-op if one exists for the alert). Returns `true` if inserted.
pub async fn schedule(
    tx: &mut Transaction<'_, Postgres>,
    alert_id: Uuid,
    notify_seq: i32,
    rule_id: &str,
    chain: &[ChainStep],
    interval: Duration,
) -> Result<bool, sqlx::Error> {
    let _ = (tx, alert_id, notify_seq, rule_id, chain, interval);
    todo!("agent A")
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EscalationStats {
    pub claimed: u64,
    /// Steps fired (a person was paged or parked by the throttle).
    pub fired: u64,
    pub jobs_inserted: u64,
    pub suppressed: u64,
    pub cancelled: u64,
    pub finished: u64,
    /// Chain entries skipped (unknown person, no address).
    pub skipped: u64,
    pub lost_lease: u64,
}

/// One pass: claim due timers and fire one step each (module doc).
pub async fn escalate_once(
    pool: &PgPool,
    dir: &Directory,
    cfg: &SchedulerConfig,
) -> Result<EscalationStats, sqlx::Error> {
    let _ = (pool, dir, cfg);
    todo!("agent A")
}

/// Loop `escalate_once` until shutdown (true or sender dropped); immediately again after a
/// full batch, else sleep `poll_interval`; DB errors are logged and retried after 1 s.
pub async fn run_scheduler(
    pool: PgPool,
    dir: Arc<Directory>,
    cfg: SchedulerConfig,
    shutdown: Shutdown,
) {
    let _ = (pool, dir, cfg, shutdown);
    todo!("agent A")
}
