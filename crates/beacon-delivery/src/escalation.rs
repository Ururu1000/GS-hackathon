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
//!   Channels = step.channels, else `prefs::resolve_channels` (severity, team, rule_id from
//!   the alert row; service, environment from the first notification); if that yields no
//!   job channel (UI only), use the distinct channels of this alert's existing jobs for
//!   `notify_seq`. Insert jobs (`notify_seq` = the
//!   timer's, `escalation_step = step + 1`, route_info `{"target": "escalation", "via",
//!   "step", "rule_id", "decision", "decided_at", "fallback"?}`, `ON CONFLICT DO NOTHING`).
//!   Non-critical alerts with a rule throttle go through `throttle::take`/`suppress`; critical
//!   ones bypass it. Then `step += 1`; past the end → `state='done'`; else
//!   `due_at = now() + interval`. Lease cleared, `updated_at = now()`, finalize fenced by
//!   `lease_token`.
//! - Ack/resolve needs no hook: the status check at fire time stops the chain.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{PgPool, Postgres, Transaction};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::Shutdown;
use crate::config::{Directory, Team, address_for};
use crate::model::RouteEntry;
use crate::prefs::{self, Decision, RoutingContext};
use crate::router::{self, SUPPORTED_JOB_CHANNELS};
use crate::throttle::{self, PlannedJob};

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
    let mut chain: Vec<ChainStep> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    for entry in route {
        let Some((prefix, id)) = entry.to.split_once(':') else {
            continue; // malformed target: no escalation from it
        };
        // The entry's explicit channels: a step first reached through it carries them.
        let channels = entry.channels.as_ref();
        match prefix {
            // The rest of the rotation after the person on call, then that team's lead.
            "oncall" => {
                let Some(team) = dir.team_for_schedule(id) else {
                    continue; // unknown schedule: ignored
                };
                let Some(rotation) = team.oncall.get(id) else {
                    continue;
                };
                let on_call = rotation.first();
                for who in rotation {
                    if Some(who) != on_call {
                        push_step(
                            &mut chain,
                            &mut seen,
                            already_notified,
                            who,
                            format!("oncall:{id}"),
                            channels,
                        );
                    }
                }
                if let Some(lead) = &team.lead {
                    push_step(
                        &mut chain,
                        &mut seen,
                        already_notified,
                        lead,
                        format!("lead:{}", team.id),
                        channels,
                    );
                }
            }
            // The team's lead.
            "team" => {
                if let Some(lead) = dir.teams.get(id).and_then(|t| t.lead.as_ref()) {
                    push_step(
                        &mut chain,
                        &mut seen,
                        already_notified,
                        lead,
                        format!("lead:{id}"),
                        channels,
                    );
                }
            }
            // A directly-routed user escalates to the lead of the alert's team.
            "user" => {
                if dir.person(id).is_none() {
                    continue; // unknown target: ignored
                }
                if let Some(lead) = dir.teams.get(alert_team).and_then(|t| t.lead.as_ref()) {
                    push_step(
                        &mut chain,
                        &mut seen,
                        already_notified,
                        lead,
                        format!("lead:{alert_team}"),
                        channels,
                    );
                }
            }
            _ => continue,
        }
    }
    chain
}

/// Add one chain candidate: people already notified by this notification and repeats are
/// skipped, and the chain is capped at `MAX_CHAIN`. The first entry to reach a person
/// decides their `via` and `channels`.
fn push_step(
    chain: &mut Vec<ChainStep>,
    seen: &mut HashSet<String>,
    already_notified: &HashSet<String>,
    recipient: &str,
    via: String,
    channels: Option<&Vec<String>>,
) {
    if chain.len() >= MAX_CHAIN
        || already_notified.contains(recipient)
        || !seen.insert(recipient.to_string())
    {
        return;
    }
    chain.push(ChainStep {
        recipient_id: recipient.to_string(),
        via,
        channels: channels.cloned(),
    });
}

/// Insert the timer (no-op if one exists for the alert). Returns `true` if inserted.
/// An empty chain inserts nothing (module doc: "Empty chain → no timer"); the router only
/// calls this with a non-empty chain. `interval` is clamped up to 1 ms because the table
/// requires `interval_ms > 0`.
pub async fn schedule(
    tx: &mut Transaction<'_, Postgres>,
    alert_id: Uuid,
    notify_seq: i32,
    ctx: &RoutingContext,
    chain: &[ChainStep],
    interval: Duration,
) -> Result<bool, sqlx::Error> {
    if chain.is_empty() {
        return Ok(false);
    }
    let interval_ms = i64::try_from(interval.as_millis())
        .unwrap_or(i64::MAX)
        .max(1);
    let res = sqlx::query(
        "INSERT INTO escalation_timers \
              (alert_id, notify_seq, rule_id, service, environment, chain, interval_ms, due_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, now() + make_interval(secs => $8::float8)) \
         ON CONFLICT (alert_id) DO NOTHING",
    )
    .bind(alert_id)
    .bind(notify_seq)
    .bind(&ctx.rule_id)
    .bind(&ctx.service)
    .bind(&ctx.environment)
    .bind(serde_json::to_value(chain).expect("chain serializes"))
    .bind(interval_ms)
    .bind(interval.as_secs_f64())
    .execute(&mut **tx)
    .await?;
    Ok(res.rows_affected() == 1)
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

/// One due timer as claimed by [`claim_due`]: everything [`fire_claimed`] needs, including
/// the lease token every write in that fire is fenced by. Public (like
/// `worker::claim_batch`) so tests can drive a fire with a hand-made, possibly stale lease.
#[derive(Debug, Clone)]
pub struct ClaimedTimer {
    pub alert_id: Uuid,
    pub notify_seq: i32,
    pub rule_id: String,
    /// The first notification's `context.service` / `context.environment`.
    pub service: Option<String>,
    pub environment: Option<String>,
    pub chain: Vec<ChainStep>,
    pub step: i32,
    pub interval: Duration,
    /// The lease this claim took: the fire's finalize is fenced by it.
    pub lease_token: Uuid,
}

/// What firing one claimed timer did ([`fire_claimed`]); [`escalate_once`] tallies these
/// into [`EscalationStats`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FireOutcome {
    /// The alert is no longer open: the timer was cancelled (`last_note = 'alert <status>'`).
    Cancelled,
    /// The step fired: the person was paged (`jobs` rows) or parked by the throttle.
    /// `finished` when the chain is exhausted and the timer `done`.
    Fired {
        jobs: u64,
        suppressed: bool,
        finished: bool,
    },
    /// The chain entry was skipped (unknown person, nothing addressable); the timer
    /// advanced past it (`finished` as above, the reason appended to `last_note`).
    Skipped { finished: bool },
    /// Another worker owns the timer now: nothing was written, the transaction was dropped.
    LostLease,
}

/// Claim up to `cfg.batch_size` due pending timers in one short transaction
/// (`FOR UPDATE SKIP LOCKED`): set `locked_by`, `locked_until = now() + cfg.lease` and a
/// fresh `lease_token` per timer. Timers whose lease expired are claimable again; the
/// previous owner's finalize then finds the token changed and writes nothing. Chains
/// written by [`schedule`] always decode; a timer whose chain cannot be decoded can never
/// fire, so it is cancelled inside the same transaction instead of blocking the queue.
pub async fn claim_due(
    pool: &PgPool,
    cfg: &SchedulerConfig,
) -> Result<Vec<ClaimedTimer>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    type Row = (
        Uuid,
        i32,
        String,
        Option<String>,
        Option<String>,
        serde_json::Value,
        i32,
        i64,
        Uuid,
    );
    let rows: Vec<Row> = sqlx::query_as(
        "WITH c AS ( \
             SELECT * FROM escalation_timers \
             WHERE state = 'pending' AND due_at <= now() \
               AND (locked_until IS NULL OR locked_until < now()) \
             ORDER BY due_at \
             LIMIT $1 \
             FOR UPDATE SKIP LOCKED) \
         UPDATE escalation_timers t \
         SET locked_by = $2, \
             locked_until = now() + make_interval(secs => $3::float8), \
             lease_token = gen_random_uuid(), updated_at = now() \
         FROM c WHERE t.alert_id = c.alert_id \
         RETURNING t.alert_id, t.notify_seq, t.rule_id, t.service, t.environment, t.chain, \
                   t.step, t.interval_ms, t.lease_token",
    )
    .bind(cfg.batch_size)
    .bind(&cfg.worker_id)
    .bind(cfg.lease.as_secs_f64())
    .fetch_all(&mut *tx)
    .await?;

    let mut claimed = Vec::with_capacity(rows.len());
    for (
        alert_id,
        notify_seq,
        rule_id,
        service,
        environment,
        raw,
        step,
        interval_ms,
        lease_token,
    ) in rows
    {
        match serde_json::from_value::<Vec<ChainStep>>(raw) {
            Ok(chain) => claimed.push(ClaimedTimer {
                alert_id,
                notify_seq,
                rule_id,
                service,
                environment,
                chain,
                step,
                interval: Duration::from_millis(u64::try_from(interval_ms).unwrap_or(0)),
                lease_token,
            }),
            Err(err) => {
                error!(%alert_id, %err, "unreadable escalation chain, cancelling the timer");
                sqlx::query(
                    "UPDATE escalation_timers \
                     SET state = 'cancelled', last_note = 'malformed chain', \
                         locked_by = NULL, locked_until = NULL, lease_token = NULL, \
                         updated_at = now() \
                     WHERE alert_id = $1 AND lease_token = $2 AND state = 'pending'",
                )
                .bind(alert_id)
                .bind(lease_token)
                .execute(&mut *tx)
                .await?;
            }
        }
    }
    tx.commit().await?;
    Ok(claimed)
}

/// Fire one claimed timer in ONE transaction (module doc): lock the alert `FOR SHARE`,
/// cancel the timer if the alert is no longer open, else resolve the chain step's
/// channels, insert its jobs (or park them in the throttle) and advance the step. The
/// final timer UPDATE is fenced by `claimed.lease_token`; a lost lease rolls the whole
/// transaction back. Public (like `worker::claim_batch`) so tests can drive a fire with a
/// hand-made lease.
pub async fn fire_claimed(
    pool: &PgPool,
    dir: &Directory,
    claimed: &ClaimedTimer,
) -> Result<FireOutcome, sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Lock the alert row FOR SHARE so the status read and the inserts in this transaction
    // agree: an ack racing the fire either lands before (timer cancelled) or after (the
    // jobs stand).
    let alert: Option<(String, String, String, String)> = sqlx::query_as(
        "SELECT status, severity, team, rule_id FROM alerts WHERE id = $1 FOR SHARE",
    )
    .bind(claimed.alert_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((status, severity, team, rule_id)) = alert else {
        return finalize_cancelled(tx, claimed, "alert missing").await;
    };
    if status != "open" {
        return finalize_cancelled(tx, claimed, &format!("alert {status}")).await;
    }

    let finished = claimed.step + 1 >= claimed.chain.len() as i32;

    // Past the end of the chain (defensive: `schedule` writes non-empty chains and every
    // advance keeps `step` in range, so this can only happen to rows written elsewhere).
    let Some(step) = claimed.chain.get(claimed.step as usize) else {
        return skip_step(tx, claimed, true, "step past the end of the chain").await;
    };

    // The chain was frozen against a directory that may have changed since.
    let Some(person) = dir.person(&step.recipient_id) else {
        let note = format!("unknown person {}", step.recipient_id);
        return skip_step(tx, claimed, finished, &note).await;
    };

    // Channels: the step's explicit list, else the person's preferences resolved against
    // the alert (severity, team, rule_id from the alert row; service, environment frozen
    // on the timer).
    let now = Utc::now();
    let (resolved, decision) = match &step.channels {
        Some(explicit) => (explicit.clone(), Decision::Explicit),
        None => {
            let ctx = RoutingContext {
                severity: severity.clone(),
                team: team.clone(),
                rule_id: rule_id.clone(),
                service: claimed.service.clone(),
                environment: claimed.environment.clone(),
            };
            let r = prefs::resolve_channels(person, team_for_via(dir, &step.via), &ctx, now);
            (r.channels, r.decision)
        }
    };

    // `ui` is the alert feed itself; anything the workers cannot send is dropped, like the
    // router does. The deduplicated remainder is what this step can page on.
    let mut channels: Vec<String> = Vec::new();
    for channel in &resolved {
        if channel == "ui" {
            continue;
        }
        if !SUPPORTED_JOB_CHANNELS.contains(&channel.as_str()) {
            warn!(alert_id = %claimed.alert_id, recipient = %step.recipient_id,
                  channel = %channel, "unsupported escalation channel, skipping it");
            continue;
        }
        if !channels.contains(channel) {
            channels.push(channel.clone());
        }
    }
    // UI only (or nothing sendable): fall back to the distinct channels this notification
    // already created jobs for (module doc).
    if channels.is_empty() {
        let existing: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT channel FROM delivery_jobs \
             WHERE alert_id = $1 AND notify_seq = $2 ORDER BY channel",
        )
        .bind(claimed.alert_id)
        .bind(claimed.notify_seq)
        .fetch_all(&mut *tx)
        .await?;
        channels = existing
            .into_iter()
            .filter(|c| SUPPORTED_JOB_CHANNELS.contains(&c.as_str()))
            .collect();
    }

    let decided_at = now.to_rfc3339_opts(SecondsFormat::Millis, true);
    let mut jobs: Vec<PlannedJob> = Vec::new();
    for channel in &channels {
        let Some(address) = address_for(person, channel) else {
            continue; // no contact on this channel: nothing to send
        };
        let mut route_info = json!({
            "target": "escalation",
            "via": step.via,
            "step": claimed.step,
            "rule_id": rule_id,
            "decision": decision,
            "decided_at": decided_at,
        });
        // The one-hop fallback for terminal failures, frozen like the router does.
        if let Some(fb) = dir.delivery.fallback_for(channel)
            && let Some(fb_address) = address_for(person, fb)
        {
            route_info["fallback"] = json!({ "channel": fb, "address": fb_address });
        }
        jobs.push(PlannedJob {
            channel: channel.clone(),
            address: address.to_string(),
            route_info,
        });
    }

    // Nothing addressable for this person: skip the entry (module doc).
    if jobs.is_empty() {
        let note = if channels.is_empty() {
            format!("no channel for {}", step.recipient_id)
        } else {
            format!("no address for {}", step.recipient_id)
        };
        return skip_step(tx, claimed, finished, &note).await;
    }

    // Non-critical escalations go through the rule's (rule, recipient) throttle; critical
    // ones bypass it (SPEC §4.4). The rule's policy at fire time comes from the rule file
    // (the notification payload's overrides belonged to the initial routing).
    let policy = dir.rules.for_payload(&rule_id, &json!({}));
    let suppressed = match &policy.throttle {
        Some(spec) if severity != "critical" => {
            if throttle::take(&mut tx, &rule_id, &step.recipient_id, spec).await? {
                false
            } else {
                throttle::suppress(
                    &mut tx,
                    &rule_id,
                    &step.recipient_id,
                    claimed.alert_id,
                    claimed.notify_seq,
                    &jobs,
                )
                .await?;
                true
            }
        }
        _ => false,
    };
    let inserted = if suppressed {
        0
    } else {
        router::insert_jobs(
            &mut tx,
            claimed.alert_id,
            claimed.notify_seq,
            &step.recipient_id,
            &jobs,
            Some(claimed.step + 1),
        )
        .await?
    };

    // Advance past this step; the whole transaction is fenced by the lease.
    if !advance(&mut tx, claimed, finished, None).await? {
        return Ok(FireOutcome::LostLease);
    }
    tx.commit().await?;
    if suppressed {
        info!(alert_id = %claimed.alert_id, recipient = %step.recipient_id,
              rule_id = %rule_id, "escalation step parked by the throttle");
    } else {
        info!(alert_id = %claimed.alert_id, recipient = %step.recipient_id,
              jobs = inserted, "escalation step fired");
    }
    Ok(FireOutcome::Fired {
        jobs: inserted,
        suppressed,
        finished,
    })
}

/// The team whose default channels apply when resolving a chain step's person: the team
/// defining the on-call schedule for `oncall:` steps, the team a `lead:` step belongs to
/// (the same choice the router's `team_for_target` makes for the route entry).
fn team_for_via<'a>(dir: &'a Directory, via: &str) -> Option<&'a Team> {
    match via.split_once(':') {
        Some(("oncall", schedule)) => dir.team_for_schedule(schedule),
        Some(("lead", team)) => dir.teams.get(team),
        _ => None,
    }
}

/// Cancel a claimed timer whose alert is no longer open: `state = 'cancelled'`,
/// `last_note = note`, the lease is cleared, fenced by the lease. Consumes the fire's
/// transaction (commits it); a lost lease drops it instead, so nothing was written.
async fn finalize_cancelled(
    mut tx: Transaction<'_, Postgres>,
    claimed: &ClaimedTimer,
    note: &str,
) -> Result<FireOutcome, sqlx::Error> {
    let res = sqlx::query(
        "UPDATE escalation_timers \
         SET state = 'cancelled', last_note = $3, \
             locked_by = NULL, locked_until = NULL, lease_token = NULL, updated_at = now() \
         WHERE alert_id = $1 AND lease_token = $2 AND state = 'pending'",
    )
    .bind(claimed.alert_id)
    .bind(claimed.lease_token)
    .bind(note)
    .execute(&mut *tx)
    .await?;
    if res.rows_affected() != 1 {
        return Ok(FireOutcome::LostLease);
    }
    tx.commit().await?;
    Ok(FireOutcome::Cancelled)
}

/// Skip the current chain entry (unknown person / nothing addressable): append `note` to
/// `last_note` and advance the timer. Consumes the fire's transaction (commits it); a
/// lost lease drops it instead, so nothing was written.
async fn skip_step(
    mut tx: Transaction<'_, Postgres>,
    claimed: &ClaimedTimer,
    finished: bool,
    note: &str,
) -> Result<FireOutcome, sqlx::Error> {
    warn!(alert_id = %claimed.alert_id, note = %note, "escalation step skipped");
    if !advance(&mut tx, claimed, finished, Some(note)).await? {
        return Ok(FireOutcome::LostLease);
    }
    tx.commit().await?;
    Ok(FireOutcome::Skipped { finished })
}

/// Advance a fired or skipped timer past its current step (fenced by the lease): the lease
/// is cleared, `step += 1`; past the end of the chain -> `state = 'done'`, else
/// `due_at = now() + interval`. `note` (if any) is appended to `last_note`. Returns `false`
/// when the lease was lost; the caller then drops the transaction.
async fn advance(
    tx: &mut Transaction<'_, Postgres>,
    claimed: &ClaimedTimer,
    finished: bool,
    note: Option<&str>,
) -> Result<bool, sqlx::Error> {
    let new_step = claimed.step + 1;
    // `concat_ws` collapses an all-NULL argument list to '', so a fire without a note
    // leaves `last_note` alone instead of blanking it.
    let res = if finished {
        sqlx::query(
            "UPDATE escalation_timers \
             SET state = 'done', step = $3, \
                 last_note = CASE WHEN $4::text IS NULL THEN last_note \
                                  ELSE concat_ws('; ', last_note, $4) END, \
                 locked_by = NULL, locked_until = NULL, lease_token = NULL, updated_at = now() \
             WHERE alert_id = $1 AND lease_token = $2 AND state = 'pending'",
        )
        .bind(claimed.alert_id)
        .bind(claimed.lease_token)
        .bind(new_step)
        .bind(note)
        .execute(&mut **tx)
        .await?
    } else {
        sqlx::query(
            "UPDATE escalation_timers \
             SET step = $3, due_at = now() + make_interval(secs => $5::float8), \
                 last_note = CASE WHEN $4::text IS NULL THEN last_note \
                                  ELSE concat_ws('; ', last_note, $4) END, \
                 locked_by = NULL, locked_until = NULL, lease_token = NULL, updated_at = now() \
             WHERE alert_id = $1 AND lease_token = $2 AND state = 'pending'",
        )
        .bind(claimed.alert_id)
        .bind(claimed.lease_token)
        .bind(new_step)
        .bind(note)
        .bind(claimed.interval.as_secs_f64())
        .execute(&mut **tx)
        .await?
    };
    Ok(res.rows_affected() == 1)
}

/// One pass: claim due timers and fire one step each (module doc).
pub async fn escalate_once(
    pool: &PgPool,
    dir: &Directory,
    cfg: &SchedulerConfig,
) -> Result<EscalationStats, sqlx::Error> {
    let claimed = claim_due(pool, cfg).await?;
    let mut stats = EscalationStats {
        claimed: claimed.len() as u64,
        ..EscalationStats::default()
    };
    let mut first_err: Option<sqlx::Error> = None;
    for timer in &claimed {
        let outcome = match fire_claimed(pool, dir, timer).await {
            Ok(outcome) => outcome,
            Err(err) => {
                // Nothing was committed for this timer; its lease expires and a later
                // pass (or another scheduler) reclaims it.
                error!(alert_id = %timer.alert_id, %err, "escalation fire failed");
                first_err.get_or_insert(err);
                continue;
            }
        };
        match outcome {
            FireOutcome::Cancelled => stats.cancelled += 1,
            FireOutcome::Fired {
                jobs,
                suppressed,
                finished,
            } => {
                stats.fired += 1;
                stats.jobs_inserted += jobs;
                if suppressed {
                    stats.suppressed += 1;
                }
                if finished {
                    stats.finished += 1;
                }
            }
            FireOutcome::Skipped { finished } => {
                stats.skipped += 1;
                if finished {
                    stats.finished += 1;
                }
            }
            FireOutcome::LostLease => {
                debug!(alert_id = %timer.alert_id, "escalation lease lost before finalize");
                stats.lost_lease += 1;
            }
        }
    }
    match first_err {
        Some(err) => Err(err),
        None => Ok(stats),
    }
}

/// Loop `escalate_once` until shutdown (true or sender dropped); immediately again after a
/// full batch, else sleep `poll_interval`; DB errors are logged and retried after 1 s.
pub async fn run_scheduler(
    pool: PgPool,
    dir: Arc<Directory>,
    cfg: SchedulerConfig,
    shutdown: Shutdown,
) {
    let mut shutdown = shutdown;
    info!(worker_id = %cfg.worker_id, "escalation scheduler started");
    while !*shutdown.borrow() && shutdown.has_changed().is_ok() {
        let wait = match escalate_once(&pool, &dir, &cfg).await {
            Ok(stats) => {
                if stats != EscalationStats::default() {
                    debug!(?stats, "escalation pass");
                }
                // Full batch: more due timers are probably waiting.
                if i64::try_from(stats.claimed).is_ok_and(|n| n >= cfg.batch_size) {
                    continue;
                }
                cfg.poll_interval
            }
            Err(err) => {
                error!(%err, "escalation pass failed, retrying in 1s");
                Duration::from_secs(1)
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            // `Err` means the sender is gone, so nobody can signal shutdown any more: stop.
            res = shutdown.changed() => {
                if res.is_err() {
                    break;
                }
            }
        }
    }
    info!(worker_id = %cfg.worker_id, "escalation scheduler stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Contact, Person, Team};

    fn person(id: &str) -> Person {
        Person {
            id: id.to_string(),
            name: None,
            contact: Contact {
                email: Some(format!("{id}@example.test")),
                phone: Some("+44 7700 900999".to_string()),
            },
            preferences: None,
            rules: Vec::new(),
        }
    }

    fn team(id: &str, members: &[&str], lead: Option<&str>, oncall: &[(&str, &[&str])]) -> Team {
        Team {
            id: id.to_string(),
            members: members.iter().map(|s| s.to_string()).collect(),
            lead: lead.map(str::to_string),
            oncall: oncall
                .iter()
                .map(|(k, v)| (k.to_string(), v.iter().map(|s| s.to_string()).collect()))
                .collect(),
            default_channels: None,
        }
    }

    fn entry(to: &str, channels: Option<&[&str]>) -> RouteEntry {
        RouteEntry {
            to: to.to_string(),
            channels: channels.map(|c| c.iter().map(|s| s.to_string()).collect()),
        }
    }

    fn notified(ids: &[&str]) -> HashSet<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    fn step(recipient: &str, via: &str, channels: Option<&[&str]>) -> ChainStep {
        ChainStep {
            recipient_id: recipient.to_string(),
            via: via.to_string(),
            channels: channels.map(|c| c.iter().map(|s| s.to_string()).collect()),
        }
    }

    /// The seed example: team settlements (rotation [bob, carol], lead carol).
    fn settlements_dir() -> Directory {
        Directory::from_parts(
            vec![person("bob"), person("carol")],
            vec![team(
                "settlements",
                &["bob", "carol"],
                Some("carol"),
                &[("settlements", &["bob", "carol"])],
            )],
        )
        .expect("directory")
    }

    #[test]
    fn seed_example_oncall_settlements() {
        // The module doc's example: bob (on call) already notified -> carol via the
        // schedule, carrying the entry's explicit channels; carol is also the lead, so she
        // is not repeated.
        let chain = plan_chain(
            &settlements_dir(),
            &[entry("oncall:settlements", Some(&["phone", "sms"]))],
            "settlements",
            &notified(&["bob"]),
        );
        assert_eq!(
            chain,
            vec![step("carol", "oncall:settlements", Some(&["phone", "sms"]))]
        );
    }

    #[test]
    fn oncall_adds_the_lead_when_not_in_the_rotation() {
        let d = Directory::from_parts(
            vec![person("p1"), person("p2"), person("p3"), person("p4")],
            vec![team(
                "t",
                &["p1", "p2", "p3"],
                Some("p4"),
                &[("s", &["p1", "p2", "p3"])],
            )],
        )
        .unwrap();
        // Nobody notified yet: the on-call person p1 is never in the chain; the rotation
        // rest comes first (via the schedule), then the lead p4 (via the team).
        let chain = plan_chain(&d, &[entry("oncall:s", None)], "t", &HashSet::new());
        assert_eq!(
            chain,
            vec![
                step("p2", "oncall:s", None),
                step("p3", "oncall:s", None),
                step("p4", "lead:t", None),
            ]
        );
    }

    #[test]
    fn team_entry_escalates_to_the_team_lead() {
        let d = Directory::from_parts(
            vec![person("anna"), person("bob")],
            vec![team("rates", &["anna", "bob"], Some("bob"), &[])],
        )
        .unwrap();
        let chain = plan_chain(
            &d,
            &[entry("team:rates", Some(&["sms"]))],
            "rates",
            &HashSet::new(),
        );
        assert_eq!(chain, vec![step("bob", "lead:rates", Some(&["sms"]))]);
        // The lead was already notified by this notification: nothing to escalate.
        let chain = plan_chain(
            &d,
            &[entry("team:rates", Some(&["sms"]))],
            "rates",
            &notified(&["bob"]),
        );
        assert!(chain.is_empty());
    }

    #[test]
    fn user_entry_escalates_to_the_alert_teams_lead() {
        let d = Directory::from_parts(
            vec![person("anna"), person("frank")],
            vec![
                team("payments", &["anna", "frank"], Some("frank"), &[]),
                team("leadless", &["anna"], None, &[]),
            ],
        )
        .unwrap();
        let chain = plan_chain(
            &d,
            &[entry("user:anna", None)],
            "payments",
            &notified(&["anna"]),
        );
        assert_eq!(chain, vec![step("frank", "lead:payments", None)]);
        // Unknown user, leadless alert team, unknown alert team: all ignored.
        assert!(
            plan_chain(
                &d,
                &[entry("user:ghost", None)],
                "payments",
                &HashSet::new()
            )
            .is_empty()
        );
        assert!(
            plan_chain(&d, &[entry("user:anna", None)], "leadless", &HashSet::new()).is_empty()
        );
        assert!(
            plan_chain(
                &d,
                &[entry("user:anna", None)],
                "no-such-team",
                &HashSet::new()
            )
            .is_empty()
        );
    }

    #[test]
    fn already_notified_people_are_skipped() {
        let chain = plan_chain(
            &settlements_dir(),
            &[entry("oncall:settlements", Some(&["phone", "sms"]))],
            "settlements",
            &notified(&["bob", "carol"]),
        );
        assert!(chain.is_empty());
    }

    #[test]
    fn repeats_across_entries_are_deduplicated() {
        // carol is reached via the rotation first; the lead repeat from the team entry is
        // skipped, and the first entry's channels win.
        let chain = plan_chain(
            &settlements_dir(),
            &[
                entry("oncall:settlements", Some(&["phone"])),
                entry("team:settlements", None),
            ],
            "settlements",
            &notified(&["bob"]),
        );
        assert_eq!(
            chain,
            vec![step("carol", "oncall:settlements", Some(&["phone"]))]
        );
    }

    #[test]
    fn chain_is_capped_at_max_chain() {
        let ids = ["a", "b", "c", "d", "e", "f", "g", "h"];
        let d = Directory::from_parts(
            ids.iter().map(|s| person(s)).collect(),
            vec![team("big", &ids, Some("h"), &[("s", &ids)])],
        )
        .unwrap();
        let chain = plan_chain(&d, &[entry("oncall:s", None)], "big", &HashSet::new());
        assert_eq!(chain.len(), MAX_CHAIN);
        // The rotation after the on-call person, in order, until the cap.
        assert_eq!(
            chain
                .iter()
                .map(|s| s.recipient_id.as_str())
                .collect::<Vec<_>>(),
            ["b", "c", "d", "e", "f"]
        );
    }

    #[test]
    fn unknown_and_malformed_targets_are_ignored() {
        let d = settlements_dir();
        for target in [
            "oncall:ghost",
            "team:ghost",
            "user:ghost",
            "settlements",
            "",
        ] {
            assert!(
                plan_chain(&d, &[entry(target, None)], "settlements", &HashSet::new()).is_empty(),
                "{target}"
            );
        }
        // An empty route escalates to nobody.
        assert!(plan_chain(&d, &[], "settlements", &HashSet::new()).is_empty());
    }

    #[test]
    fn first_entry_to_reach_a_person_decides_their_channels() {
        // x is the lead of both teams; the first entry (no explicit channels) wins.
        let d = Directory::from_parts(
            vec![person("x"), person("y")],
            vec![
                team("t1", &["x"], Some("x"), &[]),
                team("t2", &["y"], Some("x"), &[]),
            ],
        )
        .unwrap();
        let chain = plan_chain(
            &d,
            &[entry("team:t1", None), entry("team:t2", Some(&["sms"]))],
            "t1",
            &HashSet::new(),
        );
        assert_eq!(chain, vec![step("x", "lead:t1", None)]);
    }
}
