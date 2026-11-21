//! Router: outbox `kind='notify'` rows -> `delivery_jobs`, one transaction per batch (M0),
//! resolving each recipient's channels per SPEC §4.4 and freezing the decision into
//! `route_info` (M4, §5.4).

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::SecondsFormat;
use serde_json::json;
use sqlx::PgPool;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::config::{Directory, Person, Team, address_for};
use crate::escalation;
use crate::model::{NotifyPayload, NotifyReason};
use crate::prefs::{self, Decision, RoutingContext};
use crate::runbook;
use crate::throttle::{self, PlannedJob};
use crate::{RouterConfig, Shutdown};

/// Channels the router can create delivery jobs for: email via SMTP, sms and phone via the
/// fake gateway. `ui` is delivered by the alert feed itself and never becomes a job; any
/// other channel is skipped with a warning.
pub const SUPPORTED_JOB_CHANNELS: &[&str] = &["email", "sms", "phone"];

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RouteStats {
    /// Outbox rows marked processed.
    pub outbox_rows: u64,
    /// `delivery_jobs` rows actually inserted (conflicts excluded).
    pub jobs_inserted: u64,
    /// Rows whose payload failed to parse (marked processed, logged).
    pub malformed: u64,
    /// Recipients skipped (unknown target, no address for the channel, unsupported channel),
    /// counted once per `(person, channel)` pair.
    pub skipped: u64,
    /// Recipient notifications parked by the throttle (M5a).
    pub suppressed: u64,
    /// Escalation timers started (M5a).
    pub timers: u64,
    /// Runbooks frozen onto alerts (M5b).
    pub runbooks: u64,
}

/// Claim up to `cfg.batch_size` unprocessed notify rows with `FOR UPDATE SKIP LOCKED`, insert
/// their jobs `ON CONFLICT (alert_id, recipient_id, channel, notify_seq) DO NOTHING`, and set
/// `processed_at = now()`, all in ONE transaction.
pub async fn route_once(
    pool: &PgPool,
    dir: &Directory,
    cfg: &RouterConfig,
) -> Result<RouteStats, sqlx::Error> {
    let mut tx = pool.begin().await?;

    let rows: Vec<(i64, Uuid, serde_json::Value)> = sqlx::query_as(
        "SELECT id, alert_id, payload FROM outbox \
         WHERE kind = 'notify' AND processed_at IS NULL \
         ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED",
    )
    .bind(cfg.batch_size)
    .fetch_all(&mut *tx)
    .await?;

    // One decision time per call, frozen into every job's `route_info.decided_at`.
    let now = cfg.clock.now();
    let mut stats = RouteStats::default();
    if rows.is_empty() {
        tx.commit().await?;
        return Ok(stats);
    }

    let mut parsed = Vec::with_capacity(rows.len());
    for (outbox_id, alert_id, raw) in &rows {
        match serde_json::from_value::<NotifyPayload>(raw.clone()) {
            Ok(payload) => {
                let policy = dir.rules.for_payload(&payload.rule_id, raw);
                parsed.push((*outbox_id, *alert_id, raw, payload, policy));
            }
            Err(err) => {
                // Still marked processed below: a poison row must not block the queue.
                warn!(outbox_id, %alert_id, %err, "malformed notify payload, dropping");
                stats.malformed += 1;
            }
        }
    }

    // Several router tasks may run (M7, `ROUTER_TASKS`): lock every throttle bucket this
    // batch could debit, in one sorted order, before any `take` (see `throttle::lock_buckets`).
    // The set is a superset of what the loop below takes (the same `expand` per entry).
    let mut bucket_keys: Vec<(&str, &str, throttle::ThrottleSpec)> = Vec::new();
    for (_, _, _, payload, policy) in &parsed {
        let Some(spec) = policy.throttle else {
            continue;
        };
        for entry in &payload.route {
            if let Ok(people) = dir.expand(&entry.to) {
                for person in people {
                    bucket_keys.push((&payload.rule_id, &person.id, spec));
                }
            }
        }
    }
    throttle::lock_buckets(&mut tx, &bucket_keys).await?;

    for (outbox_id, alert_id, raw, payload, policy) in &parsed {
        let (outbox_id, alert_id, raw) = (*outbox_id, alert_id, *raw);
        let ctx = routing_context(payload, raw);

        // (person, channel) pairs already planned for this payload: a person reached by
        // several entries gets the union of channels, and the first decision for a pair
        // wins (SPEC §4.4).
        let mut seen: HashSet<(String, String)> = HashSet::new();
        // Planned jobs per person (ordered for deterministic inserts); written after the
        // throttle decision below (M5a: one token per recipient notification).
        let mut planned_by_person: BTreeMap<String, Vec<PlannedJob>> = BTreeMap::new();

        for entry in &payload.route {
            let people = match dir.expand(&entry.to) {
                Ok(p) => p,
                Err(err) => {
                    warn!(outbox_id, %alert_id, target = %entry.to, %err, "cannot expand route target, skipping");
                    stats.skipped += 1;
                    continue;
                }
            };

            for person in people {
                // Step 1 or steps 2-4 of the §4.4 precedence for this (entry, person).
                let planned: Vec<(String, Decision)> = match &entry.channels {
                    Some(explicit) => explicit
                        .iter()
                        .map(|c| (c.clone(), Decision::Explicit))
                        .collect(),
                    None => {
                        let team = team_for_target(dir, &entry.to, person, &payload.team);
                        let r = prefs::resolve_channels(person, team, &ctx, now);
                        r.channels
                            .into_iter()
                            .map(|c| (c, r.decision.clone()))
                            .collect()
                    }
                };

                for (channel, decision) in planned {
                    // `ui` is delivered by the alert feed itself: no job, not counted.
                    if channel == "ui" {
                        continue;
                    }
                    if !seen.insert((person.id.clone(), channel.clone())) {
                        continue; // first decision for this pair is already planned
                    }
                    if !SUPPORTED_JOB_CHANNELS.contains(&channel.as_str()) {
                        warn!(outbox_id, %alert_id, recipient = %person.id, channel, "unsupported channel, skipping");
                        stats.skipped += 1;
                        continue;
                    }
                    let Some(address) = address_for(person, &channel) else {
                        warn!(outbox_id, %alert_id, recipient = %person.id, channel, "no address for channel, skipping");
                        stats.skipped += 1;
                        continue;
                    };

                    // Provenance frozen at routing time (SPEC §4.4 "decided once"), plus
                    // the one-hop fallback used if this job later dies terminally (§5.6).
                    let mut route_info = json!({
                        "target": entry.to,
                        "rule_id": payload.rule_id,
                        "rule_revision": payload.rule_revision,
                        "reason": payload.reason,
                        "outbox_id": outbox_id,
                        "decision": decision,
                        "decided_at": now.to_rfc3339_opts(SecondsFormat::Millis, true),
                    });
                    if let Some(fb) = dir.delivery.fallback_for(&channel)
                        && let Some(fb_address) = address_for(person, fb)
                    {
                        route_info["fallback"] = json!({ "channel": fb, "address": fb_address });
                    }

                    planned_by_person
                        .entry(person.id.clone())
                        .or_default()
                        .push(PlannedJob {
                            channel: channel.clone(),
                            address: address.to_string(),
                            route_info,
                        });
                }
            }
        }

        for (recipient_id, jobs) in &planned_by_person {
            if let Some(spec) = &policy.throttle
                && !throttle::take(&mut tx, &payload.rule_id, recipient_id, spec).await?
            {
                throttle::suppress(
                    &mut tx,
                    &payload.rule_id,
                    recipient_id,
                    *alert_id,
                    payload.notify_seq,
                    jobs,
                )
                .await?;
                stats.suppressed += 1;
                continue;
            }
            stats.jobs_inserted += insert_jobs(
                &mut tx,
                *alert_id,
                payload.notify_seq,
                recipient_id,
                jobs,
                None,
            )
            .await?;
        }

        // Freeze the selected runbook, rendered from the pinned event context, on the alert's
        // first notification (SPEC §6.1, M5b).
        if payload.reason == NotifyReason::New {
            let alert = json!({
                "id": alert_id, "team": payload.team, "rule_id": payload.rule_id,
                "severity": payload.severity,
            });
            let tctx = runbook::template_context(raw, &alert);
            if let Some(rb) = dir
                .runbooks
                .select(policy.runbook.as_deref(), &payload.team, &tctx)
            {
                let steps = runbook::render(rb, &tctx, &crate::actions::ActionRegistry::risk_of);
                if runbook::attach(&mut tx, *alert_id, rb, &steps, &tctx).await? {
                    stats.runbooks += 1;
                }
            }
        }

        // Escalation clock starts at the alert's first notification (SPEC §4.4); suppressed or
        // failed sends still count as notified.
        let interval = cfg.escalate_after_override.or(policy.escalate_after);
        if payload.reason == NotifyReason::New
            && let Some(interval) = interval
        {
            let notified: HashSet<String> = planned_by_person.keys().cloned().collect();
            let chain = escalation::plan_chain(dir, &payload.route, &payload.team, &notified);
            // The channels this notification planned, frozen onto the timer (S11-8):
            // parked rows count too, so later jobs (fallback jobs, digests, prompts) cannot
            // change an escalation step's fallback channels.
            let planned_channels: Vec<String> = {
                let mut seen = HashSet::new();
                let mut out: Vec<String> = Vec::new();
                for jobs in planned_by_person.values() {
                    for job in jobs {
                        if seen.insert(job.channel.clone()) {
                            out.push(job.channel.clone());
                        }
                    }
                }
                out
            };
            if !chain.is_empty()
                && escalation::schedule(
                    &mut tx,
                    *alert_id,
                    payload.notify_seq,
                    &ctx,
                    &chain,
                    interval,
                    &planned_channels,
                )
                .await?
            {
                stats.timers += 1;
            }
        }
    }

    let ids: Vec<i64> = rows.iter().map(|(id, _, _)| *id).collect();
    sqlx::query("UPDATE outbox SET processed_at = now() WHERE id = ANY($1)")
        .bind(&ids)
        .execute(&mut *tx)
        .await?;
    stats.outbox_rows = ids.len() as u64;

    tx.commit().await?;
    Ok(stats)
}

/// Insert all planned jobs of one call in ONE statement: `unnest` the per-job arrays
/// `WITH ORDINALITY` (so insertion order follows input order) and let `ON CONFLICT DO NOTHING`
/// skip keys that already exist — including a duplicate channel twice within the same call.
/// Returns how many rows were actually inserted (conflicts excluded). Shared with the
/// escalation scheduler and the throttle digest (`escalation_step` is `Some` for escalations).
pub async fn insert_jobs(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    alert_id: Uuid,
    notify_seq: i32,
    recipient_id: &str,
    jobs: &[PlannedJob],
    escalation_step: Option<i32>,
) -> Result<u64, sqlx::Error> {
    if jobs.is_empty() {
        return Ok(0);
    }
    let channels: Vec<&str> = jobs.iter().map(|j| j.channel.as_str()).collect();
    let addresses: Vec<&str> = jobs.iter().map(|j| j.address.as_str()).collect();
    let route_info: Vec<serde_json::Value> = jobs.iter().map(|j| j.route_info.clone()).collect();
    let res = sqlx::query(
        "INSERT INTO delivery_jobs \
           (alert_id, notify_seq, recipient_id, channel, address, route_info, escalation_step) \
         SELECT $1, $2, $3, c, a, r, $7 \
         FROM unnest($4::text[], $5::text[], $6::jsonb[]) WITH ORDINALITY AS t(c, a, r, ord) \
         ORDER BY ord \
         ON CONFLICT (alert_id, recipient_id, channel, notify_seq, escalation_step) DO NOTHING",
    )
    .bind(alert_id)
    .bind(notify_seq)
    .bind(recipient_id)
    .bind(&channels)
    .bind(&addresses)
    .bind(&route_info)
    .bind(escalation_step.unwrap_or(0))
    .execute(&mut **tx)
    .await?;
    Ok(res.rows_affected())
}

/// Facts a preference can match on: the payload plus its optional raw `context` object
/// (`service`, `environment`). `NotifyPayload.context` lands with part (a); read it from
/// the raw JSON so routing works before and after that lands.
fn routing_context(payload: &NotifyPayload, raw: &serde_json::Value) -> RoutingContext {
    let ctx = raw.get("context").and_then(|c| c.as_object());
    let field = |key: &str| {
        ctx.and_then(|o| o.get(key))
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    };
    RoutingContext {
        severity: payload.severity.clone(),
        team: payload.team.clone(),
        rule_id: payload.rule_id.clone(),
        service: field("service"),
        environment: field("environment"),
    }
}

/// The team whose default applies when resolving `person` reached via `target` (SPEC §4.4
/// step 3): the routed team for `team:`/`oncall:` targets; for `user:` targets the alert's
/// team if the person is a member; otherwise `None`.
fn team_for_target<'a>(
    dir: &'a Directory,
    target: &str,
    person: &Person,
    alert_team: &str,
) -> Option<&'a Team> {
    match target.split_once(':') {
        Some(("team", id)) => dir.teams.get(id),
        Some(("oncall", schedule)) => dir.team_for_schedule(schedule),
        // `expand` already rejected malformed targets; for anything else the alert's team
        // applies when the person belongs to it.
        _ => dir
            .teams
            .get(alert_team)
            .filter(|t| t.members.contains(&person.id)),
    }
}

/// Postgres `40P01` (deadlock detected) or `40001` (serialization failure): the transaction
/// was aborted as a whole and can simply run again.
fn is_retryable_conflict(err: &sqlx::Error) -> bool {
    err.as_database_error()
        .and_then(|e| e.code())
        .is_some_and(|code| code == "40P01" || code == "40001")
}

/// Loop `route_once` until shutdown. Polls again immediately after a full batch, otherwise
/// sleeps `cfg.poll_interval` or until `cfg.wake` fires (a new outbox row). Logs and backs
/// off on DB errors instead of exiting.
///
/// The startup directory is wrapped in an [`crate::overrides::EffectiveDirectory`] and
/// re-read (one cheap stamp query) every pass, so a saved preference or team change
/// applies to the next notification.
pub async fn run_router(pool: PgPool, dir: Arc<Directory>, cfg: RouterConfig, shutdown: Shutdown) {
    let mut shutdown = shutdown;
    let effective = Arc::new(crate::overrides::EffectiveDirectory::new(dir));
    let mut conflicts: u64 = 0;
    let mut wake = cfg.wake.clone();
    while !*shutdown.borrow() {
        // Before looking: a NOTIFY for rows this pass misses wakes the next wait.
        wake.mark_seen();
        // Throttle digests ride along with routing: cheap when nothing is parked.
        if cfg.digest {
            match throttle::digest_once(&pool, 100).await {
                Ok(d) if d.digests > 0 => debug!(?d, "throttle digests"),
                Ok(_) => {}
                Err(err) => error!(%err, "throttle digest pass failed"),
            }
        }
        let dir = effective.get(&pool).await;
        let wait = match route_once(&pool, &dir, &cfg).await {
            Ok(stats) => {
                if stats != RouteStats::default() {
                    debug!(?stats, "router batch");
                }
                if i64::try_from(stats.outbox_rows).is_ok_and(|n| n >= cfg.batch_size) {
                    continue; // full batch: more is probably waiting
                }
                cfg.poll_interval
            }
            // A deadlock or serialization failure rolled the whole batch back (nothing was
            // marked processed): retry it right away instead of waiting out the error backoff.
            Err(err) if is_retryable_conflict(&err) => {
                conflicts += 1;
                warn!(%err, conflicts, "router batch hit a lock conflict, retrying");
                Duration::from_millis(10)
            }
            Err(err) => {
                error!(%err, "router batch failed, retrying in 1s");
                Duration::from_secs(1)
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = wake.changed() => {}
            // `Err` means the sender is gone, so nobody can signal shutdown any more: stop.
            res = shutdown.changed() => {
                if res.is_err() {
                    break;
                }
            }
        }
    }
}
