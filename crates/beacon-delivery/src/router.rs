//! Router: outbox `kind='notify'` rows -> `delivery_jobs`, one transaction per batch (M0),
//! resolving each recipient's channels per SPEC §4.4 and freezing the decision into
//! `route_info` (M4, §5.4).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::SecondsFormat;
use serde_json::json;
use sqlx::PgPool;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::config::{Directory, Person, Team, address_for};
use crate::model::NotifyPayload;
use crate::prefs::{self, Decision, RoutingContext};
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

    for (outbox_id, alert_id, raw) in &rows {
        let payload: NotifyPayload = match serde_json::from_value(raw.clone()) {
            Ok(p) => p,
            Err(err) => {
                // Still marked processed below: a poison row must not block the queue.
                warn!(outbox_id, %alert_id, %err, "malformed notify payload, dropping");
                stats.malformed += 1;
                continue;
            }
        };
        let ctx = routing_context(&payload, raw);

        // (person, channel) pairs already planned for this payload: a person reached by
        // several entries gets the union of channels, and the first decision for a pair
        // wins (SPEC §4.4).
        let mut seen: HashSet<(String, String)> = HashSet::new();

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

                    let res = sqlx::query(
                        "INSERT INTO delivery_jobs \
                           (alert_id, notify_seq, recipient_id, channel, address, route_info) \
                         VALUES ($1, $2, $3, $4, $5, $6) \
                         ON CONFLICT (alert_id, recipient_id, channel, notify_seq) DO NOTHING",
                    )
                    .bind(alert_id)
                    .bind(payload.notify_seq)
                    .bind(&person.id)
                    .bind(&channel)
                    .bind(address)
                    .bind(route_info)
                    .execute(&mut *tx)
                    .await?;
                    stats.jobs_inserted += res.rows_affected();
                }
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

/// Loop `route_once` until shutdown. Polls again immediately after a full batch, otherwise
/// sleeps `cfg.poll_interval`. Logs and backs off on DB errors instead of exiting.
pub async fn run_router(pool: PgPool, dir: Arc<Directory>, cfg: RouterConfig, shutdown: Shutdown) {
    let mut shutdown = shutdown;
    while !*shutdown.borrow() {
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
            Err(err) => {
                error!(%err, "router batch failed, retrying in 1s");
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
}
