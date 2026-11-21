//! Auto-suggested proposals (SPEC §6.5, M7): Beacon watches how each rule's
//! alerts were handled and files a typed proposal into the normal review flow
//! when a rule is noisy, e.g. "fired 400× this week, 95% never acknowledged →
//! suggest a throttle".
//!
//! Two signals over a lookback window (default 7 days), per enabled rule:
//! - **noisy:** at least `min_alerts` alerts, and at least `untouched_ratio`
//!   of them never acknowledged or resolved by a person;
//! - **flapping:** one incident key (dedup fingerprint) reopened at least
//!   `flap_min` times. Grouped rules are skipped: a new alert per window is
//!   their design.
//!
//! Either one proposes `throttle: {max: 3, per: 1h}` (per rule and person in
//! delivery; held-back alerts arrive as one digest, the feed keeps them all)
//! for a rule that has no throttle yet. The proposal is `kind: noisy`,
//! `auto: true`, author `system:beacon`, and passes the same validator as a
//! manual edit. The owning team accepts or rejects it like any other.
//!
//! Never nagging: at most one auto proposal per rule revision (a rejected one
//! stays rejected until the rule changes), and none while the rule already has
//! the maximum number of pending proposals. The insert re-checks both, so
//! several engine replicas can run the suggester at once.

use std::time::Duration;

use anyhow::Result;
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::sync::watch;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config_store;

pub const AUTHOR: &str = "system:beacon";
/// Matches the API's per-rule cap on pending proposals.
const MAX_PENDING_PER_RULE: i64 = 10;

#[derive(Debug, Clone)]
pub struct SuggestConfig {
    pub interval: Duration,
    pub lookback: Duration,
    pub min_alerts: i64,
    pub untouched_ratio: f64,
    pub flap_min: i64,
}

impl Default for SuggestConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(300),
            lookback: Duration::from_secs(7 * 24 * 3600),
            min_alerts: 20,
            untouched_ratio: 0.9,
            flap_min: 5,
        }
    }
}

/// How one rule's alerts were handled in the lookback window.
#[derive(Debug, Clone, PartialEq)]
pub struct RuleStats {
    pub rule_id: String,
    pub alerts: i64,
    /// Never acked, and resolved (if at all) by the system, not a person.
    pub untouched: i64,
    /// Most alerts opened for one incident key.
    pub max_per_key: i64,
}

/// What to suggest, if anything, for a rule with these stats.
pub fn suggestion(cfg: &SuggestConfig, stats: &RuleStats, rule: &Value) -> Option<(Value, String, String)> {
    if rule.get("enabled").and_then(Value::as_bool) == Some(false) || rule.get("throttle").is_some_and(|t| !t.is_null()) {
        return None;
    }
    let days = (cfg.lookback.as_secs() as f64 / 86_400.0).round().max(1.0) as u64;
    let period = if days == 1 { "a day".to_string() } else { format!("{days} days") };
    let noisy = stats.alerts >= cfg.min_alerts && stats.untouched as f64 >= cfg.untouched_ratio * stats.alerts as f64;
    let grouped = rule.get("group").is_some_and(|g| !g.is_null());
    let flapping = !grouped && stats.max_per_key >= cfg.flap_min;
    let summary = if noisy {
        let pct = (100.0 * stats.untouched as f64 / stats.alerts as f64).floor();
        format!("Fired {}× in {period}; {pct}% never acknowledged. Suggest a throttle.", stats.alerts)
    } else if flapping {
        format!("One incident reopened {}× in {period}. Suggest a throttle.", stats.max_per_key)
    } else {
        return None;
    };
    let note = "Suggested by Beacon from how this rule's alerts were handled. With throttle {max: 3, per: 1h}, \
                each person gets at most 3 notifications from this rule per hour; the rest arrive as one digest. \
                The feed still shows every alert, and dedup counts are unchanged."
        .to_string();
    let mut proposed = rule.clone();
    proposed["throttle"] = json!({"max": 3, "per": "1h"});
    Some((proposed, summary, note))
}

pub async fn rule_stats(pool: &PgPool, lookback: Duration) -> Result<Vec<RuleStats>> {
    let rows: Vec<(String, i64, i64, i64)> = sqlx::query_as(
        "WITH a AS (
             SELECT rule_id, fingerprint, acked_by, resolved_by FROM alerts
             WHERE first_seen > now() - make_interval(secs => $1)
         ), per_key AS (
             SELECT rule_id, max(n) AS max_per_key FROM (
                 SELECT rule_id, count(*) AS n FROM a GROUP BY rule_id, fingerprint
             ) k GROUP BY rule_id
         )
         SELECT a.rule_id, count(*),
                count(*) FILTER (WHERE a.acked_by IS NULL AND coalesce(a.resolved_by, 'system') = 'system'),
                max(per_key.max_per_key)
         FROM a JOIN per_key USING (rule_id)
         GROUP BY a.rule_id",
    )
    .bind(lookback.as_secs_f64())
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(rule_id, alerts, untouched, max_per_key)| RuleStats { rule_id, alerts, untouched, max_per_key })
        .collect())
}

/// One pass: files at most one proposal per rule. Returns how many it filed.
pub async fn suggest_once(pool: &PgPool, cfg: &SuggestConfig) -> Result<usize> {
    let mut filed = 0;
    for stats in rule_stats(pool, cfg.lookback).await? {
        let Some(current) = config_store::get(pool, &stats.rule_id).await? else { continue };
        if current.deleted {
            continue;
        }
        let Some((proposed, summary, note)) = suggestion(cfg, &stats, &current.spec) else { continue };
        if let Err(problems) = config_store::validate(&proposed) {
            warn!(rule_id = stats.rule_id, ?problems, "auto suggestion does not validate; skipped");
            continue;
        }
        let team = current.spec.get("team").and_then(Value::as_str).unwrap_or("").to_string();
        let name = current.spec.get("name").and_then(Value::as_str).unwrap_or(&stats.rule_id).to_string();
        let inserted = sqlx::query(
            "INSERT INTO proposals (id, rule_id, rule_name, team, kind, summary, note, base_revision, base_rule, proposed_rule, author, auto)
             SELECT $1, $2, $3, $4, 'noisy', $5, $6, $7, $8, $9, $10, true
             WHERE NOT EXISTS (SELECT 1 FROM proposals WHERE rule_id = $2 AND auto AND (state = 'pending' OR base_revision = $7))
               AND (SELECT count(*) FROM proposals WHERE rule_id = $2 AND state = 'pending') < $11",
        )
        .bind(Uuid::now_v7())
        .bind(&stats.rule_id)
        .bind(&name)
        .bind(&team)
        .bind(&summary)
        .bind(&note)
        .bind(current.revision)
        .bind(&current.spec)
        .bind(&proposed)
        .bind(AUTHOR)
        .bind(MAX_PENDING_PER_RULE)
        .execute(pool)
        .await?
        .rows_affected();
        if inserted > 0 {
            info!(rule_id = stats.rule_id, alerts = stats.alerts, untouched = stats.untouched, max_per_key = stats.max_per_key, "auto proposal filed");
            filed += 1;
        }
    }
    Ok(filed)
}

pub async fn run_suggester(pool: PgPool, cfg: SuggestConfig, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    info!(interval_s = cfg.interval.as_secs(), min_alerts = cfg.min_alerts, "proposal suggester started");
    loop {
        if let Err(e) = suggest_once(&pool, &cfg).await {
            warn!(error = format!("{e:#}"), "suggester pass failed");
        }
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            _ = tokio::time::sleep(cfg.interval) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(alerts: i64, untouched: i64, max_per_key: i64) -> RuleStats {
        RuleStats { rule_id: "r".into(), alerts, untouched, max_per_key }
    }

    #[test]
    fn noisy_and_flapping_rules_get_a_throttle() {
        let cfg = SuggestConfig::default();
        let rule = json!({"id": "r", "team": "t"});
        let (p, summary, _) = suggestion(&cfg, &stats(400, 380, 1), &rule).unwrap();
        assert_eq!(p["throttle"], json!({"max": 3, "per": "1h"}));
        assert_eq!(summary, "Fired 400× in 7 days; 95% never acknowledged. Suggest a throttle.");
        let (_, summary, _) = suggestion(&cfg, &stats(6, 0, 6), &rule).unwrap();
        assert!(summary.starts_with("One incident reopened 6×"), "{summary}");
    }

    #[test]
    fn handled_throttled_disabled_and_grouped_rules_are_left_alone() {
        let cfg = SuggestConfig::default();
        let rule = json!({"id": "r", "team": "t"});
        assert!(suggestion(&cfg, &stats(400, 200, 1), &rule).is_none(), "half were handled by people");
        assert!(suggestion(&cfg, &stats(19, 19, 1), &rule).is_none(), "too few alerts");
        assert!(suggestion(&cfg, &stats(400, 400, 1), &json!({"id": "r", "throttle": {"max": 1, "per": "1m"}})).is_none());
        assert!(suggestion(&cfg, &stats(400, 400, 1), &json!({"id": "r", "enabled": false})).is_none());
        assert!(suggestion(&cfg, &stats(3, 0, 9), &json!({"id": "r", "group": {"by": ["x"], "window": "5m"}})).is_none());
    }
}
