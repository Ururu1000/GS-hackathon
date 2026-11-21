//! Rules in Postgres (SPEC §4.6) and live reload.
//!
//! - Postgres is the authority for rules. On first start the YAML seed in
//!   `config/rules` is imported through the same validator the API uses.
//! - Every write bumps `config_state('rules')` and sends `NOTIFY beacon_config`.
//! - [`watch_config`] rebuilds the snapshot when the revision moves (NOTIFY is
//!   a hint; a periodic check catches anything missed) and publishes it on a
//!   `watch` channel. An invalid revision keeps the last good snapshot.
//!
//! Schema mappings are still read from `config/schemas` at startup.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use beacon_core::config::load_yaml_files;
use beacon_core::mapping::SchemaMappingSpec;
use beacon_core::rule::{CompiledRule, RuleError, RuleSpec};
use beacon_core::{ConfigSnapshot, template};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::PgListener;
use sqlx::{PgConnection, PgPool, Row};
use tokio::sync::watch;
use tracing::{error, info, warn};

pub const CONFIG_CHANNEL: &str = "beacon_config";
/// Size bounds that keep any valid rule's matches far below the Kafka limit.
pub const MAX_RULE_BYTES: usize = 64 * 1024;
pub const MAX_ROUTES: usize = 16;
const RECHECK: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct StoredRule {
    pub spec: Value,
    pub revision: i64,
    pub deleted: bool,
    pub updated_at: DateTime<Utc>,
    pub updated_by: String,
}

/// A validation problem with a dotted path into the submitted rule.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Invalid {
    pub path: String,
    pub message: String,
}

#[derive(Debug)]
pub enum WriteError {
    Invalid(Vec<Invalid>),
    /// Someone saved first (or the rule exists / is gone). Carries the current row.
    Conflict(Option<StoredRule>),
    NotFound,
}

/// The one validator for API writes, YAML seeds and the rule test panel.
pub fn validate(spec: &Value) -> Result<RuleSpec, Vec<Invalid>> {
    let mut problems = Vec::new();
    let inv = |path: &str, message: String| Invalid { path: path.into(), message };
    if let Some(p) = forbidden_key_path(spec, "") {
        return Err(vec![inv(&p, "this key name is not allowed".into())]);
    }
    if spec.to_string().len() > MAX_RULE_BYTES {
        return Err(vec![inv("", format!("a rule must be under {} KiB", MAX_RULE_BYTES / 1024))]);
    }
    let rule: RuleSpec = match serde_json::from_value(spec.clone()) {
        Ok(r) => r,
        Err(e) => return Err(vec![inv("", format!("not a valid rule: {e}"))]),
    };
    let id_ok = rule.id.len() >= 2
        && rule.id.len() <= 63
        && rule.id.chars().next().is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && rule.id.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if !id_ok {
        problems.push(inv("id", "2-63 characters: a-z, 0-9 and '-', starting with a letter or digit".into()));
    }
    if rule.team.trim().is_empty() {
        problems.push(inv("team", "required".into()));
    }
    if rule.route.is_empty() {
        problems.push(inv("route", "at least one recipient is required".into()));
    }
    if rule.route.len() > MAX_ROUTES {
        problems.push(inv("route", format!("at most {MAX_ROUTES} recipients")));
    }
    for (which, t) in [("title", &rule.present.title), ("body", &rule.present.body)] {
        if t.as_ref().is_some_and(|t| t.len() > beacon_core::template::MAX_TEMPLATE_LEN) {
            problems.push(inv(&format!("present.{which}"), format!("longer than {} characters", beacon_core::template::MAX_TEMPLATE_LEN)));
        }
    }
    if rule.present.title.as_deref().is_none_or(|t| t.trim().is_empty()) {
        problems.push(inv("present.title", "required".into()));
    }
    match CompiledRule::compile(rule.clone(), 0) {
        Ok(c) => {
            let mut env = template::new_env();
            for (which, src) in [("title", c.title_template()), ("body", c.body_template())] {
                if let Err(e) = env.add_template_owned(which.to_string(), src) {
                    problems.push(inv(&format!("present.{which}"), e.to_string()));
                }
            }
        }
        Err(e) => {
            let path = match &e {
                RuleError::When { .. } => "when".to_string(),
                RuleError::Recover { .. } => "auto_resolve.recover_when".into(),
                RuleError::DedupKey { rule: _, path, .. } => {
                    let i = rule.dedup.key.iter().position(|p| p == path).unwrap_or(0);
                    format!("dedup.key.{i}")
                }
                RuleError::Field { .. } => "present.fields".into(),
                RuleError::Template { which, .. } => format!("present.{which}"),
                RuleError::Color { .. } => "present.color".into(),
                RuleError::Route { to, .. } => {
                    let i = rule.route.iter().position(|r| &r.to == to).unwrap_or(0);
                    format!("route.{i}.to")
                }
                RuleError::BadId => "id".into(),
                RuleError::TooMany { .. } => "present.fields".into(),
                RuleError::Group { .. } => "group".into(),
            };
            if !problems.iter().any(|p| p.path == path) {
                problems.push(Invalid { path, message: e.to_string() });
            }
        }
    }
    if problems.is_empty() { Ok(rule) } else { Err(problems) }
}

/// Serialize rule writes. A `FOR UPDATE` on a not-yet-existing rule row locks
/// nothing, so two creates of the same id would both succeed; locking the
/// ruleset row first makes every create/update/delete strictly ordered.
async fn lock_rules(tx: &mut PgConnection) -> Result<()> {
    sqlx::query("SELECT revision FROM config_state WHERE key = 'rules' FOR UPDATE").execute(&mut *tx).await?;
    Ok(())
}

/// Keys that turn into prototype pollution in JS consumers of the rule JSON.
const FORBIDDEN_KEYS: &[&str] = &["__proto__", "constructor", "prototype"];

fn forbidden_key_path(v: &Value, path: &str) -> Option<String> {
    let join = |k: &str| if path.is_empty() { k.to_string() } else { format!("{path}.{k}") };
    match v {
        Value::Object(m) => m.iter().find_map(|(k, v)| {
            if FORBIDDEN_KEYS.contains(&k.as_str()) { Some(join(k)) } else { forbidden_key_path(v, &join(k)) }
        }),
        Value::Array(a) => a.iter().enumerate().find_map(|(i, v)| forbidden_key_path(v, &join(&i.to_string()))),
        _ => None,
    }
}

async fn bump(tx: &mut PgConnection) -> Result<i64> {
    let rev: i64 = sqlx::query_scalar("UPDATE config_state SET revision = revision + 1 WHERE key = 'rules' RETURNING revision")
        .fetch_one(&mut *tx)
        .await?;
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(CONFIG_CHANNEL)
        .bind(rev.to_string())
        .execute(&mut *tx)
        .await?;
    Ok(rev)
}

fn row(r: &sqlx::postgres::PgRow) -> Result<StoredRule> {
    Ok(StoredRule {
        spec: r.try_get("spec")?,
        revision: r.try_get("revision")?,
        deleted: r.try_get("deleted")?,
        updated_at: r.try_get("updated_at")?,
        updated_by: r.try_get("updated_by")?,
    })
}

async fn current(tx: &mut PgConnection, id: &str, lock: bool) -> Result<Option<StoredRule>> {
    let sql = if lock {
        "SELECT spec, revision, deleted, updated_at, updated_by FROM rules WHERE id = $1 FOR UPDATE"
    } else {
        "SELECT spec, revision, deleted, updated_at, updated_by FROM rules WHERE id = $1"
    };
    sqlx::query(sql).bind(id).fetch_optional(&mut *tx).await?.as_ref().map(row).transpose()
}

pub async fn get(pool: &PgPool, id: &str) -> Result<Option<StoredRule>> {
    let mut c = pool.acquire().await?;
    Ok(current(&mut c, id, false).await?.filter(|r| !r.deleted))
}

/// Including soft-deleted rows (recreate checks).
pub async fn get_any(pool: &PgPool, id: &str) -> Result<Option<StoredRule>> {
    let mut c = pool.acquire().await?;
    current(&mut c, id, false).await
}

pub async fn list(pool: &PgPool) -> Result<Vec<StoredRule>> {
    let rows = sqlx::query("SELECT spec, revision, deleted, updated_at, updated_by FROM rules WHERE NOT deleted ORDER BY id")
        .fetch_all(pool)
        .await?;
    rows.iter().map(row).collect()
}

/// Create a rule. An existing live rule with the same id is a conflict; a
/// deleted one is replaced with a higher revision.
pub async fn create(pool: &PgPool, spec: &Value, actor: &str) -> Result<Result<StoredRule, WriteError>> {
    let rule = match validate(spec) {
        Ok(r) => r,
        Err(p) => return Ok(Err(WriteError::Invalid(p))),
    };
    let mut tx = pool.begin().await?;
    lock_rules(&mut tx).await?;
    let existing = current(&mut tx, &rule.id, true).await?;
    if let Some(e) = existing.as_ref().filter(|e| !e.deleted) {
        return Ok(Err(WriteError::Conflict(Some(e.clone()))));
    }
    let revision = existing.map_or(1, |e| e.revision + 1);
    write(&mut tx, &rule.id, spec, revision, actor).await?;
    let saved = current(&mut tx, &rule.id, false).await?.context("rule just written")?;
    tx.commit().await?;
    Ok(Ok(saved))
}

/// Compare-and-set update.
pub async fn update(pool: &PgPool, id: &str, spec: &Value, expected_revision: i64, actor: &str) -> Result<Result<StoredRule, WriteError>> {
    let rule = match validate(spec) {
        Ok(r) => r,
        Err(p) => return Ok(Err(WriteError::Invalid(p))),
    };
    if rule.id != id {
        return Ok(Err(WriteError::Invalid(vec![Invalid { path: "id".into(), message: format!("must match the URL ({id})") }])));
    }
    let mut tx = pool.begin().await?;
    lock_rules(&mut tx).await?;
    let Some(existing) = current(&mut tx, id, true).await?.filter(|e| !e.deleted) else {
        return Ok(Err(WriteError::NotFound));
    };
    if existing.revision != expected_revision {
        return Ok(Err(WriteError::Conflict(Some(existing))));
    }
    write(&mut tx, id, spec, existing.revision + 1, actor).await?;
    let saved = current(&mut tx, id, false).await?.context("rule just written")?;
    tx.commit().await?;
    Ok(Ok(saved))
}

pub async fn delete(pool: &PgPool, id: &str, expected_revision: i64, actor: &str) -> Result<Result<(), WriteError>> {
    let mut tx = pool.begin().await?;
    lock_rules(&mut tx).await?;
    let Some(existing) = current(&mut tx, id, true).await?.filter(|e| !e.deleted) else {
        return Ok(Err(WriteError::NotFound));
    };
    if existing.revision != expected_revision {
        return Ok(Err(WriteError::Conflict(Some(existing))));
    }
    sqlx::query("UPDATE rules SET deleted = true, updated_at = now(), updated_by = $2 WHERE id = $1")
        .bind(id)
        .bind(actor)
        .execute(&mut *tx)
        .await?;
    bump(&mut tx).await?;
    tx.commit().await?;
    Ok(Ok(()))
}

async fn write(tx: &mut PgConnection, id: &str, spec: &Value, revision: i64, actor: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO rules (id, spec, revision, deleted, updated_at, updated_by) VALUES ($1, $2, $3, false, now(), $4)
         ON CONFLICT (id) DO UPDATE SET spec = EXCLUDED.spec, revision = EXCLUDED.revision, deleted = false,
             updated_at = now(), updated_by = EXCLUDED.updated_by",
    )
    .bind(id)
    .bind(spec)
    .bind(revision)
    .bind(actor)
    .execute(&mut *tx)
    .await?;
    bump(tx).await?;
    Ok(())
}

/// Import `config/rules/*.yaml` once, when the rules table is empty. Invalid
/// seed rules are reported and skipped.
pub async fn seed_from_yaml(pool: &PgPool, config_dir: &Path) -> Result<usize> {
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM rules").fetch_one(pool).await?;
    if n > 0 {
        return Ok(0);
    }
    let specs: Vec<Value> = load_yaml_files(&config_dir.join("rules"))?;
    let mut imported = 0;
    for spec in specs {
        match create(pool, &spec, "config").await? {
            Ok(_) => imported += 1,
            Err(e) => warn!(?e, "seed rule rejected"),
        }
    }
    info!(imported, "rules seeded from config/rules");
    Ok(imported)
}

pub async fn rules_revision(pool: &PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT revision FROM config_state WHERE key = 'rules'").fetch_one(pool).await?)
}

pub fn load_schemas(config_dir: &Path) -> Result<Vec<SchemaMappingSpec>> {
    load_yaml_files(&config_dir.join("schemas"))
}

/// Build the matcher snapshot from Postgres rules + YAML schema mappings.
/// Rules that no longer compile are skipped with an error log rather than
/// taking the whole ruleset down.
pub async fn load_snapshot(pool: &PgPool, schemas: &[SchemaMappingSpec]) -> Result<ConfigSnapshot> {
    let revision = rules_revision(pool).await?;
    let mut rules = Vec::new();
    // Each rule is validated on its own: one bad stored rule is skipped
    // (and logged) instead of making the whole ruleset fail to load.
    for r in list(pool).await? {
        match validate(&r.spec) {
            Ok(spec) => rules.push((spec, r.revision)),
            Err(problems) => {
                let id = r.spec.get("id").and_then(|v| v.as_str()).unwrap_or("?").to_string();
                error!(rule = %id, ?problems, "stored rule is invalid; skipped");
            }
        }
    }
    ConfigSnapshot::build_with(revision, schemas.to_vec(), rules)
}

/// Keep a live snapshot: initial load, then rebuild on NOTIFY or when the
/// periodic check sees a newer revision.
pub async fn watch_config(pool: PgPool, config_dir: PathBuf) -> Result<watch::Receiver<Arc<ConfigSnapshot>>> {
    let schemas = load_schemas(&config_dir).context("loading config/schemas")?;
    let first = load_snapshot(&pool, &schemas).await.context("loading rules from Postgres")?;
    info!(revision = first.revision, rules = first.rules.len(), "config snapshot loaded");
    let (tx, rx) = watch::channel(Arc::new(first));
    tokio::spawn(async move {
        loop {
            if let Err(e) = reload_loop(&pool, &schemas, &tx).await {
                warn!(error = format!("{e:#}"), "config watcher failed; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            if tx.is_closed() {
                return;
            }
        }
    });
    Ok(rx)
}

async fn reload_loop(pool: &PgPool, schemas: &[SchemaMappingSpec], tx: &watch::Sender<Arc<ConfigSnapshot>>) -> Result<()> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen(CONFIG_CHANNEL).await?;
    loop {
        let current = tx.borrow().revision;
        let latest = rules_revision(pool).await?;
        if latest != current {
            match load_snapshot(pool, schemas).await {
                Ok(s) => {
                    info!(revision = s.revision, rules = s.rules.len(), "config reloaded");
                    tx.send_replace(Arc::new(s));
                }
                Err(e) => error!(error = format!("{e:#}"), revision = latest, "invalid config revision; keeping the last good one"),
            }
        }
        if tx.is_closed() {
            return Ok(());
        }
        tokio::select! {
            n = listener.recv() => { n?; }
            _ = tokio::time::sleep(RECHECK) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rule() -> Value {
        json!({"id": "disk-full", "team": "infra", "when": {"path": "raw.used_pct", "op": "gte", "value": 95},
               "route": [{"to": "team:infra"}], "present": {"title": "Disk {{ raw.mount }} full"}})
    }

    #[test]
    fn validator_accepts_and_reports_paths() {
        assert!(validate(&rule()).is_ok());
        let mut bad = rule();
        bad["id"] = json!("Disk_Full");
        bad["route"] = json!([{"to": "nobody"}]);
        bad["present"]["title"] = json!("{{ broken");
        let errs = validate(&bad).unwrap_err();
        let paths: Vec<&str> = errs.iter().map(|e| e.path.as_str()).collect();
        assert!(paths.contains(&"id"), "{errs:?}");
        let mut bad = rule();
        bad["when"] = json!({"path": "raw.x", "op": "regex", "value": "("});
        assert_eq!(validate(&bad).unwrap_err()[0].path, "when");
        let mut bad = rule();
        bad["route"] = json!([]);
        assert_eq!(validate(&bad).unwrap_err()[0].path, "route");
        let mut bad = rule();
        bad["group"] = json!({"by": ["raw.x"], "window": "forever"});
        assert_eq!(validate(&bad).unwrap_err()[0].path, "group");
        let mut ok = rule();
        ok["group"] = json!({"by": ["raw.x"], "window": 300});
        assert_eq!(validate(&ok).unwrap_err()[0].path, "group", "grouped rules must auto-resolve");
        ok["auto_resolve"] = json!({"quiet_for": "10m"});
        assert!(validate(&ok).is_ok());
        let mut bad = rule();
        bad["present"]["title"] = json!("x".repeat(5000));
        assert_eq!(validate(&bad).unwrap_err()[0].path, "present.title");
        let mut bad = rule();
        bad["when"] = json!({"path": "raw.x", "op": "in", "value": (0..300).collect::<Vec<i32>>()});
        assert_eq!(validate(&bad).unwrap_err()[0].path, "when");
        let mut bad = rule();
        bad["present"]["__proto__"] = json!({"x": 1});
        assert_eq!(validate(&bad).unwrap_err()[0].path, "present.__proto__");
    }
}
