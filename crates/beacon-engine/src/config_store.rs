//! Rules in Postgres (SPEC §4.6) and live reload.
//!
//! - Postgres is the authority for rules. On first start the YAML seed in
//!   `config/rules` is imported through the same validator the API uses.
//! - Every write bumps `config_state('rules')` and sends `NOTIFY beacon_config`.
//! - [`watch_config`] rebuilds the snapshot when the revision moves (NOTIFY is
//!   a hint; a periodic check catches anything missed) and publishes it on a
//!   `watch` channel. An invalid revision keeps the last good snapshot.
//!
//! Schema mappings live in Postgres too (`schema_mappings`): seeded once from
//! `config/schemas`, then edited through the API and reloaded the same way.

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
    /// Existing rule (including soft-deleted) belongs to a team not visible to the caller.
    Forbidden,
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

pub async fn list(pool: &PgPool) -> Result<Vec<StoredRule>> {
    let rows = sqlx::query("SELECT spec, revision, deleted, updated_at, updated_by FROM rules WHERE NOT deleted ORDER BY id")
        .fetch_all(pool)
        .await?;
    rows.iter().map(row).collect()
}

fn team_visible(spec: &Value, visible_teams: Option<&[String]>) -> bool {
    let Some(allowed) = visible_teams else {
        return true;
    };
    let team = spec.get("team").and_then(Value::as_str).unwrap_or("");
    allowed.iter().any(|t| t == team)
}

/// Create a rule. An existing live rule with the same id is a conflict; a
/// deleted one is replaced with a higher revision.
pub async fn create(pool: &PgPool, spec: &Value, actor: &str) -> Result<Result<StoredRule, WriteError>> {
    create_scoped(pool, spec, actor, None).await
}

/// Like [`create`], checking ownership of any existing (live or soft-deleted)
/// row atomically under the ruleset lock when `visible_teams` is provided.
pub async fn create_scoped(
    pool: &PgPool,
    spec: &Value,
    actor: &str,
    visible_teams: Option<&[String]>,
) -> Result<Result<StoredRule, WriteError>> {
    let rule = match validate(spec) {
        Ok(r) => r,
        Err(p) => return Ok(Err(WriteError::Invalid(p))),
    };
    let mut tx = pool.begin().await?;
    lock_rules(&mut tx).await?;
    let existing = current(&mut tx, &rule.id, true).await?;
    if let Some(e) = existing.as_ref() {
        if !e.deleted {
            let visible = team_visible(&e.spec, visible_teams).then(|| e.clone());
            return Ok(Err(WriteError::Conflict(visible)));
        }
        if !team_visible(&e.spec, visible_teams) {
            return Ok(Err(WriteError::Forbidden));
        }
        if e.spec.get("team").and_then(Value::as_str).unwrap_or("") != rule.team {
            return Ok(Err(WriteError::Invalid(vec![Invalid {
                path: "team".into(),
                message: "team cannot be changed when recreating a rule id".into(),
            }])));
        }
    }
    let revision = existing.map_or(1, |e| e.revision + 1);
    write(&mut tx, &rule.id, spec, revision, actor).await?;
    let saved = current(&mut tx, &rule.id, false).await?.context("rule just written")?;
    tx.commit().await?;
    Ok(Ok(saved))
}

/// Compare-and-set update.
pub async fn update(pool: &PgPool, id: &str, spec: &Value, expected_revision: i64, actor: &str) -> Result<Result<StoredRule, WriteError>> {
    update_scoped(pool, id, spec, expected_revision, actor, None).await
}

/// Like [`update`], verifying team ownership of the existing row atomically
/// under the ruleset lock (returning `NotFound` for invisible teams to avoid
/// leaking hidden rule ids or team names).
pub async fn update_scoped(
    pool: &PgPool,
    id: &str,
    spec: &Value,
    expected_revision: i64,
    actor: &str,
    visible_teams: Option<&[String]>,
) -> Result<Result<StoredRule, WriteError>> {
    let mut tx = pool.begin().await?;
    let res = update_in(&mut tx, id, spec, expected_revision, actor, visible_teams).await?;
    if res.is_ok() {
        tx.commit().await?;
    }
    Ok(res)
}

/// [`update_scoped`] inside the caller's transaction, so a rule write can
/// commit atomically with other state, e.g. a proposal being accepted.
pub async fn update_in(
    tx: &mut PgConnection,
    id: &str,
    spec: &Value,
    expected_revision: i64,
    actor: &str,
    visible_teams: Option<&[String]>,
) -> Result<Result<StoredRule, WriteError>> {
    let rule = match validate(spec) {
        Ok(r) => r,
        Err(p) => return Ok(Err(WriteError::Invalid(p))),
    };
    if rule.id != id {
        return Ok(Err(WriteError::Invalid(vec![Invalid { path: "id".into(), message: format!("must match the URL ({id})") }])));
    }
    lock_rules(&mut *tx).await?;
    let Some(existing) = current(&mut *tx, id, true).await?.filter(|e| !e.deleted) else {
        return Ok(Err(WriteError::NotFound));
    };
    if !team_visible(&existing.spec, visible_teams) {
        return Ok(Err(WriteError::NotFound));
    }
    if existing.spec.get("team").and_then(Value::as_str).unwrap_or("") != rule.team {
        return Ok(Err(WriteError::Invalid(vec![Invalid {
            path: "team".into(),
            message: "team cannot be changed on an existing rule".into(),
        }])));
    }
    if existing.revision != expected_revision {
        return Ok(Err(WriteError::Conflict(Some(existing))));
    }
    write(&mut *tx, id, spec, existing.revision + 1, actor).await?;
    let saved = current(&mut *tx, id, false).await?.context("rule just written")?;
    Ok(Ok(saved))
}

pub async fn delete(pool: &PgPool, id: &str, expected_revision: i64, actor: &str) -> Result<Result<(), WriteError>> {
    delete_scoped(pool, id, expected_revision, actor, None).await
}

/// Like [`delete`], verifying team ownership atomically under the ruleset lock.
pub async fn delete_scoped(
    pool: &PgPool,
    id: &str,
    expected_revision: i64,
    actor: &str,
    visible_teams: Option<&[String]>,
) -> Result<Result<(), WriteError>> {
    let mut tx = pool.begin().await?;
    lock_rules(&mut tx).await?;
    let Some(existing) = current(&mut tx, id, true).await?.filter(|e| !e.deleted) else {
        return Ok(Err(WriteError::NotFound));
    };
    if !team_visible(&existing.spec, visible_teams) {
        return Ok(Err(WriteError::NotFound));
    }
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
/// seed rules are reported and skipped. On existing databases, also migrates
/// the legacy shipped `oncall:settlements` route to `oncall:settlements-primary`.
pub async fn seed_from_yaml(pool: &PgPool, config_dir: &Path) -> Result<usize> {
    seed_schemas_from_yaml(pool, config_dir).await?;
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM rules").fetch_one(pool).await?;
    if n > 0 {
        let mut tx = pool.begin().await?;
        lock_rules(&mut tx).await?;
        if let Some(mut r) = current(&mut tx, "settlements-workflow-failed", true).await?.filter(|r| !r.deleted) {
            let mut changed = false;
            if let Some(routes) = r.spec.get_mut("route").and_then(Value::as_array_mut) {
                for entry in routes {
                    if entry.get("to").and_then(Value::as_str) == Some("oncall:settlements") {
                        entry["to"] = Value::String("oncall:settlements-primary".into());
                        changed = true;
                    }
                }
            }
            if changed {
                write(&mut tx, "settlements-workflow-failed", &r.spec, r.revision + 1, "config").await?;
            }
        }
        tx.commit().await?;
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

/// A stored schema mapping.
#[derive(Debug, Clone)]
pub struct StoredSchema {
    pub spec: Value,
    pub revision: i64,
    pub updated_at: DateTime<Utc>,
    pub updated_by: String,
}

const MAX_SCHEMA_FIELDS: usize = 64;

/// Validate a mapping with the same compiler the matcher uses.
pub fn validate_schema(spec: &Value) -> Result<SchemaMappingSpec, Vec<Invalid>> {
    let inv = |path: &str, message: String| vec![Invalid { path: path.into(), message }];
    if let Some(p) = forbidden_key_path(spec, "") {
        return Err(inv(&p, "this key name is not allowed".into()));
    }
    if spec.to_string().len() > MAX_RULE_BYTES {
        return Err(inv("", format!("a mapping must be under {} KiB", MAX_RULE_BYTES / 1024)));
    }
    let m: SchemaMappingSpec = serde_json::from_value(spec.clone()).map_err(|e| inv("", format!("not a valid mapping: {e}")))?;
    // Each field clones its value out of the event, so the count bounds the work.
    if m.fields.len() > MAX_SCHEMA_FIELDS {
        return Err(inv("fields", format!("a mapping can have at most {MAX_SCHEMA_FIELDS} fields")));
    }
    // Redaction keys on the field name, so copying a secret into an ordinary
    // name would let it out unmasked.
    for (name, f) in &m.fields {
        if f.path.as_deref().is_some_and(beacon_core::matcher::is_secret_path_str) {
            return Err(inv(&format!("fields.{name}"), "this path holds a secret and can't be mapped".into()));
        }
    }
    if let Err(e) = beacon_core::mapping::SchemaMapping::compile(&m) {
        let msg = e.to_string();
        let path = match &e {
            beacon_core::mapping::MappingError::Detect { .. } => "detect".to_string(),
            beacon_core::mapping::MappingError::FieldPath { field, .. } | beacon_core::mapping::MappingError::FieldEmpty { field, .. } => format!("fields.{field}"),
            beacon_core::mapping::MappingError::BadName => "schema".into(),
        };
        return Err(inv(&path, msg));
    }
    Ok(m)
}

fn schema_row(r: &sqlx::postgres::PgRow) -> Result<StoredSchema> {
    Ok(StoredSchema {
        spec: r.try_get("spec")?,
        revision: r.try_get("revision")?,
        updated_at: r.try_get("updated_at")?,
        updated_by: r.try_get("updated_by")?,
    })
}

pub async fn list_schemas(pool: &PgPool) -> Result<Vec<StoredSchema>> {
    let rows = sqlx::query("SELECT spec, revision, updated_at, updated_by FROM schema_mappings ORDER BY schema").fetch_all(pool).await?;
    rows.iter().map(schema_row).collect()
}

/// Valid stored mappings, for snapshots and the rule test panel. An invalid
/// stored mapping is skipped (and logged), never fatal.
pub async fn schema_specs(pool: &PgPool) -> Result<Vec<SchemaMappingSpec>> {
    Ok(valid_schema_specs(list_schemas(pool).await?))
}

/// The CPU half of [`schema_specs`] (compiles every mapping), for callers
/// that run it on a blocking thread.
pub fn valid_schema_specs(stored: Vec<StoredSchema>) -> Vec<SchemaMappingSpec> {
    let mut out = Vec::new();
    for s in stored {
        match validate_schema(&s.spec) {
            Ok(m) => out.push(m),
            Err(problems) => error!(?problems, "stored schema mapping is invalid; skipped"),
        }
    }
    out
}

/// Create (no `expected_revision`) or compare-and-set update a mapping.
/// Returns the saved mapping and whether it was created.
pub async fn save_schema(pool: &PgPool, spec: &Value, expected_revision: Option<i64>, actor: &str) -> Result<Result<(StoredSchema, bool), WriteError>> {
    let mut tx = pool.begin().await?;
    lock_rules(&mut tx).await?;
    let res = save_schema_in(&mut tx, spec, expected_revision, actor).await?;
    if res.is_ok() {
        tx.commit().await?;
    }
    Ok(res)
}

/// [`save_schema`] inside the caller's transaction, which holds the ruleset lock.
async fn save_schema_in(tx: &mut PgConnection, spec: &Value, expected_revision: Option<i64>, actor: &str) -> Result<Result<(StoredSchema, bool), WriteError>> {
    let m = match validate_schema(spec) {
        Ok(m) => m,
        Err(p) => return Ok(Err(WriteError::Invalid(p))),
    };
    let existing = sqlx::query("SELECT spec, revision, updated_at, updated_by FROM schema_mappings WHERE schema = $1 FOR UPDATE")
        .bind(&m.schema)
        .fetch_optional(&mut *tx)
        .await?
        .as_ref()
        .map(schema_row)
        .transpose()?;
    let revision = match (&existing, expected_revision) {
        (None, None) => 1,
        (Some(e), Some(v)) if e.revision == v => e.revision + 1,
        (None, Some(_)) => return Ok(Err(WriteError::NotFound)),
        (Some(_), _) => {
            return Ok(Err(WriteError::Conflict(None)));
        }
    };
    sqlx::query(
        "INSERT INTO schema_mappings (schema, spec, revision, updated_at, updated_by) VALUES ($1, $2, $3, now(), $4)
         ON CONFLICT (schema) DO UPDATE SET spec = EXCLUDED.spec, revision = EXCLUDED.revision, updated_at = now(), updated_by = EXCLUDED.updated_by",
    )
    .bind(&m.schema)
    .bind(spec)
    .bind(revision)
    .bind(actor)
    .execute(&mut *tx)
    .await?;
    bump(&mut *tx).await?;
    let saved = sqlx::query("SELECT spec, revision, updated_at, updated_by FROM schema_mappings WHERE schema = $1")
        .bind(&m.schema)
        .fetch_one(&mut *tx)
        .await?;
    let saved = schema_row(&saved)?;
    Ok(Ok((saved, existing.is_none())))
}

/// The current stored mapping for a schema name (conflict bodies).
pub async fn get_schema(pool: &PgPool, schema: &str) -> Result<Option<StoredSchema>> {
    sqlx::query("SELECT spec, revision, updated_at, updated_by FROM schema_mappings WHERE schema = $1")
        .bind(schema)
        .fetch_optional(pool)
        .await?
        .as_ref()
        .map(schema_row)
        .transpose()
}

/// Import `config/schemas/*.yaml` once, when the table is empty. The check and
/// the import share one transaction under the ruleset lock, so concurrent
/// replicas can't both seed and a crash mid-import leaves the table empty.
async fn seed_schemas_from_yaml(pool: &PgPool, config_dir: &Path) -> Result<()> {
    let mut tx = pool.begin().await?;
    lock_rules(&mut tx).await?;
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM schema_mappings").fetch_one(&mut *tx).await?;
    if n > 0 {
        return Ok(());
    }
    let specs: Vec<Value> = load_yaml_files(&config_dir.join("schemas"))?;
    let mut imported = 0;
    for spec in specs {
        match save_schema_in(&mut tx, &spec, None, "config").await? {
            Ok(_) => imported += 1,
            Err(e) => warn!(?e, "seed schema mapping rejected"),
        }
    }
    tx.commit().await?;
    info!(imported, "schema mappings seeded from config/schemas");
    Ok(())
}

/// Build the matcher snapshot from Postgres rules and schema mappings. Rules
/// or mappings that no longer compile are skipped with an error log rather
/// than taking the whole config down.
pub async fn load_snapshot(pool: &PgPool) -> Result<ConfigSnapshot> {
    let revision = rules_revision(pool).await?;
    let schemas = schema_specs(pool).await?;
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
    ConfigSnapshot::build_with(revision, schemas, rules)
}

/// Keep a live snapshot: initial load, then rebuild on NOTIFY or when the
/// periodic check sees a newer revision.
pub async fn watch_config(pool: PgPool, config_dir: PathBuf) -> Result<watch::Receiver<Arc<ConfigSnapshot>>> {
    seed_from_yaml(&pool, &config_dir).await?;
    let first = load_snapshot(&pool).await.context("loading rules and schemas from Postgres")?;
    info!(revision = first.revision, rules = first.rules.len(), "config snapshot loaded");
    let (tx, rx) = watch::channel(Arc::new(first));
    tokio::spawn(async move {
        loop {
            if let Err(e) = reload_loop(&pool, &tx).await {
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

async fn reload_loop(pool: &PgPool, tx: &watch::Sender<Arc<ConfigSnapshot>>) -> Result<()> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen(CONFIG_CHANNEL).await?;
    loop {
        let current = tx.borrow().revision;
        let latest = rules_revision(pool).await?;
        if latest != current {
            match load_snapshot(pool).await {
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
