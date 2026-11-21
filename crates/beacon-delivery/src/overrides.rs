//! Saved people/teams edits: `directory_overrides` rows applied over the startup YAML
//! Directory (contract v0.2.0 `PUT /api/people/{id}/preferences` and `PUT /api/teams/{id}`,
//! migration `20261121060000_delivery_directory_overrides.sql`).
//!
//! The YAML config stays the base and keeps revision `1` for every person and team; the
//! API's PUTs write one override row per id, the first save at revision 2, each later
//! save `+1` (compare-and-set on `expected_revision`, see `api`). A person row's `body`
//! is `{"preferences": [...]}`; a team row's `body` is the full effective team after the
//! save (`{"members", "lead", "oncall", "default_channels"}`).
//!
//! [`apply`] replaces the overridden fields and rebuilds the whole directory with
//! `Directory::from_parts`, so a saved edit goes through exactly the parsing and
//! cross-reference validation a YAML import does (SPEC §4.6: one validator). Overrides
//! naming ids that are no longer in the base are ignored with a warning.
//!
//! [`EffectiveDirectory`] caches the applied view and rebuilds it only when the table's
//! `(count, sum(revision))` stamp moves — one cheap query per pass, the pattern the
//! router, the escalation scheduler and the actions worker use so a save applies to the
//! next notification / escalation step / action. A database that is away logs and serves
//! the last good directory; an override that no longer validates is dropped on its own
//! ([`apply_lenient`]), so the other saved edits keep applying: a broken override must
//! never stop a delivery.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;
use sqlx::PgPool;
use tracing::{error, warn};

use crate::config::{ConfigError, Directory, Person, Team};

/// `directory_overrides.kind` for a person row.
pub const KIND_PERSON: &str = "person";
/// `directory_overrides.kind` for a team row.
pub const KIND_TEAM: &str = "team";

/// One saved person edit: the raw `{"preferences": [...]}` body plus its revision.
#[derive(Debug, Clone)]
pub struct PersonOverride {
    pub body: Value,
    pub revision: i64,
}

/// One saved team edit: the full effective team body plus its revision.
#[derive(Debug, Clone)]
pub struct TeamOverride {
    pub body: Value,
    pub revision: i64,
}

/// Every `directory_overrides` row, split by kind (the table is small; it is loaded
/// whole whenever its stamp moved).
#[derive(Debug, Clone, Default)]
pub struct Overrides {
    pub people: HashMap<String, PersonOverride>,
    pub teams: HashMap<String, TeamOverride>,
}

/// Every override row.
pub async fn load(pool: &PgPool) -> Result<Overrides, sqlx::Error> {
    let rows: Vec<(String, String, Value, i64)> =
        sqlx::query_as("SELECT kind, id, body, revision FROM directory_overrides")
            .fetch_all(pool)
            .await?;
    let mut ov = Overrides::default();
    for (kind, id, body, revision) in rows {
        match kind.as_str() {
            KIND_PERSON => {
                ov.people.insert(id, PersonOverride { body, revision });
            }
            KIND_TEAM => {
                ov.teams.insert(id, TeamOverride { body, revision });
            }
            other => warn!(kind = other, "unknown directory_overrides kind, ignoring"),
        }
    }
    Ok(ov)
}

/// The id's current revision: the override's, else the YAML base's constant `1`.
pub fn revision_of(ov: &Overrides, kind: &str, id: &str) -> i64 {
    match kind {
        KIND_PERSON => ov.people.get(id).map_or(1, |o| o.revision),
        KIND_TEAM => ov.teams.get(id).map_or(1, |o| o.revision),
        _ => 1,
    }
}

/// The base directory with every override applied: a person's raw `preferences` and a
/// team's members / lead / on-call / default channels are replaced from the override
/// bodies, then the whole directory is rebuilt with `Directory::from_parts` — the same
/// parsing and validation a YAML import goes through. `delivery`, `rules` and `runbooks`
/// are carried over from the base unchanged. Overrides for ids missing from the base
/// (removed from the YAML) are ignored with a warning.
pub fn apply(base: &Directory, ov: &Overrides) -> Result<Directory, ConfigError> {
    for id in ov.people.keys() {
        if !base.people.contains_key(id) {
            warn!(person = %id, "override for a person not in the base directory, ignoring");
        }
    }
    for id in ov.teams.keys() {
        if !base.teams.contains_key(id) {
            warn!(team = %id, "override for a team not in the base directory, ignoring");
        }
    }

    let mut people: Vec<Person> = base.people.values().cloned().collect();
    for p in &mut people {
        let Some(o) = ov.people.get(&p.id) else {
            continue;
        };
        // The body is `{"preferences": [...]}`; a missing key keeps the configured value.
        if let Some(preferences) = o.body.get("preferences") {
            p.preferences = Some(preferences.clone());
        }
    }

    let mut teams: Vec<Team> = base.teams.values().cloned().collect();
    for t in &mut teams {
        let Some(o) = ov.teams.get(&t.id) else {
            continue;
        };
        apply_team_body(t, &o.body);
    }

    let mut dir = Directory::from_parts(people, teams)?;
    dir.delivery = base.delivery.clone();
    dir.rules = base.rules.clone();
    dir.runbooks = base.runbooks.clone();
    Ok(dir)
}

/// Replace a team's fields from a saved body. Bodies the API writes carry every key; a
/// missing key keeps the current value (defensive), and an empty `default_channels`
/// list means "unset" (`from_parts` rejects an explicitly empty default).
fn apply_team_body(t: &mut Team, body: &Value) {
    if let Some(members) = body.get("members").and_then(Value::as_array) {
        t.members = members
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
    }
    if let Some(lead) = body.get("lead") {
        t.lead = lead.as_str().map(str::to_string);
    }
    if let Some(oncall) = body.get("oncall").and_then(Value::as_object) {
        t.oncall = oncall
            .iter()
            .map(|(schedule, rotation)| {
                (
                    schedule.clone(),
                    rotation
                        .as_array()
                        .map(|list| {
                            list.iter()
                                .filter_map(Value::as_str)
                                .map(str::to_string)
                                .collect()
                        })
                        .unwrap_or_default(),
                )
            })
            .collect();
    }
    if let Some(defaults) = body.get("default_channels").and_then(Value::as_array) {
        let list: Vec<String> = defaults
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect();
        t.default_channels = (!list.is_empty()).then_some(list);
    }
}

/// [`apply`], but one override that no longer validates (say a team naming a person since
/// removed from the YAML) drops only itself, with a warning, instead of the whole set.
/// Returns the directory and the overrides that were applied. If the whole set applies
/// this is exactly [`apply`]; otherwise overrides are tried one at a time in a fixed
/// order (people, then teams, each by id) and kept only if the set so far still applies.
pub fn apply_lenient(base: &Directory, ov: &Overrides) -> (Directory, Overrides) {
    if let Ok(dir) = apply(base, ov) {
        return (dir, ov.clone());
    }
    let mut kept = Overrides::default();
    let mut people: Vec<_> = ov.people.iter().collect();
    people.sort_by(|a, b| a.0.cmp(b.0));
    for (id, o) in people {
        let mut trial = kept.clone();
        trial.people.insert(id.clone(), o.clone());
        match apply(base, &trial) {
            Ok(_) => kept = trial,
            Err(err) => {
                warn!(person = %id, %err, "saved preferences no longer validate, ignoring them")
            }
        }
    }
    let mut teams: Vec<_> = ov.teams.iter().collect();
    teams.sort_by(|a, b| a.0.cmp(b.0));
    for (id, o) in teams {
        let mut trial = kept.clone();
        trial.teams.insert(id.clone(), o.clone());
        match apply(base, &trial) {
            Ok(_) => kept = trial,
            Err(err) => warn!(team = %id, %err, "saved team no longer validates, ignoring it"),
        }
    }
    match apply(base, &kept) {
        Ok(dir) => (dir, kept),
        // Only reachable if the base itself does not rebuild; serve it unchanged.
        Err(err) => {
            error!(%err, "the base directory does not rebuild, ignoring every override");
            (base.clone(), Overrides::default())
        }
    }
}

/// The `(count, sum(revision))` stamp of the overrides table. Every save either inserts a
/// row (count + 1) or bumps one row's revision (sum + 1), so the stamp moves on every
/// committed save, whatever order concurrent saves commit in (a timestamp such as
/// `max(updated_at)` does not: `now()` is the transaction's start, not its commit).
type Stamp = (i64, i64);

/// One consistent effective view.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// The base with every still-valid override applied.
    pub dir: Arc<Directory>,
    /// Every override row as stored: the revisions views show and saves compare against
    /// (also for an override that no longer validates, so a fresh save can replace it).
    pub overrides: Arc<Overrides>,
    /// The overrides that were applied to `dir` (a save validates on top of these).
    pub applied: Arc<Overrides>,
}

/// The cached effective view and the stamp it was built at.
#[derive(Debug)]
struct Cache {
    stamp: Option<Stamp>,
    snap: Snapshot,
}

/// Handle around the startup base directory that serves the effective one: overrides
/// applied, rebuilt only when the table's stamp moved, never failing a caller — a
/// query or apply error logs and serves the last good directory.
pub struct EffectiveDirectory {
    base: Arc<Directory>,
    cache: tokio::sync::Mutex<Cache>,
    /// How many times a stamp change rebuilt the directory (test hook).
    rebuilds: AtomicU64,
}

impl EffectiveDirectory {
    pub fn new(base: Arc<Directory>) -> Self {
        let empty = Arc::new(Overrides::default());
        Self {
            cache: tokio::sync::Mutex::new(Cache {
                stamp: None,
                snap: Snapshot {
                    dir: base.clone(),
                    overrides: empty.clone(),
                    applied: empty,
                },
            }),
            base,
            rebuilds: AtomicU64::new(0),
        }
    }

    /// The effective directory.
    pub async fn get(&self, pool: &PgPool) -> Arc<Directory> {
        self.snapshot(pool).await.dir
    }

    /// The effective directory with the stored and the applied overrides.
    pub async fn snapshot(&self, pool: &PgPool) -> Snapshot {
        // One cheap query, outside the lock; a failure serves the last good directory.
        let stamp: Stamp = match sqlx::query_as(
            "SELECT count(*), coalesce(sum(revision), 0)::bigint FROM directory_overrides",
        )
        .fetch_one(pool)
        .await
        {
            Ok(stamp) => stamp,
            Err(err) => {
                error!(%err, "cannot read directory_overrides, serving the last good directory");
                return self.cache.lock().await.snap.clone();
            }
        };

        let mut cache = self.cache.lock().await;
        if cache.stamp == Some(stamp) {
            return cache.snap.clone();
        }
        match load(pool).await {
            Ok(ov) => {
                // A single override that no longer validates drops only itself.
                let (dir, applied) = apply_lenient(&self.base, &ov);
                cache.stamp = Some(stamp);
                cache.snap = Snapshot {
                    dir: Arc::new(dir),
                    overrides: Arc::new(ov),
                    applied: Arc::new(applied),
                };
                self.rebuilds.fetch_add(1, Ordering::Relaxed);
            }
            Err(err) => {
                error!(%err, "cannot load directory overrides, serving the last good directory");
            }
        }
        cache.snap.clone()
    }

    /// How many times the cache rebuilt the directory (test hook for the stamp cache).
    pub fn rebuilds(&self) -> u64 {
        self.rebuilds.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Contact;
    use serde_json::json;

    fn person(id: &str) -> Person {
        Person {
            id: id.into(),
            name: None,
            contact: Contact {
                email: Some(format!("{id}@example.test")),
                phone: Some("+44 7700 900999".to_string()),
            },
            preferences: None,
            rules: Vec::new(),
        }
    }

    fn team(id: &str, members: &[&str], lead: Option<&str>) -> Team {
        Team {
            id: id.into(),
            members: members.iter().map(|s| s.to_string()).collect(),
            lead: lead.map(str::to_string),
            oncall: HashMap::new(),
            default_channels: None,
        }
    }

    fn base() -> Directory {
        Directory::from_parts(
            vec![person("bob"), person("carol"), person("anna")],
            vec![
                team("rates", &["bob", "anna"], Some("bob")),
                team("payments", &["carol"], None),
            ],
        )
        .expect("base directory builds")
    }

    fn person_override(id: &str, preferences: Value) -> (String, PersonOverride) {
        (
            id.into(),
            PersonOverride {
                body: json!({ "preferences": preferences }),
                revision: 2,
            },
        )
    }

    fn insert_person(ov: &mut Overrides, id: &str, preferences: Value) {
        let (id, o) = person_override(id, preferences);
        ov.people.insert(id, o);
    }

    #[test]
    fn empty_overrides_rebuild_the_same_directory() {
        let b = base();
        let d = apply(&b, &Overrides::default()).expect("empty overrides apply");
        assert_eq!(d.people.len(), 3);
        assert_eq!(d.teams.len(), 2);
        // Person rules are re-parsed identically.
        assert!(d.people.values().all(|p| p.rules.is_empty()));
    }

    #[test]
    fn person_override_replaces_preferences_and_parses_rules() {
        let mut ov = Overrides::default();
        ov.people.insert(
            "bob".into(),
            PersonOverride {
                body: json!({ "preferences": [{ "default": ["sms"] }] }),
                revision: 2,
            },
        );
        let d = apply(&base(), &ov).expect("override applies");
        let bob = d.person("bob").unwrap();
        assert_eq!(bob.rules.len(), 1);
        // Everyone else keeps their configured (empty) preferences.
        assert!(d.person("carol").unwrap().rules.is_empty());
        // Carried over from the base.
        assert_eq!(d.teams.len(), 2);
    }

    #[test]
    fn team_override_replaces_every_field() {
        let mut ov = Overrides::default();
        ov.teams.insert(
            "rates".into(),
            TeamOverride {
                body: json!({
                    "members": ["bob", "carol"],
                    "lead": "carol",
                    "oncall": { "rates-primary": ["anna", "bob"] },
                    "default_channels": ["sms", "phone"],
                }),
                revision: 3,
            },
        );
        let d = apply(&base(), &ov).expect("override applies");
        let rates = &d.teams["rates"];
        assert_eq!(rates.members, ["bob", "carol"]);
        assert_eq!(rates.lead.as_deref(), Some("carol"));
        assert_eq!(rates.oncall["rates-primary"], ["anna", "bob"]);
        assert_eq!(
            rates.default_channels,
            Some(vec!["sms".to_string(), "phone".to_string()])
        );
        // The other team is untouched.
        assert_eq!(d.teams["payments"].members, ["carol"]);
    }

    #[test]
    fn an_empty_default_channels_list_means_unset() {
        let mut ov = Overrides::default();
        ov.teams.insert(
            "rates".into(),
            TeamOverride {
                body: json!({
                    "members": ["bob", "anna"],
                    "lead": "bob",
                    "oncall": {},
                    "default_channels": [],
                }),
                revision: 2,
            },
        );
        let d = apply(&base(), &ov).expect("empty defaults mean unset");
        assert_eq!(d.teams["rates"].default_channels, None);
    }

    #[test]
    fn overrides_for_unknown_ids_are_ignored() {
        let mut ov = Overrides::default();
        insert_person(&mut ov, "ghost", json!([{ "default": ["sms"] }]));
        ov.teams.insert(
            "ghost-team".into(),
            TeamOverride {
                body: json!({
                    "members": ["bob"],
                    "lead": "bob",
                    "oncall": {},
                    "default_channels": ["email"],
                }),
                revision: 2,
            },
        );
        let d = apply(&base(), &ov).expect("unknown ids do not fail the apply");
        assert!(!d.people.contains_key("ghost"));
        assert!(!d.teams.contains_key("ghost-team"));
        assert!(d.person("bob").unwrap().rules.is_empty());
    }

    #[test]
    fn invalid_override_bodies_fail_validation_like_yaml() {
        // Unknown channel: the same rejection the YAML loader gives.
        let mut ov = Overrides::default();
        insert_person(&mut ov, "bob", json!([{ "default": ["fax"] }]));
        let err = apply(&base(), &ov).expect_err("unknown channel must not apply");
        assert!(err.to_string().contains("fax"), "{err}");

        // A team naming an unknown person.
        let mut ov = Overrides::default();
        ov.teams.insert(
            "rates".into(),
            TeamOverride {
                body: json!({
                    "members": ["bob", "ghost"],
                    "lead": null,
                    "oncall": {},
                    "default_channels": ["email"],
                }),
                revision: 2,
            },
        );
        let err = apply(&base(), &ov).expect_err("unknown member must not apply");
        assert!(err.to_string().contains("ghost"), "{err}");
    }

    #[test]
    fn lenient_apply_drops_only_the_override_that_fails() {
        let mut ov = Overrides::default();
        insert_person(&mut ov, "bob", json!([{ "default": ["sms"] }]));
        ov.teams.insert(
            "rates".into(),
            TeamOverride {
                body: json!({ "members": ["bob", "ghost"] }),
                revision: 3,
            },
        );
        assert!(
            apply(&base(), &ov).is_err(),
            "the strict validator rejects the set"
        );
        let (dir, applied) = apply_lenient(&base(), &ov);
        assert!(applied.people.contains_key("bob"));
        assert!(applied.teams.is_empty());
        assert_eq!(dir.teams["rates"].members, base().teams["rates"].members);
        assert!(dir.person("bob").unwrap().preferences.is_some());

        // A set that applies is returned whole, exactly as `apply` builds it.
        ov.teams.clear();
        let (_, applied) = apply_lenient(&base(), &ov);
        assert_eq!(applied.people.len(), 1);
    }

    #[test]
    fn revision_of_reads_the_override_else_one() {
        let mut ov = Overrides::default();
        insert_person(&mut ov, "bob", json!([]));
        ov.teams.insert(
            "rates".into(),
            TeamOverride {
                body: json!({}),
                revision: 5,
            },
        );
        assert_eq!(revision_of(&ov, KIND_PERSON, "bob"), 2);
        assert_eq!(revision_of(&ov, KIND_PERSON, "carol"), 1);
        assert_eq!(revision_of(&ov, KIND_TEAM, "rates"), 5);
        assert_eq!(revision_of(&ov, KIND_TEAM, "payments"), 1);
        assert_eq!(revision_of(&ov, "nonsense", "bob"), 1);
    }
}
