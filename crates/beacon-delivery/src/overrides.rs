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
//! `(count, max(updated_at))` stamp moves — one cheap query per pass, the pattern the
//! router and the escalation scheduler use so a save applies to the next notification /
//! escalation step. A database that is away, or an override that no longer validates,
//! logs and serves the last good directory: a broken override must never stop a
//! delivery.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
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

/// The `(count, max(updated_at))` stamp of the overrides table: it moves on every save.
type Stamp = (i64, Option<DateTime<Utc>>);

/// The cached effective view: the applied directory plus the overrides (the revisions
/// views show) it was built from.
#[derive(Debug)]
struct Cache {
    stamp: Option<Stamp>,
    dir: Arc<Directory>,
    overrides: Arc<Overrides>,
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
        Self {
            cache: tokio::sync::Mutex::new(Cache {
                stamp: None,
                dir: base.clone(),
                overrides: Arc::new(Overrides::default()),
            }),
            base,
            rebuilds: AtomicU64::new(0),
        }
    }

    /// The effective directory.
    pub async fn get(&self, pool: &PgPool) -> Arc<Directory> {
        self.snapshot(pool).await.0
    }

    /// The effective directory plus the overrides it applied (the revisions views show).
    pub async fn snapshot(&self, pool: &PgPool) -> (Arc<Directory>, Arc<Overrides>) {
        // One cheap query, outside the lock; a failure serves the last good directory.
        let stamp: Stamp = match sqlx::query_as(
            "SELECT count(*), max(updated_at) FROM directory_overrides",
        )
        .fetch_one(pool)
        .await
        {
            Ok(stamp) => stamp,
            Err(err) => {
                error!(%err, "cannot read directory_overrides, serving the last good directory");
                let cache = self.cache.lock().await;
                return (cache.dir.clone(), cache.overrides.clone());
            }
        };

        let mut cache = self.cache.lock().await;
        if cache.stamp == Some(stamp) {
            return (cache.dir.clone(), cache.overrides.clone());
        }
        let applied = match load(pool).await {
            Ok(ov) => match apply(&self.base, &ov) {
                Ok(dir) => Some((ov, dir)),
                Err(err) => {
                    // The stamp moved but the overrides no longer apply (e.g. a body
                    // that does not validate against the base any more): keep serving
                    // the last good directory, retry on the next stamp change.
                    error!(%err, "cannot apply directory overrides, serving the last good directory");
                    None
                }
            },
            Err(err) => {
                error!(%err, "cannot load directory overrides, serving the last good directory");
                None
            }
        };
        if let Some((ov, dir)) = applied {
            cache.stamp = Some(stamp);
            cache.dir = Arc::new(dir);
            cache.overrides = Arc::new(ov);
            self.rebuilds.fetch_add(1, Ordering::Relaxed);
        }
        (cache.dir.clone(), cache.overrides.clone())
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
