//! Saved people/teams edits: `directory_overrides` rows applied over the startup YAML
//! Directory (contract v0.2.0 `PUT /api/people/{id}/preferences` and `PUT /api/teams/{id}`,
//! migration `20261121060000_delivery_directory_overrides.sql`).
//!
//! The YAML config stays the base and keeps revision `1` for every person and team; the
//! API's PUTs write one override row per id, the first save at revision 2, each later
//! save `+1` (compare-and-set on `expected_revision`, see `api`). A person row's `body`
//! is `{"preferences": [...]}`; a team row's `body` is the full effective team after the
//! save (`{"members", "lead", "oncall", "default_channels"}`). The API's POSTs also
//! CREATE ids the YAML base does not have (`POST /api/people`, `POST /api/teams`,
//! admin-only): one row at revision 1 whose body says `"created": true` — a created
//! person's body carries the whole person (`{"name", "contact", "preferences"}`), a
//! created team's is the full team body plus `"created"` — so the row alone rebuilds
//! the object. The person PUT preserves those keys when it rewrites the preferences.
//!
//! [`apply`] replaces the overridden fields and rebuilds the whole directory with
//! `Directory::from_parts`, so a saved edit goes through exactly the parsing and
//! cross-reference validation a YAML import does (SPEC §4.6: one validator). An override
//! for an id the base does not have is a runtime creation when its body says
//! `"created": true` — it becomes a whole new person or team; any other such override
//! (its id left the YAML) is ignored with a warning.
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

use crate::config::{ConfigError, Contact, Directory, Person, Team};

/// `directory_overrides.kind` for a person row.
pub const KIND_PERSON: &str = "person";
/// `directory_overrides.kind` for a team row.
pub const KIND_TEAM: &str = "team";
/// `directory_overrides.kind` for a runbook imported at runtime (`POST /api/runbooks`).
pub const KIND_RUNBOOK: &str = "runbook";

/// One saved person row: an edit's body is `{"preferences": [...]}`; a row written by
/// `POST /api/people` (revision 1) is the whole person, `{"created": true, "name",
/// "contact", "preferences"}`.
#[derive(Debug, Clone)]
pub struct PersonOverride {
    pub body: Value,
    pub revision: i64,
}

/// One saved team row: the full effective team body an edit writes, plus `"created":
/// true` on a row `POST /api/teams` wrote.
#[derive(Debug, Clone)]
pub struct TeamOverride {
    pub body: Value,
    pub revision: i64,
}

/// One runbook imported at runtime: the stored body plus its revision. The body shape and
/// how it becomes a [`crate::runbook::Runbook`] are owned by [`apply_runbooks`].
#[derive(Debug, Clone)]
pub struct RunbookOverride {
    pub body: Value,
    pub revision: i64,
}

/// Every `directory_overrides` row, split by kind (the table is small; it is loaded
/// whole whenever its stamp moved).
#[derive(Debug, Clone, Default)]
pub struct Overrides {
    pub people: HashMap<String, PersonOverride>,
    pub teams: HashMap<String, TeamOverride>,
    pub runbooks: HashMap<String, RunbookOverride>,
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
            KIND_RUNBOOK => {
                ov.runbooks.insert(id, RunbookOverride { body, revision });
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
        KIND_RUNBOOK => ov.runbooks.get(id).map_or(1, |o| o.revision),
        _ => 1,
    }
}

/// The base directory with every override applied: a person's raw `preferences` and a
/// team's members / lead / on-call / default channels are replaced from the override
/// bodies, then the whole directory is rebuilt with `Directory::from_parts` — the same
/// parsing and validation a YAML import goes through. An override with `"created": true`
/// in its body for an id the base does not have is a person/team CREATED at runtime
/// (`POST /api/people`, `POST /api/teams`): it is built from the body and joined to the
/// lists, people before teams so a created team may name a created person. Other
/// overrides for ids missing from the base (removed from the YAML) are ignored with a
/// warning. `delivery`, `rules` and `runbooks` are carried over from the base unchanged.
pub fn apply(base: &Directory, ov: &Overrides) -> Result<Directory, ConfigError> {
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
    // People created at runtime: the body IS the person (name, contact, preferences).
    // They join before the teams below, so a created team may name one of them; any
    // other override for an unknown id is a save whose person left the YAML: ignored.
    for (id, o) in &ov.people {
        if base.people.contains_key(id) {
            continue;
        }
        if is_created(&o.body) {
            people.push(created_person(id, &o.body));
        } else {
            warn!(person = %id, "override for a person not in the base directory, ignoring");
        }
    }

    let mut teams: Vec<Team> = base.teams.values().cloned().collect();
    for t in &mut teams {
        let Some(o) = ov.teams.get(&t.id) else {
            continue;
        };
        apply_team_body(t, &o.body);
    }
    // Teams created at runtime, same rule as the people above.
    for (id, o) in &ov.teams {
        if base.teams.contains_key(id) {
            continue;
        }
        if is_created(&o.body) {
            teams.push(created_team(id, &o.body));
        } else {
            warn!(team = %id, "override for a team not in the base directory, ignoring");
        }
    }

    let mut dir = Directory::from_parts(people, teams)?;
    dir.delivery = base.delivery.clone();
    dir.rules = base.rules.clone();
    dir.runbooks = base.runbooks.clone();
    apply_runbooks(&mut dir, &ov.runbooks)?;
    Ok(dir)
}

/// Add the runbooks imported at runtime to `dir.runbooks` (runtime runbook import).
/// Placeholder: the import task fills this in (parse each body through the same
/// validator `config/runbooks` files go through; an id that exists in the YAML base is
/// an error). Until then imported rows are ignored.
pub fn apply_runbooks(
    _dir: &mut Directory,
    _runbooks: &HashMap<String, RunbookOverride>,
) -> Result<(), ConfigError> {
    Ok(())
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

/// Whether an override body is a runtime creation (`POST /api/people`, `POST /api/teams`)
/// rather than an edit of an id the YAML base already has.
fn is_created(body: &Value) -> bool {
    body.get("created").and_then(Value::as_bool) == Some(true)
}

/// The [`Person`] a created row's body builds: `{"created": true, "name", "contact":
/// {"email","phone"}, "preferences"}`. Missing keys mean `None`; `from_parts` parses the
/// preferences into rules exactly like a YAML import.
fn created_person(id: &str, body: &Value) -> Person {
    let contact = body.get("contact");
    Person {
        id: id.to_string(),
        name: body.get("name").and_then(Value::as_str).map(str::to_string),
        contact: Contact {
            email: contact
                .and_then(|c| c.get("email"))
                .and_then(Value::as_str)
                .map(str::to_string),
            phone: contact
                .and_then(|c| c.get("phone"))
                .and_then(Value::as_str)
                .map(str::to_string),
        },
        preferences: body.get("preferences").cloned(),
        rules: Vec::new(),
    }
}

/// The [`Team`] a created row's body builds — the same body shape a team save stores,
/// plus `"created": true` — so both paths read one shape through [`apply_team_body`].
fn created_team(id: &str, body: &Value) -> Team {
    let mut t = Team {
        id: id.to_string(),
        members: Vec::new(),
        lead: None,
        oncall: HashMap::new(),
        default_channels: None,
    };
    apply_team_body(&mut t, body);
    t
}

/// [`apply`], but one override that no longer validates (say a team naming a person since
/// removed from the YAML, or a created row whose stored body went bad) drops only
/// itself, with a warning, instead of the whole set. Returns the directory and the
/// overrides that were applied. If the whole set applies this is exactly [`apply`];
/// otherwise overrides are tried one at a time in a fixed order (people, then teams,
/// each by id — created rows included) and kept only if the set so far still applies.
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
    fn created_overrides_become_people_and_teams() {
        let mut ov = Overrides::default();
        ov.people.insert(
            "newbie".into(),
            PersonOverride {
                body: json!({
                    "created": true,
                    "name": "New Bee",
                    "contact": { "email": "newbie@example.test", "phone": "+44 7700 900777" },
                    "preferences": [{ "default": ["sms"] }],
                }),
                revision: 1,
            },
        );
        // A created team may name the created person as member, lead and on-call.
        ov.teams.insert(
            "newteam".into(),
            TeamOverride {
                body: json!({
                    "created": true,
                    "members": ["newbie", "bob"],
                    "lead": "newbie",
                    "oncall": { "newteam-primary": ["newbie"] },
                    "default_channels": ["email"],
                }),
                revision: 1,
            },
        );
        let d = apply(&base(), &ov).expect("created rows apply");
        let newbie = d.person("newbie").expect("newbie is in the directory");
        assert_eq!(newbie.name.as_deref(), Some("New Bee"));
        assert_eq!(newbie.contact.email.as_deref(), Some("newbie@example.test"));
        assert_eq!(newbie.contact.phone.as_deref(), Some("+44 7700 900777"));
        assert_eq!(
            newbie.rules.len(),
            1,
            "preferences parsed like a YAML import"
        );
        let newteam = &d.teams["newteam"];
        assert_eq!(newteam.members, ["newbie", "bob"]);
        assert_eq!(newteam.lead.as_deref(), Some("newbie"));
        assert_eq!(newteam.default_channels, Some(vec!["email".to_string()]));
        assert_eq!(d.expand("oncall:newteam-primary").unwrap().len(), 1);
        // The base is untouched.
        assert_eq!(d.people.len(), 4);
        assert_eq!(d.teams.len(), 3);
    }

    #[test]
    fn a_created_team_naming_an_unknown_person_fails_validation() {
        let mut ov = Overrides::default();
        ov.teams.insert(
            "newteam".into(),
            TeamOverride {
                body: json!({
                    "created": true,
                    "members": ["ghost"],
                    "lead": null,
                    "oncall": {},
                    "default_channels": ["email"],
                }),
                revision: 1,
            },
        );
        let err = apply(&base(), &ov).expect_err("unknown member must not apply");
        assert!(err.to_string().contains("ghost"), "{err}");
    }

    #[test]
    fn a_created_row_that_goes_bad_drops_alone_in_lenient_apply() {
        let mut ov = Overrides::default();
        // newbie was created, but her stored preferences have since gone bad.
        ov.people.insert(
            "newbie".into(),
            PersonOverride {
                body: json!({
                    "created": true,
                    "name": null,
                    "contact": { "email": "newbie@example.test" },
                    "preferences": [{ "default": ["fax"] }],
                }),
                revision: 2,
            },
        );
        // A valid save for a base person, and a created team naming only base people.
        insert_person(&mut ov, "bob", json!([{ "default": ["sms"] }]));
        ov.teams.insert(
            "newteam".into(),
            TeamOverride {
                body: json!({
                    "created": true,
                    "members": ["bob"],
                    "lead": "bob",
                    "oncall": {},
                    "default_channels": ["email"],
                }),
                revision: 1,
            },
        );
        assert!(
            apply(&base(), &ov).is_err(),
            "the strict validator rejects the set"
        );
        let (dir, applied) = apply_lenient(&base(), &ov);
        assert!(!dir.people.contains_key("newbie"), "the bad row drops");
        assert!(!applied.people.contains_key("newbie"));
        assert!(applied.people.contains_key("bob"));
        assert!(applied.teams.contains_key("newteam"));
        assert!(dir.teams.contains_key("newteam"));
        assert!(dir.person("bob").unwrap().preferences.is_some());
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
