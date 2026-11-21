//! Recipient directory and delivery settings, loaded once at startup from YAML (board #35).
//! Postgres-backed versioned config replaces this at M2 (SPEC §4.6).
//!
//! `config/people/<id>.yaml` (SPEC §4.4; `preferences` syntax in `crate::prefs`):
//! ```yaml
//! user: anna
//! name: Anna Example            # optional
//! contact: { email: anna@example.test, phone: "+44 7700 900123" }
//! preferences: [...]            # optional, parsed into `Person::rules` at load
//! ```
//!
//! `config/teams/<id>.yaml`:
//! ```yaml
//! team: rates
//! members: [anna, bob]
//! lead: bob                     # optional
//! oncall:                       # optional: schedule id -> rotation, first entry is on call (M0)
//!   rates-primary: [bob, anna]
//! default_channels: [email]     # optional: team default when no personal preference matches
//! ```
//!
//! `config/delivery.yaml` (optional; missing file = defaults):
//! ```yaml
//! channels:
//!   phone: { fallback: sms }    # one-hop fallback channel when a job finally fails
//!   sms:   { fallback: email }
//!   email: {}
//! ```

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::prefs::{PreferenceRule, parse_preferences};

/// Channel names config may use. `ui` is the alert feed itself and never becomes a job.
pub const KNOWN_CHANNELS: &[&str] = &["email", "sms", "phone", "ui"];

#[derive(Debug, Clone, Deserialize)]
pub struct Contact {
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub phone: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Person {
    #[serde(rename = "user")]
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    pub contact: Contact,
    /// Raw `preferences:` as written; parsed and validated into `rules` by `Directory`.
    #[serde(default)]
    pub preferences: Option<serde_json::Value>,
    /// Parsed preference rules, first match wins (filled by `Directory::from_parts`).
    #[serde(skip)]
    pub rules: Vec<PreferenceRule>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Team {
    #[serde(rename = "team")]
    pub id: String,
    #[serde(default)]
    pub members: Vec<String>,
    #[serde(default)]
    pub lead: Option<String>,
    #[serde(default)]
    pub oncall: HashMap<String, Vec<String>>,
    /// Team default channels (SPEC §4.4 precedence step 3).
    #[serde(default)]
    pub default_channels: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct ChannelSettings {
    #[serde(default)]
    pub fallback: Option<String>,
}

/// `config/delivery.yaml`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DeliverySettings {
    #[serde(default)]
    pub channels: HashMap<String, ChannelSettings>,
}

impl Default for DeliverySettings {
    /// phone -> sms -> email, email has no fallback.
    fn default() -> Self {
        let fb = |c: &str| ChannelSettings {
            fallback: Some(c.to_string()),
        };
        Self {
            channels: HashMap::from([
                ("phone".to_string(), fb("sms")),
                ("sms".to_string(), fb("email")),
                ("email".to_string(), ChannelSettings::default()),
            ]),
        }
    }
}

impl DeliverySettings {
    /// Load `<root>/delivery.yaml`; a missing file gives `Default`. Channel names and fallback
    /// targets must be in `KNOWN_CHANNELS` (not `ui`), and a channel may not fall back to itself.
    pub fn load(root: &Path) -> Result<Self, ConfigError> {
        let path = root.join("delivery.yaml");
        let text = match std::fs::read_to_string(&path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => {
                return Err(ConfigError::Io {
                    path: path.display().to_string(),
                    source: e,
                });
            }
        };
        let s: Self = serde_saphyr::from_str(&text).map_err(|e| ConfigError::Parse {
            path: path.display().to_string(),
            message: e.to_string(),
        })?;
        s.validate()?;
        Ok(s)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let job_channel = |c: &str| KNOWN_CHANNELS.contains(&c) && c != "ui";
        for (ch, cs) in &self.channels {
            if !job_channel(ch) {
                return Err(ConfigError::Invalid(format!(
                    "delivery.yaml: unknown channel {ch:?}"
                )));
            }
            if let Some(fb) = &cs.fallback
                && (!job_channel(fb) || fb == ch)
            {
                return Err(ConfigError::Invalid(format!(
                    "delivery.yaml: channel {ch:?} has invalid fallback {fb:?}"
                )));
            }
        }
        Ok(())
    }

    /// The configured fallback channel for `channel`, if any.
    pub fn fallback_for(&self, channel: &str) -> Option<&str> {
        self.channels.get(channel)?.fallback.as_deref()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("parsing {path}: {message}")]
    Parse { path: String, message: String },
    #[error("{0}")]
    Invalid(String),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ExpandError {
    #[error("malformed route target {0:?} (want team:<id>, user:<id> or oncall:<schedule>)")]
    Malformed(String),
    #[error("unknown {kind} {id:?}")]
    Unknown { kind: &'static str, id: String },
}

/// People and teams, indexed by id.
#[derive(Debug, Clone, Default)]
pub struct Directory {
    pub people: HashMap<String, Person>,
    pub teams: HashMap<String, Team>,
    /// `config/delivery.yaml` (fallback chain). `from_parts` uses the default.
    pub delivery: DeliverySettings,
    /// `escalate_after` / `throttle` per rule from `config/rules` (part (a)'s files, optional).
    pub rules: crate::rules::RulePolicies,
    /// `config/runbooks` (M5b). `from_parts` gives an empty library.
    pub runbooks: crate::runbook::RunbookLibrary,
}

impl Directory {
    /// Load `<root>/people/*.yaml` and `<root>/teams/*.yaml`. Missing subdirectories are empty.
    /// Rejects duplicate ids and team members / on-call entries naming unknown people.
    /// Load `<root>/people` and `<root>/teams` (both required, QA M11: a typo in
    /// `BEACON_CONFIG_DIR` must fail at startup, not route every alert to nobody) plus the
    /// optional `<root>/delivery.yaml`.
    pub fn load(root: &Path) -> Result<Self, ConfigError> {
        for sub in ["people", "teams"] {
            let dir = root.join(sub);
            if !dir.is_dir() {
                return Err(ConfigError::Invalid(format!(
                    "config directory {} is missing (set BEACON_CONFIG_DIR to the repo's config/)",
                    dir.display()
                )));
            }
        }
        let people: Vec<Person> = load_dir(&root.join("people"))?;
        let teams: Vec<Team> = load_dir(&root.join("teams"))?;
        let mut dir = Self::from_parts(people, teams)?;
        dir.delivery = DeliverySettings::load(root)?;
        dir.rules = crate::rules::RulePolicies::load(&root.join("rules"));
        dir.runbooks = crate::runbook::RunbookLibrary::load(
            &root.join("runbooks"),
            crate::actions::ActionRegistry::known(),
        )?;
        Ok(dir)
    }

    /// Build from already-parsed parts (used by tests), with the same validation as `load`.
    pub fn from_parts(people: Vec<Person>, teams: Vec<Team>) -> Result<Self, ConfigError> {
        let mut people_map = HashMap::with_capacity(people.len());
        for p in people {
            if p.id.trim().is_empty() {
                return Err(ConfigError::Invalid("person with empty id".into()));
            }
            let id = p.id.clone();
            if people_map.insert(id.clone(), p).is_some() {
                return Err(ConfigError::Invalid(format!("duplicate person id {id:?}")));
            }
        }

        let mut teams_map: HashMap<String, Team> = HashMap::with_capacity(teams.len());
        // schedule id -> team that defines it
        let mut schedules: HashMap<String, String> = HashMap::new();
        for t in teams {
            if t.id.trim().is_empty() {
                return Err(ConfigError::Invalid("team with empty id".into()));
            }
            let known = |who: &str, what: &str| -> Result<(), ConfigError> {
                if people_map.contains_key(who) {
                    Ok(())
                } else {
                    Err(ConfigError::Invalid(format!(
                        "team {:?}: {what} names unknown person {who:?}",
                        t.id
                    )))
                }
            };
            for m in &t.members {
                known(m, "members")?;
            }
            if let Some(lead) = &t.lead {
                known(lead, "lead")?;
            }
            for (schedule, rotation) in &t.oncall {
                if schedule.trim().is_empty() {
                    return Err(ConfigError::Invalid(format!(
                        "team {:?}: on-call schedule with empty id",
                        t.id
                    )));
                }
                if rotation.is_empty() {
                    return Err(ConfigError::Invalid(format!(
                        "team {:?}: on-call rotation {schedule:?} is empty",
                        t.id
                    )));
                }
                for who in rotation {
                    known(who, &format!("on-call rotation {schedule:?}"))?;
                }
                if let Some(other) = schedules.insert(schedule.clone(), t.id.clone()) {
                    return Err(ConfigError::Invalid(format!(
                        "on-call schedule {schedule:?} is defined by both team {other:?} and team {:?}",
                        t.id
                    )));
                }
            }
            // Team defaults are step 3 of the routing precedence (SPEC §4.4): they must be
            // usable channels, and an empty default would silently mean "never here".
            if let Some(defaults) = &t.default_channels {
                if defaults.is_empty() {
                    return Err(ConfigError::Invalid(format!(
                        "team {:?}: default_channels must not be empty",
                        t.id
                    )));
                }
                for c in defaults {
                    if !KNOWN_CHANNELS.contains(&c.as_str()) {
                        return Err(ConfigError::Invalid(format!(
                            "team {:?}: default_channels names unknown channel {c:?}",
                            t.id
                        )));
                    }
                }
            }
            let id = t.id.clone();
            if teams_map.insert(id.clone(), t).is_some() {
                return Err(ConfigError::Invalid(format!("duplicate team id {id:?}")));
            }
        }

        // Parse and validate each person's `preferences:` into rules (SPEC §4.4); the
        // error names the person and the failing entry.
        for (id, p) in people_map.iter_mut() {
            let rules = match p.preferences.as_ref() {
                None => Vec::new(),
                Some(raw) => parse_preferences(id, raw, KNOWN_CHANNELS)?,
            };
            // The off-hours digest is an email: without an address it would silently send
            // nothing (#767 D).
            let wants_digest = rules.iter().any(|r| {
                matches!(r, PreferenceRule::Match { otherwise: Some(o), .. }
                    if o.iter().any(|c| c == crate::prefs::DIGEST))
            });
            if wants_digest && p.contact.email.is_none() {
                return Err(ConfigError::Invalid(format!(
                    "person {id}: `digest` needs an email address (contact.email)"
                )));
            }
            p.rules = rules;
        }

        Ok(Self {
            people: people_map,
            teams: teams_map,
            delivery: DeliverySettings::default(),
            rules: Default::default(),
            runbooks: Default::default(),
        })
    }

    pub fn person(&self, id: &str) -> Option<&Person> {
        self.people.get(id)
    }

    /// The team that defines the on-call `schedule`. Schedule ids are unique across teams
    /// (validated in `from_parts`), so at most one team matches.
    pub fn team_for_schedule(&self, schedule: &str) -> Option<&Team> {
        self.teams
            .values()
            .find(|t| t.oncall.contains_key(schedule))
    }

    /// Expand one route target into people, de-duplicated, in a stable order.
    /// `team:<id>` -> members; `user:<id>` -> that person; `oncall:<schedule>` -> the first
    /// person in the rotation of whichever team defines that schedule.
    pub fn expand(&self, target: &str) -> Result<Vec<&Person>, ExpandError> {
        let malformed = || ExpandError::Malformed(target.to_string());
        let (prefix, id) = target.split_once(':').ok_or_else(malformed)?;
        if id.is_empty() {
            return Err(malformed());
        }
        let ids: Vec<&String> = match prefix {
            "team" => {
                let team = self.teams.get(id).ok_or_else(|| ExpandError::Unknown {
                    kind: "team",
                    id: id.to_string(),
                })?;
                team.members.iter().collect()
            }
            "user" => {
                let (key, _) =
                    self.people
                        .get_key_value(id)
                        .ok_or_else(|| ExpandError::Unknown {
                            kind: "user",
                            id: id.to_string(),
                        })?;
                vec![key]
            }
            "oncall" => {
                // Schedule ids are unique across teams (validated), so at most one matches.
                let first = self
                    .teams
                    .values()
                    .find_map(|t| t.oncall.get(id))
                    .and_then(|rotation| rotation.first())
                    .ok_or_else(|| ExpandError::Unknown {
                        kind: "schedule",
                        id: id.to_string(),
                    })?;
                vec![first]
            }
            _ => return Err(malformed()),
        };

        let mut seen = HashSet::new();
        let mut out = Vec::with_capacity(ids.len());
        for pid in ids {
            if !seen.insert(pid.as_str()) {
                continue;
            }
            // Validated at construction, but stay total if `people` was mutated afterwards.
            let person = self.people.get(pid).ok_or_else(|| ExpandError::Unknown {
                kind: "user",
                id: pid.clone(),
            })?;
            out.push(person);
        }
        Ok(out)
    }
}

/// The person's address on `channel`: `email` -> `contact.email`, `sms`/`phone` ->
/// `contact.phone`, anything else -> `None`. The router freezes it into
/// `delivery_jobs.address` when planning a job.
pub fn address_for<'a>(person: &'a Person, channel: &str) -> Option<&'a str> {
    match channel {
        "email" => person.contact.email.as_deref(),
        "sms" | "phone" => person.contact.phone.as_deref(),
        _ => None,
    }
}

/// Parse every `*.yaml` / `*.yml` file in `dir`, sorted by file name. A missing dir is empty.
fn load_dir<T: serde::de::DeserializeOwned>(dir: &Path) -> Result<Vec<T>, ConfigError> {
    let io_err = |path: &Path, source| ConfigError::Io {
        path: path.display().to_string(),
        source,
    };
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io_err(dir, e)),
    };
    let mut paths: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let path = entry.map_err(|e| io_err(dir, e))?.path();
        let is_yaml = path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("yaml") || e.eq_ignore_ascii_case("yml"));
        if is_yaml && path.is_file() {
            paths.push(path);
        }
    }
    paths.sort();
    paths
        .iter()
        .map(|path| {
            let text = std::fs::read_to_string(path).map_err(|e| io_err(path, e))?;
            serde_saphyr::from_str::<T>(&text).map_err(|e| ConfigError::Parse {
                path: path.display().to_string(),
                message: e.to_string(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn person(id: &str) -> Person {
        Person {
            id: id.into(),
            name: None,
            contact: Contact {
                email: Some(format!("{id}@example.test")),
                phone: None,
            },
            preferences: None,
            rules: Vec::new(),
        }
    }

    fn team(id: &str, members: &[&str], oncall: &[(&str, &[&str])]) -> Team {
        Team {
            id: id.into(),
            members: members.iter().map(|s| s.to_string()).collect(),
            lead: None,
            oncall: oncall
                .iter()
                .map(|(k, v)| (k.to_string(), v.iter().map(|s| s.to_string()).collect()))
                .collect(),
            default_channels: None,
        }
    }

    fn dir() -> Directory {
        Directory::from_parts(
            vec![person("anna"), person("bob"), person("carol")],
            vec![
                team(
                    "rates",
                    &["bob", "anna", "bob"],
                    &[("rates-primary", &["carol", "bob"])],
                ),
                team("empty", &[], &[]),
            ],
        )
        .unwrap()
    }

    fn ids(v: Vec<&Person>) -> Vec<&str> {
        v.into_iter().map(|p| p.id.as_str()).collect()
    }

    fn invalid(r: Result<Directory, ConfigError>) -> String {
        match r {
            Err(ConfigError::Invalid(m)) => m,
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn expand_team_keeps_order_and_dedups() {
        assert_eq!(ids(dir().expand("team:rates").unwrap()), ["bob", "anna"]);
        assert!(dir().expand("team:empty").unwrap().is_empty());
    }

    #[test]
    fn expand_user() {
        assert_eq!(ids(dir().expand("user:anna").unwrap()), ["anna"]);
    }

    #[test]
    fn expand_oncall_is_first_in_rotation() {
        assert_eq!(
            ids(dir().expand("oncall:rates-primary").unwrap()),
            ["carol"]
        );
    }

    #[test]
    fn expand_unknown() {
        let d = dir();
        let unknown = |kind, id: &str| ExpandError::Unknown {
            kind,
            id: id.into(),
        };
        assert_eq!(d.expand("team:nope").unwrap_err(), unknown("team", "nope"));
        assert_eq!(d.expand("user:nope").unwrap_err(), unknown("user", "nope"));
        assert_eq!(
            d.expand("oncall:nope").unwrap_err(),
            unknown("schedule", "nope")
        );
    }

    #[test]
    fn expand_malformed() {
        let d = dir();
        for t in [
            "",
            "rates",
            "anna",
            "team:",
            "user:",
            "oncall:",
            "group:rates",
            ":rates",
            "TEAM:rates",
        ] {
            assert_eq!(
                d.expand(t).unwrap_err(),
                ExpandError::Malformed(t.into()),
                "{t:?}"
            );
        }
    }

    #[test]
    fn rejects_duplicate_person() {
        let m = invalid(Directory::from_parts(
            vec![person("a"), person("a")],
            vec![],
        ));
        assert!(m.contains("duplicate person") && m.contains("\"a\""), "{m}");
    }

    #[test]
    fn rejects_duplicate_team() {
        let m = invalid(Directory::from_parts(
            vec![],
            vec![team("t", &[], &[]), team("t", &[], &[])],
        ));
        assert!(m.contains("duplicate team"), "{m}");
    }

    #[test]
    fn rejects_unknown_member_lead_and_oncall() {
        let p = || vec![person("a")];
        let m = invalid(Directory::from_parts(p(), vec![team("t", &["ghost"], &[])]));
        assert!(m.contains("members") && m.contains("ghost"), "{m}");

        let mut t = team("t", &["a"], &[]);
        t.lead = Some("ghost".into());
        let m = invalid(Directory::from_parts(p(), vec![t]));
        assert!(m.contains("lead") && m.contains("ghost"), "{m}");

        let m = invalid(Directory::from_parts(
            p(),
            vec![team("t", &["a"], &[("s", &["a", "ghost"])])],
        ));
        assert!(m.contains("on-call") && m.contains("ghost"), "{m}");
    }

    #[test]
    fn rejects_empty_rotation() {
        let m = invalid(Directory::from_parts(
            vec![person("a")],
            vec![team("t", &[], &[("s", &[])])],
        ));
        assert!(m.contains("empty"), "{m}");
    }

    #[test]
    fn rejects_schedule_defined_by_two_teams() {
        let m = invalid(Directory::from_parts(
            vec![person("a")],
            vec![
                team("t1", &[], &[("s", &["a"])]),
                team("t2", &[], &[("s", &["a"])]),
            ],
        ));
        assert!(m.contains("\"s\"") && m.contains("both"), "{m}");
    }

    fn person_with_prefs(id: &str, prefs: serde_json::Value) -> Person {
        Person {
            preferences: Some(prefs),
            ..person(id)
        }
    }

    #[test]
    fn from_parts_parses_preferences_into_rules() {
        let p = person_with_prefs(
            "a",
            serde_json::json!([
                {"match": {"severity": "critical"}, "channels": ["sms"],
                 "when": {"between": "08:00-20:00", "tz": "Europe/London"},
                 "otherwise": ["email"]},
                {"default": ["ui"]},
            ]),
        );
        let d = Directory::from_parts(vec![p], vec![]).unwrap();
        assert_eq!(d.person("a").unwrap().rules.len(), 2);
    }

    #[test]
    fn digest_preference_needs_an_email_address() {
        let prefs = serde_json::json!([{ "match": {}, "channels": ["sms"],
            "when": { "between": "08:00-20:00", "tz": "UTC" }, "otherwise": ["digest"] }]);
        let mut no_email = person("a");
        no_email.contact.email = None;
        no_email.contact.phone = Some("+44 7700 900777".into());
        no_email.preferences = Some(prefs.clone());
        let err = Directory::from_parts(vec![no_email], vec![]).unwrap_err();
        assert!(
            err.to_string().contains("`digest` needs an email address"),
            "{err}"
        );

        let mut with_email = person("a");
        with_email.preferences = Some(prefs);
        assert!(Directory::from_parts(vec![with_email], vec![]).is_ok());
    }

    #[test]
    fn from_parts_rejects_bad_preferences() {
        let p = person_with_prefs(
            "a",
            serde_json::json!([
                {"match": {}, "channels": ["fax"]},
            ]),
        );
        let m = invalid(Directory::from_parts(vec![p], vec![]));
        assert!(
            m.contains("person a: preferences[0]") && m.contains("fax"),
            "{m}"
        );
    }

    #[test]
    fn from_parts_rejects_bad_default_channels() {
        let mut t = team("t", &["a"], &[]);
        t.default_channels = Some(vec![]);
        let m = invalid(Directory::from_parts(vec![person("a")], vec![t]));
        assert!(m.contains("default_channels") && m.contains("empty"), "{m}");

        let mut t = team("t", &["a"], &[]);
        t.default_channels = Some(vec!["fax".into()]);
        let m = invalid(Directory::from_parts(vec![person("a")], vec![t]));
        assert!(m.contains("default_channels") && m.contains("fax"), "{m}");
    }

    #[test]
    fn team_for_schedule_finds_defining_team() {
        let d = dir();
        assert_eq!(d.team_for_schedule("rates-primary").unwrap().id, "rates");
        assert!(d.team_for_schedule("nope").is_none());
    }

    #[test]
    fn address_for_maps_channels_to_contacts() {
        let d = dir();
        let anna = d.person("anna").unwrap();
        assert_eq!(address_for(anna, "email"), Some("anna@example.test"));
        assert_eq!(address_for(anna, "sms"), None, "fixture has no phone");

        let mut with_phone = person("p");
        with_phone.contact.phone = Some("+44 7700 900777".into());
        assert_eq!(address_for(&with_phone, "sms"), Some("+44 7700 900777"));
        assert_eq!(address_for(&with_phone, "phone"), Some("+44 7700 900777"));
        assert_eq!(address_for(&with_phone, "ui"), None);
        assert_eq!(address_for(&with_phone, "fax"), None);
    }

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("beacon-config-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn load_parses_yaml_and_yml_and_ignores_other_files() {
        let root = tmp("parse");
        std::fs::create_dir_all(root.join("people")).unwrap();
        std::fs::create_dir_all(root.join("teams")).unwrap();
        std::fs::write(
            root.join("people/a.yaml"),
            "user: a\nname: Aye\ncontact: { email: a@example.test, phone: \"+1\" }\n\
             preferences:\n  - default: [ui]\n",
        )
        .unwrap();
        std::fs::write(
            root.join("people/b.yml"),
            "user: b\ncontact: { email: b@example.test }\n",
        )
        .unwrap();
        std::fs::write(root.join("people/notes.txt"), "not yaml: [").unwrap();
        std::fs::write(
            root.join("teams/t.yaml"),
            "team: t\nmembers: [a, b]\nlead: b\noncall:\n  t-primary: [b, a]\n",
        )
        .unwrap();

        let d = Directory::load(&root).unwrap();
        let a = d.person("a").unwrap();
        assert_eq!(a.name.as_deref(), Some("Aye"));
        assert_eq!(a.contact.email.as_deref(), Some("a@example.test"));
        assert_eq!(a.contact.phone.as_deref(), Some("+1"));
        assert!(a.preferences.is_some());
        assert_eq!(d.person("b").unwrap().contact.phone, None);
        assert_eq!(d.teams["t"].lead.as_deref(), Some("b"));
        assert_eq!(ids(d.expand("oncall:t-primary").unwrap()), ["b"]);
        assert_eq!(ids(d.expand("team:t").unwrap()), ["a", "b"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn load_fails_fast_on_missing_subdirs() {
        let root = tmp("missing");
        let err = Directory::load(&root).unwrap_err().to_string();
        assert!(err.contains("people") && err.contains("missing"), "{err}");
        std::fs::create_dir_all(root.join("people")).unwrap();
        let err = Directory::load(&root).unwrap_err().to_string();
        assert!(err.contains("teams"), "{err}");
        std::fs::create_dir_all(root.join("teams")).unwrap();
        let d = Directory::load(&root).unwrap();
        assert!(d.people.is_empty() && d.teams.is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn load_reports_parse_error_with_path() {
        let root = tmp("bad");
        std::fs::create_dir_all(root.join("people")).unwrap();
        std::fs::create_dir_all(root.join("teams")).unwrap();
        std::fs::write(root.join("people/x.yaml"), "contact: {}\n").unwrap();
        match Directory::load(&root) {
            Err(ConfigError::Parse { path, .. }) => assert!(path.ends_with("x.yaml"), "{path}"),
            other => panic!("expected Parse, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn load_rejects_duplicate_ids_across_files() {
        let root = tmp("dup");
        std::fs::create_dir_all(root.join("people")).unwrap();
        std::fs::create_dir_all(root.join("teams")).unwrap();
        for f in ["a.yaml", "b.yaml"] {
            std::fs::write(root.join("people").join(f), "user: same\ncontact: {}\n").unwrap();
        }
        assert!(invalid(Directory::load(&root)).contains("duplicate person"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn repo_seed_config_loads() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");
        let d = Directory::load(&root).unwrap();
        let anna = d.person("anna").unwrap();
        assert_eq!(anna.contact.email.as_deref(), Some("anna@example.test"));
        assert_eq!(anna.contact.phone.as_deref(), Some("+44 7700 900123"));
        assert!(anna.preferences.is_some());
        // Her preferences also parse and validate into rules (3 in the seed file).
        assert_eq!(anna.rules.len(), 3);
        for t in ["settlements", "pricing", "rates", "payments"] {
            assert!(!d.expand(&format!("team:{t}")).unwrap().is_empty(), "{t}");
        }
        assert_eq!(d.expand("oncall:settlements-primary").unwrap().len(), 1);

        // Every route target in the seed rules (owned by part (a)) must expand against the
        // seed directory, so a renamed team or schedule fails here instead of at runtime.
        let rules: Vec<serde_json::Value> = load_dir(&root.join("rules")).unwrap();
        assert!(!rules.is_empty(), "no seed rules found");
        for rule in &rules {
            // A rule without a route list would silently route to nobody: fail loudly.
            let route = rule["route"]
                .as_array()
                .unwrap_or_else(|| panic!("rule {}: route must be a list", rule["id"]));
            for entry in route {
                let to = entry["to"].as_str().unwrap();
                let people = d
                    .expand(to)
                    .unwrap_or_else(|e| panic!("rule {}: {e}", rule["id"]));
                assert!(
                    !people.is_empty(),
                    "rule {}: {to} expands to nobody",
                    rule["id"]
                );
            }
        }
        assert!(d.people.values().all(|p| {
            p.contact
                .email
                .as_deref()
                .is_some_and(|e| e.ends_with("@example.test"))
        }));
    }
}
