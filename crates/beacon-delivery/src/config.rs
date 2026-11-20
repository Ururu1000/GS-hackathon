//! M0 recipient directory, loaded once at startup from YAML (board #35 item 4).
//! Postgres-backed versioned config replaces this at M2 (SPEC §4.6).
//!
//! `config/people/<id>.yaml` (SPEC §4.4; `preferences` is kept but unused in M0):
//! ```yaml
//! user: anna
//! name: Anna Example            # optional
//! contact: { email: anna@example.test, phone: "+44 7700 900123" }
//! preferences: [...]            # optional, opaque in M0
//! ```
//!
//! `config/teams/<id>.yaml`:
//! ```yaml
//! team: rates
//! members: [anna, bob]
//! lead: bob                     # optional
//! oncall:                       # optional: schedule id -> rotation, first entry is on call (M0)
//!   rates-primary: [bob, anna]
//! ```

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde::Deserialize;

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
    #[serde(default)]
    pub preferences: Option<serde_json::Value>,
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
}

impl Directory {
    /// Load `<root>/people/*.yaml` and `<root>/teams/*.yaml`. Missing subdirectories are empty.
    /// Rejects duplicate ids and team members / on-call entries naming unknown people.
    pub fn load(root: &Path) -> Result<Self, ConfigError> {
        let people: Vec<Person> = load_dir(&root.join("people"))?;
        let teams: Vec<Team> = load_dir(&root.join("teams"))?;
        Self::from_parts(people, teams)
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
            let id = t.id.clone();
            if teams_map.insert(id.clone(), t).is_some() {
                return Err(ConfigError::Invalid(format!("duplicate team id {id:?}")));
            }
        }

        Ok(Self {
            people: people_map,
            teams: teams_map,
        })
    }

    pub fn person(&self, id: &str) -> Option<&Person> {
        self.people.get(id)
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
    fn load_missing_subdirs_is_empty() {
        let root = tmp("missing");
        let d = Directory::load(&root).unwrap();
        assert!(d.people.is_empty() && d.teams.is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn load_reports_parse_error_with_path() {
        let root = tmp("bad");
        std::fs::create_dir_all(root.join("people")).unwrap();
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
        for t in ["settlements", "pricing", "rates", "payments"] {
            assert!(!d.expand(&format!("team:{t}")).unwrap().is_empty(), "{t}");
        }
        assert_eq!(d.expand("oncall:settlements-primary").unwrap().len(), 1);
        assert!(d.people.values().all(|p| {
            p.contact
                .email
                .as_deref()
                .is_some_and(|e| e.ends_with("@example.test"))
        }));
    }
}
