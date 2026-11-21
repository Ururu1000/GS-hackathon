//! M0 demo session: one fixed user who sees every configured team. Writes
//! must echo the CSRF token from `GET /api/session` in `X-CSRF-Token`.

use std::collections::HashMap;
use std::path::Path;

use axum::Json;
use axum::extract::State;
use axum::http::HeaderMap;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::ApiState;
use crate::error::ApiError;

#[derive(Debug, Clone, Serialize)]
pub struct Named {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Session {
    pub user: Named,
    pub teams: Vec<Named>,
    pub csrf_token: String,
    /// Admins see every team and the cross-team views (recent events,
    /// discovered schemas). Others see only teams they belong to or lead.
    pub admin: bool,
    /// Map of configured on-call schedule id → owning team id (from `config/teams`).
    #[serde(skip)]
    pub oncall_owners: HashMap<String, String>,
}

#[derive(Deserialize)]
struct PersonFile {
    user: String,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Deserialize)]
struct TeamFile {
    team: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    members: Vec<String>,
    #[serde(default)]
    lead: Option<String>,
    #[serde(default)]
    oncall: HashMap<String, Vec<String>>,
}

fn title_case(id: &str) -> String {
    let mut c = id.chars();
    match c.next() {
        Some(f) => f.to_uppercase().chain(c).collect(),
        None => String::new(),
    }
}

impl Session {
    /// Build the demo session from `config/people` and `config/teams`.
    /// `admin` grants every team; otherwise only the user's own teams.
    pub fn demo(config_dir: &Path, user_id: &str) -> anyhow::Result<Session> {
        Self::demo_with(config_dir, user_id, true)
    }

    pub fn demo_with(config_dir: &Path, user_id: &str, admin: bool) -> anyhow::Result<Session> {
        let people: Vec<PersonFile> = beacon_core::config::load_yaml_files(&config_dir.join("people"))?;
        let teams: Vec<TeamFile> = beacon_core::config::load_yaml_files(&config_dir.join("teams"))?;
        let user = people
            .into_iter()
            .find(|p| p.user == user_id)
            .map(|p| Named { name: p.name.unwrap_or_else(|| title_case(&p.user)), id: p.user })
            .unwrap_or_else(|| Named { id: user_id.to_string(), name: title_case(user_id) });
        let mut oncall_owners = HashMap::new();
        for t in &teams {
            for sched in t.oncall.keys() {
                oncall_owners.insert(sched.clone(), t.team.clone());
            }
        }
        let mut teams: Vec<Named> = teams
            .into_iter()
            .filter(|t| admin || t.members.iter().any(|m| m == user_id) || t.lead.as_deref() == Some(user_id))
            .map(|t| Named { name: t.name.unwrap_or_else(|| title_case(&t.team)), id: t.team })
            .collect();
        teams.sort_by(|a, b| a.id.cmp(&b.id));
        let csrf_token = hex::encode(rand::rng().random::<[u8; 16]>());
        Ok(Session { user, teams, csrf_token, admin, oncall_owners })
    }

    pub fn actor(&self) -> String {
        format!("user:{}", self.user.id)
    }

    pub fn can_see(&self, team: &str) -> bool {
        self.teams.iter().any(|t| t.id == team)
    }

    /// True if `schedule` is a configured on-call schedule belonging to a team
    /// visible to this session.
    pub fn can_see_schedule(&self, schedule: &str) -> bool {
        self.oncall_owners.get(schedule).is_some_and(|owner| self.can_see(owner))
    }

    pub fn check_csrf(&self, headers: &HeaderMap) -> Result<(), ApiError> {
        match headers.get("x-csrf-token").and_then(|v| v.to_str().ok()) {
            Some(t) if constant_time_eq(t.as_bytes(), self.csrf_token.as_bytes()) => Ok(()),
            _ => Err(ApiError::new(axum::http::StatusCode::FORBIDDEN, "csrf", "missing or wrong X-CSRF-Token")),
        }
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The session, plus `capabilities` the UI uses to show or hide features.
pub async fn get_session(State(state): State<ApiState>) -> Json<serde_json::Value> {
    let mut v = serde_json::to_value(&*state.session).expect("session serializes");
    v["capabilities"] = serde_json::json!({ "llm_suggest": crate::mapping_suggest::enabled() });
    Json(v)
}
