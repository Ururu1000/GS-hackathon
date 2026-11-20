//! M0 demo session: one fixed user who sees every configured team. Writes
//! must echo the CSRF token from `GET /api/session` in `X-CSRF-Token`.

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
    pub fn demo(config_dir: &Path, user_id: &str) -> anyhow::Result<Session> {
        let people: Vec<PersonFile> = beacon_core::config::load_yaml_files(&config_dir.join("people"))?;
        let teams: Vec<TeamFile> = beacon_core::config::load_yaml_files(&config_dir.join("teams"))?;
        let user = people
            .into_iter()
            .find(|p| p.user == user_id)
            .map(|p| Named { name: p.name.unwrap_or_else(|| title_case(&p.user)), id: p.user })
            .unwrap_or_else(|| Named { id: user_id.to_string(), name: title_case(user_id) });
        let mut teams: Vec<Named> = teams
            .into_iter()
            .map(|t| Named { name: t.name.unwrap_or_else(|| title_case(&t.team)), id: t.team })
            .collect();
        teams.sort_by(|a, b| a.id.cmp(&b.id));
        let csrf_token = hex::encode(rand::rng().random::<[u8; 16]>());
        Ok(Session { user, teams, csrf_token })
    }

    pub fn actor(&self) -> String {
        format!("user:{}", self.user.id)
    }

    pub fn can_see(&self, team: &str) -> bool {
        self.teams.iter().any(|t| t.id == team)
    }

    pub fn check_csrf(&self, headers: &HeaderMap) -> Result<(), ApiError> {
        match headers.get("x-csrf-token").and_then(|v| v.to_str().ok()) {
            Some(t) if t == self.csrf_token => Ok(()),
            _ => Err(ApiError::new(axum::http::StatusCode::FORBIDDEN, "csrf", "missing or wrong X-CSRF-Token")),
        }
    }
}

pub async fn get_session(State(state): State<ApiState>) -> Json<Session> {
    Json((*state.session).clone())
}
