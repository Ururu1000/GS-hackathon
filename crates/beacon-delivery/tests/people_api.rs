//! Read-only people/teams endpoints: `GET /api/people/directory`, `GET /api/people`,
//! `GET /api/teams` — shapes per ui/contract/openapi.yaml v0.2.0, redaction of other
//! users' contact details and preferences, and the lead/admin `can_edit` rules.
//!
//! Needs NO Postgres: the views read the router's effective directory, whose override
//! stamp query fails fast against the pool below (a lazy `PgPool` pointed at a port
//! nothing listens on, with a short acquire timeout) and falls back to the startup
//! base — exactly the "never fail a read because of an override" path. With no
//! overrides to apply, every view equals the base one (revisions all 1). The router is
//! driven in-process with `tower::ServiceExt::oneshot` (the middleware layer inserting
//! the `Actor` / `Admin` extensions stands in for beacon-api's session layer).
//!
//! Run: `cargo test -p beacon-delivery --test people_api`

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use axum::http::StatusCode;
use beacon_delivery::actions::ActionRegistry;
use beacon_delivery::alert_ops::SqlAlertOps;
use beacon_delivery::api::{self, Actor, Admin, ApiState};
use beacon_delivery::config::{Contact, Directory, Person, Team};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------------------------

fn person(
    id: &str,
    name: Option<&str>,
    email: Option<&str>,
    phone: Option<&str>,
    preferences: Option<Value>,
) -> Person {
    Person {
        id: id.to_string(),
        name: name.map(str::to_string),
        contact: Contact {
            email: email.map(str::to_string),
            phone: phone.map(str::to_string),
        },
        preferences,
        rules: Vec::new(),
    }
}

/// Four people, three teams. anna has a configured name plus preferences; bob has no
/// name but valid preferences; carol has no phone; dave gets a legacy non-array
/// `preferences:` value injected after `from_parts` (which would reject it as invalid).
fn fixture_dir() -> Directory {
    let mut dir = Directory::from_parts(
        vec![
            person(
                "anna",
                Some("Anna Example"),
                Some("anna@example.test"),
                Some("+44 7700 900123"),
                Some(json!([{ "default": ["ui"] }])),
            ),
            person(
                "bob",
                None,
                Some("bob@example.test"),
                Some("+44 7700 900124"),
                Some(json!([
                    { "match": { "severity": "warning" }, "channels": ["email"] }
                ])),
            ),
            person("carol", None, Some("carol@example.test"), None, None),
            person(
                "dave",
                Some("Dave Legacy"),
                Some("dave@example.test"),
                None,
                Some(json!([{ "default": ["ui"] }])),
            ),
        ],
        vec![
            Team {
                id: "rates".into(),
                members: vec!["bob".into(), "anna".into()],
                lead: Some("bob".into()),
                oncall: HashMap::from([(
                    "rates-primary".to_string(),
                    vec!["carol".to_string(), "bob".to_string()],
                )]),
                default_channels: Some(vec!["email".into()]),
            },
            Team {
                id: "payments".into(),
                members: vec!["carol".into()],
                lead: None,
                oncall: HashMap::new(),
                default_channels: None,
            },
            Team {
                id: "counterparty-support".into(),
                members: vec!["dave".into()],
                lead: Some("dave".into()),
                oncall: HashMap::new(),
                default_channels: None,
            },
        ],
    )
    .expect("fixture directory builds");
    // A raw `preferences:` that is not a JSON array: the views render `[]` for it.
    dir.people.get_mut("dave").unwrap().preferences = Some(json!({ "format": "legacy-object" }));
    dir
}

/// An `ApiState` that never touches Postgres: a lazy pool pointed at a dead port, with
/// a short acquire timeout so the effective directory's stamp query fails (and falls
/// back to the base) in milliseconds instead of sqlx's 30 s default.
fn api_state(dir: Directory) -> ApiState {
    ApiState {
        pool: sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_millis(250))
            .connect_lazy("postgres://unused@127.0.0.1:1/unused")
            .unwrap(),
        dir: Arc::new(dir),
        registry: ActionRegistry::default(),
        alert_ops: Arc::new(SqlAlertOps),
        reply_secret: None,
    }
}

// ---------------------------------------------------------------------------------------------
// HTTP plumbing (in-process, the pattern of tests/api.rs' session layer)
// ---------------------------------------------------------------------------------------------

/// `api::router` + a middleware layer inserting `Extension(Actor)` (when `actor` is
/// given) and `Extension(Admin(true))` (when `admin`), standing in for beacon-api's
/// session layer.
fn app_as(state: &ApiState, actor: Option<&str>, admin: bool) -> axum::Router {
    let actor = actor.map(str::to_string);
    api::router(state.clone()).layer(axum::middleware::from_fn(
        move |mut req: axum::extract::Request, next: axum::middleware::Next| {
            let actor = actor.clone();
            async move {
                if let Some(actor) = actor {
                    req.extensions_mut().insert(Actor(actor));
                }
                if admin {
                    req.extensions_mut().insert(Admin(true));
                }
                next.run(req).await
            }
        },
    ))
}

/// GET `uri` in-process; returns the status, the raw body text (for PII scans) and the
/// parsed JSON (`null` when the body is not JSON).
async fn get(app: axum::Router, uri: &str) -> (StatusCode, String, Value) {
    let resp = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::builder()
            .uri(uri)
            .body(axum::body::Body::empty())
            .expect("request builds"),
    )
    .await
    .expect("router is infallible");
    let status = resp.status();
    let bytes = http_body_util::BodyExt::collect(resp.into_body())
        .await
        .expect("body collects")
        .to_bytes();
    let text = String::from_utf8_lossy(&bytes).into_owned();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, text, body)
}

/// The item's JSON keys, sorted.
fn keys_of(v: &Value) -> Vec<&str> {
    let mut keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort_unstable();
    keys
}

fn items(body: &Value) -> &Vec<Value> {
    body["items"].as_array().expect("items array")
}

/// The `items` ids, asserting they are sorted ascending.
fn sorted_ids(body: &Value) -> Vec<&str> {
    let ids: Vec<&str> = items(body)
        .iter()
        .map(|i| i["id"].as_str().unwrap())
        .collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted, "items must be sorted by id");
    ids
}

// ---------------------------------------------------------------------------------------------
// 401 without an actor
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn unauthorized_without_actor_on_all_three_routes() {
    let state = api_state(fixture_dir());
    let app = app_as(&state, None, false);
    for uri in ["/api/people/directory", "/api/people", "/api/teams"] {
        let (status, _text, body) = get(app.clone(), uri).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}");
        assert_eq!(body["error"]["code"], "unauthorized", "{uri}");
    }
}

// ---------------------------------------------------------------------------------------------
// GET /api/people/directory
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn directory_entries_sorted_with_exact_keys_and_fallback_names() {
    let state = api_state(fixture_dir());
    let app = app_as(&state, Some("user:bob"), false);
    let (status, _text, body) = get(app, "/api/people/directory").await;
    assert_eq!(status, StatusCode::OK);

    assert_eq!(sorted_ids(&body), ["anna", "bob", "carol", "dave"]);

    for item in items(&body) {
        // The exact PersonDirectoryEntry key set — no `contact`, no `preferences`.
        assert_eq!(keys_of(item), ["id", "name", "teams"], "{item}");
    }
    // A configured name wins; otherwise the id title-cased.
    assert_eq!(body["items"][0]["name"], "Anna Example");
    assert_eq!(body["items"][1]["name"], "Bob");
    assert_eq!(body["items"][2]["name"], "Carol");
    assert_eq!(body["items"][3]["name"], "Dave Legacy");

    // teams = the ids where the person is a member or the lead; being in an on-call
    // rotation alone does not put carol in rates.
    assert_eq!(body["items"][0]["teams"], json!(["rates"]));
    assert_eq!(body["items"][1]["teams"], json!(["rates"]));
    assert_eq!(body["items"][2]["teams"], json!(["payments"]));
    assert_eq!(body["items"][3]["teams"], json!(["counterparty-support"]));
}

// ---------------------------------------------------------------------------------------------
// GET /api/people
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn people_redacts_everyone_but_the_actor() {
    let state = api_state(fixture_dir());
    let app = app_as(&state, Some("user:bob"), false);
    let (status, text, body) = get(app, "/api/people").await;
    assert_eq!(status, StatusCode::OK);

    assert_eq!(sorted_ids(&body), ["anna", "bob", "carol", "dave"]);
    let by_id: HashMap<&str, &Value> = items(&body)
        .iter()
        .map(|i| (i["id"].as_str().unwrap(), i))
        .collect();

    // The exact PersonView key set (the contract's required list + can_edit).
    for item in items(&body) {
        assert_eq!(
            keys_of(item),
            [
                "can_edit",
                "contact",
                "id",
                "name",
                "preferences",
                "revision",
                "teams",
            ],
            "{item}"
        );
    }

    // Bob sees himself in full.
    let bob = by_id["bob"];
    assert_eq!(bob["can_edit"], true);
    assert_eq!(bob["contact"]["email"], "bob@example.test");
    assert_eq!(bob["contact"]["phone"], "+44 7700 900124");
    assert_eq!(
        bob["preferences"],
        json!([{ "match": { "severity": "warning" }, "channels": ["email"] }])
    );
    assert_eq!(bob["revision"], 1);

    // Everyone else is redacted: no contact keys, no preferences.
    for id in ["anna", "carol", "dave"] {
        let item = by_id[id];
        assert_eq!(item["can_edit"], false, "{id}");
        assert_eq!(item["contact"], json!({}), "{id}");
        assert_eq!(item["preferences"], json!([]), "{id}");
    }

    // No other person's email, phone or preferences string appears anywhere in the raw
    // response body.
    for secret in [
        "anna@example.test",
        "+44 7700 900123",
        "carol@example.test",
        "dave@example.test",
        "\"default\"",
        "legacy-object",
    ] {
        assert!(!text.contains(secret), "{secret} leaked: {text}");
    }
}

#[tokio::test]
async fn admin_sees_everyones_contact_and_all_can_edit() {
    let state = api_state(fixture_dir());
    // bob is the actor, but the Admin flag makes everyone editable and visible.
    let app = app_as(&state, Some("user:bob"), true);
    let (status, _text, body) = get(app, "/api/people").await;
    assert_eq!(status, StatusCode::OK);

    for item in items(&body) {
        assert_eq!(item["can_edit"], true, "{item}");
        let id = item["id"].as_str().unwrap();
        assert_eq!(
            item["contact"]["email"],
            format!("{id}@example.test"),
            "{item}"
        );
    }
    // anna's phone and raw preferences are visible to the admin.
    assert_eq!(body["items"][0]["contact"]["phone"], "+44 7700 900123");
    assert_eq!(
        body["items"][0]["preferences"],
        json!([{ "default": ["ui"] }])
    );
    // dave's legacy non-array preferences still render as [].
    assert_eq!(body["items"][3]["preferences"], json!([]));
}

#[tokio::test]
async fn self_view_omits_missing_phone_and_non_array_preferences() {
    let state = api_state(fixture_dir());

    // carol has no phone: her own view carries only the `email` key, never `phone: null`.
    let app = app_as(&state, Some("user:carol"), false);
    let (_, _, body) = get(app, "/api/people").await;
    let carol = items(&body).iter().find(|i| i["id"] == "carol").unwrap();
    assert_eq!(carol["can_edit"], true);
    assert_eq!(carol["contact"], json!({ "email": "carol@example.test" }));

    // dave sees himself, but a non-array `preferences:` value renders as [].
    let app = app_as(&state, Some("user:dave"), false);
    let (_, _, body) = get(app, "/api/people").await;
    let dave = items(&body).iter().find(|i| i["id"] == "dave").unwrap();
    assert_eq!(dave["can_edit"], true);
    assert_eq!(dave["preferences"], json!([]));
}

// ---------------------------------------------------------------------------------------------
// GET /api/teams
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn teams_lead_member_and_admin_can_edit_rules() {
    let state = api_state(fixture_dir());

    // dave leads counterparty-support only.
    let app = app_as(&state, Some("user:dave"), false);
    let (status, _text, body) = get(app, "/api/teams").await;
    assert_eq!(status, StatusCode::OK);

    assert_eq!(
        sorted_ids(&body),
        ["counterparty-support", "payments", "rates"]
    );
    for item in items(&body) {
        // The exact TeamView key set (the contract's required list + can_edit).
        assert_eq!(
            keys_of(item),
            [
                "can_edit",
                "default_channels",
                "id",
                "lead",
                "members",
                "name",
                "oncall",
                "revision",
            ],
            "{item}"
        );
    }

    let rates = items(&body).iter().find(|i| i["id"] == "rates").unwrap();
    assert_eq!(rates["name"], "Rates");
    assert_eq!(rates["members"], json!(["bob", "anna"]));
    assert_eq!(rates["lead"], "bob");
    assert_eq!(
        rates["oncall"],
        json!({ "rates-primary": ["carol", "bob"] })
    );
    assert_eq!(rates["default_channels"], json!(["email"]));
    assert_eq!(rates["revision"], 1);
    assert_eq!(rates["can_edit"], false, "dave is not the rates lead");

    let payments = items(&body).iter().find(|i| i["id"] == "payments").unwrap();
    assert_eq!(payments["name"], "Payments");
    assert_eq!(payments["lead"], Value::Null, "an unset lead is null");
    assert_eq!(payments["oncall"], json!({}));
    assert_eq!(payments["default_channels"], json!([]));
    assert_eq!(payments["can_edit"], false);

    let cps = items(&body)
        .iter()
        .find(|i| i["id"] == "counterparty-support")
        .unwrap();
    assert_eq!(cps["name"], "Counterparty-support");
    assert_eq!(cps["lead"], "dave");
    assert_eq!(cps["oncall"], json!({}));
    assert_eq!(cps["default_channels"], json!([]));
    assert_eq!(cps["can_edit"], true, "dave leads counterparty-support");

    // The lead may edit his team; a plain member may not.
    let app = app_as(&state, Some("user:bob"), false);
    let (_, _, body) = get(app, "/api/teams").await;
    let rates = items(&body).iter().find(|i| i["id"] == "rates").unwrap();
    assert_eq!(rates["can_edit"], true, "bob leads rates");

    let app = app_as(&state, Some("user:anna"), false);
    let (_, _, body) = get(app, "/api/teams").await;
    for team in items(&body) {
        assert_eq!(team["can_edit"], false, "anna leads nothing: {team}");
    }

    // An admin may edit every team.
    let app = app_as(&state, Some("user:anna"), true);
    let (_, _, body) = get(app, "/api/teams").await;
    for team in items(&body) {
        assert_eq!(team["can_edit"], true, "{team}");
    }
}

// ---------------------------------------------------------------------------------------------
// The repo's real config
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn repo_config_views_match_the_contract_types() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");
    let dir = Directory::load(&root).expect("the repo's seed config loads");
    let state = api_state(dir);
    let app = app_as(&state, Some("user:anna"), false);

    // PersonView: every contract-required key with the right JSON type.
    let (status, _, body) = get(app.clone(), "/api/people").await;
    assert_eq!(status, StatusCode::OK);
    sorted_ids(&body);
    for item in items(&body) {
        let obj = item.as_object().unwrap();
        for key in [
            "id",
            "name",
            "contact",
            "teams",
            "preferences",
            "revision",
            "can_edit",
        ] {
            assert!(
                obj.contains_key(key),
                "person {} misses {key}: {item}",
                item["id"]
            );
        }
        assert!(obj["id"].is_string());
        assert!(obj["name"].is_string());
        assert!(obj["contact"].is_object());
        assert!(obj["teams"].is_array());
        assert!(obj["preferences"].is_array());
        assert!(obj["revision"].is_i64());
        assert!(obj["can_edit"].is_boolean());
    }

    // TeamView: same, with `lead` a string or null.
    let (status, _, body) = get(app.clone(), "/api/teams").await;
    assert_eq!(status, StatusCode::OK);
    sorted_ids(&body);
    for item in items(&body) {
        let obj = item.as_object().unwrap();
        for key in [
            "id",
            "name",
            "members",
            "lead",
            "oncall",
            "default_channels",
            "revision",
            "can_edit",
        ] {
            assert!(
                obj.contains_key(key),
                "team {} misses {key}: {item}",
                item["id"]
            );
        }
        assert!(obj["id"].is_string());
        assert!(obj["name"].is_string());
        assert!(obj["members"].is_array());
        assert!(obj["lead"].is_string() || obj["lead"].is_null());
        assert!(obj["oncall"].is_object());
        assert!(obj["default_channels"].is_array());
        assert!(obj["revision"].is_i64());
        assert!(obj["can_edit"].is_boolean());
    }

    // PersonDirectoryEntry.
    let (status, _, body) = get(app, "/api/people/directory").await;
    assert_eq!(status, StatusCode::OK);
    sorted_ids(&body);
    for item in items(&body) {
        let obj = item.as_object().unwrap();
        for key in ["id", "name", "teams"] {
            assert!(
                obj.contains_key(key),
                "directory {} misses {key}: {item}",
                item["id"]
            );
        }
        assert!(obj["id"].is_string());
        assert!(obj["name"].is_string());
        assert!(obj["teams"].is_array());
    }
}
