//! Runtime people/teams creation over real Postgres: `POST /api/people` and
//! `POST /api/teams` — admin-only authorization, the id/contact/team validation, the
//! duplicate `409 revision_conflict`, rows at revision 1 with `"created": true`, and
//! the existing PUTs editing created ids (CAS from revision 1, the created person's
//! name/contact surviving a preferences save). The live-pickup test pins the point of
//! it all: a created person in a created team is routed by `router::route_once` reading
//! the effective directory, with no restart.
//!
//! Needs Postgres (`DATABASE_URL`; sqlx creates a throwaway database per test). The
//! router is driven in-process with `tower::ServiceExt::oneshot`; the middleware layer
//! inserting the `Actor` / `Admin` extensions stands in for beacon-api's session layer.
//!
//! Run: `cargo test -p beacon-delivery --test directory_create`

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::{Method, StatusCode, header};
use beacon_delivery::RouterConfig;
use beacon_delivery::actions::ActionRegistry;
use beacon_delivery::alert_ops::SqlAlertOps;
use beacon_delivery::api::{self, Actor, Admin, ApiState};
use beacon_delivery::config::{Contact, Directory, Person, Team};
use beacon_delivery::overrides::{self, EffectiveDirectory};
use beacon_delivery::prefs;
use beacon_delivery::router::route_once;
use serde_json::{Value, json};
use sqlx::PgPool;

const JSON: &str = "application/json";

// ---------------------------------------------------------------------------------------------
// Fixtures (the pattern of tests/people_api_db.rs)
// ---------------------------------------------------------------------------------------------

fn person(
    id: &str,
    email: Option<&str>,
    phone: Option<&str>,
    preferences: Option<Value>,
) -> Person {
    Person {
        id: id.to_string(),
        name: None,
        contact: Contact {
            email: email.map(str::to_string),
            phone: phone.map(str::to_string),
        },
        preferences,
        rules: Vec::new(),
    }
}

/// Three people, two teams: bob leads `rates` (anna a plain member, carol only in the
/// on-call rotation) with email defaults; `payments` (carol) has no lead.
fn fixture_dir() -> Directory {
    Directory::from_parts(
        vec![
            person(
                "bob",
                Some("bob@example.test"),
                Some("+44 7700 900124"),
                Some(json!([
                    { "match": { "severity": "warning" }, "channels": ["email"] }
                ])),
            ),
            person("carol", Some("carol@example.test"), None, None),
            person("anna", Some("anna@example.test"), None, None),
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
        ],
    )
    .expect("fixture directory builds")
}

fn api_state(pool: &PgPool, dir: Directory) -> ApiState {
    ApiState {
        pool: pool.clone(),
        dir: Arc::new(dir),
        registry: ActionRegistry::default(),
        alert_ops: Arc::new(SqlAlertOps),
        reply_secret: None,
        email_link: None,
    }
}

// ---------------------------------------------------------------------------------------------
// HTTP plumbing (in-process, the pattern of tests/people_api_db.rs' session layer)
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

/// POST `uri` with `body` as the given media type, in-process.
async fn post(
    app: axum::Router,
    uri: &str,
    body: &Value,
    content_type: &str,
) -> (StatusCode, Value) {
    let resp = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header(header::CONTENT_TYPE, content_type)
            .body(axum::body::Body::from(body.to_string()))
            .expect("request builds"),
    )
    .await
    .expect("router is infallible");
    read(resp).await
}

/// PUT `uri` with `body` as the given media type, in-process.
async fn put(
    app: axum::Router,
    uri: &str,
    body: &Value,
    content_type: &str,
) -> (StatusCode, Value) {
    let resp = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::builder()
            .method(Method::PUT)
            .uri(uri)
            .header(header::CONTENT_TYPE, content_type)
            .body(axum::body::Body::from(body.to_string()))
            .expect("request builds"),
    )
    .await
    .expect("router is infallible");
    read(resp).await
}

/// GET `uri` in-process.
async fn get(app: axum::Router, uri: &str) -> (StatusCode, Value) {
    let resp = tower::ServiceExt::oneshot(
        app,
        axum::http::Request::builder()
            .uri(uri)
            .body(axum::body::Body::empty())
            .expect("request builds"),
    )
    .await
    .expect("router is infallible");
    read(resp).await
}

async fn read(resp: axum::response::Response) -> (StatusCode, Value) {
    let status = resp.status();
    let bytes = http_body_util::BodyExt::collect(resp.into_body())
        .await
        .expect("body collects")
        .to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

/// The `{items}` entry with this id.
fn find<'a>(body: &'a Value, id: &str) -> &'a Value {
    body["items"]
        .as_array()
        .expect("items array")
        .iter()
        .find(|i| i["id"] == id)
        .unwrap_or_else(|| panic!("no item {id} in {body}"))
}

/// A routing context for preference resolution (a `default` line matches anything).
fn routing_ctx() -> prefs::RoutingContext {
    prefs::RoutingContext {
        severity: "warning".into(),
        team: "rates".into(),
        rule_id: "rates-rejected-trades".into(),
        service: None,
        environment: None,
    }
}

// ---------------------------------------------------------------------------------------------
// POST /api/people
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn admin_creates_a_person_and_the_views_list_her(pool: PgPool) {
    let state = api_state(&pool, fixture_dir());
    let app = app_as(&state, Some("user:bob"), true);

    // 201 with the new PersonView as the admin sees it: revision 1 (the row's), full
    // contact, the submitted preferences parsed like a YAML import.
    let (status, body) = post(
        app.clone(),
        "/api/people",
        &json!({
            "id": "zoe",
            "name": "Zoe Example",
            "contact": { "email": "zoe@example.test", "phone": "+44 7700 900999" },
            "preferences": [{ "default": ["sms"] }]
        }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["id"], "zoe");
    assert_eq!(body["name"], "Zoe Example");
    assert_eq!(body["revision"], 1);
    assert_eq!(body["can_edit"], true);
    assert_eq!(body["contact"]["email"], "zoe@example.test");
    assert_eq!(body["contact"]["phone"], "+44 7700 900999");
    assert_eq!(body["preferences"], json!([{ "default": ["sms"] }]));
    assert_eq!(body["teams"], json!([]));

    // Both read-only views list her — no restart, the stamp moved.
    let (status, list) = get(app.clone(), "/api/people").await;
    assert_eq!(status, StatusCode::OK);
    let zoe = find(&list, "zoe");
    assert_eq!(zoe["revision"], 1);
    assert_eq!(zoe["name"], "Zoe Example");
    assert_eq!(zoe["contact"]["email"], "zoe@example.test");

    let (status, entries) = get(app.clone(), "/api/people/directory").await;
    assert_eq!(status, StatusCode::OK);
    let zoe = find(&entries, "zoe");
    assert_eq!(zoe["name"], "Zoe Example");
    assert_eq!(zoe["teams"], json!([]));

    // A name-less person is title-cased from her id, like a YAML person.
    let (status, body) = post(
        app.clone(),
        "/api/people",
        &json!({ "id": "no-name", "contact": { "email": "noname@example.test" } }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["name"], "No-name");
    assert_eq!(body["preferences"], json!([]));

    // Another user's view redacts her like anyone else.
    let app = app_as(&state, Some("user:anna"), false);
    let (status, list) = get(app, "/api/people").await;
    assert_eq!(status, StatusCode::OK);
    let zoe = find(&list, "zoe");
    assert_eq!(zoe["contact"], json!({}));
    assert_eq!(zoe["preferences"], json!([]));
    assert_eq!(zoe["can_edit"], false);

    // One audit row per create.
    let (count,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM audit_log \
         WHERE kind = 'person.created' AND actor = 'user:bob' AND alert_id IS NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("audit count");
    assert_eq!(count, 2);
}

#[sqlx::test(migrations = "../../migrations")]
async fn creating_needs_an_admin_session_and_a_json_body(pool: PgPool) {
    let state = api_state(&pool, fixture_dir());

    // No actor: 401 on both routes.
    let app = app_as(&state, None, false);
    for uri in ["/api/people", "/api/teams"] {
        let (status, body) = post(app.clone(), uri, &json!({ "id": "x" }), JSON).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}: {body}");
        assert_eq!(body["error"]["code"], "unauthorized", "{uri}: {body}");
    }

    // A plain session: 403 — creation is admin-only.
    let app = app_as(&state, Some("user:bob"), false);
    for uri in ["/api/people", "/api/teams"] {
        let (status, body) = post(app.clone(), uri, &json!({ "id": "x" }), JSON).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri}: {body}");
        assert_eq!(body["error"]["code"], "forbidden", "{uri}: {body}");
    }

    // Admin(false) is as good as no flag at all.
    let app = api::router(state.clone()).layer(axum::middleware::from_fn(
        |mut req: axum::extract::Request, next: axum::middleware::Next| async move {
            req.extensions_mut().insert(Actor("user:bob".into()));
            req.extensions_mut().insert(Admin(false));
            next.run(req).await
        },
    ));
    let (status, body) = post(app, "/api/people", &json!({ "id": "x" }), JSON).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    // An admin without the JSON content type: 415 (audit #11).
    let app = app_as(&state, Some("user:bob"), true);
    for uri in ["/api/people", "/api/teams"] {
        let (status, body) = post(app.clone(), uri, &json!({ "id": "x" }), "text/plain").await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{uri}: {body}");
    }

    // Nothing above wrote a row.
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM directory_overrides")
        .fetch_one(&pool)
        .await
        .expect("override count");
    assert_eq!(count, 0);
}

#[sqlx::test(migrations = "../../migrations")]
async fn duplicate_ids_conflict_and_creations_are_validated(pool: PgPool) {
    let state = api_state(&pool, fixture_dir());
    let app = app_as(&state, Some("user:bob"), true);

    // A base id is taken: 409, and nothing is written.
    let (status, body) = post(
        app.clone(),
        "/api/people",
        &json!({ "id": "anna", "contact": { "email": "anna2@example.test" } }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "revision_conflict");

    // A created id conflicts on the second create too.
    let (status, body) = post(
        app.clone(),
        "/api/people",
        &json!({ "id": "zoe", "contact": { "email": "zoe@example.test" } }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = post(
        app.clone(),
        "/api/people",
        &json!({ "id": "zoe", "contact": { "phone": "+44 7700 900999" } }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "revision_conflict");

    // Teams: a base id conflicts, then the second create of a created one does.
    let (status, body) = post(
        app.clone(),
        "/api/teams",
        &json!({ "id": "rates", "members": ["bob"] }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    let (status, body) = post(
        app.clone(),
        "/api/teams",
        &json!({ "id": "newteam", "members": ["zoe"] }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = post(
        app.clone(),
        "/api/teams",
        &json!({ "id": "newteam", "members": ["bob"] }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "revision_conflict");

    // Only the two successes wrote rows.
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM directory_overrides")
        .fetch_one(&pool)
        .await
        .expect("override count");
    assert_eq!(count, 2);

    // Bad ids: 422 (missing, non-string, wrong case, bad first char, spaces, too long).
    for bad in [
        json!({ "contact": { "email": "x@example.test" } }),
        json!({ "id": 7, "contact": { "email": "x@example.test" } }),
        json!({ "id": "Anna", "contact": { "email": "x@example.test" } }),
        json!({ "id": "-nope", "contact": { "email": "x@example.test" } }),
        json!({ "id": "_x", "contact": { "email": "x@example.test" } }),
        json!({ "id": "has space", "contact": { "email": "x@example.test" } }),
        json!({ "id": "a".repeat(65), "contact": { "email": "x@example.test" } }),
    ] {
        let (status, body) = post(app.clone(), "/api/people", &bad, JSON).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{bad}: {body}");
        assert_eq!(body["error"]["code"], "invalid", "{bad}: {body}");
    }

    // 64 characters is still a valid id.
    let (status, body) = post(
        app.clone(),
        "/api/people",
        &json!({ "id": "a".repeat(64), "contact": { "email": "long@example.test" } }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // No address at all: 422.
    for bad in [
        json!({ "id": "n1" }),
        json!({ "id": "n2", "contact": {} }),
        json!({ "id": "n3", "contact": null }),
    ] {
        let (status, body) = post(app.clone(), "/api/people", &bad, JSON).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{bad}: {body}");
        assert_eq!(body["error"]["code"], "invalid", "{bad}: {body}");
    }

    // Bad preferences: 422 from the shared validator, naming the person.
    for bad in [
        json!([{ "match": {}, "channels": ["fax"] }]),
        json!("not-a-list"),
    ] {
        let (status, body) = post(
            app.clone(),
            "/api/people",
            &json!({
                "id": "bad-prefs",
                "contact": { "email": "bad@example.test" },
                "preferences": bad
            }),
            JSON,
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{bad}: {body}");
        assert_eq!(body["error"]["code"], "invalid", "{bad}: {body}");
        assert!(
            body["error"]["message"]
                .as_str()
                .is_some_and(|m| m.contains("bad-prefs")),
            "{bad}: {body}"
        );
    }

    // A non-string name or a non-object contact: 422.
    for bad in [
        json!({ "id": "n4", "name": 3, "contact": { "email": "x@example.test" } }),
        json!({ "id": "n5", "contact": "x@example.test" }),
        json!({ "id": "n6", "contact": { "email": 3 } }),
    ] {
        let (status, body) = post(app.clone(), "/api/people", &bad, JSON).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{bad}: {body}");
    }

    // Every rejected create above left the table at the two successes plus the 64-char
    // person; zoe's duplicate never overwrote her row.
    let (count,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM directory_overrides WHERE kind = 'person'")
            .fetch_one(&pool)
            .await
            .expect("person override count");
    assert_eq!(count, 2);
    let (revision,): (i64,) = sqlx::query_as(
        "SELECT revision FROM directory_overrides WHERE kind = 'person' AND id = 'zoe'",
    )
    .fetch_one(&pool)
    .await
    .expect("zoe's row");
    assert_eq!(revision, 1, "the duplicate create must not bump zoe");
}

// ---------------------------------------------------------------------------------------------
// POST /api/teams
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn teams_are_created_with_full_validation(pool: PgPool) {
    let state = api_state(&pool, fixture_dir());
    let app = app_as(&state, Some("user:bob"), true);

    // A created person may be a created team's member, lead and on-call.
    let (status, body) = post(
        app.clone(),
        "/api/people",
        &json!({
            "id": "dave",
            "contact": { "email": "dave@example.test", "phone": "+44 7700 900777" }
        }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = post(
        app.clone(),
        "/api/teams",
        &json!({
            "id": "newrates",
            "members": ["dave", "anna"],
            "lead": "dave",
            "oncall": { "newrates-primary": ["dave"] },
            "default_channels": ["email"]
        }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["id"], "newrates");
    assert_eq!(body["revision"], 1);
    assert_eq!(body["members"], json!(["dave", "anna"]));
    assert_eq!(body["lead"], "dave");
    assert_eq!(body["oncall"], json!({ "newrates-primary": ["dave"] }));
    assert_eq!(body["default_channels"], json!(["email"]));
    assert_eq!(body["can_edit"], true);

    // The views list the team, and dave's view names it.
    let (status, list) = get(app.clone(), "/api/teams").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(find(&list, "newrates")["revision"], 1);
    let (_, list) = get(app.clone(), "/api/people").await;
    assert_eq!(find(&list, "dave")["teams"], json!(["newrates"]));

    // Unknown people (member, lead, on-call), unknown channel, empty defaults, bad
    // shapes and bad ids: 422, and nothing is written.
    for bad in [
        json!({ "id": "t1", "members": ["ghost"] }),
        json!({ "id": "t2", "members": ["dave"], "lead": "ghost" }),
        json!({ "id": "t3", "members": ["dave"], "oncall": { "s": ["ghost"] } }),
        json!({ "id": "t4", "members": ["dave"], "default_channels": ["fax"] }),
        json!({ "id": "t5", "members": ["dave"], "default_channels": [] }),
        json!({ "id": "t6", "members": ["dave"], "oncall": { "s": [] } }),
        json!({ "id": "T7", "members": ["dave"] }),
        json!({ "members": ["dave"] }),
        json!({ "id": "t8" }),
        json!({ "id": "t9", "members": "dave" }),
        json!({ "id": "t10", "members": ["dave"], "lead": 3 }),
        json!({ "id": "t11", "members": ["dave"], "oncall": [] }),
        json!({ "id": "t12", "members": ["dave"], "default_channels": "email" }),
    ] {
        let (status, body) = post(app.clone(), "/api/teams", &bad, JSON).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{bad}: {body}");
        assert_eq!(body["error"]["code"], "invalid", "{bad}: {body}");
    }
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM directory_overrides")
        .fetch_one(&pool)
        .await
        .expect("override count");
    assert_eq!(count, 2, "only dave and newrates were written");
}

// ---------------------------------------------------------------------------------------------
// Editing created ids through the existing PUTs
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn created_ids_edit_through_the_existing_puts(pool: PgPool) {
    let state = api_state(&pool, fixture_dir());
    let app = app_as(&state, Some("user:bob"), true);

    // dave, with a name and both contact channels.
    let (status, body) = post(
        app.clone(),
        "/api/people",
        &json!({
            "id": "dave",
            "name": "Dave Example",
            "contact": { "email": "dave@example.test", "phone": "+44 7700 900777" },
            "preferences": [{ "default": ["email"] }]
        }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // His own team, which he leads.
    let (status, body) = post(
        app.clone(),
        "/api/teams",
        &json!({
            "id": "newrates",
            "members": ["dave"],
            "lead": "dave",
            "default_channels": ["email"]
        }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // The preferences PUT on a created id: a created row sits at revision 1, so the
    // first edit sends expected_revision 1 -> 2. The name and contact survive the
    // rewrite (the row is the whole person).
    let (status, body) = put(
        app.clone(),
        "/api/people/dave/preferences",
        &json!({ "preferences": [{ "default": ["sms"] }], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["revision"], 2);
    assert_eq!(body["preferences"], json!([{ "default": ["sms"] }]));
    assert_eq!(body["contact"]["email"], "dave@example.test");
    assert_eq!(body["contact"]["phone"], "+44 7700 900777");
    assert_eq!(body["name"], "Dave Example");
    assert_eq!(body["teams"], json!(["newrates"]));

    // A stale replay of revision 1: 409 whose `current` is the database's revision 2 —
    // with the contact the preserved row still carries.
    let (status, err) = put(
        app.clone(),
        "/api/people/dave/preferences",
        &json!({ "preferences": [], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{err}");
    assert_eq!(err["error"]["code"], "revision_conflict");
    assert_eq!(err["error"]["current"]["revision"], 2);
    assert_eq!(
        err["error"]["current"]["contact"]["phone"],
        "+44 7700 900777"
    );

    // GET shows the survived name and contact, at revision 2.
    let (status, list) = get(app.clone(), "/api/people").await;
    assert_eq!(status, StatusCode::OK);
    let dave = find(&list, "dave");
    assert_eq!(dave["revision"], 2);
    assert_eq!(dave["name"], "Dave Example");
    assert_eq!(dave["contact"]["phone"], "+44 7700 900777");
    assert_eq!(dave["preferences"], json!([{ "default": ["sms"] }]));

    // dave himself (no admin) can save his own preferences: the actor check reads the
    // effective directory, which has him.
    let app = app_as(&state, Some("user:dave"), false);
    let (status, body) = put(
        app,
        "/api/people/dave/preferences",
        &json!({ "preferences": [{ "default": ["phone"] }], "expected_revision": 2 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["revision"], 3);

    // The team PUT on a created team: revision 1 -> 2, the created marker kept (the
    // team is still in the directory after the save), untouched fields intact.
    let app = app_as(&state, Some("user:dave"), false);
    let (status, body) = put(
        app.clone(),
        "/api/teams/newrates",
        &json!({ "members": ["dave", "anna"], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["revision"], 2);
    assert_eq!(body["members"], json!(["dave", "anna"]));
    assert_eq!(body["lead"], "dave");

    let (status, list) = get(app.clone(), "/api/teams").await;
    assert_eq!(status, StatusCode::OK);
    let newrates = find(&list, "newrates");
    assert_eq!(newrates["revision"], 2);
    assert_eq!(newrates["members"], json!(["dave", "anna"]));
    assert_eq!(newrates["lead"], "dave");

    // The audit trail: two creates, two preference saves, one team save.
    for (kind, expected) in [
        ("person.created", 1),
        ("team.created", 1),
        ("person.preferences_updated", 2),
        ("team.updated", 1),
    ] {
        let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM audit_log WHERE kind = $1")
            .bind(kind)
            .fetch_one(&pool)
            .await
            .expect("audit count");
        assert_eq!(count, expected, "{kind}");
    }
}

// ---------------------------------------------------------------------------------------------
// The directory the router reads, live
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn created_people_and_teams_route_without_a_restart(pool: PgPool) {
    let state = api_state(&pool, fixture_dir());
    let app = app_as(&state, Some("user:bob"), true);

    // zoe and her team, created through the API.
    let (status, body) = post(
        app.clone(),
        "/api/people",
        &json!({ "id": "zoe", "contact": { "email": "zoe@example.test" } }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = post(
        app.clone(),
        "/api/teams",
        &json!({ "id": "newteam", "members": ["zoe"], "default_channels": ["email"] }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // A notify row for the new team: the effective directory a router pass reads — the
    // same stamp-following view a running router holds — has both creations, with no
    // restart and nothing rebuilt in code.
    let effective = EffectiveDirectory::new(Arc::new(fixture_dir()));
    let alert = common::insert_alert(&pool, "open", "Created team routes zoe").await;
    let _outbox = common::insert_notify(
        &pool,
        alert,
        1,
        // No explicit channels: zoe has no preferences, so the team default applies.
        json!([{ "to": "team:newteam" }]),
    )
    .await;

    let dir = effective.get(&pool).await;
    let stats = route_once(&pool, &dir, &RouterConfig::default())
        .await
        .expect("route_once");
    assert_eq!(stats.outbox_rows, 1, "{stats:?}");
    assert_eq!(stats.jobs_inserted, 1, "{stats:?}");

    // One email job for zoe, addressed to the contact the create stored.
    let jobs = common::job_rows(&pool, alert).await;
    assert_eq!(jobs.len(), 1, "{jobs:?}");
    assert_eq!(jobs[0].recipient_id, "zoe");
    assert_eq!(jobs[0].channel, "email");
    assert_eq!(jobs[0].address, "zoe@example.test");
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_created_row_that_no_longer_validates_drops_alone(pool: PgPool) {
    // A created person whose stored preferences went bad, next to a valid created team
    // and a valid save for a base person — written straight to the table, as if the row
    // was corrupted after the create.
    sqlx::query(
        "INSERT INTO directory_overrides (kind, id, body, revision, updated_by) VALUES \
         ('person', 'newbie', $1, 1, 'user:bob'), \
         ('person', 'bob', $2, 2, 'user:bob'), \
         ('team', 'newteam', $3, 1, 'user:bob')",
    )
    .bind(json!({
        "created": true,
        "name": "New Bee",
        "contact": { "email": "newbie@example.test" },
        "preferences": [{ "default": ["fax"] }],
    }))
    .bind(json!({ "preferences": [{ "default": ["sms"] }] }))
    .bind(json!({
        "created": true,
        "members": ["bob"],
        "lead": "bob",
        "oncall": {},
        "default_channels": ["email"],
    }))
    .execute(&pool)
    .await
    .unwrap();

    // Only the bad created row drops: the created team and bob's save still apply, and
    // the stored revisions stay readable so a fresh save can replace the bad row.
    let snap = EffectiveDirectory::new(Arc::new(fixture_dir()))
        .snapshot(&pool)
        .await;
    assert!(!snap.dir.people.contains_key("newbie"), "the bad row drops");
    assert!(snap.dir.teams.contains_key("newteam"));
    assert_eq!(snap.dir.teams["newteam"].members, ["bob"]);
    let bob = snap.dir.people.get("bob").expect("bob is base");
    let resolution = prefs::resolve_channels(bob, None, &routing_ctx(), chrono::Utc::now());
    assert_eq!(resolution.channels, ["sms"]);
    assert!(!snap.applied.people.contains_key("newbie"));
    assert!(snap.applied.people.contains_key("bob"));
    assert!(snap.applied.teams.contains_key("newteam"));
    assert_eq!(
        overrides::revision_of(&snap.overrides, overrides::KIND_PERSON, "newbie"),
        1,
        "the stored revision stays readable"
    );
}
