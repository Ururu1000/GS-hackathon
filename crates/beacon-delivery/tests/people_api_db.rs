//! The writable people/teams endpoints over real Postgres: `PUT
//! /api/people/{personId}/preferences` and `PUT /api/teams/{teamId}` — authorization
//! (self / current lead / admin), validation through the one YAML validator, the
//! `expected_revision` compare-and-set (first save at revision 2), audit rows, the
//! effective directory the router/scheduler will read, and the redaction rules the
//! read-only suite (`people_api.rs`) pins down.
//!
//! Needs Postgres (`DATABASE_URL`; sqlx creates a throwaway database per test). The
//! router is driven in-process with `tower::ServiceExt::oneshot`; the middleware layer
//! inserting the `Actor` / `Admin` extensions stands in for beacon-api's session layer.
//!
//! Run: `cargo test -p beacon-delivery --test people_api_db`

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::{Method, StatusCode, header};
use beacon_delivery::actions::ActionRegistry;
use beacon_delivery::alert_ops::SqlAlertOps;
use beacon_delivery::api::{self, Actor, Admin, ApiState};
use beacon_delivery::config::{Contact, Directory, Person, Team};
use beacon_delivery::overrides::{self, EffectiveDirectory};
use beacon_delivery::prefs;
use serde_json::{Value, json};
use sqlx::PgPool;

const JSON: &str = "application/json";

// ---------------------------------------------------------------------------------------------
// Fixtures
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
    }
}

// ---------------------------------------------------------------------------------------------
// HTTP plumbing (in-process, the pattern of tests/people_api.rs' session layer)
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
// PUT /api/people/{personId}/preferences
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn bob_saves_his_preferences_and_revisions_move(pool: PgPool) {
    let state = api_state(&pool, fixture_dir());
    let app = app_as(&state, Some("user:bob"), false);

    // The first save (the YAML base is revision 1) writes revision 2 and answers with
    // the updated PersonView as bob sees it.
    let (status, body) = put(
        app.clone(),
        "/api/people/bob/preferences",
        &json!({ "preferences": [{ "default": ["sms"] }], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["id"], "bob");
    assert_eq!(body["revision"], 2);
    assert_eq!(body["can_edit"], true);
    assert_eq!(body["contact"]["email"], "bob@example.test");
    assert_eq!(body["contact"]["phone"], "+44 7700 900124");
    assert_eq!(body["preferences"], json!([{ "default": ["sms"] }]));
    assert_eq!(body["teams"], json!(["rates"]));

    // GET /api/people shows the saved preferences and the moved revision to bob; carol
    // stays redacted at her base revision.
    let (status, list) = get(app.clone(), "/api/people").await;
    assert_eq!(status, StatusCode::OK);
    let bob = find(&list, "bob");
    assert_eq!(bob["revision"], 2);
    assert_eq!(bob["preferences"], json!([{ "default": ["sms"] }]));
    let carol = find(&list, "carol");
    assert_eq!(carol["revision"], 1);
    assert_eq!(carol["contact"], json!({}));
    assert_eq!(carol["preferences"], json!([]));

    // A stale expected_revision conflicts, with `error.current` = the view as it is now.
    let (status, err) = put(
        app.clone(),
        "/api/people/bob/preferences",
        &json!({ "preferences": [{ "default": ["email"] }], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{err}");
    assert_eq!(err["error"]["code"], "revision_conflict");
    assert_eq!(err["error"]["current"]["id"], "bob");
    assert_eq!(err["error"]["current"]["revision"], 2);
    assert_eq!(
        err["error"]["current"]["preferences"],
        json!([{ "default": ["sms"] }])
    );

    // Saving against the current revision moves it to 3.
    let (status, body) = put(
        app,
        "/api/people/bob/preferences",
        &json!({ "preferences": [{ "default": ["email"] }], "expected_revision": 2 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["revision"], 3);
    assert_eq!(body["preferences"], json!([{ "default": ["email"] }]));

    // One audit row per successful save, none for the conflict.
    let (count,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM audit_log \
         WHERE kind = 'person.preferences_updated' AND actor = 'user:bob' \
           AND alert_id IS NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("audit count");
    assert_eq!(count, 2);
}

#[sqlx::test(migrations = "../../migrations")]
async fn preferences_auth_shape_and_validation(pool: PgPool) {
    let state = api_state(&pool, fixture_dir());

    // bob may not save carol's preferences.
    let app = app_as(&state, Some("user:bob"), false);
    let (status, body) = put(
        app,
        "/api/people/carol/preferences",
        &json!({ "preferences": [{ "default": ["sms"] }], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["code"], "forbidden");

    // An admin may save anyone's.
    let app = app_as(&state, Some("user:bob"), true);
    let (status, body) = put(
        app.clone(),
        "/api/people/carol/preferences",
        &json!({ "preferences": [{ "default": ["phone"] }], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["revision"], 2);
    assert_eq!(body["preferences"], json!([{ "default": ["phone"] }]));
    assert_eq!(body["can_edit"], true, "the admin sees carol in full");

    // Unknown person: 404 (even for an admin).
    let (status, body) = put(
        app.clone(),
        "/api/people/ghost/preferences",
        &json!({ "preferences": [], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"]["code"], "not_found");

    // Validation failures are 422 `invalid`, mapped from the YAML validator: an
    // unknown channel, a bad `when` window, and preferences that are not a list.
    for bad in [
        json!([{ "match": {}, "channels": ["fax"] }]),
        json!([{ "match": {}, "channels": ["email"],
                 "when": { "between": "8am-9pm", "tz": "Europe/London" } }]),
        json!("not-a-list"),
    ] {
        let (status, body) = put(
            app.clone(),
            "/api/people/carol/preferences",
            &json!({ "preferences": bad, "expected_revision": 2 }),
            JSON,
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{bad}: {body}");
        assert_eq!(body["error"]["code"], "invalid", "{bad}: {body}");
        assert!(
            body["error"]["message"]
                .as_str()
                .is_some_and(|m| !m.is_empty()),
            "{bad}: {body}"
        );
    }

    // An expected revision past 1 with no override row is a conflict, not an insert.
    let app = app_as(&state, Some("user:bob"), false);
    let (status, body) = put(
        app.clone(),
        "/api/people/bob/preferences",
        &json!({ "preferences": [{ "default": ["sms"] }], "expected_revision": 7 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "revision_conflict");
    assert_eq!(body["error"]["current"]["revision"], 1, "no override yet");

    // Missing actor: 401.
    let app = app_as(&state, None, false);
    let (status, body) = put(
        app,
        "/api/people/bob/preferences",
        &json!({ "preferences": [], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(body["error"]["code"], "unauthorized");

    // Wrong content type: 415.
    let app = app_as(&state, Some("user:bob"), false);
    let (status, body) = put(
        app,
        "/api/people/bob/preferences",
        &json!({ "preferences": [], "expected_revision": 1 }),
        "text/plain",
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{body}");
    assert_eq!(body["error"]["code"], "unsupported_media_type");
}

// ---------------------------------------------------------------------------------------------
// PUT /api/teams/{teamId}
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn team_lead_saves_membership_and_defaults(pool: PgPool) {
    let state = api_state(&pool, fixture_dir());

    // A plain member may not save the team (no self-promotion).
    let app = app_as(&state, Some("user:anna"), false);
    let (status, body) = put(
        app,
        "/api/teams/rates",
        &json!({ "members": ["anna", "bob"], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["code"], "forbidden");

    // The lead saves members and defaults: revision 2, the untouched fields unchanged.
    let app = app_as(&state, Some("user:bob"), false);
    let (status, body) = put(
        app.clone(),
        "/api/teams/rates",
        &json!({
            "members": ["bob", "carol"],
            "default_channels": ["sms"],
            "expected_revision": 1
        }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["id"], "rates");
    assert_eq!(body["revision"], 2);
    assert_eq!(body["members"], json!(["bob", "carol"]));
    assert_eq!(body["default_channels"], json!(["sms"]));
    assert_eq!(body["lead"], "bob");
    assert_eq!(body["oncall"], json!({ "rates-primary": ["carol", "bob"] }));
    assert_eq!(body["can_edit"], true);

    // GET /api/teams reflects the save; the untouched team stays at revision 1.
    let (status, list) = get(app, "/api/teams").await;
    assert_eq!(status, StatusCode::OK);
    let rates = find(&list, "rates");
    assert_eq!(rates["revision"], 2);
    assert_eq!(rates["members"], json!(["bob", "carol"]));
    assert_eq!(rates["default_channels"], json!(["sms"]));
    assert_eq!(find(&list, "payments")["revision"], 1);

    // One audit row for the save.
    let (count,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM audit_log \
         WHERE kind = 'team.updated' AND actor = 'user:bob' AND alert_id IS NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("audit count");
    assert_eq!(count, 1);

    // An admin saves with only expected_revision: omitted fields keep their values.
    let app = app_as(&state, Some("user:carol"), true);
    let (status, body) = put(
        app,
        "/api/teams/rates",
        &json!({ "expected_revision": 2 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["revision"], 3);
    assert_eq!(body["members"], json!(["bob", "carol"]));
    assert_eq!(body["default_channels"], json!(["sms"]));
    assert_eq!(body["lead"], "bob");
    assert_eq!(body["oncall"], json!({ "rates-primary": ["carol", "bob"] }));
}

#[sqlx::test(migrations = "../../migrations")]
async fn team_validation_conflicts_and_lead_clearing(pool: PgPool) {
    let state = api_state(&pool, fixture_dir());
    let app = app_as(&state, Some("user:bob"), false);

    // 422 `invalid` from the shared validator: unknown member, unknown channel,
    // non-string lead, unknown on-call person.
    for bad in [
        json!({ "members": ["bob", "ghost"], "expected_revision": 1 }),
        json!({ "default_channels": ["fax"], "expected_revision": 1 }),
        json!({ "lead": 3, "expected_revision": 1 }),
        json!({ "oncall": { "rates-backup": ["bob", "ghost"] }, "expected_revision": 1 }),
    ] {
        let (status, body) = put(app.clone(), "/api/teams/rates", &bad, JSON).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{bad}: {body}");
        assert_eq!(body["error"]["code"], "invalid", "{bad}: {body}");
    }

    // A save, then a replay of the same expected_revision: 409 with the current view.
    let (status, body) = put(
        app.clone(),
        "/api/teams/rates",
        &json!({ "members": ["bob", "anna"], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["revision"], 2);
    let (status, err) = put(
        app.clone(),
        "/api/teams/rates",
        &json!({ "members": ["bob"], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{err}");
    assert_eq!(err["error"]["code"], "revision_conflict");
    assert_eq!(err["error"]["current"]["id"], "rates");
    assert_eq!(err["error"]["current"]["revision"], 2);
    assert_eq!(err["error"]["current"]["members"], json!(["bob", "anna"]));

    // `lead: null` clears the lead; bob then loses the right to save his old team.
    let (status, body) = put(
        app.clone(),
        "/api/teams/rates",
        &json!({ "lead": null, "expected_revision": 2 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["revision"], 3);
    assert_eq!(body["lead"], Value::Null);
    assert_eq!(body["can_edit"], false, "bob is no longer the lead");

    let (status, body) = put(
        app.clone(),
        "/api/teams/rates",
        &json!({ "expected_revision": 3 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    // An admin can still save — and re-appoint the lead.
    let app = app_as(&state, Some("user:anna"), true);
    let (status, body) = put(
        app,
        "/api/teams/rates",
        &json!({ "lead": "bob", "expected_revision": 3 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["revision"], 4);
    assert_eq!(body["lead"], "bob");
}

#[sqlx::test(migrations = "../../migrations")]
async fn team_unknown_shape_and_auth_errors(pool: PgPool) {
    let state = api_state(&pool, fixture_dir());

    // Unknown team: 404, even for an admin.
    let app = app_as(&state, Some("user:bob"), true);
    let (status, body) = put(
        app.clone(),
        "/api/teams/ghost",
        &json!({ "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"]["code"], "not_found");

    // An expected revision past 1 with no override row is a conflict, not an insert
    // (payments has no lead, hence the admin actor).
    let (status, body) = put(
        app,
        "/api/teams/payments",
        &json!({ "members": ["carol"], "expected_revision": 5 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "revision_conflict");
    assert_eq!(body["error"]["current"]["revision"], 1, "no override yet");

    // Missing actor: 401.
    let app = app_as(&state, None, false);
    let (status, body) = put(
        app,
        "/api/teams/rates",
        &json!({ "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");

    // Wrong content type: 415.
    let app = app_as(&state, Some("user:bob"), false);
    let (status, body) = put(
        app.clone(),
        "/api/teams/rates",
        &json!({ "expected_revision": 1 }),
        "text/plain",
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{body}");
    assert_eq!(body["error"]["code"], "unsupported_media_type");

    // Missing or non-integer expected_revision: 400 bad_request.
    for bad in [
        json!({ "members": ["bob"] }),
        json!({ "expected_revision": "one" }),
    ] {
        let (status, body) = put(app.clone(), "/api/teams/rates", &bad, JSON).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}: {body}");
        assert_eq!(body["error"]["code"], "bad_request", "{bad}: {body}");
    }

    // members not a list of strings: 422.
    let (status, body) = put(
        app,
        "/api/teams/rates",
        &json!({ "members": "bob", "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

// ---------------------------------------------------------------------------------------------
// The effective directory the router / scheduler read
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn effective_directory_reflects_saves_and_the_stamp_cache_stays(pool: PgPool) {
    let state = api_state(&pool, fixture_dir());

    // bob saves his own preferences through the API.
    let app = app_as(&state, Some("user:bob"), false);
    let (status, body) = put(
        app,
        "/api/people/bob/preferences",
        &json!({ "preferences": [{ "default": ["sms"] }], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // The effective directory resolves bob to sms — exactly what the router's
    // `prefs::resolve_channels` call will plan for his next notification.
    let effective = EffectiveDirectory::new(Arc::new(fixture_dir()));
    let first = effective.get(&pool).await;
    let bob = first.people.get("bob").expect("bob is in the directory");
    let resolution = prefs::resolve_channels(bob, None, &routing_ctx(), chrono::Utc::now());
    assert_eq!(resolution.channels, ["sms"]);

    // Nothing changed: the same Arc comes back, no rebuild.
    let again = effective.get(&pool).await;
    assert!(Arc::ptr_eq(&first, &again), "the cache must not rebuild");
    assert_eq!(effective.rebuilds(), 1);

    // Another save moves the stamp: one rebuild picks it up.
    let app = app_as(&state, Some("user:bob"), false);
    let (status, body) = put(
        app,
        "/api/people/bob/preferences",
        &json!({ "preferences": [{ "default": ["email"] }], "expected_revision": 2 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let after = effective.get(&pool).await;
    assert!(
        !Arc::ptr_eq(&first, &after),
        "a save must rebuild the cache"
    );
    assert_eq!(effective.rebuilds(), 2);
    let bob = after.people.get("bob").expect("bob is in the directory");
    let resolution = prefs::resolve_channels(bob, None, &routing_ctx(), chrono::Utc::now());
    assert_eq!(resolution.channels, ["email"]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_saved_team_change_reaches_a_new_effective_view(pool: PgPool) {
    let state = api_state(&pool, fixture_dir());

    // bob (the lead) replaces the rates membership.
    let app = app_as(&state, Some("user:bob"), false);
    let (status, body) = put(
        app,
        "/api/teams/rates",
        &json!({ "members": ["bob", "carol"], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // The effective directory a router pass would read has the new membership (and
    // still everyone's base preferences — a team save does not touch them).
    let snap = EffectiveDirectory::new(Arc::new(fixture_dir()))
        .snapshot(&pool)
        .await;
    let (dir, ov) = (&snap.dir, &snap.overrides);
    let rates = dir.teams.get("rates").expect("rates is in the directory");
    assert_eq!(rates.members, ["bob", "carol"]);
    assert_eq!(rates.lead.as_deref(), Some("bob"));
    assert_eq!(rates.oncall["rates-primary"], ["carol", "bob"]);
    assert_eq!(rates.default_channels, Some(vec!["email".to_string()]));
    assert_eq!(overrides::revision_of(ov, overrides::KIND_TEAM, "rates"), 2);
    assert_eq!(overrides::revision_of(ov, overrides::KIND_PERSON, "bob"), 1);
    // The untouched team is unchanged.
    assert_eq!(dir.teams.get("payments").unwrap().members, ["carol"]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_stale_override_drops_alone_and_can_be_replaced(pool: PgPool) {
    // A team override naming a person no longer in the YAML (written straight to the
    // table, as if `ghost` was removed from config/people after the save), next to a
    // valid saved preference for bob.
    sqlx::query(
        "INSERT INTO directory_overrides (kind, id, body, revision, updated_by) VALUES \
         ('team', 'rates', $1, 3, 'user:bob'), ('person', 'bob', $2, 2, 'user:bob')",
    )
    .bind(json!({ "members": ["bob", "ghost"], "lead": "bob", "oncall": {}, "default_channels": ["email"] }))
    .bind(json!({ "preferences": [{ "default": ["sms"] }] }))
    .execute(&pool)
    .await
    .unwrap();

    // Only the stale team override is dropped: bob's saved preference still applies.
    let snap = EffectiveDirectory::new(Arc::new(fixture_dir()))
        .snapshot(&pool)
        .await;
    assert_eq!(snap.dir.teams["rates"].members, ["bob", "anna"]);
    let bob = snap.dir.people.get("bob").unwrap();
    let resolution = prefs::resolve_channels(bob, None, &routing_ctx(), chrono::Utc::now());
    assert_eq!(resolution.channels, ["sms"]);
    assert!(snap.applied.teams.is_empty());
    assert_eq!(
        overrides::revision_of(&snap.overrides, overrides::KIND_TEAM, "rates"),
        3
    );

    let state = api_state(&pool, fixture_dir());
    // Views carry the stored revision, so a client can replace the stale row.
    let (status, teams) = get(app_as(&state, Some("user:bob"), false), "/api/teams").await;
    assert_eq!(status, StatusCode::OK, "{teams}");
    assert_eq!(find(&teams, "rates")["revision"], 3);

    // A stale expected revision: 409 whose `current` is the database's revision.
    let (status, err) = put(
        app_as(&state, Some("user:bob"), false),
        "/api/teams/rates",
        &json!({ "members": ["bob"], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{err}");
    assert_eq!(err["error"]["current"]["revision"], 3);

    // Retrying at the current revision replaces the stale row.
    let (status, body) = put(
        app_as(&state, Some("user:bob"), false),
        "/api/teams/rates",
        &json!({ "members": ["bob"], "expected_revision": 3 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["revision"], 4);

    // Another person's save is never blocked by someone else's stale row.
    let (status, body) = put(
        app_as(&state, Some("user:carol"), false),
        "/api/people/carol/preferences",
        &json!({ "preferences": [{ "default": ["email"] }], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["revision"], 2);
}

#[sqlx::test(migrations = "../../migrations")]
async fn out_of_range_revisions_and_empty_defaults_are_rejected(pool: PgPool) {
    let state = api_state(&pool, fixture_dir());
    for expected in [json!(i64::MAX), json!(0), json!(-1)] {
        let (status, body) = put(
            app_as(&state, Some("user:bob"), false),
            "/api/people/bob/preferences",
            &json!({ "preferences": [], "expected_revision": expected }),
            JSON,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{expected}: {body}");
    }
    let (status, body) = put(
        app_as(&state, Some("user:bob"), false),
        "/api/teams/rates",
        &json!({ "default_channels": [], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

// ---------------------------------------------------------------------------------------------
// Off-hours digest preference (crate::offhours)
// ---------------------------------------------------------------------------------------------

#[sqlx::test(migrations = "../../migrations")]
async fn a_saved_digest_preference_validates_and_holds_on_the_next_pass(pool: PgPool) {
    let state = api_state(&pool, fixture_dir());

    // `digest` outside `otherwise` is rejected by the shared validator.
    let app = app_as(&state, Some("user:bob"), false);
    let (status, body) = put(
        app,
        "/api/people/bob/preferences",
        &json!({ "preferences": [{ "default": ["digest"] }], "expected_revision": 1 }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

    // In `otherwise` it saves.
    let app = app_as(&state, Some("user:bob"), false);
    let (status, body) = put(
        app,
        "/api/people/bob/preferences",
        &json!({
            "preferences": [{ "match": {}, "channels": ["email"],
                              "when": { "between": "08:00-20:00", "tz": "UTC" },
                              "otherwise": ["digest"] }],
            "expected_revision": 1
        }),
        JSON,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // The router's next pass (EffectiveDirectory, no restart) holds bob's 03:00 warning
    // and resolves his 10:00 one to email.
    let effective = EffectiveDirectory::new(Arc::new(fixture_dir()));
    let dir = effective.get(&pool).await;
    let bob = dir.people.get("bob").expect("bob");
    let night = "2026-01-15T03:00:00Z".parse().unwrap();
    let r = prefs::resolve_with_hold(bob, None, &routing_ctx(), night);
    assert_eq!(r.channels, [prefs::DIGEST]);
    assert_eq!(r.hold_until, Some("2026-01-15T08:00:00Z".parse().unwrap()));
    let day = "2026-01-15T10:00:00Z".parse().unwrap();
    let r = prefs::resolve_with_hold(bob, None, &routing_ctx(), day);
    assert_eq!(
        (r.channels, r.hold_until),
        (vec!["email".to_string()], None)
    );
}
