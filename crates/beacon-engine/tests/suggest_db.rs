//! Auto-suggested proposals against a real Postgres (needs DATABASE_URL).

use std::path::PathBuf;

use beacon_engine::config_store;
use beacon_engine::suggest::{SuggestConfig, suggest_once};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

fn config() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config")
}

/// `n` alerts of the rates rule: `acked` of them acked by a person, the rest untouched.
async fn alerts(pool: &PgPool, n: usize, acked: usize, same_key: bool) {
    for i in 0..n {
        let fp = if same_key { "fp-same".to_string() } else { format!("fp-{i}") };
        sqlx::query(
            "INSERT INTO alerts (id, rule_id, rule_revision, rule_name, team, fingerprint, dedup_window_ms, severity, color, title,
                                 first_seen, last_seen, source, schema, status, acked_by, dedup_active)
             VALUES ($1, 'rates-rejected-trades', 1, 'r', 'rates', $2, 600000, 'warning', 'amber', 't', now(), now(),
                     'http:t', 's', 'resolved', $3, false)",
        )
        .bind(Uuid::now_v7())
        .bind(fp)
        .bind((i < acked).then_some("user:erin"))
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn proposals(pool: &PgPool) -> Vec<(String, String, bool, String, Value)> {
    sqlx::query_as("SELECT rule_id, kind, auto, author, proposed_rule FROM proposals ORDER BY created_at")
        .fetch_all(pool)
        .await
        .unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn noisy_rule_gets_one_throttle_proposal_per_revision(pool: PgPool) {
    config_store::seed_from_yaml(&pool, &config()).await.unwrap();
    let cfg = SuggestConfig::default();
    alerts(&pool, 25, 1, false).await;

    assert_eq!(suggest_once(&pool, &cfg).await.unwrap(), 1);
    let ps = proposals(&pool).await;
    assert_eq!(ps.len(), 1);
    let (rule, kind, auto, author, proposed) = &ps[0];
    assert_eq!((rule.as_str(), kind.as_str(), *auto, author.as_str()), ("rates-rejected-trades", "noisy", true, "system:beacon"));
    assert_eq!(proposed["throttle"], json!({"max": 3, "per": "1h"}));
    // The proposed rule is the current one plus the throttle.
    let current = config_store::get(&pool, "rates-rejected-trades").await.unwrap().unwrap();
    let mut expected = current.spec.clone();
    expected["throttle"] = json!({"max": 3, "per": "1h"});
    assert_eq!(proposed, &expected);

    // No repeat while it's pending, nor after a rejection on the same revision.
    assert_eq!(suggest_once(&pool, &cfg).await.unwrap(), 0);
    sqlx::query("UPDATE proposals SET state = 'rejected'").execute(&pool).await.unwrap();
    assert_eq!(suggest_once(&pool, &cfg).await.unwrap(), 0);
}

#[sqlx::test(migrations = "../../migrations")]
async fn handled_rules_are_left_alone_and_flapping_counts(pool: PgPool) {
    config_store::seed_from_yaml(&pool, &config()).await.unwrap();
    let cfg = SuggestConfig::default();
    // People acked half of them: not noisy, and every key is distinct.
    alerts(&pool, 25, 12, false).await;
    assert_eq!(suggest_once(&pool, &cfg).await.unwrap(), 0);
    // One key reopening 5 times is flapping, however it was handled.
    alerts(&pool, 5, 5, true).await;
    assert_eq!(suggest_once(&pool, &cfg).await.unwrap(), 1);
}
