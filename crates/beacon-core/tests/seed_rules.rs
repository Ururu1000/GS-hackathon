//! Acceptance tests: the shipped config/ against the Appendix A samples.

use std::path::PathBuf;

use beacon_core::{ConfigSnapshot, Event, MatchKind, load_config_dir};
use serde_json::{Value, json};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn snapshot() -> ConfigSnapshot {
    load_config_dir(&root().join("config")).expect("seed config must compile")
}

fn sample(name: &str) -> Value {
    let p = root().join("samples/events").join(name);
    serde_json::from_str(&std::fs::read_to_string(p).unwrap()).unwrap()
}

fn run(cfg: &ConfigSnapshot, source: &str, raw: Value) -> (Event, Vec<beacon_core::MatchRecord>) {
    let (e, m, _) = cfg.process(Event::new("evt-1".into(), source.into(), raw));
    (e, m)
}

#[test]
fn settlements_workflow_failure() {
    let cfg = snapshot();
    let (e, m) = run(&cfg, "kafka:workflows", sample("workflow-failed.json"));
    assert_eq!(e.schema, "workflow.failed");
    assert_eq!(m.len(), 1, "{m:?}");
    let m = &m[0];
    assert_eq!(m.rule_id, "settlements-workflow-failed");
    assert_eq!(m.kind, MatchKind::Fire);
    assert_eq!(m.title, "Settlement workflow WF-20251023-0042 failed at validate-counterparty");
    assert_eq!(m.body, "TIMEOUT: Counterparty service did not respond in 5000 ms");
    assert_eq!(m.color, "red");
    assert_eq!(m.severity, "critical");
    assert_eq!(m.route[0].to, "oncall:settlements-primary");
    assert_eq!(m.fields[0].label, "Step");
}

#[test]
fn settlements_ignores_staging() {
    let cfg = snapshot();
    let mut raw = sample("workflow-failed.json");
    raw["environment"] = json!("staging");
    assert!(run(&cfg, "kafka:workflows", raw).1.is_empty());
}

#[test]
fn rates_threshold_is_strict() {
    let cfg = snapshot();
    let (e, m) = run(&cfg, "kafka:trades", sample("trade-rejected.json"));
    assert_eq!(e.schema, "envelope.meta-payload");
    assert_eq!(e.fields["type"], json!("trade.rejected.v1"));
    assert_eq!(e.fields["occurred_at"], json!("2025-10-23T09:14:02.000Z"));
    assert_eq!(m.len(), 1);
    assert_eq!(m[0].title, "Rejected EUR 25,000,000 trade T-99877");

    let mut at = sample("trade-rejected.json");
    at["payload"]["notional"] = json!(10_000_000);
    assert!(run(&cfg, "kafka:trades", at).1.is_empty(), "equality at the threshold must not match");

    let mut fx = sample("trade-rejected.json");
    fx["payload"]["desk"] = json!("fx");
    assert!(run(&cfg, "kafka:trades", fx).1.is_empty());
}

#[test]
fn anna_fires_and_recovers_on_same_fingerprint() {
    let cfg = snapshot();
    let (e, fire) = run(&cfg, "http:alertmanager", sample("high-error-rate.json"));
    assert_eq!(e.schema, "prometheus.alert");
    assert_eq!(fire.len(), 1);
    assert_eq!(fire[0].kind, MatchKind::Fire);
    assert_eq!(fire[0].route[0].to, "user:anna");
    assert_eq!(fire[0].title, "HighErrorRate on payments-api (eu-west)");
    assert_eq!(fire[0].context.service.as_deref(), Some("payments-api"));
    assert_eq!(fire[0].context.event_type.as_deref(), Some("HighErrorRate"));
    assert_eq!(fire[0].context.schema.as_deref(), Some("prometheus.alert"));
    // Rules with a runbook pin the opening event for templates.
    let ev = fire[0].event.as_ref().expect("anna's rule has a runbook");
    assert_eq!(ev.raw["labels"]["region"], json!("eu-west"));
    assert_eq!(ev.fields["service"], json!("payments-api"));
    // Team runbooks can match the event even without an explicit rule binding.
    let (_, p) = run(&cfg, "http:logs", sample("pricing-log-warn.json"));
    let ev = p[0].event.as_ref().expect("pricing carries context for team runbooks");
    assert_eq!(ev.raw["instrument"], json!("EURUSD"));
    assert_eq!(ev.fields["severity"], json!("warning"));

    let (_, rec) = run(&cfg, "http:alertmanager", sample("high-error-rate-resolved.json"));
    assert_eq!(rec.len(), 1, "{rec:?}");
    assert_eq!(rec[0].kind, MatchKind::Recover);
    assert_eq!(rec[0].fingerprint, fire[0].fingerprint);
    assert!(rec[0].event.is_none());
}

#[test]
fn pricing_warn_and_above_by_instrument() {
    let cfg = snapshot();
    let (e, m) = run(&cfg, "http:logs", sample("pricing-log-warn.json"));
    assert_eq!(e.schema, "log.line");
    assert_eq!(e.fields["severity"], json!("warning"));
    assert_eq!(m.len(), 1);
    assert_eq!(m[0].rule_id, "pricing-engine-warnings");

    let mut info = sample("pricing-log-warn.json");
    info["level"] = json!("INFO");
    assert!(run(&cfg, "http:logs", info).1.is_empty());

    let mut other = sample("pricing-log-warn.json");
    other["instrument"] = json!("GBPUSD");
    let (_, m2) = run(&cfg, "http:logs", other);
    assert_ne!(m2[0].fingerprint, m[0].fingerprint, "one alert per instrument");

    // Grouping: same instrument in the same UTC 5-minute bucket = same digest;
    // the next bucket starts a new one. The card shows the window.
    use chrono::TimeZone;
    let at = |min: u32, sec: u32| {
        let mut e = Event::new(format!("p{min}{sec}"), "http:logs".into(), sample("pricing-log-warn.json"));
        e.ingested_at = chrono::Utc.with_ymd_and_hms(2026, 11, 21, 9, min, sec).unwrap();
        cfg.process(e).1.remove(0)
    };
    let (a, b, c) = (at(15, 1), at(19, 59), at(20, 0));
    assert_eq!(a.fingerprint, b.fingerprint);
    assert_ne!(b.fingerprint, c.fingerprint);
    assert_eq!(a.dedup_window_ms, 300_000);
    let w = a.fields.iter().find(|f| f.label == "Window").expect("window field");
    assert_eq!(w.value, json!("09:15–09:20 UTC"));
    assert_eq!(a.quiet_for_ms, Some(600_000));
}

#[test]
fn duplicates_share_a_fingerprint() {
    let cfg = snapshot();
    let a = run(&cfg, "kafka:workflows", sample("workflow-failed.json")).1;
    let (_, b, _) = cfg.process(Event::new("evt-2".into(), "kafka:workflows".into(), sample("workflow-failed.json")));
    assert_eq!(a[0].fingerprint, b[0].fingerprint);
    assert_ne!(a[0].match_id, b[0].match_id);
}

#[test]
fn unknown_shape_is_accepted_and_matchable_on_raw() {
    let cfg = snapshot();
    let (e, m) = run(&cfg, "http:judges", sample("unknown-shape.json"));
    assert!(e.schema.starts_with("unknown:"), "{}", e.schema);
    assert!(m.is_empty());

    // A rule written against raw.* works without any mapping.
    let rule: beacon_core::rule::RuleSpec = serde_saphyr::from_str(
        r#"
id: disk-full
team: infra
when: { all: [ { path: raw.event_kind, op: eq, value: disk_full }, { path: raw.used_pct, op: gte, value: 95 } ] }
present: { title: "Disk {{ raw.mount }} on {{ raw.host }} at {{ raw.used_pct }}%" }
"#,
    )
    .unwrap();
    let cfg = ConfigSnapshot::build(2, vec![], vec![rule]).unwrap();
    let (_, m) = run(&cfg, "http:judges", sample("unknown-shape.json"));
    assert_eq!(m.len(), 1);
    assert_eq!(m[0].title, "Disk /var/data on batch-07 at 97.5%");
}

#[test]
fn index_never_changes_results() {
    // Every rule against every sample, with and without the index, must agree.
    let cfg = snapshot();
    for name in [
        "workflow-failed.json",
        "high-error-rate.json",
        "high-error-rate-resolved.json",
        "trade-rejected.json",
        "pricing-log-warn.json",
        "unknown-shape.json",
    ] {
        let mut e = Event::new("x".into(), "s".into(), sample(name));
        cfg.normalize(&mut e);
        let indexed: Vec<_> = cfg.match_event(&e).into_iter().map(|m| (m.rule_id, m.kind)).collect();
        let mut exhaustive = Vec::new();
        for r in &cfg.rules {
            let rec = r.recover_when.as_ref().is_some_and(|c| c.eval(&e));
            if rec {
                exhaustive.push((r.spec.id.clone(), MatchKind::Recover));
            }
        }
        for r in &cfg.rules {
            let rec = r.recover_when.as_ref().is_some_and(|c| c.eval(&e));
            if !rec && r.when.eval(&e) {
                exhaustive.push((r.spec.id.clone(), MatchKind::Fire));
            }
        }
        assert_eq!(indexed, exhaustive, "{name}");
    }
}

#[test]
fn matches_carry_the_rules_delivery_policy() {
    use beacon_core::model::{NotifyPolicy, NotifyThrottle};
    let cfg = snapshot();
    let (_, m) = run(&cfg, "kafka:workflows", sample("workflow-failed.json"));
    assert_eq!(
        m[0].policy,
        NotifyPolicy { escalate_after_ms: Some(600_000), throttle: None, runbook: Some("settlement-timeout".into()) }
    );

    // Throttle: duration strings and plain seconds both normalise to ms; junk is dropped.
    assert_eq!(NotifyThrottle::from_rule_value(&json!({"max": 20, "per": "1m"})), Some(NotifyThrottle { max: 20, per_ms: 60_000 }));
    assert_eq!(NotifyThrottle::from_rule_value(&json!({"max": 3, "per": 90})), Some(NotifyThrottle { max: 3, per_ms: 90_000 }));
    assert_eq!(NotifyThrottle::from_rule_value(&json!({"max": 0, "per": "1m"})), None);
    assert_eq!(NotifyThrottle::from_rule_value(&json!({"max": 2, "per": "soon"})), None);
}

#[test]
fn notify_payload_puts_policy_keys_at_the_top_level() {
    use beacon_core::model::{NotifyPayload, NotifyPolicy, NotifyReason, NotifyThrottle};
    let p = NotifyPayload {
        alert_id: uuid::Uuid::nil(),
        notify_seq: 1,
        rule_id: "r".into(),
        rule_revision: 2,
        team: "t".into(),
        severity: "critical".into(),
        reason: NotifyReason::New,
        route: vec![],
        created_at: chrono::Utc::now(),
        context: Default::default(),
        event: None,
        policy: NotifyPolicy { escalate_after_ms: Some(5000), throttle: Some(NotifyThrottle { max: 2, per_ms: 1000 }), runbook: Some("rb".into()) },
    };
    let v = serde_json::to_value(&p).unwrap();
    // The keys delivery's RulePolicies::for_payload reads.
    assert_eq!(v["escalate_after_ms"], json!(5000));
    assert_eq!(v["throttle"], json!({"max": 2, "per_ms": 1000}));
    assert_eq!(v["runbook"], json!("rb"));
    let back: NotifyPayload = serde_json::from_value(v).unwrap();
    assert_eq!(back.policy, p.policy);
    // A rule without them adds no keys, so delivery keeps its rule-file values.
    let none = serde_json::to_value(NotifyPayload { policy: NotifyPolicy::default(), ..p }).unwrap();
    assert!(none.get("throttle").is_none() && none.get("runbook").is_none() && none.get("escalate_after_ms").is_none());
}
