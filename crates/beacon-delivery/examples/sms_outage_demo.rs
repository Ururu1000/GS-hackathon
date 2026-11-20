//! SMS outage demo (SPEC §9 scenario c): a scripted run against the full stack.
//!
//! Starts a simulated `sms` outage at the fake gateway, posts `--events N` copies of
//! `samples/events/workflow-failed.json` through the ingest webhook (each with a unique
//! `workflowId`, so the rule `settlements-workflow-failed` routes every copy to
//! `oncall:settlements` -> bob on `phone`+`sms`), then watches the delivery jobs drain and
//! reconciles them against the gateway's persisted receipts.
//!
//! The stack must already be running:
//! `beacon --role ingest,matcher,aggregator,relay,router,delivery,gateway`
//! (plus Kafka and Postgres). This demo never wipes anything; its alerts are identified by
//! the random run tag embedded in every `workflowId`.
//!
//! Environment:
//! - `DATABASE_URL`     required, e.g. `postgres://beacon:beacon@localhost:5432/beacon`
//! - `INGEST_URL`       default `http://localhost:8090`
//! - `GATEWAY_URL`      default `http://localhost:8070`
//! - `GATEWAY_TOKEN`    optional; sent as `Authorization: Bearer` on the outage POST when the
//!   gateway runs authenticated
//!
//! Usage: `cargo run -p beacon-delivery --example sms_outage_demo -- [--events N] [--outage-secs S]`
//! (or `scripts/demo_sms_outage.sh`). Exits 0 only if every expected sms job was found and
//! sent, nothing died, and every job reconciles against exactly one gateway receipt.

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("sms_outage_demo: {err}");
        std::process::exit(2);
    }
}

async fn run() -> Result<(), String> {
    // ---- environment and arguments -------------------------------------------------------------
    let db_url = std::env::var("DATABASE_URL").map_err(|_| {
        "DATABASE_URL is not set (e.g. postgres://beacon:beacon@localhost:5432/beacon)".to_string()
    })?;
    let ingest_url =
        std::env::var("INGEST_URL").unwrap_or_else(|_| "http://localhost:8090".to_string());
    let gateway_url =
        std::env::var("GATEWAY_URL").unwrap_or_else(|_| "http://localhost:8070".to_string());
    // When the gateway runs authenticated, its outage control API needs the same token.
    let gateway_token = std::env::var("GATEWAY_TOKEN").ok();

    let mut events: usize = 50;
    let mut outage_secs: u64 = 60;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let (key, inline): (String, Option<String>) = match arg.split_once('=') {
            Some((k, v)) => (k.to_string(), Some(v.to_string())),
            None => (arg, None),
        };
        if key == "--help" || key == "-h" {
            println!("sms_outage_demo: SPEC §9 scenario c against the full stack.");
            println!();
            println!("usage: sms_outage_demo [--events N] [--outage-secs S]");
            println!();
            println!("env:   DATABASE_URL (required)");
            println!("       INGEST_URL   default http://localhost:8090");
            println!("       GATEWAY_URL  default http://localhost:8070");
            println!("       GATEWAY_TOKEN  optional bearer token for the gateway's outage API");
            std::process::exit(0);
        }
        let raw = match inline {
            Some(v) => v,
            None => args.next().ok_or_else(|| format!("{key} needs a value"))?,
        };
        match key.as_str() {
            "--events" => {
                events = raw
                    .parse()
                    .map_err(|_| format!("--events must be a positive number, got {raw:?}"))?;
            }
            "--outage-secs" => {
                outage_secs = raw
                    .parse()
                    .map_err(|_| format!("--outage-secs must be a positive number, got {raw:?}"))?;
            }
            other => {
                return Err(format!(
                    "unknown argument {other:?}; usage: sms_outage_demo [--events N] [--outage-secs S]"
                ));
            }
        }
    }

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db_url)
        .await
        .map_err(|e| format!("cannot connect to Postgres ({db_url}): {e}"))?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| format!("cannot build an http client: {e}"))?;

    // Short random run id: identifies this run's alerts by their title (workflow id).
    let tag = Uuid::new_v4().simple().to_string()[..6].to_string();
    let title_pattern = format!("%WF-{tag}-%");
    println!(
        "run {tag}: {events} events, sms outage {outage_secs} s, ingest {ingest_url}, gateway {gateway_url}"
    );

    // ---- 1. start the outage at the gateway -----------------------------------------------------
    let outage_start_utc = Utc::now();
    let outage_started = Instant::now();
    let outage_url = format!("{}/api/simulator/outage", gateway_url.trim_end_matches('/'));
    let mut outage_req = http
        .post(&outage_url)
        .json(&json!({ "channel": "sms", "seconds": outage_secs }));
    if let Some(token) = gateway_token.as_deref() {
        outage_req = outage_req.bearer_auth(token);
    }
    let resp = outage_req
        .send()
        .await
        .map_err(|e| format!("cannot reach the gateway at {gateway_url}: {e}"))?;
    if resp.status().as_u16() == 401 {
        return Err(
            "gateway refused the outage request: HTTP 401 unauthorized (is GATEWAY_TOKEN set \
             to the gateway's token?)"
                .to_string(),
        );
    }
    if !resp.status().is_success() {
        return Err(format!(
            "gateway refused the outage request: HTTP {} {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        ));
    }
    let outage_end_utc = outage_start_utc + chrono::Duration::seconds(outage_secs as i64);
    println!(
        "run {tag}: sms outage (mode down) started, ends at {} UTC",
        outage_end_utc.format("%H:%M:%S")
    );

    // ---- 2. post the events through the ingest webhook -----------------------------------------
    let sample_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("samples/events/workflow-failed.json");
    let sample_text = std::fs::read_to_string(&sample_path)
        .map_err(|e| format!("cannot read {}: {e}", sample_path.display()))?;
    let sample: Value = serde_json::from_str(&sample_text)
        .map_err(|e| format!("{} is not valid JSON: {e}", sample_path.display()))?;

    let webhook = format!("{}/ingest/webhook", ingest_url.trim_end_matches('/'));
    let mut posted = 0usize;
    for i in 0..events {
        let workflow_id = format!("WF-{tag}-{i}");
        let mut event = sample.clone();
        event["workflowId"] = Value::String(workflow_id.clone());
        let resp = http
            .post(&webhook)
            .header("Idempotency-Key", &workflow_id)
            .json(&event)
            .send()
            .await
            .map_err(|e| format!("cannot reach the ingest at {webhook}: {e}"))?;
        let status = resp.status();
        if status.as_u16() != 202 {
            return Err(format!(
                "ingest rejected event {workflow_id}: HTTP {status} {}",
                resp.text().await.unwrap_or_default()
            ));
        }
        posted += 1;
    }
    println!(
        "run {tag}: posted {posted}/{events} WORKFLOW_FAILED events \
         (rule settlements-workflow-failed -> oncall:settlements -> sms+phone for bob)"
    );

    // ---- 3. poll the database until every expected sms job is terminal -------------------------
    let deadline = Instant::now() + Duration::from_secs(outage_secs + 120);
    #[allow(unused_assignments)] // the last poll's values are kept for the FAIL report
    let mut summary = (0i64, 0i64, 0i64, 0i64, 0i64); // found, pending, in_flight, sent, dead
    loop {
        let row: (i64, i64, i64, i64, i64) = sqlx::query_as(
            "SELECT count(*), \
                    count(*) FILTER (WHERE j.status = 'pending'), \
                    count(*) FILTER (WHERE j.status = 'in_flight'), \
                    count(*) FILTER (WHERE j.status = 'sent'), \
                    count(*) FILTER (WHERE j.status = 'dead') \
             FROM delivery_jobs j \
             JOIN alerts a ON a.id = j.alert_id \
             WHERE a.title LIKE $1 AND j.channel = 'sms'",
        )
        .bind(&title_pattern)
        .fetch_one(&pool)
        .await
        .map_err(|e| format!("poll query failed: {e}"))?;
        summary = row;
        let breaker: Option<(String, i32)> = sqlx::query_as(
            "SELECT state, consecutive_failures FROM channel_breakers WHERE channel = 'sms'",
        )
        .fetch_optional(&pool)
        .await
        .map_err(|e| format!("breaker query failed: {e}"))?;
        let breaker_desc = match breaker {
            Some((state, failures)) => format!("{state} ({failures} consecutive failures)"),
            None => "closed (no row yet)".to_string(),
        };
        let (found, pending, in_flight, sent, dead) = row;
        let t = outage_started.elapsed().as_secs();
        println!(
            "[t+{t:>3}s] sms jobs: found={found}/{events} pending={pending} in_flight={in_flight} sent={sent} dead={dead} | breaker: {breaker_desc}"
        );
        if found == events as i64 && pending == 0 && in_flight == 0 {
            break;
        }
        if Instant::now() >= deadline {
            println!(
                "run {tag}: giving up after the outage + 120 s; not every expected sms job is terminal"
            );
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // ---- 4. final reconciliation ---------------------------------------------------------------
    let job_ids: Vec<Uuid> = sqlx::query_scalar(
        "SELECT j.id FROM delivery_jobs j \
         JOIN alerts a ON a.id = j.alert_id \
         WHERE a.title LIKE $1 AND j.channel = 'sms'",
    )
    .bind(&title_pattern)
    .fetch_all(&pool)
    .await
    .map_err(|e| format!("job id query failed: {e}"))?;
    let found = job_ids.len();

    let (sent, dead, max_attempts): (i64, i64, i32) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE status = 'sent'), \
                count(*) FILTER (WHERE status = 'dead'), \
                COALESCE(max(attempts), 0) \
         FROM delivery_jobs WHERE id = ANY($1)",
    )
    .bind(&job_ids)
    .fetch_one(&pool)
    .await
    .map_err(|e| format!("job stats query failed: {e}"))?;

    let fallbacks: i64 =
        sqlx::query_scalar("SELECT count(*) FROM delivery_jobs WHERE fallback_of = ANY($1)")
            .bind(&job_ids)
            .fetch_one(&pool)
            .await
            .map_err(|e| format!("fallback query failed: {e}"))?;

    let (receipt_rows, distinct_receipts, duplicates): (i64, i64, i64) = sqlx::query_as(
        "SELECT count(*), count(DISTINCT job_id), COALESCE(SUM(duplicates), 0) \
         FROM gateway_receipts WHERE job_id = ANY($1)",
    )
    .bind(&job_ids)
    .fetch_one(&pool)
    .await
    .map_err(|e| format!("receipt query failed: {e}"))?;

    let last_sent: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT max(sent_at) FROM delivery_jobs WHERE id = ANY($1)")
            .bind(&job_ids)
            .fetch_one(&pool)
            .await
            .map_err(|e| format!("sent_at query failed: {e}"))?;
    let drain_after_outage = last_sent.map(|t| {
        let delta = (t - outage_end_utc).num_milliseconds();
        if delta < 0 {
            "0.0 s (last send raced the outage end)".to_string()
        } else {
            format!("{:.1} s", delta as f64 / 1000.0)
        }
    });

    println!();
    println!("================ SMS outage demo (run {tag}) ================");
    println!("{:<32}{posted}", "events posted");
    println!("{:<32}{events}", "sms jobs expected");
    println!("{:<32}{found}", "sms jobs found");
    println!("{:<32}{sent}", "sent");
    println!("{:<32}{dead}", "dead");
    println!("{:<32}{fallbacks}", "fallbacks created");
    println!("{:<32}{receipt_rows}", "gateway receipt rows");
    println!("{:<32}{distinct_receipts}", "distinct gateway receipts");
    println!("{:<32}{duplicates}", "duplicates (SUM)");
    println!("{:<32}{max_attempts}", "max attempts");
    println!(
        "{:<32}{}",
        "outage ended -> last sms sent",
        drain_after_outage
            .as_deref()
            .unwrap_or("n/a (nothing sent)")
    );
    println!("==============================================================");

    let pass =
        found == events && sent == events as i64 && dead == 0 && distinct_receipts == events as i64;
    if pass {
        println!(
            "RESULT: PASS (found={found} sent={sent} dead={dead} distinct receipts={distinct_receipts}, duplicates={duplicates})"
        );
        Ok(())
    } else {
        println!(
            "RESULT: FAIL (found={found}/{} sent={sent}/{} dead={dead} distinct receipts={distinct_receipts}/{}; last poll {:?})",
            events, events, events, summary
        );
        std::process::exit(1);
    }
}
