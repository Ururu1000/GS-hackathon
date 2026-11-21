//! `beacon-loadgen`: open-loop load generator for the ingest → alert pipeline.
//!
//! It sends Appendix A-shaped events at a fixed rate (batches are scheduled
//! on a clock, never "send when the last one finished", so a slow server
//! can't hide its latency), and measures **pipeline latency**: from the
//! *intended* send time of an event to the moment the alert carrying that
//! event id appears on `alerts.updates`.
//!
//! ```text
//! beacon-loadgen --rate 5000 --duration 30 --batch 100 --distinct-keys 2000 --match-ratio 0.8
//! ```
//!
//! Results (with every workload parameter) are printed and written to
//! `loadtest/results/<timestamp>.md`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use beacon_core::FeedAlert;
use beacon_ingest::kafka::{self, ALERTS_UPDATES};
use clap::Parser;
use hdrhistogram::Histogram;
use rand::Rng;
use rdkafka::Message;
use rdkafka::consumer::Consumer;
use serde_json::{Value, json};
use tokio::sync::Semaphore;

#[derive(Parser, Debug, Clone)]
#[command(about = "Open-loop load generator for Beacon")]
struct Args {
    /// Ingest base URL.
    #[arg(long, default_value = "http://127.0.0.1:8090")]
    url: String,
    /// Kafka bootstrap (to observe alerts.updates).
    #[arg(long, env = "BEACON_KAFKA", default_value = "127.0.0.1:19092")]
    kafka: String,
    /// Target events per second.
    #[arg(long, default_value_t = 2000)]
    rate: u64,
    /// Seconds to send for.
    #[arg(long, default_value_t = 20)]
    duration: u64,
    /// Events per HTTP request (always sent to `POST /ingest/{source}/batch`).
    #[arg(long, default_value_t = 100)]
    batch: usize,
    /// Distinct incident keys per event type: fewer keys = more duplicates.
    #[arg(long, default_value_t = 1000)]
    distinct_keys: u64,
    /// Fraction of events that match a rule (the rest are filtered out).
    #[arg(long, default_value_t = 0.8)]
    match_ratio: f64,
    /// Maximum concurrent HTTP requests.
    #[arg(long, default_value_t = 256)]
    concurrency: usize,
    /// Seconds to keep observing after sending stops (drain).
    #[arg(long, default_value_t = 10)]
    drain: u64,
    /// Scenario label for the report.
    #[arg(long, default_value = "a-baseline")]
    scenario: String,
    /// Write the markdown report here (directory).
    #[arg(long, default_value = "loadtest/results")]
    out: String,
}

/// Generate one event; `matching` false produces a near-miss that no rule matches.
fn make_event(rng: &mut impl Rng, run: &str, keys: u64, matching: bool) -> Value {
    // Keys are namespaced per run so alerts from earlier runs are never reused.
    let k = format!("{run}-{:06}", rng.random_range(0..keys));
    match rng.random_range(0..4) {
        0 => json!({
            "type": "WORKFLOW_FAILED",
            "workflowId": format!("WF-LG-{k}"),
            "workflowName": "order-settlement",
            "step": "validate-counterparty",
            "severity": "ERROR",
            "environment": if matching { "prod" } else { "staging" },
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "error": {"code": "TIMEOUT", "message": "Counterparty service did not respond in 5000 ms"}
        }),
        1 => json!({
            "meta": {"schema": "trade.rejected.v1", "producer": "trade-gateway", "ts": chrono::Utc::now().timestamp_millis()},
            "payload": {"tradeId": format!("T-LG-{k}"), "desk": if matching { "rates" } else { "fx" },
                        "notional": 25_000_000, "currency": "EUR", "reason": "LIMIT_BREACH"}
        }),
        2 => json!({
            "ts": chrono::Utc::now().to_rfc3339(), "level": if matching { "WARN" } else { "INFO" },
            "logger": "pricing-engine", "instrument": format!("INS-{k}"), "msg": "Quote latency over budget"
        }),
        _ => json!({
            "alertname": "HighErrorRate",
            "labels": {"service": "payments-api", "region": format!("region-{k}"), "severity": if matching { "critical" } else { "warning" }},
            "annotations": {"summary": "Error rate 12% over the last 5 minutes"},
            "startsAt": chrono::Utc::now().to_rfc3339(), "status": "firing"
        }),
    }
}

#[derive(Default)]
struct Counters {
    sent: AtomicU64,
    accepted: AtomicU64,
    rejected_429: AtomicU64,
    failed: AtomicU64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("warn").init();
    let args = Args::parse();
    let run_id = uuid::Uuid::now_v7().simple().to_string()[..12].to_string();

    // Intended send time per event id, for latency correlation.
    let sent_at: Arc<Mutex<HashMap<String, Instant>>> = Arc::new(Mutex::new(HashMap::new()));
    let latency = Arc::new(Mutex::new(Histogram::<u64>::new_with_bounds(1, 120_000_000, 3)?));
    let first_alert = Arc::new(Mutex::new(Histogram::<u64>::new_with_bounds(1, 120_000_000, 3)?));
    let updates_seen = Arc::new(AtomicU64::new(0));
    let alerts_seen = Arc::new(Mutex::new(std::collections::HashSet::new()));

    // Observer: alerts.updates from now on, in a fresh consumer group.
    let consumer = kafka::consumer(&args.kafka, &format!("loadgen-{run_id}"), &[ALERTS_UPDATES], false)?;
    // Wait for partition assignment so we don't miss early updates.
    let t = Instant::now();
    while consumer.assignment().map(|a| a.count()).unwrap_or(0) == 0 && t.elapsed() < Duration::from_secs(15) {
        let _ = tokio::time::timeout(Duration::from_millis(200), consumer.recv()).await;
    }
    let observer = {
        let (sent_at, latency, first_alert, updates_seen, alerts_seen) =
            (sent_at.clone(), latency.clone(), first_alert.clone(), updates_seen.clone(), alerts_seen.clone());
        tokio::spawn(async move {
            loop {
                let Ok(m) = consumer.recv().await else { continue };
                let Some(Ok(a)) = m.payload().map(serde_json::from_slice::<FeedAlert>) else { continue };
                let now = Instant::now();
                updates_seen.fetch_add(1, Ordering::Relaxed);
                // Only alerts carrying an event from this run are sampled, so
                // unrelated traffic on the shared feed is ignored.
                let Some(eid) = a.last_event_id.as_deref() else { continue };
                let Some(t0) = sent_at.lock().unwrap().get(eid).copied() else { continue };
                let us = now.duration_since(t0).as_micros() as u64;
                let _ = latency.lock().unwrap().record(us.max(1));
                if alerts_seen.lock().unwrap().insert(a.id) {
                    // Opening latency on the server clock: ingest receipt (first_seen)
                    // to the first update observed for this alert. Unaffected by the
                    // relay coalescing the opening with later duplicates.
                    let open_us = (chrono::Utc::now() - a.first_seen).num_microseconds().unwrap_or(0);
                    let _ = first_alert.lock().unwrap().record(open_us.max(1) as u64);
                }
            }
        })
    };

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(args.concurrency)
        .timeout(Duration::from_secs(30))
        .build()?;

    // Warm-up: one matching probe must travel the whole pipeline before the
    // clock starts, so consumer-group joins don't count as latency.
    let probe_key = format!("{run_id}-warmup");
    let probe_id = format!("http:lg:{probe_key}");
    let probe = json!({"type": "WORKFLOW_FAILED", "workflowId": format!("WF-WARMUP-{run_id}"),
        "workflowName": "order-settlement", "step": "warmup", "severity": "ERROR", "environment": "prod",
        "timestamp": chrono::Utc::now().to_rfc3339(), "error": {"code": "TIMEOUT", "message": "warm-up probe"}});
    sent_at.lock().unwrap().insert(probe_id.clone(), Instant::now());
    let warm_start = Instant::now();
    client
        .post(format!("{}/ingest/lg", args.url))
        .header("Idempotency-Key", &probe_key)
        .json(&probe)
        .send()
        .await
        .context("sending warm-up probe")?
        .error_for_status()
        .context("warm-up probe rejected")?;
    while updates_seen.load(Ordering::Relaxed) == 0 {
        if warm_start.elapsed() > Duration::from_secs(90) {
            anyhow::bail!("pipeline did not deliver the warm-up probe within 90 s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    println!("pipeline warm after {:.1}s", warm_start.elapsed().as_secs_f64());
    sent_at.lock().unwrap().remove(&probe_id);
    *latency.lock().unwrap() = Histogram::<u64>::new_with_bounds(1, 120_000_000, 3)?;
    *first_alert.lock().unwrap() = Histogram::<u64>::new_with_bounds(1, 120_000_000, 3)?;
    updates_seen.store(0, Ordering::Relaxed);
    alerts_seen.lock().unwrap().clear();
    let counters = Arc::new(Counters::default());
    let sem = Arc::new(Semaphore::new(args.concurrency));
    let late = Arc::new(AtomicU64::new(0));

    let batches_per_sec = (args.rate as f64 / args.batch as f64).max(0.001);
    let period = Duration::from_secs_f64(1.0 / batches_per_sec);
    let total_batches = (batches_per_sec * args.duration as f64).round() as u64;
    let start = Instant::now();
    println!(
        "loadgen {run_id}: {} ev/s for {}s in batches of {} ({total_batches} requests), keys={}/type/run, match={}",
        args.rate, args.duration, args.batch, args.distinct_keys, args.match_ratio
    );

    let mut rng = rand::rng();
    let mut handles = Vec::with_capacity(total_batches as usize);
    for b in 0..total_batches {
        let intended = start + period.mul_f64(b as f64);
        tokio::time::sleep_until(intended.into()).await;
        let events: Vec<Value> = (0..args.batch)
            .map(|_| {
                let matching = rng.random_bool(args.match_ratio.clamp(0.0, 1.0));
                make_event(&mut rng, &run_id, args.distinct_keys, matching)
            })
            .collect();
        let key = format!("{run_id}-{b}");
        // Open loop: never wait for a slot. Over the concurrency budget the
        // batch is shed on the client side and reported, and the clock keeps going.
        let Ok(permit) = sem.clone().try_acquire_owned() else {
            late.fetch_add(events.len() as u64, Ordering::Relaxed);
            continue;
        };
        {
            let mut map = sent_at.lock().unwrap();
            for i in 0..events.len() {
                map.insert(format!("http:lg:{key}-{i}"), intended);
            }
        }
        let (client, url, counters) = (client.clone(), args.url.clone(), counters.clone());
        let n = events.len() as u64;
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            counters.sent.fetch_add(n, Ordering::Relaxed);
            let res = client
                .post(format!("{url}/ingest/lg/batch"))
                .header("Idempotency-Key", key)
                .json(&events)
                .send()
                .await;
            match res.map(|r| r.status().as_u16()) {
                Ok(202) => counters.accepted.fetch_add(n, Ordering::Relaxed),
                Ok(429) => counters.rejected_429.fetch_add(n, Ordering::Relaxed),
                _ => counters.failed.fetch_add(n, Ordering::Relaxed),
            };
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let send_elapsed = start.elapsed();

    // Drain: wait until updates stop arriving (or the drain timeout).
    let mut last = updates_seen.load(Ordering::Relaxed);
    let drain_start = Instant::now();
    let mut quiet = 0;
    while drain_start.elapsed() < Duration::from_secs(args.drain) {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let now = updates_seen.load(Ordering::Relaxed);
        quiet = if now == last { quiet + 1 } else { 0 };
        last = now;
        if quiet >= 4 {
            break;
        }
    }
    let drain_elapsed = drain_start.elapsed();
    observer.abort();

    let accepted = counters.accepted.load(Ordering::Relaxed);
    let report = Report {
        args: args.clone(),
        run_id,
        sent: counters.sent.load(Ordering::Relaxed),
        accepted,
        rejected_429: counters.rejected_429.load(Ordering::Relaxed),
        failed: counters.failed.load(Ordering::Relaxed),
        late_batches: late.load(Ordering::Relaxed),
        send_secs: send_elapsed.as_secs_f64(),
        drain_secs: drain_elapsed.as_secs_f64(),
        updates: updates_seen.load(Ordering::Relaxed),
        alerts: alerts_seen.lock().unwrap().len(),
        latency: latency.lock().unwrap().clone(),
        first_alert: first_alert.lock().unwrap().clone(),
    };
    let md = report.markdown();
    println!("\n{md}");
    std::fs::create_dir_all(&args.out)?;
    let path = format!(
        "{}/{}-{}.md",
        args.out,
        chrono::Utc::now().format("%Y%m%dT%H%M%SZ"),
        args.scenario
    );
    std::fs::write(&path, md).with_context(|| format!("writing {path}"))?;
    println!("report written to {path}");
    Ok(())
}

struct Report {
    args: Args,
    run_id: String,
    sent: u64,
    accepted: u64,
    rejected_429: u64,
    failed: u64,
    late_batches: u64,
    send_secs: f64,
    drain_secs: f64,
    updates: u64,
    alerts: usize,
    latency: Histogram<u64>,
    first_alert: Histogram<u64>,
}

fn ms(h: &Histogram<u64>, q: f64) -> String {
    if h.is_empty() {
        "n/a".into()
    } else {
        format!("{:.1}", h.value_at_quantile(q) as f64 / 1000.0)
    }
}

impl Report {
    fn markdown(&self) -> String {
        let a = &self.args;
        let host = std::process::Command::new("sysctl")
            .args(["-n", "machdep.cpu.brand_string"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| std::env::consts::ARCH.to_string());
        let row = |name: &str, h: &Histogram<u64>| {
            format!(
                "| {name} | {} | {} | {} | {} | {} |\n",
                h.len(),
                ms(h, 0.50),
                ms(h, 0.90),
                ms(h, 0.99),
                if h.is_empty() { "n/a".into() } else { format!("{:.1}", h.max() as f64 / 1000.0) }
            )
        };
        let mut s = String::new();
        s.push_str(&format!("# Load test `{}` ({})\n\n", a.scenario, self.run_id));
        s.push_str("## Workload\n\n| Parameter | Value |\n|---|---|\n");
        s.push_str(&format!("| Target rate | {} events/s |\n| Duration | {} s |\n| Events per request | {} |\n", a.rate, a.duration, a.batch));
        s.push_str(&format!("| Distinct keys per event type | {} |\n| Match ratio | {} |\n| Max concurrent requests | {} |\n", a.distinct_keys, a.match_ratio, a.concurrency));
        s.push_str(&format!("| Host | {host} |\n| Ingest URL | {} |\n\n", a.url));
        s.push_str("## Throughput\n\n| Metric | Value |\n|---|---|\n");
        s.push_str(&format!(
            "| Sent | {} |\n| Accepted (202) | {} |\n| Rejected (429) | {} |\n| Failed | {} |\n| Accepted rate | {:.0} events/s |\n| Shed by client (over concurrency budget) | {} events |\n| Alert updates observed | {} |\n| Distinct alerts observed | {} |\n| Drain after send stopped | {:.1} s |\n\n",
            self.sent,
            self.accepted,
            self.rejected_429,
            self.failed,
            self.accepted as f64 / self.send_secs.max(0.001),
            self.late_batches,
            self.updates,
            self.alerts,
            self.drain_secs
        ));
        s.push_str("## Latency (ms), intended send → alert on `alerts.updates`\n\n");
        s.push_str("| Measure | Samples | p50 | p90 | p99 | max |\n|---|---|---|---|---|---|\n");
        s.push_str(&row("Any update (count bump or new alert)", &self.latency));
        s.push_str(&row("First alert: ingest receipt → first update (server clock)", &self.first_alert));
        s.push_str("\nSamples are the events whose id is the alert's `last_event_id` when an update is published; duplicates folded into the same batch are not individually timed.\n");
        s
    }
}
