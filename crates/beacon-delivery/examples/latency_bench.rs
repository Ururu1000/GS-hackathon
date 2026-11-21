//! Ingest → delivery latency benchmark (SPEC §9, M6): an open-loop run against the
//! full stack, in the shape of `sms_outage_demo.rs`.
//!
//! Posts unique copies of the Appendix A samples through the ingest webhook on a
//! fixed schedule — an open loop: the next send never waits for a response — and
//! measures, per event, from t0 = the client wall clock just before each POST:
//!
//! - **gateway acceptance**: `gateway_receipts.received_at − t0` for the phone and
//!   sms jobs of the event's alert (join `delivery_jobs.id = gateway_receipts.job_id`),
//! - **email**: the Mailpit message `Created` time − t0 (Mailpit API search by the
//!   run tag; falls back to `delivery_jobs.sent_at`),
//! - **the internal split**: `alerts.first_seen − t0` (ingest → alert) and
//!   `sent_at − first_seen` (alert → delivered).
//!
//! Workload: `samples/events/workflow-failed.json` (rule `settlements-workflow-failed`
//! → `oncall:settlements-primary` → phone + sms through the fake gateway) and
//! `samples/events/trade-rejected.json` (rule `rates-rejected-trades` → `team:rates`
//! → email through Mailpit; the rule has no throttle and dedups on `tradeId`, so a
//! unique tradeId makes every event a new alert). Every event embeds the run tag in
//! its id field (`workflowId` / `payload.tradeId`), so dedup never merges them; the
//! alerts are found by title, like the outage demo.
//!
//! The stack must already be running:
//! `beacon --role ingest,matcher,aggregator,relay,router,delivery,gateway`
//! (plus Postgres, Redpanda and Mailpit). Nothing is wiped; the run is identified by
//! its random tag.
//!
//! Environment:
//! - `DATABASE_URL`     required, e.g. `postgres://beacon:beacon@localhost:5432/beacon`
//! - `INGEST_URL`      default `http://localhost:8090`
//! - `GATEWAY_URL`     default `http://localhost:8070` — only the pre-flight read
//!   touches it; the receipts themselves are read from Postgres
//! - `MAILPIT_URL`     default `http://localhost:8025`
//! - `GATEWAY_TOKEN`   optional; sent as `Authorization: Bearer` on the gateway read
//!   when the gateway runs authenticated
//!
//! Usage: `cargo run --release -p beacon-delivery --example latency_bench -- [args]`
//! (or `scripts/demo_latency.sh`). Exits 0 only if every expected receipt arrived
//! and no ingest POST failed; 1 if something is missing or errored; 2 on a bad
//! argument or an unreachable stack.
//!
//! **Same-host assumption:** t0 is this client's clock, while gateway receipts are
//! timestamped by Postgres and email by Mailpit. The numbers are only valid when
//! the client, Postgres and Mailpit share a clock (same machine / NTP); the run
//! says so before it starts.

use std::collections::HashMap;
use std::process::Command;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc;
use uuid::Uuid;

const DEFAULT_EVENTS: u64 = 500;
const DEFAULT_RATE: f64 = 50.0;
const DEFAULT_WARMUP: u64 = 50;
/// Longest wait for the warmup events to drain before the measured stream starts.
const SETTLE_LIMIT: Duration = Duration::from_secs(60);
const DEFAULT_TIMEOUT: u64 = 120;
/// One run posts at most this many events: keeps the send loop and the t0 table
/// honest on a small machine.
const MAX_EVENTS: u64 = 1_000_000;

#[tokio::main]
async fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = match parse_args(&argv) {
        Ok(Some(args)) => args,
        Ok(None) => return usage(),
        Err(err) => {
            eprintln!("latency_bench: {err}");
            usage();
            std::process::exit(2);
        }
    };
    if let Err(err) = run(args).await {
        eprintln!("latency_bench: {err}");
        std::process::exit(2);
    }
}

async fn run(args: Args) -> Result<(), String> {
    let Args {
        events,
        rate,
        mix,
        warmup,
        timeout,
        out,
    } = args;
    let total = warmup + events;

    // ---- environment ------------------------------------------------------------------------
    let db_url = std::env::var("DATABASE_URL").map_err(|_| {
        "DATABASE_URL is not set (e.g. postgres://beacon:beacon@localhost:5432/beacon)".to_string()
    })?;
    let ingest_url =
        std::env::var("INGEST_URL").unwrap_or_else(|_| "http://localhost:8090".to_string());
    let gateway_url =
        std::env::var("GATEWAY_URL").unwrap_or_else(|_| "http://localhost:8070".to_string());
    let mailpit_url =
        std::env::var("MAILPIT_URL").unwrap_or_else(|_| "http://localhost:8025".to_string());
    // Only the pre-flight gateway read uses the token; receipts come from Postgres.
    let gateway_token = std::env::var("GATEWAY_TOKEN").ok();

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&db_url)
        .await
        .map_err(|e| format!("cannot connect to Postgres (DATABASE_URL): {e}"))?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| format!("cannot build an http client: {e}"))?;

    // ---- run identity and samples ------------------------------------------------------------
    // Short random run id: identifies this run's alerts by their title (event id).
    let tag = Uuid::new_v4().simple().to_string()[..6].to_string();
    let title_pattern = format!("%{tag}%");
    // Alerts created before the first POST cannot belong to this run; the 5 s slack
    // covers clock jitter between the client and the ingest process.
    let run_start = Utc::now() - chrono::Duration::seconds(5);

    let samples_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("samples/events");
    let workflow_sample = read_sample(&samples_dir.join("workflow-failed.json"))?;
    let trade_sample = read_sample(&samples_dir.join("trade-rejected.json"))?;
    let webhook = format!("{}/ingest/webhook", ingest_url.trim_end_matches('/'));

    println!(
        "run {tag}: posting {total} events ({warmup} warmup + {events} measured) at {} events/s, mix {}",
        fmt_rate(rate),
        mix.as_str()
    );
    println!("       ingest {ingest_url}, gateway {gateway_url}, mailpit {mailpit_url}");
    println!("NOTE: t0 is this client's clock; gateway receipts are timestamped by Postgres and");
    println!("      email by Mailpit. The numbers are only valid when the client, Postgres and");
    println!("      Mailpit share a clock (same machine / NTP).");

    // ---- pre-flight --------------------------------------------------------------------------
    // Any HTTP answer proves the ingest is up; only a connection error is fatal.
    if http.get(&webhook).send().await.is_err() {
        return Err(format!(
            "cannot reach the ingest at {webhook} — is the stack running?"
        ));
    }
    if mix.includes_sms() {
        let mut req = http.get(format!(
            "{}/api/simulator/outage",
            gateway_url.trim_end_matches('/')
        ));
        if let Some(token) = gateway_token.as_deref() {
            req = req.bearer_auth(token);
        }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => {}
            Ok(resp) if resp.status().as_u16() == 401 => println!(
                "warning: the gateway read answered 401 — set GATEWAY_TOKEN to the gateway's \
                 token (the receipts are read from Postgres either way)"
            ),
            Ok(resp) => println!(
                "warning: gateway pre-flight read answered HTTP {} (continuing; the receipts \
                 are read from Postgres)",
                resp.status()
            ),
            Err(e) => println!(
                "warning: cannot reach the gateway at {gateway_url}: {e} (continuing; sms \
                 jobs will fail if it is really down)"
            ),
        }
    }
    let mut mailpit_ok = false;
    if mix.includes_email() {
        match mailpit_search(&http, &mailpit_url, &tag).await {
            Ok(_) => mailpit_ok = true,
            Err(err) => println!(
                "warning: {err}; email latency will fall back to delivery_jobs.sent_at and \
                 Mailpit receipts cannot be verified"
            ),
        }
    }

    // ---- 1. open-loop send --------------------------------------------------------------------
    let period = Duration::from_secs_f64(1.0 / rate);
    let first_send_at = tokio::time::Instant::now() + Duration::from_millis(250);
    let (tx, mut rx) = mpsc::unbounded_channel::<SendRecord>();
    let started_at = Utc::now();
    let mut next = first_send_at;
    for i in 0..total {
        if i == warmup && warmup > 0 {
            // Let the warmup drain (Kafka consumer groups joined, caches and connections
            // warm) so the measured events see a steady pipeline, not the cold start.
            settle(&pool, &title_pattern, run_start, warmup).await?;
            next = tokio::time::Instant::now() + Duration::from_secs(1);
        }
        // Fixed schedule: sleep to the slot, then fire the POST without ever waiting
        // for its response before the next send (open loop).
        tokio::time::sleep_until(next).await;
        next += period;
        let kind = kind_at(mix, i);
        let id = event_id(kind, &tag, i);
        let body = event_body(kind, &tag, i, &workflow_sample, &trade_sample);
        let http = http.clone();
        let tx = tx.clone();
        let webhook = webhook.clone();
        tokio::spawn(async move {
            // t0: the client wall clock just before the POST goes out.
            let t0 = Utc::now();
            let attempt = http
                .post(&webhook)
                .header("Idempotency-Key", id)
                .json(&body)
                .send()
                .await;
            let record = match attempt {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        SendRecord {
                            index: i,
                            t0,
                            status: Some(status.as_u16()),
                            error: None,
                        }
                    } else {
                        let detail = resp.text().await.unwrap_or_default();
                        let detail = detail.chars().take(120).collect::<String>();
                        SendRecord {
                            index: i,
                            t0,
                            status: Some(status.as_u16()),
                            error: Some(format!("HTTP {status}: {detail}")),
                        }
                    }
                }
                Err(e) => SendRecord {
                    index: i,
                    t0,
                    status: None,
                    error: Some(format!("send failed: {e}")),
                },
            };
            let _ = tx.send(record);
        });
        if (i + 1) % 200 == 0 {
            println!("[send] {}/{total} scheduled", i + 1);
        }
    }
    drop(tx);

    // ---- 2. collect the responses --------------------------------------------------------------
    let mut records: Vec<SendRecord> = Vec::with_capacity(total as usize);
    while let Some(record) = rx.recv().await {
        records.push(record);
    }
    let mut t0: Vec<Option<DateTime<Utc>>> = vec![None; total as usize];
    for record in &records {
        t0[record.index as usize] = Some(record.t0);
    }
    let ingest_errors = records
        .iter()
        .filter(|r| r.index >= warmup && r.error.is_some())
        .count();
    let warmup_errors = records
        .iter()
        .filter(|r| r.index < warmup && r.error.is_some())
        .count();
    let error_notes: Vec<String> = records
        .iter()
        .filter(|r| r.error.is_some())
        .take(3)
        .map(|r| match r.status {
            Some(status) => format!(
                "event #{} ({}): HTTP {status} — {}",
                r.index,
                event_id(kind_at(mix, r.index), &tag, r.index),
                r.error.as_deref().unwrap_or_default()
            ),
            None => format!(
                "event #{} ({}): {}",
                r.index,
                event_id(kind_at(mix, r.index), &tag, r.index),
                r.error.as_deref().unwrap_or_default()
            ),
        })
        .collect();
    let ok_responses = records.len() - ingest_errors - warmup_errors;
    println!(
        "run {tag}: {ok_responses}/{total} posts answered 2xx ({} error(s) measured, {} warmup)",
        ingest_errors, warmup_errors
    );

    // ---- 3. wait for every expected receipt ----------------------------------------------------
    let wait_deadline = tokio::time::Instant::now() + Duration::from_secs(timeout);
    let wait_started = tokio::time::Instant::now();
    let measured = (warmup..total).count();
    let mut mail: HashMap<Uuid, DateTime<Utc>> = HashMap::new();
    let mut mailpit_search_errors = 0usize;
    // The last poll's fold is kept for the report after the loop breaks.
    #[allow(unused_assignments)] // the first poll's value is never read
    let mut stats = RunStats::default();
    loop {
        let snap = snapshot(&pool, &tag, mix, total, run_start, &title_pattern).await?;
        if mix.includes_email() && mailpit_ok {
            match mailpit_search(&http, &mailpit_url, &tag).await {
                Ok((found, _)) => mail = found,
                Err(err) => {
                    mailpit_search_errors += 1;
                    if mailpit_search_errors == 1 {
                        println!(
                            "warning: Mailpit search failed mid-run ({err}); keeping the last snapshot"
                        );
                    }
                }
            }
        }
        stats = evaluate(warmup, total, &t0, &snap, &mail, mailpit_ok);

        let t = wait_started.elapsed().as_secs();
        let mailpit_disp = if mix.includes_email() {
            format!(
                "{}/{}",
                stats.email.len(),
                stats.email.len() + stats.missing_mailpit
            )
        } else {
            "n/a".to_string()
        };
        println!(
            "[wait t+{t:>3}s] alerts {}/{} · gateway {}/{} · mailpit {mailpit_disp} · pending {} · dead {}",
            measured - stats.missing_alerts,
            measured,
            stats.gateway.len(),
            stats.gateway.len() + stats.missing_gateway,
            stats.pending,
            stats.dead
        );
        if stats.settled() {
            break;
        }
        if tokio::time::Instant::now() >= wait_deadline {
            println!(
                "run {tag}: giving up after {timeout} s; not every expected receipt has arrived"
            );
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // ---- 4. report -------------------------------------------------------------------------------
    let workload = Workload {
        tag: tag.clone(),
        events,
        warmup,
        rate,
        mix,
        timeout_s: timeout,
        started_at,
        git_commit: git_commit(),
        host_cpus: std::thread::available_parallelism().map_or(0, |n| n.get()),
        ingest_errors,
        warmup_errors,
    };
    let mut notes = error_notes;
    if !mailpit_ok && mix.includes_email() {
        notes.push(
            "Mailpit unreachable: every email latency above comes from delivery_jobs.sent_at \
             (fallback)"
                .to_string(),
        );
    }
    if mailpit_search_errors > 0 {
        notes.push(format!(
            "{mailpit_search_errors} Mailpit search(es) failed mid-run; the last successful \
             snapshot was used"
        ));
    }
    println!("{}", render_report(&workload, &stats, &notes));
    if let Some(path) = out {
        let text = serde_json::to_string_pretty(&report_json(&workload, &stats, &notes))
            .map_err(|e| format!("cannot serialize results: {e}"))?;
        std::fs::write(&path, text).map_err(|e| format!("cannot write {path}: {e}"))?;
        println!("results (json) written to {path}");
    }
    if report_pass(&workload, &stats) {
        Ok(())
    } else {
        std::process::exit(1);
    }
}

// ---- arguments ------------------------------------------------------------------------------------

struct Args {
    events: u64,
    rate: f64,
    mix: Mix,
    warmup: u64,
    timeout: u64,
    out: Option<String>,
}

/// Parse `--key value` / `--key=value`; `Ok(None)` means `--help` was requested.
fn parse_args(argv: &[String]) -> Result<Option<Args>, String> {
    let mut args = Args {
        events: DEFAULT_EVENTS,
        rate: DEFAULT_RATE,
        mix: Mix::Both,
        warmup: DEFAULT_WARMUP,
        timeout: DEFAULT_TIMEOUT,
        out: None,
    };
    let mut it = argv.iter();
    while let Some(arg) = it.next() {
        let (key, inline): (&str, Option<&str>) = match arg.split_once('=') {
            Some((k, v)) => (k, Some(v)),
            None => (arg.as_str(), None),
        };
        if key == "--help" || key == "-h" {
            return Ok(None);
        }
        let raw = match inline {
            Some(v) => v,
            None => it.next().ok_or_else(|| format!("{key} needs a value"))?,
        };
        match key {
            "--events" => args.events = parse_u64(raw, "--events", 1)?,
            "--warmup" => args.warmup = parse_u64(raw, "--warmup", 0)?,
            "--timeout" => args.timeout = parse_u64(raw, "--timeout", 1)?,
            "--rate" => {
                let value: f64 = raw
                    .parse()
                    .map_err(|_| format!("--rate must be a positive number, got {raw:?}"))?;
                if !value.is_finite() || value <= 0.0 {
                    return Err(format!("--rate must be a positive number, got {raw:?}"));
                }
                args.rate = value;
            }
            "--mix" => args.mix = Mix::parse(raw)?,
            "--out" => args.out = Some(raw.to_string()),
            other => {
                return Err(format!(
                    "unknown argument {other:?}; usage: latency_bench [--events N] [--rate R] \
                     [--mix sms|email|both] [--warmup W] [--timeout S] [--out PATH] (see --help)"
                ));
            }
        }
    }
    if args.events + args.warmup > MAX_EVENTS {
        return Err(format!(
            "--events + --warmup must stay within {MAX_EVENTS} events per run"
        ));
    }
    Ok(Some(args))
}

fn parse_u64(raw: &str, name: &str, min: u64) -> Result<u64, String> {
    let value = raw
        .parse::<u64>()
        .map_err(|_| format!("{name} must be a number >= {min}, got {raw:?}"))?;
    if value < min {
        return Err(format!("{name} must be a number >= {min}, got {raw:?}"));
    }
    Ok(value)
}

fn usage() {
    println!(
        "latency_bench: ingest → delivery latency benchmark (SPEC §9, M6) against the full stack."
    );
    println!();
    println!("Open-loop load: posts unique sample events through the ingest webhook on a fixed");
    println!("schedule (never waiting for a response before the next send), then measures, per");
    println!("event, from t0 = the client wall clock just before each POST:");
    println!("  ingest → gateway receipt   gateway_receipts.received_at - t0 (phone + sms jobs)");
    println!("  ingest → email receipt     Mailpit message Created - t0 (falls back to sent_at)");
    println!("  ingest → alert             alerts.first_seen - t0");
    println!("  alert → delivered          delivery_jobs.sent_at - first_seen");
    println!();
    println!("Workload (--mix):");
    println!("  sms    samples/events/workflow-failed.json → rule settlements-workflow-failed");
    println!("         → oncall:settlements-primary → phone + sms through the fake gateway");
    println!("  email  samples/events/trade-rejected.json → rule rates-rejected-trades");
    println!("         → team:rates → email through Mailpit (no throttle; unique tradeId)");
    println!("  both   the two alternating (default)");
    println!();
    println!("usage: latency_bench [--events N] [--rate R] [--mix sms|email|both]");
    println!("                     [--warmup W] [--timeout S] [--out PATH]");
    println!();
    println!("  --events N    measured events, at most 1,000,000 per run (default 500)");
    println!("  --rate R      events/s, open loop (default 50)");
    println!("  --mix MIX     sms | email | both (default both)");
    println!("  --warmup W    extra warmup events first, excluded from stats (default 50); the");
    println!("                measured stream starts once their deliveries have drained");
    println!("  --timeout S   seconds to wait after the posts for every receipt (default 120)");
    println!("  --out PATH    also write the report as JSON to PATH (overwritten)");
    println!();
    println!("env:   DATABASE_URL   required, e.g. postgres://beacon:beacon@localhost:5432/beacon");
    println!("       INGEST_URL     default http://localhost:8090");
    println!("       GATEWAY_URL    default http://localhost:8070 (pre-flight read only)");
    println!("       MAILPIT_URL    default http://localhost:8025");
    println!("       GATEWAY_TOKEN  optional bearer for the gateway pre-flight read");
    println!();
    println!("The stack must already be running:");
    println!("  beacon --role ingest,matcher,aggregator,relay,router,delivery,gateway");
    println!("plus Postgres, Redpanda and Mailpit. Nothing is wiped; every event is unique");
    println!("(the run tag in its id field), so dedup never merges them.");
    println!();
    println!("NOTE: gateway receipts are timestamped by Postgres and email by Mailpit, so the");
    println!("numbers are only valid when the client, Postgres and Mailpit share a clock (same");
    println!("machine / NTP).");
    println!();
    println!("Exit status: 0 every expected receipt arrived and no ingest POST failed;");
    println!("             1 something missing or an error (see the report);");
    println!("             2 bad usage or an unreachable stack.");
}

// ---- workload ------------------------------------------------------------------------------------

/// Which sample event a sequence index posts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EventKind {
    /// `workflow-failed.json`: gateway phone + sms jobs.
    Sms,
    /// `trade-rejected.json`: Mailpit email jobs.
    Email,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mix {
    Sms,
    Email,
    Both,
}

impl Mix {
    fn parse(raw: &str) -> Result<Mix, String> {
        match raw {
            "sms" => Ok(Mix::Sms),
            "email" => Ok(Mix::Email),
            "both" => Ok(Mix::Both),
            other => Err(format!("--mix must be sms, email or both, got {other:?}")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Mix::Sms => "sms",
            Mix::Email => "email",
            Mix::Both => "both",
        }
    }

    fn includes_sms(self) -> bool {
        !matches!(self, Mix::Email)
    }

    fn includes_email(self) -> bool {
        !matches!(self, Mix::Sms)
    }
}

/// The event kind posted at sequence index `i`; deterministic so runs are comparable.
fn kind_at(mix: Mix, i: u64) -> EventKind {
    match mix {
        Mix::Sms => EventKind::Sms,
        Mix::Email => EventKind::Email,
        Mix::Both if i.is_multiple_of(2) => EventKind::Sms,
        Mix::Both => EventKind::Email,
    }
}

/// The id embedded in event `i`'s id field; also its `Idempotency-Key`.
fn event_id(kind: EventKind, tag: &str, i: u64) -> String {
    match kind {
        EventKind::Sms => format!("WF-{tag}-{i}"),
        EventKind::Email => format!("T-{tag}-{i}"),
    }
}

/// The sample event for sequence index `i`, with the run tag in its id field, so dedup
/// never merges it into another run's (or another index's) alert.
fn event_body(kind: EventKind, tag: &str, i: u64, workflow: &Value, trade: &Value) -> Value {
    let mut event = match kind {
        EventKind::Sms => workflow.clone(),
        EventKind::Email => trade.clone(),
    };
    let id = event_id(kind, tag, i);
    match kind {
        EventKind::Sms => event["workflowId"] = Value::String(id),
        EventKind::Email => event["payload"]["tradeId"] = Value::String(id),
    }
    event
}

/// The run tag's position in an alert title: `WF-<tag>-<i>` (sms events) or
/// `T-<tag>-<i>` (email events). Returns the kind and sequence index encoded in the
/// title; `None` when the title merely contains the tag by coincidence (it then also
/// reads the whole digit run after the tag, so index 7 is never read out of 70).
fn title_index(title: &str, tag: &str) -> Option<(EventKind, u64)> {
    let mut from = 0usize;
    while let Some(found) = title[from..].find(tag) {
        let at = from + found;
        let kind = kind_before(&title[..at]);
        let rest = &title[at + tag.len()..];
        if let (Some(kind), Some(digits)) = (kind, rest.strip_prefix('-')) {
            let end = digits
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(digits.len());
            if end > 0
                && let Ok(index) = digits[..end].parse::<u64>()
            {
                return Some((kind, index));
            }
        }
        from = at + tag.len();
    }
    None
}

fn kind_before(prefix: &str) -> Option<EventKind> {
    if prefix.ends_with("WF-") {
        Some(EventKind::Sms)
    } else if prefix.ends_with("T-") {
        Some(EventKind::Email)
    } else {
        None
    }
}

// ---- stats ----------------------------------------------------------------------------------------

/// Signed milliseconds from `a` to `b` on the wall clock.
fn ms_between(a: DateTime<Utc>, b: DateTime<Utc>) -> i64 {
    (b - a).num_milliseconds()
}

/// Nearest-rank percentile of ascending `samples`: the ceil(p·n/100)-th value,
/// 1-based. Integer math, so there is no rounding at the rank.
fn percentile(sorted: &[i64], p: u32) -> Option<i64> {
    if sorted.is_empty() {
        return None;
    }
    let n = sorted.len() as u64;
    let rank = (p as u64 * n).div_ceil(100).min(n);
    Some(sorted[(rank - 1) as usize])
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MetricStats {
    count: usize,
    p50: i64,
    p90: i64,
    p99: i64,
    max: i64,
}

/// count + nearest-rank p50/p90/p99 + max of a sample set (sorts a copy).
fn metric_stats(samples: &[i64]) -> Option<MetricStats> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    Some(MetricStats {
        count: sorted.len(),
        p50: percentile(&sorted, 50)?,
        p90: percentile(&sorted, 90)?,
        p99: percentile(&sorted, 99)?,
        max: sorted[sorted.len() - 1],
    })
}

/// `50` for 50.0, `12.5` for 12.5 — no trailing `.0` in the report.
fn fmt_rate(rate: f64) -> String {
    if rate.fract() == 0.0 {
        format!("{rate:.0}")
    } else {
        format!("{rate}")
    }
}

/// One markdown row of the latency table; `n/a` fields when there are no samples.
fn metric_row(name: &str, stats: Option<&MetricStats>) -> String {
    match stats {
        Some(s) => format!(
            "| {name} | {} | {} | {} | {} | {} |",
            s.count, s.p50, s.p90, s.p99, s.max
        ),
        None => format!("| {name} | 0 | n/a | n/a | n/a | n/a |"),
    }
}

/// The latency table: header plus one row per metric, values in ms.
fn latency_table(rows: &[(&str, Option<&MetricStats>)]) -> String {
    let mut out = String::from(
        "| metric (ms) | count | p50 | p90 | p99 | max |\n|---|---:|---:|---:|---:|---:|",
    );
    for &(name, stats) in rows {
        out.push('\n');
        out.push_str(&metric_row(name, stats));
    }
    out
}

// ---- the run's rows, folded into samples ----------------------------------------------------------

/// One ingest POST's outcome.
struct SendRecord {
    index: u64,
    t0: DateTime<Utc>,
    /// The HTTP status when the server answered at all.
    status: Option<u16>,
    /// Set for any non-2xx answer or a failed send; counted as an error.
    error: Option<String>,
}

struct AlertHit {
    id: Uuid,
    first_seen: DateTime<Utc>,
}

struct JobHit {
    id: Uuid,
    channel: String,
    status: String,
    sent_at: Option<DateTime<Utc>>,
    fallback_of: Option<Uuid>,
    /// 0 = the initial notification; >0 = a later escalation step (NOT NULL DEFAULT 0).
    escalation_step: i32,
}

struct Snapshot {
    /// By sequence index (parsed from the alert title).
    alerts: HashMap<u64, AlertHit>,
    jobs: HashMap<Uuid, Vec<JobHit>>,
    /// gateway_receipts: job id → received_at.
    receipts: HashMap<Uuid, DateTime<Utc>>,
}

#[derive(Default)]
struct RunStats {
    /// ingest → gateway receipt, per sms/phone job.
    gateway: Vec<i64>,
    /// ingest → email receipt, per email job (Mailpit `Created`, else sent_at).
    email: Vec<i64>,
    email_fallback: usize,
    /// ingest → alert, per event.
    ingest_alert: Vec<i64>,
    /// alert → delivered, per job.
    alert_delivered: Vec<i64>,
    missing_alerts: usize,
    missing_gateway: usize,
    missing_mailpit: usize,
    pending: usize,
    dead: usize,
    cancelled: usize,
    fallback_jobs: usize,
    escalation_jobs: usize,
}

impl RunStats {
    /// True when every measured event has its alert and every one of its receipts.
    fn settled(&self) -> bool {
        self.missing_alerts == 0 && self.missing_gateway == 0 && self.missing_mailpit == 0
    }
}

/// Fold one snapshot of the run's rows into latency samples and shortfall counts.
/// `mail` maps an email job id to the Mailpit `Created` time of its message;
/// `mailpit_ok` says whether the Mailpit search is working (when it is not, a sent
/// email without a message falls back to sent_at instead of counting as missing).
fn evaluate(
    warmup: u64,
    total: u64,
    t0: &[Option<DateTime<Utc>>],
    snap: &Snapshot,
    mail: &HashMap<Uuid, DateTime<Utc>>,
    mailpit_ok: bool,
) -> RunStats {
    let mut st = RunStats::default();
    for i in warmup..total {
        let Some(t0_i) = t0.get(i as usize).copied().flatten() else {
            // The POST never got a recorded response: the event never made it in.
            st.missing_alerts += 1;
            continue;
        };
        let Some(alert) = snap.alerts.get(&i) else {
            st.missing_alerts += 1;
            continue;
        };
        let jobs = snap
            .jobs
            .get(&alert.id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        // The initial wave only: a retry's fallback job and later escalation steps
        // are separate notifications, reported below as diagnostics.
        for job in jobs {
            if job.fallback_of.is_some() {
                st.fallback_jobs += 1;
            }
            if job.escalation_step > 0 {
                st.escalation_jobs += 1;
            }
        }
        let initial: Vec<&JobHit> = jobs
            .iter()
            .filter(|j| j.fallback_of.is_none() && j.escalation_step == 0)
            .collect();
        // The alert exists, so ingest → alert is measurable even if the router has
        // not expanded it (yet); that state is counted as a missing alert below.
        st.ingest_alert.push(ms_between(t0_i, alert.first_seen));
        if initial.is_empty() {
            st.missing_alerts += 1;
            continue;
        }
        for job in initial {
            match job.status.as_str() {
                "pending" | "in_flight" => st.pending += 1,
                "dead" => st.dead += 1,
                "cancelled" => st.cancelled += 1,
                _ => {}
            }
            if let Some(sent_at) = job.sent_at {
                st.alert_delivered
                    .push(ms_between(alert.first_seen, sent_at));
            }
            match job.channel.as_str() {
                "sms" | "phone" => match (job.status == "sent", snap.receipts.get(&job.id)) {
                    (true, Some(received)) => st.gateway.push(ms_between(t0_i, *received)),
                    _ => st.missing_gateway += 1, // not sent (yet/dead) or sent with no receipt
                },
                "email" => match (job.status == "sent", mail.get(&job.id)) {
                    (true, Some(created)) => st.email.push(ms_between(t0_i, *created)),
                    (true, None) => {
                        // Sent but the search has not seen the message: fall back to
                        // sent_at; only "missing" when the search is actually working.
                        if mailpit_ok {
                            st.missing_mailpit += 1;
                        }
                        if let Some(sent_at) = job.sent_at {
                            st.email.push(ms_between(t0_i, sent_at));
                            st.email_fallback += 1;
                        }
                    }
                    _ => st.missing_mailpit += 1, // pending/in_flight/dead/cancelled
                },
                _ => {}
            }
        }
    }
    st
}

// ---- stack reads -----------------------------------------------------------------------------------

/// One poll: the run's alerts (by title), their delivery jobs, and the gateway
/// receipts of those jobs.
/// Wait until the run's `warmup` alerts exist, each has been routed (has a delivery job) and
/// none of their jobs is still pending or in flight (at most [`SETTLE_LIMIT`]; then it warns and goes on).
async fn settle(
    pool: &sqlx::PgPool,
    title_pattern: &str,
    run_start: DateTime<Utc>,
    warmup: u64,
) -> Result<(), String> {
    let started = tokio::time::Instant::now();
    loop {
        let (alerts, unrouted, jobs, open): (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT count(DISTINCT a.id), count(DISTINCT a.id) FILTER (WHERE j.id IS NULL), \
                    count(j.id), count(j.id) FILTER (WHERE j.status IN ('pending', 'in_flight')) \
             FROM alerts a LEFT JOIN delivery_jobs j ON j.alert_id = a.id \
             WHERE a.title LIKE $1 AND a.first_seen >= $2",
        )
        .bind(title_pattern)
        .bind(run_start)
        .fetch_one(pool)
        .await
        .map_err(|e| format!("settle query failed: {e}"))?;
        if alerts as u64 >= warmup && unrouted == 0 && open == 0 {
            println!(
                "[warmup] {alerts} alerts, {jobs} jobs drained in {:.1}s; measuring",
                started.elapsed().as_secs_f64()
            );
            return Ok(());
        }
        if started.elapsed() >= SETTLE_LIMIT {
            println!(
                "warning: warmup not drained after {}s ({alerts}/{warmup} alerts, {unrouted} not \
                 routed, {open} of {jobs} jobs open); measuring anyway",
                SETTLE_LIMIT.as_secs()
            );
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn snapshot(
    pool: &sqlx::PgPool,
    tag: &str,
    mix: Mix,
    total: u64,
    run_start: DateTime<Utc>,
    title_pattern: &str,
) -> Result<Snapshot, String> {
    let alert_rows: Vec<(Uuid, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT id, title, first_seen FROM alerts WHERE title LIKE $1 AND first_seen >= $2",
    )
    .bind(title_pattern)
    .bind(run_start)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("alerts query failed: {e}"))?;

    let mut alerts = HashMap::with_capacity(alert_rows.len());
    let mut alert_ids = Vec::with_capacity(alert_rows.len());
    for (id, title, first_seen) in alert_rows {
        alert_ids.push(id);
        if let Some((kind, i)) = title_index(&title, tag) {
            // Defensive: the tag also has to sit in this run's id scheme and kind.
            if i < total && kind == kind_at(mix, i) {
                alerts.insert(i, AlertHit { id, first_seen });
            }
        }
    }

    // delivery_jobs row: id, alert_id, channel, status, sent_at, fallback_of,
    // escalation_step.
    type JobRow = (
        Uuid,
        Uuid,
        String,
        String,
        Option<DateTime<Utc>>,
        Option<Uuid>,
        i32,
    );
    let job_rows: Vec<JobRow> = sqlx::query_as(
        "SELECT id, alert_id, channel, status, sent_at, fallback_of, escalation_step \
             FROM delivery_jobs WHERE alert_id = ANY($1)",
    )
    .bind(&alert_ids)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("delivery jobs query failed: {e}"))?;
    let mut jobs: HashMap<Uuid, Vec<JobHit>> = HashMap::new();
    let mut job_ids = Vec::with_capacity(job_rows.len());
    for (id, alert_id, channel, status, sent_at, fallback_of, escalation_step) in job_rows {
        job_ids.push(id);
        jobs.entry(alert_id).or_default().push(JobHit {
            id,
            channel,
            status,
            sent_at,
            fallback_of,
            escalation_step,
        });
    }

    let receipt_rows: Vec<(Uuid, DateTime<Utc>)> =
        sqlx::query_as("SELECT job_id, received_at FROM gateway_receipts WHERE job_id = ANY($1)")
            .bind(&job_ids)
            .fetch_all(pool)
            .await
            .map_err(|e| format!("gateway receipts query failed: {e}"))?;

    Ok(Snapshot {
        alerts,
        jobs,
        receipts: receipt_rows.into_iter().collect(),
    })
}

/// Mailpit search for the run tag: every page of `subject:{tag}`, folded into
/// `job id → message Created`. The email Message-ID is `<{job_id}@beacon>`, so the
/// job id is the precise join key — no title parsing. Returns the map and the query's
/// total message count.
async fn mailpit_search(
    http: &reqwest::Client,
    mailpit_url: &str,
    tag: &str,
) -> Result<(HashMap<Uuid, DateTime<Utc>>, usize), String> {
    let base = format!("{}/api/v1/search", mailpit_url.trim_end_matches('/'));
    let query = format!("subject:{tag}");
    let mut found = HashMap::new();
    let mut total = 0usize;
    let mut start = 0usize;
    // 100 pages × 1000 messages is far beyond any run here; past that the extra
    // messages would surface as "missing" in the report.
    for _ in 0..100 {
        let params = [
            ("query", query.clone()),
            ("limit", "1000".to_string()),
            ("start", start.to_string()),
        ];
        let resp = http
            .get(&base)
            .query(&params)
            .timeout(Duration::from_secs(20))
            .send()
            .await
            .map_err(|e| format!("Mailpit unreachable at {mailpit_url}: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("Mailpit search answered HTTP {}", resp.status()));
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| format!("Mailpit search response: {e}"))?;
        let messages = body["messages"].as_array().cloned().unwrap_or_default();
        total = body["messages_count"]
            .as_u64()
            .map_or(start + messages.len(), |n| n as usize);
        for message in &messages {
            let Some(message_id) = message["MessageID"].as_str() else {
                continue;
            };
            let Some(created) = message["Created"].as_str() else {
                continue;
            };
            let job = message_id
                .trim_matches(|c| c == '<' || c == '>')
                .split('@')
                .next()
                .and_then(|s| Uuid::parse_str(s).ok());
            if let (Some(job), Ok(time)) = (job, DateTime::parse_from_rfc3339(created)) {
                found.insert(job, time.with_timezone(&Utc));
            }
        }
        let fetched = start + messages.len();
        // Advance by what the server actually returned, so a server-side cap below
        // our page size cannot make the paging skip messages.
        if messages.is_empty() || fetched >= total {
            break;
        }
        start += messages.len();
    }
    Ok((found, total))
}

// ---- report ---------------------------------------------------------------------------------------

struct Workload {
    tag: String,
    events: u64,
    warmup: u64,
    rate: f64,
    mix: Mix,
    timeout_s: u64,
    started_at: DateTime<Utc>,
    git_commit: String,
    host_cpus: usize,
    ingest_errors: usize,
    warmup_errors: usize,
}

fn report_pass(w: &Workload, stats: &RunStats) -> bool {
    stats.settled() && w.ingest_errors == 0
}

fn render_report(w: &Workload, stats: &RunStats, notes: &[String]) -> String {
    let gateway = metric_stats(&stats.gateway);
    let email = metric_stats(&stats.email);
    let ingest_alert = metric_stats(&stats.ingest_alert);
    let alert_delivered = metric_stats(&stats.alert_delivered);

    let mut out = String::new();
    out.push_str(&format!("# ingest → delivery latency (run {})\n\n", w.tag));
    out.push_str("| workload | value |\n|---|---|\n");
    out.push_str(&format!("| events (measured) | {} |\n", w.events));
    out.push_str(&format!(
        "| warmup events | {} (excluded from stats) |\n",
        w.warmup
    ));
    out.push_str(&format!(
        "| rate | {} events/s (open loop) |\n",
        fmt_rate(w.rate)
    ));
    out.push_str(&format!("| mix | {} |\n", w.mix.as_str()));
    out.push_str(&format!("| timeout | {} s |\n", w.timeout_s));
    out.push_str(&format!(
        "| started at | {} |\n",
        w.started_at.to_rfc3339_opts(SecondsFormat::Millis, true)
    ));
    out.push_str(&format!("| git commit | {} |\n", w.git_commit));
    out.push_str(&format!("| host CPUs | {} |\n", w.host_cpus));
    out.push_str(&format!(
        "| ingest errors | {} measured + {} warmup |\n\n",
        w.ingest_errors, w.warmup_errors
    ));

    out.push_str(&latency_table(&[
        (
            "ingest → gateway receipt (phone + sms jobs)",
            gateway.as_ref(),
        ),
        (
            "ingest → email receipt (Mailpit, per recipient)",
            email.as_ref(),
        ),
        ("ingest → alert (first_seen - t0)", ingest_alert.as_ref()),
        (
            "alert → delivered (sent_at - first_seen, per job)",
            alert_delivered.as_ref(),
        ),
    ]));

    out.push_str("\n\n| outcome | count |\n|---|---|\n");
    out.push_str(&format!(
        "| email latencies from sent_at (Mailpit fallback) | {} |\n",
        stats.email_fallback
    ));
    out.push_str(&format!("| missing alerts | {} |\n", stats.missing_alerts));
    out.push_str(&format!(
        "| missing gateway receipts | {} |\n",
        stats.missing_gateway
    ));
    out.push_str(&format!(
        "| missing Mailpit messages | {} |\n",
        stats.missing_mailpit
    ));
    out.push_str(&format!(
        "| pending / dead / cancelled jobs | {} / {} / {} |\n",
        stats.pending, stats.dead, stats.cancelled
    ));
    out.push_str(&format!(
        "| fallback / escalation jobs | {} / {} |\n",
        stats.fallback_jobs, stats.escalation_jobs
    ));

    if !notes.is_empty() {
        out.push_str("\nnotes:\n");
        for note in notes {
            out.push_str(&format!("- {note}\n"));
        }
    }
    out.push_str(&format!("\nRESULT: {}\n", result_line(w, stats)));
    out
}

fn result_line(w: &Workload, stats: &RunStats) -> String {
    if report_pass(w, stats) {
        return "PASS — nothing missing, no ingest errors".to_string();
    }
    let mut parts: Vec<String> = Vec::new();
    if w.ingest_errors > 0 {
        parts.push(format!("{} ingest error(s)", w.ingest_errors));
    }
    if stats.missing_alerts > 0 {
        parts.push(format!(
            "{} alert(s) never created or routed",
            stats.missing_alerts
        ));
    }
    if stats.missing_gateway > 0 {
        parts.push(format!(
            "{} gateway receipt(s) missing",
            stats.missing_gateway
        ));
    }
    if stats.missing_mailpit > 0 {
        parts.push(format!(
            "{} Mailpit message(s) missing",
            stats.missing_mailpit
        ));
    }
    if stats.dead > 0 {
        parts.push(format!("{} dead job(s)", stats.dead));
    }
    if stats.cancelled > 0 {
        parts.push(format!("{} cancelled job(s)", stats.cancelled));
    }
    if parts.is_empty() {
        parts.push("run incomplete".to_string());
    }
    format!("FAIL — {}", parts.join("; "))
}

fn metric_json(samples: &[i64]) -> Value {
    match metric_stats(samples) {
        Some(s) => json!({
            "count": s.count,
            "p50": s.p50,
            "p90": s.p90,
            "p99": s.p99,
            "max": s.max,
            "samples_ms": samples,
        }),
        None => json!({ "count": 0, "samples_ms": [] }),
    }
}

/// The same report as JSON (written by `--out`).
fn report_json(w: &Workload, stats: &RunStats, notes: &[String]) -> Value {
    json!({
        "run": {
            "tag": w.tag.clone(),
            "started_at": w.started_at.to_rfc3339_opts(SecondsFormat::Millis, true),
            "git_commit": w.git_commit.clone(),
            "host_cpus": w.host_cpus,
        },
        "workload": {
            "events": w.events,
            "warmup": w.warmup,
            "rate_per_s": w.rate,
            "mix": w.mix.as_str(),
            "timeout_s": w.timeout_s,
        },
        "metrics_ms": {
            "gateway_receipt": metric_json(&stats.gateway),
            "email_receipt": metric_json(&stats.email),
            "ingest_to_alert": metric_json(&stats.ingest_alert),
            "alert_to_delivered": metric_json(&stats.alert_delivered),
        },
        "counts": {
            "ingest_errors_measured": w.ingest_errors,
            "ingest_errors_warmup": w.warmup_errors,
            "email_sent_at_fallbacks": stats.email_fallback,
            "missing_alerts": stats.missing_alerts,
            "missing_gateway_receipts": stats.missing_gateway,
            "missing_mailpit_messages": stats.missing_mailpit,
            "pending_jobs": stats.pending,
            "dead_jobs": stats.dead,
            "cancelled_jobs": stats.cancelled,
            "fallback_jobs": stats.fallback_jobs,
            "escalation_jobs": stats.escalation_jobs,
        },
        "notes": notes,
        "pass": report_pass(w, stats),
    })
}

// ---- host facts ------------------------------------------------------------------------------------

fn git_commit() -> String {
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..");
    for dir in [None, Some(manifest_root.as_path())] {
        let mut cmd = Command::new("git");
        cmd.args(["rev-parse", "--short", "HEAD"]);
        if let Some(d) = dir {
            cmd.current_dir(d);
        }
        if let Ok(output) = cmd.output()
            && output.status.success()
        {
            return String::from_utf8_lossy(&output.stdout).trim().to_string();
        }
    }
    "unknown".to_string()
}

fn read_sample(path: &std::path::Path) -> Result<Value, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    serde_json::from_str(&text).map_err(|e| format!("{} is not valid JSON: {e}", path.display()))
}

// ---- tests ----------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn dt(millis: i64) -> DateTime<Utc> {
        Utc.timestamp_millis_opt(millis)
            .single()
            .expect("valid epoch millis")
    }

    fn snap() -> Snapshot {
        Snapshot {
            alerts: HashMap::new(),
            jobs: HashMap::new(),
            receipts: HashMap::new(),
        }
    }

    #[test]
    fn percentile_is_nearest_rank() {
        assert_eq!(percentile(&[], 50), None);
        let one = [42];
        assert_eq!(percentile(&one, 50), Some(42));
        assert_eq!(percentile(&one, 99), Some(42));
        let ten: Vec<i64> = (1..=10).collect();
        assert_eq!(percentile(&ten, 50), Some(5));
        assert_eq!(percentile(&ten, 90), Some(9));
        assert_eq!(percentile(&ten, 99), Some(10));
        let four = [10, 20, 30, 40];
        assert_eq!(percentile(&four, 50), Some(20));
        assert_eq!(percentile(&four, 90), Some(40));
        assert_eq!(percentile(&four, 99), Some(40));
        let hundred: Vec<i64> = (1..=100).collect();
        assert_eq!(percentile(&hundred, 99), Some(99));
        assert_eq!(percentile(&hundred, 90), Some(90));
    }

    #[test]
    fn metric_stats_sorts_and_folds() {
        assert_eq!(metric_stats(&[]), None);
        let stats = metric_stats(&[30, 10, 20]).unwrap();
        assert_eq!(
            stats,
            MetricStats {
                count: 3,
                p50: 20,
                p90: 30,
                p99: 30,
                max: 30
            }
        );
    }

    #[test]
    fn metric_rows_are_markdown() {
        let stats = MetricStats {
            count: 3,
            p50: 20,
            p90: 30,
            p99: 30,
            max: 30,
        };
        assert_eq!(
            metric_row("x", Some(&stats)),
            "| x | 3 | 20 | 30 | 30 | 30 |"
        );
        assert_eq!(metric_row("x", None), "| x | 0 | n/a | n/a | n/a | n/a |");
        let table = latency_table(&[("a", Some(&stats)), ("b", None)]);
        assert_eq!(
            table,
            "| metric (ms) | count | p50 | p90 | p99 | max |\n|---|---:|---:|---:|---:|---:|\
             \n| a | 3 | 20 | 30 | 30 | 30 |\n| b | 0 | n/a | n/a | n/a | n/a |"
        );
    }

    #[test]
    fn rate_prints_compactly() {
        assert_eq!(fmt_rate(50.0), "50");
        assert_eq!(fmt_rate(12.5), "12.5");
    }

    #[test]
    fn milliseconds_between() {
        assert_eq!(ms_between(dt(1_000), dt(2_500)), 1_500);
        assert_eq!(ms_between(dt(2_500), dt(1_000)), -1_500);
        assert_eq!(ms_between(dt(0), dt(0)), 0);
    }

    #[test]
    fn titles_carry_the_run_index() {
        let tag = "ab12cd";
        let sms = "Settlement workflow WF-ab12cd-42 failed at validate-counterparty";
        assert_eq!(title_index(sms, tag), Some((EventKind::Sms, 42)));
        let email = "Rejected EUR 25,000,000 trade T-ab12cd-7";
        assert_eq!(title_index(email, tag), Some((EventKind::Email, 7)));
        // a longer index must not be read as a prefix of a shorter one
        assert_eq!(
            title_index("Rejected EUR 25,000,000 trade T-ab12cd-70", tag),
            Some((EventKind::Email, 70))
        );
        // the tag appearing earlier in the title by coincidence is skipped
        assert_eq!(
            title_index("trade ab12cd for T-ab12cd-9", tag),
            Some((EventKind::Email, 9))
        );
        assert_eq!(title_index("unrelated title", tag), None);
        assert_eq!(title_index("WF-ab12cd-no-digits", tag), None);
        assert_eq!(
            title_index("Settlement workflow WF-ab12cd- failed", tag),
            None
        );
    }

    #[test]
    fn ids_and_kinds_follow_the_mix() {
        assert_eq!(event_id(EventKind::Sms, "t1", 7), "WF-t1-7");
        assert_eq!(event_id(EventKind::Email, "t1", 7), "T-t1-7");
        assert_eq!(kind_at(Mix::Sms, 9), EventKind::Sms);
        assert_eq!(kind_at(Mix::Email, 9), EventKind::Email);
        assert_eq!(kind_at(Mix::Both, 4), EventKind::Sms);
        assert_eq!(kind_at(Mix::Both, 5), EventKind::Email);
    }

    #[test]
    fn event_bodies_embed_unique_ids() {
        let workflow = json!({
            "type": "WORKFLOW_FAILED",
            "workflowId": "WF-20251023-0042",
            "workflowName": "order-settlement",
            "step": "validate-counterparty",
        });
        let trade = json!({
            "meta": { "schema": "trade.rejected.v1" },
            "payload": { "tradeId": "T-99877", "desk": "rates", "notional": 25000000 },
        });
        let sms = event_body(EventKind::Sms, "tag1", 3, &workflow, &trade);
        assert_eq!(sms["workflowId"], "WF-tag1-3");
        assert_eq!(sms["step"], "validate-counterparty");
        let email = event_body(EventKind::Email, "tag1", 3, &workflow, &trade);
        assert_eq!(email["payload"]["tradeId"], "T-tag1-3");
        assert_eq!(email["payload"]["desk"], "rates");
    }

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn args_parse() {
        let default = parse_args(&argv(&[])).unwrap().unwrap();
        assert_eq!(
            (
                default.events,
                default.rate,
                default.warmup,
                default.timeout
            ),
            (500, 50.0, 50, 120)
        );
        assert_eq!(default.mix, Mix::Both);
        assert!(default.out.is_none());

        let args = parse_args(&argv(&[
            "--events",
            "200",
            "--rate",
            "20",
            "--mix",
            "email",
            "--warmup",
            "0",
            "--timeout",
            "60",
            "--out",
            "r.json",
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(
            (args.events, args.rate, args.warmup, args.timeout),
            (200, 20.0, 0, 60)
        );
        assert_eq!(args.mix, Mix::Email);
        assert_eq!(args.out.as_deref(), Some("r.json"));

        let inline = parse_args(&argv(&["--events=7"])).unwrap().unwrap();
        assert_eq!(inline.events, 7);

        assert!(parse_args(&argv(&["--help"])).unwrap().is_none());
        assert!(parse_args(&argv(&["-h"])).unwrap().is_none());

        assert!(parse_args(&argv(&["--wat"])).is_err());
        assert!(parse_args(&argv(&["--events"])).is_err());
        assert!(parse_args(&argv(&["--events", "zero"])).is_err());
        assert!(parse_args(&argv(&["--rate", "0"])).is_err());
        assert!(parse_args(&argv(&["--warmup", "-1"])).is_err());
        assert!(parse_args(&argv(&["--mix", "carrier-pigeon"])).is_err());
        assert!(parse_args(&argv(&["--events", "999999", "--warmup", "999999"])).is_err());
    }

    #[test]
    fn evaluate_folds_a_healthy_snapshot() {
        let mut s = snap();
        let alert_id = Uuid::new_v4();
        s.alerts.insert(
            0,
            AlertHit {
                id: alert_id,
                first_seen: dt(1_000),
            },
        );

        let sms_job = JobHit {
            id: Uuid::new_v4(),
            channel: "sms".into(),
            status: "sent".into(),
            sent_at: Some(dt(1_400)),
            fallback_of: None,
            escalation_step: 0,
        };
        let phone_job = JobHit {
            id: Uuid::new_v4(),
            channel: "phone".into(),
            status: "sent".into(),
            sent_at: Some(dt(1_200)),
            fallback_of: None,
            escalation_step: 0,
        };
        s.receipts.insert(sms_job.id, dt(1_500));
        s.receipts.insert(phone_job.id, dt(1_300));
        s.jobs.insert(alert_id, vec![sms_job, phone_job]);

        // one measured event; t0 at 0 ms.
        let t0 = vec![Some(dt(0))];
        let stats = evaluate(0, 1, &t0, &s, &HashMap::new(), false);
        assert!(stats.settled());
        assert_eq!(stats.gateway, [1_500, 1_300]); // received_at - t0
        assert_eq!(stats.ingest_alert, [1_000]); // first_seen - t0
        assert_eq!(stats.alert_delivered, [400, 200]); // sent_at - first_seen
        assert!(stats.email.is_empty());
        assert_eq!(stats.missing_gateway, 0);
    }

    #[test]
    fn evaluate_counts_missing_and_fallbacks() {
        let mut s = snap();
        let alert_id = Uuid::new_v4();
        s.alerts.insert(
            0,
            AlertHit {
                id: alert_id,
                first_seen: dt(1_000),
            },
        );
        let sent_email = JobHit {
            id: Uuid::new_v4(),
            channel: "email".into(),
            status: "sent".into(),
            sent_at: Some(dt(1_900)),
            fallback_of: None,
            escalation_step: 0,
        };
        let dead_phone = JobHit {
            id: Uuid::new_v4(),
            channel: "phone".into(),
            status: "dead".into(),
            sent_at: None,
            fallback_of: None,
            escalation_step: 0,
        };
        let in_flight_sms = JobHit {
            id: Uuid::new_v4(),
            channel: "sms".into(),
            status: "in_flight".into(),
            sent_at: None,
            fallback_of: None,
            escalation_step: 0,
        };
        let fallback_job = JobHit {
            id: Uuid::new_v4(),
            channel: "email".into(),
            status: "pending".into(),
            sent_at: None,
            fallback_of: Some(dead_phone.id),
            escalation_step: 0,
        };
        // A later escalation step is a separate notification: counted, not measured.
        let escalated_sms = JobHit {
            id: Uuid::new_v4(),
            channel: "sms".into(),
            status: "pending".into(),
            sent_at: None,
            fallback_of: None,
            escalation_step: 1,
        };
        let sent_email_id = sent_email.id;
        s.jobs.insert(
            alert_id,
            vec![
                sent_email,
                dead_phone,
                in_flight_sms,
                fallback_job,
                escalated_sms,
            ],
        );

        let t0 = vec![Some(dt(0))];
        // Mailpit is up but never saw the sent email → missing + sent_at fallback.
        let stats = evaluate(0, 1, &t0, &s, &HashMap::new(), true);
        assert!(!stats.settled());
        assert_eq!(stats.missing_alerts, 0);
        assert_eq!(stats.missing_gateway, 2); // dead phone + in-flight sms
        assert_eq!(stats.missing_mailpit, 1); // sent email with no message
        assert_eq!(stats.email_fallback, 1);
        assert_eq!(stats.email, [1_900]);
        assert_eq!(stats.pending, 1);
        assert_eq!(stats.dead, 1);
        assert_eq!(stats.fallback_jobs, 1); // the retry's fallback is not in the stats
        assert_eq!(stats.escalation_jobs, 1); // step 1 is not in the stats either

        // Same snapshot with Mailpit down: the sent email is no longer "missing".
        let stats = evaluate(0, 1, &t0, &s, &HashMap::new(), false);
        assert_eq!(stats.missing_mailpit, 0);
        assert!(!stats.settled()); // the sms/phone receipts still are

        // A sent email that Mailpit did see is a real sample, not a fallback.
        let mut mail = HashMap::new();
        mail.insert(sent_email_id, dt(1_800));
        let stats = evaluate(0, 1, &t0, &s, &mail, true);
        assert_eq!(stats.email, [1_800]);
        assert_eq!(stats.email_fallback, 0);
        assert_eq!(stats.missing_mailpit, 0);
    }

    #[test]
    fn evaluate_skips_warmup_and_missing_alerts() {
        let mut s = snap();
        s.alerts.insert(
            0,
            AlertHit {
                id: Uuid::new_v4(),
                first_seen: dt(500),
            },
        );
        let t0 = vec![Some(dt(0)), Some(dt(0))];

        let stats = evaluate(0, 2, &t0, &s, &HashMap::new(), false);
        // the alert at index 0 exists (with no jobs yet): it still carries the
        // ingest → alert sample, but counts as unrouted
        assert_eq!(stats.ingest_alert, [500]);
        assert_eq!(stats.missing_alerts, 2); // index 0 unrouted + index 1 missing

        let stats = evaluate(1, 2, &t0, &s, &HashMap::new(), false);
        assert!(stats.ingest_alert.is_empty()); // index 0 is warmup
        assert_eq!(stats.missing_alerts, 1); // index 1 has no alert
    }

    #[test]
    fn report_renders_markdown_and_json() {
        let w = Workload {
            tag: "e1e1e1".into(),
            events: 2,
            warmup: 1,
            rate: 50.0,
            mix: Mix::Both,
            timeout_s: 120,
            started_at: dt(0),
            git_commit: "abc1234".into(),
            host_cpus: 8,
            ingest_errors: 0,
            warmup_errors: 0,
        };
        let mut stats = RunStats::default();
        stats.gateway = vec![10, 30, 20];
        stats.ingest_alert = vec![5];

        let report = render_report(&w, &stats, &["a note".to_string()]);
        assert!(report.contains("# ingest → delivery latency (run e1e1e1)"));
        assert!(report.contains("| events (measured) | 2 |"));
        assert!(report.contains("| rate | 50 events/s (open loop) |"));
        assert!(report.contains("| git commit | abc1234 |"));
        assert!(report.contains("| host CPUs | 8 |"));
        assert!(
            report.contains(
                "| ingest → gateway receipt (phone + sms jobs) | 3 | 20 | 30 | 30 | 30 |"
            )
        );
        assert!(report.contains(
            "| ingest → email receipt (Mailpit, per recipient) | 0 | n/a | n/a | n/a | n/a |"
        ));
        assert!(report.contains("- a note"));
        assert!(report.contains("RESULT: PASS — nothing missing, no ingest errors"));

        let j = report_json(&w, &stats, &["a note".to_string()]);
        assert_eq!(j["pass"], json!(true));
        assert_eq!(j["workload"]["events"], json!(2));
        assert_eq!(j["metrics_ms"]["gateway_receipt"]["p50"], json!(20));
        assert_eq!(j["metrics_ms"]["ingest_to_alert"]["samples_ms"], json!([5]));
        assert_eq!(j["counts"]["missing_gateway_receipts"], json!(0));

        // and a failing run spells out what went wrong
        let w = Workload {
            ingest_errors: 1,
            ..w
        };
        let mut bad = RunStats::default();
        bad.missing_gateway = 2;
        bad.dead = 1;
        let report = render_report(&w, &bad, &[]);
        assert!(report.contains("RESULT: FAIL"));
        assert!(report.contains("1 ingest error(s)"));
        assert!(report.contains("2 gateway receipt(s) missing"));
        assert!(report.contains("1 dead job(s)"));
        assert_eq!(report_json(&w, &bad, &[])["pass"], json!(false));
    }
}
