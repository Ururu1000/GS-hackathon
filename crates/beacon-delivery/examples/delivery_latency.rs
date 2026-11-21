//! Ingest → delivery latency reconciler (SPEC §9, M6): runs AFTER a `beacon-loadgen`
//! run, against the same stack, and reads only Postgres and Mailpit — it sends no
//! events itself.
//!
//! The load generator puts a 12-hex run id into every incident key
//! (`WF-LG-{run}-{k}`, `T-LG-{run}-{k}`, `INS-{run}-{k}`, `region-{run}-{k}`) and
//! prints it in its report title (`# Load test \`scenario\` (<run id>)`), so this
//! tool selects the run's alerts by that id: rows whose `title`, `body` or
//! `fields::text` contain it, always parameterised as `LIKE '%' || $1 || '%'`
//! (never string-formatted SQL), optionally bounded by `--since`/`--until` on
//! `alerts.first_seen`. It then
//!
//! - waits for the run's jobs: polls once a second, `--wait-secs` at most, until no
//!   measured job is still unsent (`sent_at IS NULL` and not dead), and reports how
//!   long the drain took;
//! - measures, per channel (sms, phone, email — `ui` is delivered by the alert feed
//!   and never has a job), each a nearest-rank histogram in ms (count, p50, p90,
//!   p99, max):
//!   - **gateway acceptance** (sms, phone): `gateway_receipts.received_at −
//!     alerts.first_seen`, joined `gateway_receipts.job_id = delivery_jobs.id`,
//!   - **email in Mailpit**: message `Created − alerts.first_seen`, matched by
//!     `Message-ID = <{job_id}@beacon>`, paged from `GET /api/v1/messages` (message
//!     bodies are never fetched),
//!   - **the internal split**: `delivery_jobs.created_at − first_seen` (ingest → job
//!     planned) and `sent_at − first_seen` (ingest → provider ack);
//! - reconciles per channel: expected, sent, still unsent, dead, missing (sent but
//!   no gateway receipt / no Mailpit message), gateway duplicates, attempts > 1.
//!
//! Jobs counted are the alert's FIRST notification only: the lowest `notify_seq`
//! per alert and `escalation_step = 0` (escalation steps, the runbook `escalate`
//! action and approval prompts carry other steps; a fallback job shares the first
//! wave's seq and is part of that wave). Each job is one sample.
//!
//! Environment:
//! - `DATABASE_URL`   required, e.g. `postgres://beacon:beacon@localhost:5432/beacon`
//! - `MAILPIT_URL`    default `http://localhost:8025` (also `--mailpit-url`)
//!
//! Usage: `scripts/delivery_latency.sh --run-id <id> [options]` (or
//! `cargo run -p beacon-delivery --example delivery_latency -- [options]`; see
//! `--help`). The report goes to stdout (so it can be piped to a file) and,
//! optionally, to `--out` as markdown and `--json` as JSON. Exit status: 0 clean;
//! 1 something unsent, dead or missing; 2 usage error, no alerts matched, or an
//! unreachable stack.
//!
//! Clock assumption: `first_seen` is written by the Beacon processes,
//! `received_at`/`sent_at` by Postgres, `Created` by Mailpit, so the numbers are
//! valid only when they share a host clock (same machine or NTP).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

use chrono::{DateTime, SecondsFormat, Utc};
use clap::Parser;
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

/// Channels with delivery jobs; `ui` is delivered by the alert feed and never
/// becomes a `delivery_jobs` row (the router skips it), so it is simply not here.
const CHANNELS: [&str; 3] = ["sms", "phone", "email"];

/// Mailpit list page size (`GET /api/v1/messages?limit=`); 500–1000 per the spec.
const MAILPIT_PAGE: usize = 500;
/// 2 000 pages × 500 messages = 1 M listings — far beyond any run; the cap only
/// exists so a server that keeps answering cannot loop forever. Stopping there
/// surfaces as "missing" in the report.
const MAILPIT_MAX_PAGES: usize = 2_000;

/// The one-line clock caveat, printed in every report (SPEC §9 wording).
const CLOCK_NOTE: &str = "Clock assumption: `first_seen` is written by the Beacon \
                          processes, `received_at`/`sent_at` by Postgres, `Created` \
                          by Mailpit, so the numbers are valid only when they share \
                          a host clock (same machine or NTP).";

const AFTER_HELP: &str = "\
The beacon-loadgen run must be finished and Postgres + Mailpit reachable; this tool
sends no events and changes nothing.

Env:
  DATABASE_URL  required, e.g. postgres://beacon:beacon@localhost:5432/beacon
  MAILPIT_URL   default http://localhost:8025 (also --mailpit-url)

Exit status:
  0  clean — every measured job sent; nothing missing, unsent or dead
  1  the reconciliation found something unsent, dead or missing
  2  usage error, no alerts matched, or Postgres/Mailpit unreachable";

// ---- arguments -------------------------------------------------------------------------------------

#[derive(Debug, Parser)]
#[command(name = "delivery_latency", after_help = AFTER_HELP)]
struct Args {
    /// 12-hex run id printed by beacon-loadgen in its report title
    /// (`# Load test \`scenario\` (<run id>)`). Selects the alerts whose title,
    /// body or fields contain it. Required unless --since is given; with --since
    /// it is an extra filter.
    #[arg(long)]
    run_id: Option<String>,
    /// RFC3339 lower bound on alerts.first_seen: the alternative to --run-id, or
    /// an extra filter when both are given.
    #[arg(long, value_parser = parse_rfc3339)]
    since: Option<DateTime<Utc>>,
    /// RFC3339 upper bound on alerts.first_seen.
    #[arg(long, value_parser = parse_rfc3339)]
    until: Option<DateTime<Utc>>,
    /// Poll every 1 s until no measured job of the run is still unsent, at most
    /// this many seconds. The drain time is reported either way.
    #[arg(long, default_value_t = 60)]
    wait_secs: u64,
    /// Mailpit base URL (env MAILPIT_URL).
    #[arg(long, env = "MAILPIT_URL", default_value = "http://localhost:8025")]
    mailpit_url: String,
    /// Also write the markdown report to this path.
    #[arg(long)]
    out: Option<String>,
    /// Also write the same data as JSON to this path.
    #[arg(long)]
    json: Option<String>,
    /// Scenario label for the report title.
    #[arg(long, default_value = "delivery")]
    scenario: String,
    /// Free text copied into the report verbatim (e.g. the beacon-loadgen
    /// command line).
    #[arg(long)]
    workload: Option<String>,
}

/// `--since`/`--until` values, RFC3339 (clap value parser).
fn parse_rfc3339(raw: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(raw)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| format!("{raw:?} is not a valid RFC3339 timestamp: {e}"))
}

/// Selection and window sanity: `--run-id` xor/and `--since`, and a window that is
/// not empty. Pure, so the rules are unit-testable.
fn validate(args: &Args) -> Result<(), String> {
    if args.run_id.is_none() && args.since.is_none() {
        return Err("either --run-id or --since is required (see --help)".to_string());
    }
    if let Some(run) = args.run_id.as_deref()
        && run.is_empty()
    {
        return Err("--run-id must not be empty".to_string());
    }
    if let (Some(since), Some(until)) = (args.since, args.until)
        && since > until
    {
        return Err(format!(
            "--since ({}) is after --until ({})",
            fmt_utc(since),
            fmt_utc(until)
        ));
    }
    Ok(())
}

/// A run id containing a LIKE wildcard would match more than the literal string;
/// worth a warning, not an error.
fn like_wildcard_note(run_id: &str) -> Option<&'static str> {
    if run_id.contains('%') || run_id.contains('_') {
        Some(
            "the run id contains a LIKE wildcard (% or _): it matches more than the literal string",
        )
    } else {
        None
    }
}

// ---- entry ----------------------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Err(err) = validate(&args) {
        eprintln!("delivery_latency: {err}");
        std::process::exit(2);
    }
    if let Some(note) = args.run_id.as_deref().and_then(like_wildcard_note) {
        eprintln!("delivery_latency: warning: {note}");
    }
    match run(&args).await {
        // 0 only if nothing is missing, unsent or dead.
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(err) => {
            eprintln!("delivery_latency: {err}");
            std::process::exit(2);
        }
    }
}

async fn run(args: &Args) -> Result<bool, String> {
    let db_url = std::env::var("DATABASE_URL").map_err(|_| {
        "DATABASE_URL is not set (e.g. postgres://beacon:beacon@localhost:5432/beacon)".to_string()
    })?;
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

    // ---- select the run's alerts, then wait for its jobs to drain ------------------------
    // Each poll re-runs the selection: alerts and their first wave of jobs can still
    // be appearing while the router works off its outbox backlog.
    eprintln!(
        "delivery_latency: scenario {:?}, run id {:?}, window {:?}..{:?}; waiting up to {} s",
        args.scenario, args.run_id, args.since, args.until, args.wait_secs
    );
    let drain_start = Instant::now();
    let wait = Duration::from_secs(args.wait_secs);
    // Assigned by the first poll; the loop's only exit (break) follows the
    // assignments, so the report always sees the last poll's values.
    let (mut alerts, mut jobs, mut unsent): (Vec<AlertRow>, Vec<Job>, usize);
    let mut first_poll = true;
    loop {
        alerts = select_alerts(&pool, args.run_id.as_deref(), args.since, args.until).await?;
        if alerts.is_empty() {
            return Err(empty_selection_message(args));
        }
        if first_poll {
            // Printed once, right when matching happens; the report carries the
            // final (possibly larger) counts.
            eprintln!("alerts matched per rule_id:{}", rule_counts(&alerts));
            first_poll = false;
        }
        let alert_ids: Vec<Uuid> = alerts.iter().map(|(id, _, _)| *id).collect();
        jobs = select_jobs(&pool, &alert_ids).await?;
        unsent = jobs.iter().filter(|j| is_unsent(j)).count();
        if unsent == 0 || drain_start.elapsed() >= wait {
            break;
        }
        eprintln!(
            "[t+{:>3}s] alerts={} jobs={} unsent={unsent}",
            drain_start.elapsed().as_secs(),
            alerts.len(),
            jobs.len()
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let drain_secs = drain_start.elapsed().as_secs_f64();
    let drain_timed_out = unsent > 0;
    if drain_timed_out {
        eprintln!(
            "drain: gave up after {drain_secs:.1} s; {unsent} job(s) still unsent (see the report)"
        );
    } else {
        eprintln!("drain: {drain_secs:.1} s — nothing unsent");
    }

    // ---- receipts: the fake gateway in Postgres, Mailpit over HTTP -----------------------
    let job_ids: Vec<Uuid> = jobs.iter().map(|j| j.id).collect();
    let receipts = select_receipts(&pool, &job_ids).await?;
    let email_ids: HashSet<Uuid> = jobs
        .iter()
        .filter(|j| j.channel == "email")
        .map(|j| j.id)
        .collect();
    let mail = if email_ids.is_empty() {
        eprintln!("no email jobs in the selection; Mailpit not read");
        HashMap::new()
    } else {
        mailpit_created(&http, &args.mailpit_url, &email_ids).await?
    };

    // ---- fold, render, write -------------------------------------------------------------
    let data = fold(&alerts, &jobs, &receipts, &mail);
    let facts = RunFacts {
        scenario: args.scenario.clone(),
        run_id: args.run_id.clone(),
        since: args.since,
        until: args.until,
        workload_text: args.workload.clone(),
        git_commit: git_commit(),
        host_cpus: std::thread::available_parallelism().map_or(0, |n| n.get()),
        generated_at: Utc::now(),
        drain_secs,
        drain_timed_out,
        unsent_at_stop: unsent,
    };
    let md = render_report(&facts, &data);
    println!("{md}");
    if let Some(path) = &args.out {
        std::fs::write(path, md.as_bytes()).map_err(|e| format!("cannot write {path}: {e}"))?;
        eprintln!("report written to {path}");
    }
    if let Some(path) = &args.json {
        let text = serde_json::to_string_pretty(&report_json(&facts, &data))
            .map_err(|e| format!("cannot serialize the json report: {e}"))?;
        std::fs::write(path, text.as_bytes()).map_err(|e| format!("cannot write {path}: {e}"))?;
        eprintln!("json written to {path}");
    }
    Ok(data.pass())
}

/// The error for an empty selection: why there is nothing to measure.
fn empty_selection_message(args: &Args) -> String {
    let mut msg = "no alerts matched".to_string();
    if let Some(run) = &args.run_id {
        msg.push_str(&format!(" run id {run:?}"));
    }
    if let Some(since) = args.since {
        msg.push_str(&format!(" (first_seen >= {})", fmt_utc(since)));
    }
    if let Some(until) = args.until {
        msg.push_str(&format!(" (first_seen <= {})", fmt_utc(until)));
    }
    msg.push_str(
        " — nothing to measure: the loadgen run may not have finished ingesting, its \
         events may not match any rule, or the run id / window is wrong",
    );
    msg
}

/// `  rule: N` lines for the stderr progress print.
fn rule_counts(alerts: &[AlertRow]) -> String {
    let mut per_rule: BTreeMap<&str, usize> = BTreeMap::new();
    for (_, rule, _) in alerts {
        *per_rule.entry(rule.as_str()).or_insert(0) += 1;
    }
    let mut out = String::new();
    for (rule, n) in per_rule {
        out.push_str(&format!("\n  {rule}: {n}"));
    }
    out
}

// ---- stack reads (all SQL parameterised; values are never formatted into it) ------------------------

/// One run alert: (id, rule_id, first_seen).
type AlertRow = (Uuid, String, DateTime<Utc>);

/// The run's alerts: `title`, `body` or `fields::text` containing the run id
/// (`LIKE '%' || $1 || '%'`, parameterised), bounded by the optional window. A
/// NULL bound disables its filter, so this stays one static statement.
async fn select_alerts(
    pool: &sqlx::PgPool,
    run_id: Option<&str>,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
) -> Result<Vec<AlertRow>, String> {
    let sql = "\
        SELECT id, rule_id, first_seen FROM alerts \
        WHERE ($1::text IS NULL OR \
               title LIKE '%' || $1 || '%' OR \
               body LIKE '%' || $1 || '%' OR \
               fields::text LIKE '%' || $1 || '%') \
          AND ($2::timestamptz IS NULL OR first_seen >= $2) \
          AND ($3::timestamptz IS NULL OR first_seen <= $3)";
    sqlx::query_as(sql)
        .bind(run_id)
        .bind(since)
        .bind(until)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("alerts query failed: {e}"))
}

/// One measured delivery job.
#[derive(Debug, Clone)]
struct Job {
    id: Uuid,
    alert_id: Uuid,
    channel: String,
    status: String,
    attempts: i32,
    sent_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

/// One `delivery_jobs` row as read: (id, alert_id, channel, status, attempts,
/// sent_at, created_at).
type JobRow = (
    Uuid,
    Uuid,
    String,
    String,
    i32,
    Option<DateTime<Utc>>,
    DateTime<Utc>,
);

/// The alert's FIRST notification only: the lowest `notify_seq` among all its
/// `delivery_jobs` rows (whatever the channel or escalation step — later waves only
/// increase it), plain notification rows (`escalation_step = 0`), on the channels we
/// measure.
async fn select_jobs(pool: &sqlx::PgPool, alert_ids: &[Uuid]) -> Result<Vec<Job>, String> {
    let sql = "\
        WITH first_seq AS ( \
            SELECT alert_id, MIN(notify_seq) AS notify_seq \
            FROM delivery_jobs WHERE alert_id = ANY($1) GROUP BY alert_id) \
        SELECT j.id, j.alert_id, j.channel, j.status, j.attempts, j.sent_at, j.created_at \
        FROM delivery_jobs j \
        JOIN first_seq f ON f.alert_id = j.alert_id AND f.notify_seq = j.notify_seq \
        WHERE j.channel = ANY($2) AND j.escalation_step = 0";
    let channels: Vec<String> = CHANNELS.iter().map(|c| c.to_string()).collect();
    let rows: Vec<JobRow> = sqlx::query_as(sql)
        .bind(alert_ids.to_vec())
        .bind(channels)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("delivery jobs query failed: {e}"))?;
    Ok(rows
        .into_iter()
        .map(
            |(id, alert_id, channel, status, attempts, sent_at, created_at)| Job {
                id,
                alert_id,
                channel,
                status,
                attempts,
                sent_at,
                created_at,
            },
        )
        .collect())
}

/// One fake-gateway receipt (`gateway_receipts.job_id` is unique).
#[derive(Debug, Clone, Copy)]
struct Receipt {
    received_at: DateTime<Utc>,
    duplicates: i32,
}

/// The gateway receipts of the measured jobs (sms and phone; the table's CHECK
/// constraint only allows those two channels).
async fn select_receipts(
    pool: &sqlx::PgPool,
    job_ids: &[Uuid],
) -> Result<HashMap<Uuid, Receipt>, String> {
    let sql = "SELECT job_id, received_at, duplicates FROM gateway_receipts WHERE job_id = ANY($1)";
    let rows: Vec<(Uuid, DateTime<Utc>, i32)> = sqlx::query_as(sql)
        .bind(job_ids.to_vec())
        .fetch_all(pool)
        .await
        .map_err(|e| format!("gateway receipts query failed: {e}"))?;
    Ok(rows
        .into_iter()
        .map(|(job, received_at, duplicates)| {
            (
                job,
                Receipt {
                    received_at,
                    duplicates,
                },
            )
        })
        .collect())
}

// ---- Mailpit ---------------------------------------------------------------------------------------

/// The wanted email jobs' message `Created` times, by paging `GET /api/v1/messages`
/// (newest first) until every wanted job id is found or the list ends. Message
/// bodies are never fetched: the list endpoint's summary already carries `MessageID`
/// (the `<{job_id}@beacon>` header) and `Created`.
async fn mailpit_created(
    http: &reqwest::Client,
    mailpit_url: &str,
    wanted: &HashSet<Uuid>,
) -> Result<HashMap<Uuid, DateTime<Utc>>, String> {
    if wanted.is_empty() {
        return Ok(HashMap::new());
    }
    let base = format!("{}/api/v1/messages", mailpit_url.trim_end_matches('/'));
    let mut found: HashMap<Uuid, DateTime<Utc>> = HashMap::new();
    let mut start = 0usize;
    for page in 0..MAILPIT_MAX_PAGES {
        let resp = http
            .get(&base)
            .query(&[("start", start), ("limit", MAILPIT_PAGE)])
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| {
                format!(
                    "cannot read Mailpit at {base}: {e} — email reconciliation needs it \
                     (is MAILPIT_URL right?)"
                )
            })?;
        if !resp.status().is_success() {
            return Err(format!(
                "Mailpit answered HTTP {} for {base} — email reconciliation needs it",
                resp.status()
            ));
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| format!("Mailpit list response is not JSON: {e}"))?;
        let messages = body["messages"].as_array().cloned().unwrap_or_default();
        for message in &messages {
            if let Some((job, created)) = parse_mailpit_message(message)
                && wanted.contains(&job)
                && !found.contains_key(&job)
            {
                found.insert(job, created);
            }
        }
        // Advance by what the server actually returned, so a server-side cap below
        // our page size cannot make the paging skip messages.
        let fetched = start + messages.len();
        if found.len() == wanted.len() || messages.is_empty() {
            return Ok(found);
        }
        // `total` (newer Mailpit) / `messages_count` (older): the whole list's size.
        if let Some(total) = body["total"]
            .as_u64()
            .or_else(|| body["messages_count"].as_u64())
            && fetched >= total as usize
        {
            return Ok(found);
        }
        start = fetched;
        if page + 1 == MAILPIT_MAX_PAGES {
            eprintln!(
                "delivery_latency: warning: Mailpit paging stopped at {} pages; {}/{} wanted \
                 message id(s) found — the rest show up as missing",
                MAILPIT_MAX_PAGES,
                found.len(),
                wanted.len()
            );
        }
    }
    Ok(found)
}

/// (job id, Created) from one Mailpit list summary: `MessageID` is the raw
/// `Message-ID` header (`<{job_id}@beacon>`), `Created` is RFC3339. Pure.
fn parse_mailpit_message(message: &Value) -> Option<(Uuid, DateTime<Utc>)> {
    let job = message_id_job(message["MessageID"].as_str()?)?;
    let created = message["Created"].as_str()?;
    let time = DateTime::parse_from_rfc3339(created).ok()?;
    Some((job, time.with_timezone(&Utc)))
}

/// The job id inside a `Message-ID` of `<{job_id}@beacon>` — angle brackets stripped
/// when present. Pure.
fn message_id_job(raw: &str) -> Option<Uuid> {
    let inner = raw.trim().trim_matches(|c| c == '<' || c == '>');
    let local = inner.split('@').next()?;
    Uuid::parse_str(local).ok()
}

// ---- folding (pure) --------------------------------------------------------------------------------

/// Signed milliseconds from `a` to `b` on the wall clock.
fn ms_between(a: DateTime<Utc>, b: DateTime<Utc>) -> i64 {
    (b - a).num_milliseconds()
}

/// A job still waiting to happen: `sent_at IS NULL` and not dead (`dead` is this
/// schema's failed terminal state — there is no separate `failed` status). A
/// cancelled job counts as unsent: the alert's first notification never went out on
/// that channel.
fn is_unsent(job: &Job) -> bool {
    job.sent_at.is_none() && job.status != "dead"
}

/// Reconciliation counts for one channel.
#[derive(Debug, Default, PartialEq, Eq)]
struct Recon {
    expected: usize,
    sent: usize,
    /// pending, in_flight or cancelled (see [`is_unsent`]).
    unsent: usize,
    cancelled: usize,
    dead: usize,
    /// Sent but no gateway receipt (sms, phone) or no Mailpit message (email).
    missing: usize,
    /// SUM(gateway_receipts.duplicates) over the channel's jobs (n/a for email).
    duplicates: i64,
    attempts_over_one: usize,
}

/// One channel's samples and reconciliation counts.
#[derive(Debug, Default)]
struct ChannelData {
    recon: Recon,
    /// `created_at − first_seen`: ingest → job planned.
    planned: Vec<i64>,
    /// `sent_at − first_seen`: ingest → provider ack.
    ack: Vec<i64>,
    /// `received_at − first_seen`: ingest → gateway receipt (sms, phone).
    gateway: Vec<i64>,
    /// Mailpit `Created − first_seen`: ingest → message created (email).
    mailpit: Vec<i64>,
}

/// Everything the report needs, folded from the final snapshot.
struct ReportData {
    alerts_total: usize,
    /// Alerts with no measured job at all: ui-only rules, a throttle, or routing
    /// that had not happened yet. Informational — it is not a channel count.
    alerts_without_jobs: usize,
    per_rule: BTreeMap<String, usize>,
    channels: HashMap<String, ChannelData>,
}

impl ReportData {
    fn channel(&self, name: &str) -> &ChannelData {
        static EMPTY: ChannelData = ChannelData {
            recon: Recon {
                expected: 0,
                sent: 0,
                unsent: 0,
                cancelled: 0,
                dead: 0,
                missing: 0,
                duplicates: 0,
                attempts_over_one: 0,
            },
            planned: Vec::new(),
            ack: Vec::new(),
            gateway: Vec::new(),
            mailpit: Vec::new(),
        };
        self.channels.get(name).unwrap_or(&EMPTY)
    }

    /// (measured jobs, unsent, dead, missing) across the channels.
    fn totals(&self) -> (usize, usize, usize, usize) {
        let (mut jobs, mut unsent, mut dead, mut missing) = (0, 0, 0, 0);
        for name in CHANNELS {
            let r = &self.channel(name).recon;
            jobs += r.expected;
            unsent += r.unsent;
            dead += r.dead;
            missing += r.missing;
        }
        (jobs, unsent, dead, missing)
    }

    /// Nothing missing, unsent or dead — and at least one job was measured (an
    /// empty selection of jobs cannot be a clean reconciliation).
    fn pass(&self) -> bool {
        let (jobs, unsent, dead, missing) = self.totals();
        jobs > 0 && unsent == 0 && dead == 0 && missing == 0
    }
}

/// Fold the final snapshot into [`ReportData`]. Pure: every number in the report
/// comes from here, so the whole report shape is unit-testable.
fn fold(
    alerts: &[AlertRow],
    jobs: &[Job],
    receipts: &HashMap<Uuid, Receipt>,
    mail: &HashMap<Uuid, DateTime<Utc>>,
) -> ReportData {
    let first_seen: HashMap<Uuid, DateTime<Utc>> =
        alerts.iter().map(|(id, _, fs)| (*id, *fs)).collect();
    let mut per_rule: BTreeMap<String, usize> = BTreeMap::new();
    for (_, rule, _) in alerts {
        *per_rule.entry(rule.clone()).or_insert(0) += 1;
    }
    let mut channels: HashMap<String, ChannelData> = HashMap::new();
    let mut with_jobs: HashSet<Uuid> = HashSet::new();

    for job in jobs {
        // The jobs query already filters to CHANNELS; this keeps fold honest on
        // anything hand-fed to it (tests, future channels).
        if !CHANNELS.contains(&job.channel.as_str()) {
            continue;
        }
        let Some(fs) = first_seen.get(&job.alert_id) else {
            continue;
        };
        with_jobs.insert(job.alert_id);
        let ch = channels.entry(job.channel.clone()).or_default();
        ch.recon.expected += 1;
        if job.attempts > 1 {
            ch.recon.attempts_over_one += 1;
        }
        // Duplicates are the gateway's own counter for the job, whatever the job's
        // current state (an ambiguous retry can leave a receipt behind).
        if let Some(receipt) = receipts.get(&job.id) {
            ch.recon.duplicates += i64::from(receipt.duplicates);
        }
        ch.planned.push(ms_between(*fs, job.created_at));
        match job.sent_at {
            Some(sent_at) => {
                ch.recon.sent += 1;
                ch.ack.push(ms_between(*fs, sent_at));
                match job.channel.as_str() {
                    "sms" | "phone" => match receipts.get(&job.id) {
                        Some(receipt) => ch.gateway.push(ms_between(*fs, receipt.received_at)),
                        None => ch.recon.missing += 1,
                    },
                    "email" => match mail.get(&job.id) {
                        Some(created) => ch.mailpit.push(ms_between(*fs, *created)),
                        None => ch.recon.missing += 1,
                    },
                    _ => {}
                }
            }
            None if job.status == "dead" => ch.recon.dead += 1,
            None => {
                ch.recon.unsent += 1;
                if job.status == "cancelled" {
                    ch.recon.cancelled += 1;
                }
            }
        }
    }

    ReportData {
        alerts_total: alerts.len(),
        alerts_without_jobs: alerts.len() - with_jobs.len(),
        per_rule,
        channels,
    }
}

// ---- percentiles ----------------------------------------------------------------------------------

/// Nearest-rank percentile of ascending `samples`: the ceil(p·n/100)-th value,
/// 1-based. Integer math, so there is no rounding at the rank.
fn percentile(sorted: &[i64], p: u32) -> Option<i64> {
    if sorted.is_empty() {
        return None;
    }
    let n = sorted.len() as u64;
    let rank = (u64::from(p) * n).div_ceil(100).min(n);
    Some(sorted[(rank - 1) as usize])
}

/// One histogram: count + nearest-rank p50/p90/p99 + max, in ms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MetricStats {
    count: usize,
    p50: i64,
    p90: i64,
    p99: i64,
    max: i64,
}

/// Fold a sample set into [`MetricStats`] (sorts a copy); `None` when empty.
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

// ---- report rendering (pure) -----------------------------------------------------------------------

/// The non-pure facts a report carries (timestamps, host, git, CLI strings).
struct RunFacts {
    scenario: String,
    run_id: Option<String>,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    workload_text: Option<String>,
    git_commit: String,
    host_cpus: usize,
    generated_at: DateTime<Utc>,
    drain_secs: f64,
    drain_timed_out: bool,
    unsent_at_stop: usize,
}

/// Compact RFC3339, UTC, no sub-second digits.
fn fmt_utc(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// How the run's alerts were selected, as one line.
fn selection_line(f: &RunFacts) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(run) = &f.run_id {
        parts.push(format!("run id `{run}` in alerts.title/body/fields"));
    }
    match (f.since, f.until) {
        (Some(since), Some(until)) => {
            parts.push(format!(
                "first_seen {} .. {}",
                fmt_utc(since),
                fmt_utc(until)
            ));
        }
        (Some(since), None) => parts.push(format!("first_seen >= {}", fmt_utc(since))),
        (None, Some(until)) => parts.push(format!("first_seen <= {}", fmt_utc(until))),
        (None, None) => {}
    }
    parts.join("; ")
}

fn report_title(f: &RunFacts) -> String {
    match &f.run_id {
        Some(run) => format!("# Delivery latency: `{}` (run `{run}`)", f.scenario),
        None => format!(
            "# Delivery latency: `{}` ({})",
            f.scenario,
            selection_line(f)
        ),
    }
}

fn drain_line(f: &RunFacts) -> String {
    if f.drain_timed_out {
        format!(
            "gave up after {:.1} s — {} job(s) still unsent",
            f.drain_secs, f.unsent_at_stop
        )
    } else {
        format!("{:.1} s — nothing unsent", f.drain_secs)
    }
}

/// One row of the reconciliation table.
fn recon_row(name: &str, recon: &Recon) -> String {
    // The fake gateway only serves sms/phone, so email's duplicates column is n/a
    // (the table's CHECK constraint admits no email receipts).
    let duplicates = if name == "email" {
        "n/a".to_string()
    } else {
        recon.duplicates.to_string()
    };
    format!(
        "| {name} | {} | {} | {} | {} | {} | {duplicates} | {} |",
        recon.expected,
        recon.sent,
        recon.unsent,
        recon.dead,
        recon.missing,
        recon.attempts_over_one
    )
}

/// One row of the latency table (rows are metric × channel).
fn latency_row(metric: &str, name: &str, samples: &[i64]) -> String {
    match metric_stats(samples) {
        Some(s) => format!(
            "| {metric} | {name} | {} | {} | {} | {} | {} |",
            s.count, s.p50, s.p90, s.p99, s.max
        ),
        None => format!("| {metric} | {name} | 0 | n/a | n/a | n/a | n/a |"),
    }
}

/// PASS/FAIL with the totals spelled out.
fn result_line(d: &ReportData) -> String {
    let (jobs, unsent, dead, missing) = d.totals();
    if jobs == 0 {
        return "FAIL — no sms/phone/email delivery jobs in the selection (ui-only rules, a \
                throttle, or routing that never happened)"
            .to_string();
    }
    if d.pass() {
        return "PASS — every sms/phone/email job sent; nothing missing, unsent or dead"
            .to_string();
    }
    format!("FAIL — {unsent} unsent, {dead} dead, {missing} missing (see the reconciliation table)")
}

/// The full markdown report: title with scenario + run id, workload section, the
/// reconciliation table, then one latency table (rows = metric × channel).
fn render_report(f: &RunFacts, d: &ReportData) -> String {
    let mut out = String::new();
    out.push_str(&report_title(f));
    out.push_str("\n\n## Workload\n\n");
    match &f.workload_text {
        Some(text) => {
            out.push_str(text);
            out.push_str("\n\n");
        }
        None => out.push_str("(no --workload text given)\n\n"),
    }
    out.push_str("| fact | value |\n|---|---|\n");
    out.push_str(&format!("| git commit | `{}` |\n", f.git_commit));
    out.push_str(&format!("| host CPUs | {} |\n", f.host_cpus));
    out.push_str(&format!(
        "| generated at (UTC) | {} |\n",
        f.generated_at.to_rfc3339_opts(SecondsFormat::Millis, true)
    ));
    out.push_str(&format!("| drain | {} |\n", drain_line(f)));
    out.push_str(&format!("| selection | {} |\n\n", selection_line(f)));
    out.push_str(&format!("Alerts matched: {}", d.alerts_total));
    if d.alerts_without_jobs > 0 {
        out.push_str(&format!(
            " ({} without sms/phone/email jobs)",
            d.alerts_without_jobs
        ));
    }
    out.push_str("\n\n");
    if !d.per_rule.is_empty() {
        out.push_str("| rule | alerts |\n|---|---:|\n");
        for (rule, n) in &d.per_rule {
            out.push_str(&format!("| {rule} | {n} |\n"));
        }
        out.push('\n');
    }

    out.push_str("## Reconciliation\n\n");
    out.push_str(
        "Jobs: the alerts' first notification only (lowest `notify_seq` per alert, \
         `escalation_step = 0`), channels sms/phone/email.\n\n",
    );
    out.push_str(
        "| channel | expected | sent | unsent | dead | missing | duplicates | attempts>1 |\n\
         |---|---:|---:|---:|---:|---:|---:|---:|\n",
    );
    for name in CHANNELS {
        out.push_str(&recon_row(name, &d.channel(name).recon));
        out.push('\n');
    }
    out.push('\n');
    out.push_str(
        "missing = sent but no gateway receipt (sms, phone) or no Mailpit message (email); \
         unsent = `sent_at IS NULL` and status not dead (pending, in_flight, cancelled); \
         duplicates = SUM(`gateway_receipts.duplicates`).\n\n",
    );

    out.push_str("## Latency (ms)\n\n");
    out.push_str(
        "| metric | channel | count | p50 | p90 | p99 | max |\n\
         |---|---|---:|---:|---:|---:|---:|\n",
    );
    for name in ["sms", "phone"] {
        out.push_str(&latency_row(
            "gateway acceptance: ingest → receipt (`received_at − first_seen`)",
            name,
            &d.channel(name).gateway,
        ));
        out.push('\n');
    }
    out.push_str(&latency_row(
        "email in Mailpit: ingest → message (`Created − first_seen`)",
        "email",
        &d.channel("email").mailpit,
    ));
    out.push('\n');
    for name in CHANNELS {
        out.push_str(&latency_row(
            "internal: ingest → job planned (`created_at − first_seen`)",
            name,
            &d.channel(name).planned,
        ));
        out.push('\n');
    }
    for name in CHANNELS {
        out.push_str(&latency_row(
            "internal: ingest → provider ack (`sent_at − first_seen`)",
            name,
            &d.channel(name).ack,
        ));
        out.push('\n');
    }
    out.push('\n');
    out.push_str(CLOCK_NOTE);
    out.push_str("\n\n");
    out.push_str(&format!("RESULT: {}\n", result_line(d)));
    out
}

// ---- the same data as JSON -------------------------------------------------------------------------

/// One histogram as JSON: the report's stats plus the raw samples (sorted, so the
/// file is diffable and other percentiles can be derived later).
fn histogram_json(samples: &[i64]) -> Value {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    match metric_stats(samples) {
        Some(s) => json!({
            "count": s.count,
            "p50": s.p50,
            "p90": s.p90,
            "p99": s.p99,
            "max": s.max,
            "samples_ms": sorted,
        }),
        None => json!({
            "count": 0,
            "p50": null,
            "p90": null,
            "p99": null,
            "max": null,
            "samples_ms": sorted,
        }),
    }
}

fn recon_json(recon: &Recon, name: &str) -> Value {
    json!({
        "expected": recon.expected,
        "sent": recon.sent,
        "unsent": recon.unsent,
        "cancelled": recon.cancelled,
        "dead": recon.dead,
        "missing": recon.missing,
        // The fake gateway serves sms/phone only; email can never have receipts.
        "duplicates": if name == "email" { Value::Null } else { json!(recon.duplicates) },
        "attempts_over_one": recon.attempts_over_one,
    })
}

/// The same data as the markdown report, as JSON (`--json`).
fn report_json(f: &RunFacts, d: &ReportData) -> Value {
    let mut per_rule = serde_json::Map::new();
    for (rule, n) in &d.per_rule {
        per_rule.insert(rule.clone(), json!(n));
    }
    let mut reconciliation = serde_json::Map::new();
    for name in CHANNELS {
        reconciliation.insert(name.to_string(), recon_json(&d.channel(name).recon, name));
    }
    let mut gateway = serde_json::Map::new();
    for name in ["sms", "phone"] {
        gateway.insert(name.to_string(), histogram_json(&d.channel(name).gateway));
    }
    let mut mailpit = serde_json::Map::new();
    mailpit.insert("email".into(), histogram_json(&d.channel("email").mailpit));
    let mut planned = serde_json::Map::new();
    let mut ack = serde_json::Map::new();
    for name in CHANNELS {
        planned.insert(name.to_string(), histogram_json(&d.channel(name).planned));
        ack.insert(name.to_string(), histogram_json(&d.channel(name).ack));
    }
    json!({
        "tool": "delivery_latency",
        "scenario": f.scenario,
        "run_id": f.run_id,
        "since": f.since.map(fmt_utc),
        "until": f.until.map(fmt_utc),
        "selection": selection_line(f),
        "generated_at": f.generated_at.to_rfc3339_opts(SecondsFormat::Millis, true),
        "workload_text": f.workload_text,
        "git_commit": f.git_commit,
        "host_cpus": f.host_cpus,
        "drain": {
            "secs": f.drain_secs,
            "timed_out": f.drain_timed_out,
            "unsent_at_stop": f.unsent_at_stop,
        },
        "alerts": {
            "total": d.alerts_total,
            "without_delivery_jobs": d.alerts_without_jobs,
            "per_rule": per_rule,
        },
        "reconciliation": reconciliation,
        "latency_ms": {
            "gateway_acceptance": gateway,
            "email_in_mailpit": mailpit,
            "ingest_to_job_planned": planned,
            "ingest_to_provider_ack": ack,
        },
        "pass": d.pass(),
    })
}

// ---- host facts ------------------------------------------------------------------------------------

/// `git rev-parse --short HEAD` from the repo (or the manifest's parents), or
/// "unknown" when git is missing / the checkout is gone.
fn git_commit() -> String {
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..");
    for dir in [None, Some(manifest_root.as_path())] {
        let mut cmd = std::process::Command::new("git");
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

    fn job(
        alert: Uuid,
        channel: &str,
        status: &str,
        attempts: i32,
        sent_at: Option<DateTime<Utc>>,
        created_at: DateTime<Utc>,
    ) -> Job {
        Job {
            id: Uuid::new_v4(),
            alert_id: alert,
            channel: channel.into(),
            status: status.into(),
            attempts,
            sent_at,
            created_at,
        }
    }

    fn args(run_id: Option<&str>) -> Args {
        Args {
            run_id: run_id.map(str::to_string),
            since: None,
            until: None,
            wait_secs: 60,
            mailpit_url: "http://localhost:8025".into(),
            out: None,
            json: None,
            scenario: "delivery".into(),
            workload: None,
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
    fn milliseconds_between_are_signed() {
        assert_eq!(ms_between(dt(1_000), dt(2_500)), 1_500);
        assert_eq!(ms_between(dt(2_500), dt(1_000)), -1_500);
        assert_eq!(ms_between(dt(0), dt(0)), 0);
    }

    #[test]
    fn message_id_job_strips_brackets_and_keeps_the_uuid() {
        let id = Uuid::new_v4();
        assert_eq!(message_id_job(&format!("<{id}@beacon>")), Some(id));
        assert_eq!(message_id_job(&format!("{id}@beacon")), Some(id));
        assert_eq!(message_id_job(&format!("  <{id}@beacon>  ")), Some(id));
        for bad in [
            "",
            "<>",
            "@beacon",
            "not-a-uuid@beacon",
            "<not-a-uuid@beacon>",
            "plain text",
        ] {
            assert_eq!(message_id_job(bad), None, "{bad:?} must not parse");
        }
    }

    #[test]
    fn parse_mailpit_message_reads_id_and_created() {
        let id = Uuid::new_v4();
        let message = json!({
            "ID": "internal-id",
            "MessageID": format!("<{id}@beacon>"),
            "Subject": "Rejected EUR trade",
            "Created": "2026-11-21T12:00:00.123456789Z",
            "Snippet": "no body is ever fetched",
        });
        // Mailpit writes RFC3339Nano; the parse keeps the sub-millisecond digits
        // (only ms_between ever truncates).
        let parsed = parse_mailpit_message(&message).expect("parses");
        assert_eq!(parsed.0, id);
        assert_eq!(parsed.1.timestamp_millis(), 1_795_262_400_123);
        // Missing fields, or a foreign message id, do not yield a sample.
        assert_eq!(parse_mailpit_message(&json!({})), None);
        assert_eq!(parse_mailpit_message(&json!({"MessageID": "<x@y>"})), None);
        assert_eq!(
            parse_mailpit_message(&json!({
                "MessageID": format!("<{id}@beacon>"),
                "Created": "not-a-timestamp",
            })),
            None
        );
    }

    #[test]
    fn rfc3339_args_parse_to_utc() {
        assert_eq!(
            parse_rfc3339("2026-11-21T12:00:00+01:00").unwrap(),
            dt(1_795_258_800_000)
        );
        assert_eq!(
            parse_rfc3339("2026-11-21T12:00:00Z").unwrap(),
            dt(1_795_262_400_000)
        );
        assert!(parse_rfc3339("not-a-time").is_err());
        assert!(parse_rfc3339("").is_err());
    }

    #[test]
    fn validate_requires_a_run_id_or_a_window() {
        assert!(validate(&args(None)).is_err());
        assert!(validate(&args(Some("f47ac10b58cc"))).is_ok());
        let mut window = args(None);
        window.since = Some(dt(0));
        assert!(validate(&window).is_ok());
        // both together is fine, an inverted window is not
        window.run_id = Some("f47ac10b58cc".into());
        window.until = Some(dt(1_000));
        assert!(validate(&window).is_ok());
        window.until = Some(dt(-1));
        assert!(validate(&window).is_err());
        // an empty run id is as good as none, but silently selects nothing
        assert!(validate(&args(Some(""))).is_err());
    }

    #[test]
    fn like_wildcards_in_a_run_id_get_a_warning() {
        assert_eq!(like_wildcard_note("f47ac10b58cc"), None);
        assert!(like_wildcard_note("f47ac1%").is_some());
        assert!(like_wildcard_note("f47_ac1").is_some());
    }

    #[test]
    fn unsent_means_pending_in_flight_or_cancelled() {
        let alert = Uuid::new_v4();
        let pending = job(alert, "sms", "pending", 0, None, dt(0));
        let in_flight = job(alert, "sms", "in_flight", 1, None, dt(0));
        let cancelled = job(alert, "sms", "cancelled", 1, None, dt(0));
        let dead = job(alert, "sms", "dead", 8, None, dt(0));
        let sent = job(alert, "sms", "sent", 1, Some(dt(10)), dt(0));
        for j in [&pending, &in_flight, &cancelled] {
            assert!(is_unsent(j), "{:?} is still waiting to happen", j.status);
        }
        assert!(!is_unsent(&dead), "dead is a terminal failure, not a wait");
        assert!(!is_unsent(&sent));
    }

    /// A clean run: one workflow alert (phone + sms), one trade alert (email), one
    /// ui-only pricing alert with no jobs, and every receipt where it should be.
    fn healthy_fixture() -> (
        Vec<AlertRow>,
        Vec<Job>,
        HashMap<Uuid, Receipt>,
        HashMap<Uuid, DateTime<Utc>>,
    ) {
        let wf = Uuid::new_v4();
        let trade = Uuid::new_v4();
        let ui_only = Uuid::new_v4();
        let alerts = vec![
            (wf, "settlements-workflow-failed".into(), dt(0)),
            (trade, "rates-rejected-trades".into(), dt(0)),
            (ui_only, "pricing-engine-warnings".into(), dt(500)),
        ];
        let phone = job(wf, "phone", "sent", 1, Some(dt(400)), dt(50));
        let sms = job(wf, "sms", "sent", 2, Some(dt(700)), dt(50));
        let email = job(trade, "email", "sent", 1, Some(dt(300)), dt(80));
        let mut receipts = HashMap::new();
        receipts.insert(
            phone.id,
            Receipt {
                received_at: dt(600),
                duplicates: 0,
            },
        );
        receipts.insert(
            sms.id,
            Receipt {
                received_at: dt(900),
                duplicates: 2,
            },
        );
        let mut mail = HashMap::new();
        mail.insert(email.id, dt(500));
        (alerts, vec![phone, sms, email], receipts, mail)
    }

    #[test]
    fn fold_reconciles_a_healthy_run() {
        let (alerts, jobs, receipts, mail) = healthy_fixture();
        let d = fold(&alerts, &jobs, &receipts, &mail);
        assert_eq!(d.alerts_total, 3);
        assert_eq!(d.alerts_without_jobs, 1); // the pricing (ui-only) alert
        assert_eq!(d.per_rule["settlements-workflow-failed"], 1);
        assert_eq!(d.per_rule["rates-rejected-trades"], 1);
        assert_eq!(d.per_rule["pricing-engine-warnings"], 1);

        let sms = d.channel("sms");
        assert_eq!(
            sms.recon,
            Recon {
                expected: 1,
                sent: 1,
                unsent: 0,
                cancelled: 0,
                dead: 0,
                missing: 0,
                duplicates: 2,
                attempts_over_one: 1,
            }
        );
        assert_eq!(sms.planned, [50]);
        assert_eq!(sms.ack, [700]);
        assert_eq!(sms.gateway, [900]);
        let phone = d.channel("phone");
        assert_eq!(phone.recon.expected, 1);
        assert_eq!(phone.recon.sent, 1);
        assert_eq!(phone.recon.duplicates, 0);
        assert_eq!(phone.gateway, [600]);
        let email = d.channel("email");
        assert_eq!(email.recon.sent, 1);
        assert_eq!(email.mailpit, [500]);
        assert_eq!(email.gateway, Vec::<i64>::new());
        assert!(d.pass());
    }

    #[test]
    fn fold_counts_dead_unsent_missing_and_duplicates() {
        let alert = Uuid::new_v4();
        let alerts = vec![(alert, "settlements-workflow-failed".into(), dt(0))];
        let dead = job(alert, "sms", "dead", 8, None, dt(60));
        let pending = job(alert, "phone", "in_flight", 1, None, dt(55));
        let sent_mail_lost = job(alert, "email", "sent", 3, Some(dt(300)), dt(70));
        let cancelled = job(alert, "email", "cancelled", 1, None, dt(70));
        let mut receipts = HashMap::new();
        receipts.insert(
            dead.id,
            Receipt {
                received_at: dt(500), // an ambiguous retry left a receipt behind
                duplicates: 4,
            },
        );
        let jobs = vec![dead, pending, sent_mail_lost, cancelled];
        let d = fold(&alerts, &jobs, &receipts, &HashMap::new());

        let sms = d.channel("sms");
        assert_eq!((sms.recon.dead, sms.recon.unsent), (1, 0));
        assert_eq!(sms.recon.duplicates, 4); // counted even for a dead job
        assert_eq!(sms.gateway, Vec::<i64>::new()); // dead: no latency sample
        let phone = d.channel("phone");
        assert_eq!((phone.recon.unsent, phone.recon.missing), (1, 0));
        let email = d.channel("email");
        assert_eq!(
            (
                email.recon.sent,
                email.recon.unsent,
                email.recon.cancelled,
                email.recon.missing
            ),
            (1, 1, 1, 1)
        );
        assert_eq!(email.recon.attempts_over_one, 1);
        assert_eq!(email.mailpit, Vec::<i64>::new());
        assert!(!d.pass());
        // 2 unsent (phone in flight + email cancelled), 1 dead, 1 missing
        assert_eq!(d.totals(), (4, 2, 1, 1));
    }

    #[test]
    fn an_empty_job_selection_never_passes() {
        let alert = Uuid::new_v4();
        let alerts = vec![(alert, "pricing-engine-warnings".into(), dt(0))];
        let d = fold(&alerts, &[], &HashMap::new(), &HashMap::new());
        assert!(!d.pass());
        assert!(result_line(&d).starts_with("FAIL — no sms/phone/email delivery jobs"));
        assert_eq!(d.alerts_without_jobs, 1);
    }

    fn fixture_facts() -> RunFacts {
        RunFacts {
            scenario: "a-baseline".into(),
            run_id: Some("f47ac10b58cc".into()),
            since: None,
            until: None,
            workload_text: Some(
                "beacon-loadgen --rate 5000 --duration 30 --batch 100 --distinct-keys 2000 \
                 --match-ratio 0.8"
                    .into(),
            ),
            git_commit: "0c16b6e".into(),
            host_cpus: 8,
            generated_at: dt(1_795_262_400_000),
            drain_secs: 12.4,
            drain_timed_out: false,
            unsent_at_stop: 0,
        }
    }

    fn golden_report() -> String {
        let (alerts, jobs, receipts, mail) = healthy_fixture();
        render_report(&fixture_facts(), &fold(&alerts, &jobs, &receipts, &mail))
    }

    #[test]
    fn report_renders_the_golden_fixture() {
        assert_eq!(
            golden_report(),
            "# Delivery latency: `a-baseline` (run `f47ac10b58cc`)\n\
             \n\
             ## Workload\n\
             \n\
             beacon-loadgen --rate 5000 --duration 30 --batch 100 --distinct-keys 2000 --match-ratio 0.8\n\
             \n\
             | fact | value |\n\
             |---|---|\n\
             | git commit | `0c16b6e` |\n\
             | host CPUs | 8 |\n\
             | generated at (UTC) | 2026-11-21T12:00:00.000Z |\n\
             | drain | 12.4 s — nothing unsent |\n\
             | selection | run id `f47ac10b58cc` in alerts.title/body/fields |\n\
             \n\
             Alerts matched: 3 (1 without sms/phone/email jobs)\n\
             \n\
             | rule | alerts |\n\
             |---|---:|\n\
             | pricing-engine-warnings | 1 |\n\
             | rates-rejected-trades | 1 |\n\
             | settlements-workflow-failed | 1 |\n\
             \n\
             ## Reconciliation\n\
             \n\
             Jobs: the alerts' first notification only (lowest `notify_seq` per alert, `escalation_step = 0`), channels sms/phone/email.\n\
             \n\
             | channel | expected | sent | unsent | dead | missing | duplicates | attempts>1 |\n\
             |---|---:|---:|---:|---:|---:|---:|---:|\n\
             | sms | 1 | 1 | 0 | 0 | 0 | 2 | 1 |\n\
             | phone | 1 | 1 | 0 | 0 | 0 | 0 | 0 |\n\
             | email | 1 | 1 | 0 | 0 | 0 | n/a | 0 |\n\
             \n\
             missing = sent but no gateway receipt (sms, phone) or no Mailpit message (email); unsent = `sent_at IS NULL` and status not dead (pending, in_flight, cancelled); duplicates = SUM(`gateway_receipts.duplicates`).\n\
             \n\
             ## Latency (ms)\n\
             \n\
             | metric | channel | count | p50 | p90 | p99 | max |\n\
             |---|---|---:|---:|---:|---:|---:|\n\
             | gateway acceptance: ingest → receipt (`received_at − first_seen`) | sms | 1 | 900 | 900 | 900 | 900 |\n\
             | gateway acceptance: ingest → receipt (`received_at − first_seen`) | phone | 1 | 600 | 600 | 600 | 600 |\n\
             | email in Mailpit: ingest → message (`Created − first_seen`) | email | 1 | 500 | 500 | 500 | 500 |\n\
             | internal: ingest → job planned (`created_at − first_seen`) | sms | 1 | 50 | 50 | 50 | 50 |\n\
             | internal: ingest → job planned (`created_at − first_seen`) | phone | 1 | 50 | 50 | 50 | 50 |\n\
             | internal: ingest → job planned (`created_at − first_seen`) | email | 1 | 80 | 80 | 80 | 80 |\n\
             | internal: ingest → provider ack (`sent_at − first_seen`) | sms | 1 | 700 | 700 | 700 | 700 |\n\
             | internal: ingest → provider ack (`sent_at − first_seen`) | phone | 1 | 400 | 400 | 400 | 400 |\n\
             | internal: ingest → provider ack (`sent_at − first_seen`) | email | 1 | 300 | 300 | 300 | 300 |\n\
             \n\
             Clock assumption: `first_seen` is written by the Beacon processes, `received_at`/`sent_at` by Postgres, `Created` by Mailpit, so the numbers are valid only when they share a host clock (same machine or NTP).\n\
             \n\
             RESULT: PASS — every sms/phone/email job sent; nothing missing, unsent or dead\n"
        );
    }

    #[test]
    fn a_failed_run_spells_out_the_totals() {
        let alert = Uuid::new_v4();
        let alerts = vec![(alert, "settlements-workflow-failed".into(), dt(0))];
        let jobs = vec![
            job(alert, "sms", "dead", 8, None, dt(60)),
            job(alert, "phone", "in_flight", 1, None, dt(55)),
            job(alert, "email", "cancelled", 1, None, dt(70)),
            job(alert, "email", "sent", 3, Some(dt(300)), dt(70)),
        ];
        let d = fold(&alerts, &jobs, &HashMap::new(), &HashMap::new());
        let report = render_report(&fixture_facts(), &d);
        assert!(report.contains("RESULT: FAIL — 2 unsent, 1 dead, 1 missing"));
        // the drain line flips with the wait outcome
        let mut facts = fixture_facts();
        facts.drain_timed_out = true;
        facts.unsent_at_stop = 3;
        let report = render_report(&facts, &d);
        assert!(report.contains("| drain | gave up after 12.4 s — 3 job(s) still unsent |"));
        // and a window-only selection says so in the title and the facts
        let mut facts = fixture_facts();
        facts.run_id = None;
        facts.since = Some(dt(1_795_262_400_000));
        facts.until = Some(dt(1_795_262_460_000));
        facts.workload_text = None;
        let report = render_report(&facts, &d);
        assert!(report.contains(&format!(
            "# Delivery latency: `a-baseline` (first_seen {} .. {})",
            fmt_utc(dt(1_795_262_400_000)),
            fmt_utc(dt(1_795_262_460_000))
        )));
        assert!(report.contains("(no --workload text given)"));
        assert!(
            report.contains(
                "| selection | first_seen 2026-11-21T12:00:00Z .. 2026-11-21T12:01:00Z |"
            )
        );
    }

    #[test]
    fn json_carries_the_same_data_as_the_report() {
        let (alerts, jobs, receipts, mail) = healthy_fixture();
        let f = fixture_facts();
        let d = fold(&alerts, &jobs, &receipts, &mail);
        let j = report_json(&f, &d);

        assert_eq!(j["tool"], "delivery_latency");
        assert_eq!(j["scenario"], "a-baseline");
        assert_eq!(j["run_id"], "f47ac10b58cc");
        assert_eq!(j["since"], Value::Null);
        assert_eq!(
            j["workload_text"],
            "beacon-loadgen --rate 5000 --duration 30 --batch 100 --distinct-keys 2000 \
             --match-ratio 0.8"
        );
        assert_eq!(j["git_commit"], "0c16b6e");
        assert_eq!(j["host_cpus"], 8);
        assert_eq!(j["generated_at"], "2026-11-21T12:00:00.000Z");
        assert_eq!(j["drain"]["secs"], 12.4);
        assert_eq!(j["drain"]["timed_out"], false);
        assert_eq!(j["alerts"]["total"], 3);
        assert_eq!(j["alerts"]["without_delivery_jobs"], 1);
        assert_eq!(j["alerts"]["per_rule"]["pricing-engine-warnings"], 1);
        assert_eq!(j["reconciliation"]["sms"]["expected"], 1);
        assert_eq!(j["reconciliation"]["sms"]["duplicates"], 2);
        assert_eq!(j["reconciliation"]["sms"]["attempts_over_one"], 1);
        assert_eq!(j["reconciliation"]["phone"]["duplicates"], 0);
        assert_eq!(j["reconciliation"]["email"]["duplicates"], Value::Null);
        assert_eq!(
            j["latency_ms"]["gateway_acceptance"]["sms"],
            json!({
                "count": 1, "p50": 900, "p90": 900, "p99": 900, "max": 900,
                "samples_ms": [900],
            })
        );
        assert_eq!(j["latency_ms"]["gateway_acceptance"]["phone"]["p50"], 600);
        assert_eq!(
            j["latency_ms"]["email_in_mailpit"]["email"]["samples_ms"],
            json!([500])
        );
        assert_eq!(
            j["latency_ms"]["ingest_to_job_planned"]["email"]["samples_ms"],
            json!([80])
        );
        assert_eq!(
            j["latency_ms"]["ingest_to_provider_ack"]["phone"]["samples_ms"],
            json!([400])
        );
        assert_eq!(j["pass"], true);
    }

    #[test]
    fn rule_counts_lines_are_stable() {
        let a = Uuid::new_v4();
        let alerts = vec![
            (a, "settlements-workflow-failed".into(), dt(0)),
            (a, "rates-rejected-trades".into(), dt(0)),
            (a, "settlements-workflow-failed".into(), dt(0)),
        ];
        assert_eq!(
            rule_counts(&alerts),
            "\n  rates-rejected-trades: 1\n  settlements-workflow-failed: 2"
        );
    }

    #[test]
    fn empty_selection_message_names_the_filters() {
        let mut a = args(Some("f47ac10b58cc"));
        a.since = Some(dt(0));
        let msg = empty_selection_message(&a);
        assert!(msg.contains("run id \"f47ac10b58cc\""));
        assert!(msg.contains("first_seen >= 1970-01-01T00:00:00Z"));
        assert!(msg.contains("nothing to measure"));
    }
}
