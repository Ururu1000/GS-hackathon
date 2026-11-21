//! Off-hours digest: hold non-critical notifications outside a person's working hours and send
//! them as ONE email when the hours start again (table `offhours_held`, migration
//! `20261121073000_delivery_offhours_digest.sql`).
//!
//! - **Opt-in per person.** A preference rule with a `when` window lists the pseudo-channel
//!   `digest` in `otherwise` (`crate::prefs::DIGEST`). Outside the window the router calls
//!   [`hold`] instead of creating a job; `release_at` is the window's next start.
//! - **Never for critical.** The router sends `digest` as email for a critical alert.
//!   Escalation steps and runbook actions resolve `digest` to email too
//!   (`prefs::resolve_channels`), so nothing urgent waits for the morning.
//! - **Release.** [`release_once`], run by the scheduler role each pass with its current
//!   directory (the email address is read at release, not frozen at hold), picks people with due rows. Per person, in one transaction,
//!   it takes a per-person advisory lock (skipped if another pass holds it) and locks their due rows,
//!   files ONE email job under the latest held row's `(alert_id, notify_seq)` with
//!   `escalation_step = -1` ([`OFFHOURS_DIGEST_STEP`], so it never collides with that
//!   notification's own jobs), and marks the rows released. The job's `route_info` carries `"target": "offhours"`
//!   and `"offhours": {count, alert_ids, held_since, release_at}`; the email worker reads the
//!   alerts' CURRENT status when it sends, so the digest can say which ones resolved meanwhile.
//! - Throttles don't apply to held rows: the digest is already one message per person.

use std::collections::HashSet;

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use tracing::warn;

use crate::config::{Directory, address_for};
use crate::throttle::PlannedJob;

/// `delivery_jobs.escalation_step` of a digest job. 0 is the plain notification, 1.. escalation
/// steps, 100+n the `escalate` action and 200+n approval prompts (`approvals` counts every
/// step >= 200 as a taken prompt slot). A negative value is outside every one of those ranges,
/// so nothing reads the digest as an escalation step or an approval slot.
pub const OFFHOURS_DIGEST_STEP: i32 = -1;

/// `route_info.target` of a digest job.
pub const TARGET: &str = "offhours";

/// Hold one recipient notification until `release_at`. Idempotent per
/// `(alert_id, recipient_id, notify_seq)`; returns whether a row was inserted.
#[allow(clippy::too_many_arguments)]
pub async fn hold(
    tx: &mut Transaction<'_, Postgres>,
    recipient_id: &str,
    alert_id: Uuid,
    notify_seq: i32,
    rule_id: &str,
    route_info: &Value,
    release_at: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let res = sqlx::query(
        "INSERT INTO offhours_held \
           (recipient_id, alert_id, notify_seq, rule_id, route_info, release_at) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (alert_id, recipient_id, notify_seq) DO NOTHING",
    )
    .bind(recipient_id)
    .bind(alert_id)
    .bind(notify_seq)
    .bind(rule_id)
    .bind(route_info)
    .bind(release_at)
    .execute(&mut **tx)
    .await?;
    Ok(res.rows_affected() == 1)
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReleaseStats {
    /// People who got a digest.
    pub digests: u64,
    /// Held rows covered by those digests.
    pub rows_released: u64,
}

/// One pass: send a digest to every person (up to `limit`) with held rows due at `now`.
pub async fn release_once(
    pool: &PgPool,
    dir: &Directory,
    now: DateTime<Utc>,
    limit: i64,
) -> Result<ReleaseStats, sqlx::Error> {
    let people: Vec<String> = sqlx::query_scalar(
        "SELECT recipient_id FROM offhours_held \
         WHERE released_at IS NULL AND release_at <= $1 \
         GROUP BY recipient_id ORDER BY min(release_at), recipient_id LIMIT $2",
    )
    .bind(now)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let mut stats = ReleaseStats::default();
    for recipient_id in &people {
        // One person's failure (a DB error mid-release) must not block everyone ordered after
        // them: log it and go on; their rows stay held and the next pass retries.
        match release_person(pool, dir, recipient_id, now).await {
            Ok(Some(rows)) => {
                stats.digests += 1;
                stats.rows_released += rows;
            }
            Ok(None) => {}
            Err(err) => warn!(recipient = %recipient_id, %err, "off-hours digest release failed"),
        }
    }
    Ok(stats)
}

/// Release one person's due rows. `None` when another pass holds them or none are due.
async fn release_person(
    pool: &PgPool,
    dir: &Directory,
    recipient_id: &str,
    now: DateTime<Utc>,
) -> Result<Option<u64>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    // One release per person at a time (#767 B): row locks alone would let two scheduler
    // replicas with slightly different `now` split one person's rows into two digests.
    let (granted,): (bool,) =
        sqlx::query_as("SELECT pg_try_advisory_xact_lock(hashtext('offhours-digest:' || $1))")
            .bind(recipient_id)
            .fetch_one(&mut *tx)
            .await?;
    if !granted {
        tx.rollback().await?;
        return Ok(None);
    }
    #[allow(clippy::type_complexity)]
    let rows: Vec<(i64, Uuid, i32, Value, DateTime<Utc>, DateTime<Utc>)> = sqlx::query_as(
        "SELECT id, alert_id, notify_seq, route_info, created_at, release_at \
             FROM offhours_held \
             WHERE recipient_id = $1 AND released_at IS NULL AND release_at <= $2 \
             ORDER BY id FOR UPDATE SKIP LOCKED",
    )
    .bind(recipient_id)
    .bind(now)
    .fetch_all(&mut *tx)
    .await?;
    let Some(latest) = rows.last() else {
        tx.rollback().await?;
        return Ok(None);
    };

    // Distinct alerts, oldest hold first (a renotify of the same alert is held as its own row).
    let mut seen = HashSet::new();
    let alert_ids: Vec<String> = rows
        .iter()
        .filter(|r| seen.insert(r.1))
        .map(|r| r.1.to_string())
        .collect();
    let held_since = rows.iter().map(|r| r.4).min().unwrap_or(now);
    let release_at = rows.iter().map(|r| r.5).max().unwrap_or(now);

    let mut route_info = latest.3.clone();
    if let Some(obj) = route_info.as_object_mut() {
        obj.remove("fallback");
        obj.insert("target".into(), Value::String(TARGET.into()));
        obj.insert(
            "offhours".into(),
            json!({
                "count": alert_ids.len(),
                "alert_ids": alert_ids,
                "held_since": held_since.to_rfc3339_opts(SecondsFormat::Millis, true),
                "release_at": release_at.to_rfc3339_opts(SecondsFormat::Millis, true),
            }),
        );
    }
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();

    // The CURRENT address, not the one frozen at hold time: the digest waits hours, and the
    // person may have changed or removed their email, or left the directory, meanwhile.
    // Known limit: team membership isn't rechecked here, so someone removed from a team in the
    // evening still gets that night's held titles for it in the morning.
    // Known limit: during a rolling deploy a scheduler on an older directory can drop holds
    // for a person only the newer router knows. Keeping such rows pending would need a
    // backoff so a really removed person doesn't stall the batch; out of scope for the demo.
    let Some(address) = dir
        .person(recipient_id)
        .and_then(|p| address_for(p, "email"))
        .map(str::to_string)
    else {
        warn!(recipient = %recipient_id, rows = ids.len(),
              "off-hours digest dropped: person or email address no longer in the directory");
        sqlx::query("UPDATE offhours_held SET released_at = now() WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        return Ok(None);
    };
    let job = PlannedJob {
        channel: "email".into(),
        address,
        route_info,
    };
    crate::router::insert_jobs(
        &mut tx,
        latest.1,
        latest.2,
        recipient_id,
        std::slice::from_ref(&job),
        Some(OFFHOURS_DIGEST_STEP),
    )
    .await?;
    let job_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM delivery_jobs \
         WHERE alert_id = $1 AND recipient_id = $2 AND channel = 'email' \
           AND notify_seq = $3 AND escalation_step = $4",
    )
    .bind(latest.1)
    .bind(recipient_id)
    .bind(latest.2)
    .bind(OFFHOURS_DIGEST_STEP)
    .fetch_optional(&mut *tx)
    .await?;

    sqlx::query(
        "UPDATE offhours_held SET released_at = now(), digest_alert = $2, digest_job = $3 \
         WHERE id = ANY($1)",
    )
    .bind(&ids)
    .bind(latest.1)
    .bind(job_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(ids.len() as u64))
}

/// One line per covered alert, as the digest email lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DigestLine {
    pub status: String,
    pub severity: String,
    pub title: String,
    pub count: i64,
}

/// At most this many alerts are listed in a digest body (the counts above still cover all).
const MAX_LINES: usize = 50;
/// Titles are cut to this many characters in a digest line.
const MAX_TITLE: usize = 200;

/// `s` on one line (control characters become spaces), cut to `max` characters with `…`.
fn one_line(s: &str, max: usize) -> String {
    let mut out: String = s
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .take(max)
        .collect();
    if s.chars().count() > max {
        out.push('…');
    }
    out
}

/// Title and body of a digest email from the covered alerts' CURRENT rows (oldest first).
pub fn render(lines: &[DigestLine]) -> (String, String) {
    let total = lines.len();
    let resolved = lines.iter().filter(|l| l.status == "resolved").count();
    let mut by_severity: Vec<(String, usize)> = Vec::new();
    for l in lines {
        match by_severity.iter_mut().find(|(s, _)| *s == l.severity) {
            Some((_, n)) => *n += 1,
            None => by_severity.push((l.severity.clone(), 1)),
        }
    }
    let plural = |n: usize, one: &str, many: &str| {
        if n == 1 {
            format!("{n} {one}")
        } else {
            format!("{n} {many}")
        }
    };
    let mut title = format!("While you were off: {}", plural(total, "alert", "alerts"));
    if resolved > 0 {
        title.push_str(&format!(", {resolved} resolved since"));
    }
    let mut body = String::new();
    if !by_severity.is_empty() {
        let parts: Vec<String> = by_severity
            .iter()
            .map(|(s, n)| format!("{n} {s}"))
            .collect();
        body.push_str(&format!(
            "Held outside your working hours: {}.\n\n",
            parts.join(", ")
        ));
    }
    // Bounded body (a noisy night must not make an email SMTP rejects as too big, which would
    // kill the whole digest) and one line per alert (a newline in a title can't forge lines).
    for l in lines.iter().take(MAX_LINES) {
        let repeats = if l.count > 1 {
            format!(" (x{})", l.count)
        } else {
            String::new()
        };
        body.push_str(&format!(
            "- [{}] {}: {}{}\n",
            l.status,
            l.severity,
            one_line(&l.title, MAX_TITLE),
            repeats
        ));
    }
    if lines.len() > MAX_LINES {
        body.push_str(&format!("...and {} more.\n", lines.len() - MAX_LINES));
    }
    body.push_str("\nOpen the Beacon feed for details.");
    (title, body)
}

/// The most severe status among the covered alerts (`critical > error > warning > info`), for
/// the digest email's subject. Unknown severities rank lowest.
pub fn highest_severity(lines: &[DigestLine]) -> Option<String> {
    let rank = |s: &str| match s {
        "critical" => 4,
        "error" => 3,
        "warning" => 2,
        "info" => 1,
        _ => 0,
    };
    lines
        .iter()
        .max_by_key(|l| rank(&l.severity))
        .map(|l| l.severity.clone())
}

/// The covered alerts of a digest job, read now (status can have changed since they were held).
pub async fn digest_lines(
    pool: &PgPool,
    route_info: &Value,
) -> Result<Vec<DigestLine>, sqlx::Error> {
    let ids: Vec<Uuid> = route_info
        .pointer("/offhours/alert_ids")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .filter_map(|s| s.parse().ok())
                .collect()
        })
        .unwrap_or_default();
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<(String, String, String, i64)> = sqlx::query_as(
        "SELECT a.status, a.severity, a.title, a.count \
         FROM unnest($1::uuid[]) WITH ORDINALITY AS k(id, ord) \
         JOIN alerts a ON a.id = k.id ORDER BY k.ord",
    )
    .bind(&ids)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(status, severity, title, count)| DigestLine {
            status,
            severity,
            title,
            count,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(status: &str, severity: &str, title: &str, count: i64) -> DigestLine {
        DigestLine {
            status: status.into(),
            severity: severity.into(),
            title: title.into(),
            count,
        }
    }

    #[test]
    fn render_counts_resolved_and_severities() {
        let (title, body) = render(&[
            line("open", "warning", "Disk 91% on db-2", 4),
            line("resolved", "warning", "Queue lag 12 s", 1),
            line("acked", "info", "Nightly batch slow", 1),
        ]);
        assert_eq!(title, "While you were off: 3 alerts, 1 resolved since");
        assert!(body.starts_with("Held outside your working hours: 2 warning, 1 info.\n\n"));
        assert!(body.contains("- [open] warning: Disk 91% on db-2 (x4)\n"));
        assert!(body.contains("- [resolved] warning: Queue lag 12 s\n"));
        assert!(body.ends_with("Open the Beacon feed for details."));
    }

    #[test]
    fn highest_severity_ranks_known_levels() {
        let lines = [
            line("open", "info", "a", 1),
            line("open", "error", "b", 1),
            line("open", "warning", "c", 1),
        ];
        assert_eq!(highest_severity(&lines).as_deref(), Some("error"));
        assert_eq!(highest_severity(&[]), None);
    }

    #[test]
    fn render_caps_lines_and_keeps_titles_on_one_line() {
        let mut lines: Vec<DigestLine> = (0..60)
            .map(|i| line("open", "warning", &format!("alert {i}"), 1))
            .collect();
        lines[0].title = "fake\n- [resolved] critical: all clear".into();
        lines[1].title = "x".repeat(500);
        let (title, body) = render(&lines);
        assert_eq!(title, "While you were off: 60 alerts");
        assert!(body.starts_with("Held outside your working hours: 60 warning."));
        assert_eq!(body.matches("\n- [").count(), 50, "{body}");
        assert!(body.contains("...and 10 more.\n"), "{body}");
        assert!(
            !body.contains("\n- [resolved]"),
            "a title can't forge a line: {body}"
        );
        assert!(body.contains(&format!("{}…", "x".repeat(200))));
        assert!(!body.contains(&"x".repeat(201)));
    }

    #[test]
    fn render_single_alert_without_resolved() {
        let (title, _) = render(&[line("open", "warning", "x", 1)]);
        assert_eq!(title, "While you were off: 1 alert");
    }
}
