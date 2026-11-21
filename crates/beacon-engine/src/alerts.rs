//! Alert status transitions shared by the API (ack/resolve buttons) and
//! delivery (SMS "ACK" replies, runbook steps that resolve, runbook activity).
//!
//! Every change bumps the alert `version` and queues an `alert_update` outbox
//! row in the caller's transaction, so all live feeds see it.

use beacon_core::FeedAlert;
use beacon_core::model::outbox_kind;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::OUTBOX_CHANNEL;
use crate::feed::{FEED_COLUMNS, row_to_feed};

#[derive(Debug, Clone, PartialEq)]
pub enum Transition {
    Ack,
    /// `reason` is stored as `resolved_reason` (e.g. `user`, `runbook`).
    Resolve { reason: String },
}

#[derive(Debug)]
pub enum TransitionError {
    NotFound,
    /// e.g. ack on a resolved alert. Carries the alert as it is now.
    InvalidTransition(Box<FeedAlert>),
    /// The status changed after `expected_version`. Carries the current alert.
    VersionConflict(Box<FeedAlert>),
}

/// Ack or resolve under the row lock, in the caller's transaction.
///
/// - Repeating the same transition is idempotent (returns the current alert).
/// - With `expected_version`, a conflict is reported only when the *status*
///   changed after that version; count bumps never conflict. `None` skips the
///   check (system actors such as an SMS reply or a runbook step).
pub async fn transition(
    tx: &mut PgConnection,
    id: Uuid,
    t: Transition,
    actor: &str,
    expected_version: Option<i64>,
) -> anyhow::Result<Result<FeedAlert, TransitionError>> {
    let Some(row) = sqlx::query(&format!("SELECT {FEED_COLUMNS}, status_version FROM alerts WHERE id = $1 FOR UPDATE"))
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(Err(TransitionError::NotFound));
    };
    let current = row_to_feed(&row)?;
    let status_version: i64 = row.try_get("status_version")?;
    match (&t, current.status.as_str()) {
        (Transition::Ack, "acked") | (Transition::Resolve { .. }, "resolved") => return Ok(Ok(current)),
        (Transition::Ack, "resolved") => return Ok(Err(TransitionError::InvalidTransition(Box::new(current)))),
        _ if expected_version.is_some_and(|v| status_version > v) => {
            return Ok(Err(TransitionError::VersionConflict(Box::new(current))));
        }
        _ => {}
    }
    match &t {
        Transition::Ack => {
            sqlx::query(
                "UPDATE alerts SET status = 'acked', acked_by = $2, acked_at = now(),
                    version = version + 1, status_version = version + 1
                 WHERE id = $1",
            )
            .bind(id)
            .bind(actor)
            .execute(&mut *tx)
            .await?;
        }
        Transition::Resolve { reason } => {
            sqlx::query(
                "UPDATE alerts SET status = 'resolved', resolved_by = $2, resolved_at = now(),
                    resolved_reason = $3, dedup_active = false,
                    version = version + 1, status_version = version + 1
                 WHERE id = $1",
            )
            .bind(id)
            .bind(actor)
            .bind(reason)
            .execute(&mut *tx)
            .await?;
        }
    }
    queue_update(tx, id).await?;
    let row = sqlx::query(&format!("SELECT {FEED_COLUMNS} FROM alerts WHERE id = $1"))
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
    Ok(Ok(row_to_feed(&row)?))
}

/// Bump the version without changing status (e.g. runbook activity that the
/// alert detail shows), so open feeds and detail views refresh. Returns false
/// if the alert does not exist.
pub async fn touch(tx: &mut PgConnection, id: Uuid) -> anyhow::Result<bool> {
    let n = sqlx::query("UPDATE alerts SET version = version + 1 WHERE id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if n == 0 {
        return Ok(false);
    }
    queue_update(tx, id).await?;
    Ok(true)
}

async fn queue_update(tx: &mut PgConnection, id: Uuid) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO outbox (kind, alert_id) VALUES ($1, $2)")
        .bind(outbox_kind::ALERT_UPDATE)
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("SELECT pg_notify($1, '')").bind(OUTBOX_CHANNEL).execute(&mut *tx).await?;
    Ok(())
}
