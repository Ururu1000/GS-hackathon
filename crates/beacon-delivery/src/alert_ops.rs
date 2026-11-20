//! Alert status transitions delivery needs (SMS `ACK`, runbook steps with `resolves: true`,
//! "runbook activity bumps the alert version"). Part (a) owns the alerts table and is moving
//! this SQL to `beacon_engine::alerts::{transition, touch}` (board #172/#174). Until that lands
//! on this branch's base, `SqlAlertOps` mirrors beacon-api's `apply_transition` SQL; the binary
//! switches to the beacon-engine implementation once the PR stacks merge. Delivery code only
//! ever uses the `AlertOps` trait.

use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::channel::BoxFuture;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transition {
    Ack,
    Resolve { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionOutcome {
    /// Status changed (version bumped, alert_update outbox row written).
    Applied,
    /// Already in that state: nothing changed (idempotent repeat).
    Unchanged,
    /// `ack` on a resolved alert, or the alert is gone.
    Invalid(String),
}

pub trait AlertOps: Send + Sync {
    /// Ack/resolve inside the caller's tx; `expected_version: None` skips the conflict check
    /// (system actors such as SMS replies or runbook steps).
    fn transition<'a>(
        &'a self,
        tx: &'a mut Transaction<'static, Postgres>,
        alert_id: Uuid,
        t: Transition,
        actor: &'a str,
    ) -> BoxFuture<'a, Result<TransitionOutcome, sqlx::Error>>;

    /// Bump `version` and write an `alert_update` outbox row so the feed and detail refetch.
    fn touch<'a>(
        &'a self,
        tx: &'a mut Transaction<'static, Postgres>,
        alert_id: Uuid,
    ) -> BoxFuture<'a, Result<(), sqlx::Error>>;
}

/// Temporary local implementation (see module doc).
#[derive(Debug, Default, Clone, Copy)]
pub struct SqlAlertOps;

impl AlertOps for SqlAlertOps {
    fn transition<'a>(
        &'a self,
        tx: &'a mut Transaction<'static, Postgres>,
        alert_id: Uuid,
        t: Transition,
        actor: &'a str,
    ) -> BoxFuture<'a, Result<TransitionOutcome, sqlx::Error>> {
        Box::pin(async move {
            let status: Option<(String,)> =
                sqlx::query_as("SELECT status FROM alerts WHERE id = $1 FOR UPDATE")
                    .bind(alert_id)
                    .fetch_optional(&mut **tx)
                    .await?;
            let Some((status,)) = status else {
                return Ok(TransitionOutcome::Invalid("alert not found".into()));
            };
            let sql = match (&t, status.as_str()) {
                (Transition::Ack, "acked") | (Transition::Resolve { .. }, "resolved") => {
                    return Ok(TransitionOutcome::Unchanged);
                }
                (Transition::Ack, "resolved") => {
                    return Ok(TransitionOutcome::Invalid("alert is resolved".into()));
                }
                (Transition::Ack, _) => {
                    "UPDATE alerts SET status = 'acked', acked_by = $2, acked_at = now(), \
                     version = version + 1, status_version = version + 1 WHERE id = $1"
                }
                (Transition::Resolve { .. }, _) => {
                    "UPDATE alerts SET status = 'resolved', resolved_by = $2, resolved_at = now(), \
                     resolved_reason = $3, version = version + 1, status_version = version + 1 \
                     WHERE id = $1"
                }
            };
            let reason = match &t {
                Transition::Resolve { reason } => reason.as_str(),
                Transition::Ack => "",
            };
            sqlx::query(sql)
                .bind(alert_id)
                .bind(actor)
                .bind(reason)
                .execute(&mut **tx)
                .await?;
            sqlx::query("INSERT INTO outbox (kind, alert_id) VALUES ('alert_update', $1)")
                .bind(alert_id)
                .execute(&mut **tx)
                .await?;
            Ok(TransitionOutcome::Applied)
        })
    }

    fn touch<'a>(
        &'a self,
        tx: &'a mut Transaction<'static, Postgres>,
        alert_id: Uuid,
    ) -> BoxFuture<'a, Result<(), sqlx::Error>> {
        Box::pin(async move {
            sqlx::query("UPDATE alerts SET version = version + 1 WHERE id = $1")
                .bind(alert_id)
                .execute(&mut **tx)
                .await?;
            sqlx::query("INSERT INTO outbox (kind, alert_id) VALUES ('alert_update', $1)")
                .bind(alert_id)
                .execute(&mut **tx)
                .await?;
            Ok(())
        })
    }
}
