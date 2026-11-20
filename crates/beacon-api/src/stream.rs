//! `GET /api/stream?team=`: Server-Sent Events.
//!
//! MVP protocol (SPEC §8): subscribe and buffer live updates **first**, then
//! read the feed page from Postgres and send it as `snapshot`, then stream
//! `alert.upsert` (full `FeedAlert`), dropping any update whose version is not
//! newer than what this client already has. When the client falls behind
//! the buffer, the stream ends and the browser reconnects for a new snapshot.

use std::collections::HashMap;
use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{RawQuery, State};
use axum::http::{HeaderValue, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use beacon_core::FeedAlert;
use chrono::{SecondsFormat, Utc};
use futures::stream::{self, Stream, StreamExt};
use serde_json::json;
use tokio::sync::{broadcast, watch};
use uuid::Uuid;

use crate::ApiState;
use crate::alerts::{FeedQuery, api_alert, feed_page};
use crate::error::ApiError;

const READY_WAIT: Duration = Duration::from_secs(10);

pub async fn stream(State(state): State<ApiState>, RawQuery(raw): RawQuery) -> Result<Response, ApiError> {
    let q = FeedQuery::parse(raw.as_deref())?;
    // Only the team filter applies to the stream; the snapshot is the default page.
    let q = FeedQuery { team: q.team, ..FeedQuery::parse(None)? };

    // Without an assigned consumer, updates between snapshot and assignment
    // would be lost; wait briefly, else ask the client to retry.
    let mut ready = state.ready.clone();
    if tokio::time::timeout(READY_WAIT, ready.wait_for(|r| *r)).await.is_err() {
        return Err(ApiError::new(axum::http::StatusCode::SERVICE_UNAVAILABLE, "not_ready", "feed not ready, retry"));
    }

    // 1. Subscribe first, so nothing published during the snapshot read is missed.
    let rx = state.updates.subscribe();
    // 2. Snapshot from Postgres.
    let (items, next) = feed_page(&state.pool, &state.session, &q).await?;
    let seen: HashMap<Uuid, i64> = items.iter().map(|a| (a.id, a.version)).collect();
    let snapshot = json!({
        "team": q.team,
        "items": items.iter().map(api_alert).collect::<Vec<_>>(),
        "next_cursor": next,
        "server_time": Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
    });
    let first = Event::default()
        .retry(Duration::from_millis(1500))
        .event("snapshot")
        .data(snapshot.to_string());

    let visible: Vec<String> = state.session.teams.iter().map(|t| t.id.clone()).collect();
    let live = upserts(rx, state.ready.clone(), seen, q.team.clone(), visible);
    let events = stream::once(async move { Ok::<_, Infallible>(first) }).chain(live);

    let mut resp = Sse::new(events)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)).text("ping"))
        .into_response();
    let h = resp.headers_mut();
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    h.insert("x-accel-buffering", HeaderValue::from_static("no"));
    Ok(resp)
}

/// Live `alert.upsert` events for one client, newest version per alert only.
/// Ends on lag or when the feed consumer stops being ready (outage,
/// rebalance), so the client reconnects and gets a consistent snapshot.
pub fn upserts(
    rx: broadcast::Receiver<FeedAlert>,
    ready: watch::Receiver<bool>,
    seen: HashMap<Uuid, i64>,
    team: Option<String>,
    visible: Vec<String>,
) -> impl Stream<Item = Result<Event, Infallible>> + Send {
    stream::unfold((rx, ready, seen), move |(mut rx, mut ready, mut seen)| {
        let (team, visible) = (team.clone(), visible.clone());
        async move {
            loop {
                let next = tokio::select! {
                    r = rx.recv() => r,
                    changed = ready.changed() => {
                        if changed.is_err() || !*ready.borrow() {
                            tracing::info!("feed not ready; closing sse stream for resync");
                            return None;
                        }
                        continue;
                    }
                };
                match next {
                    Ok(a) => {
                        let in_scope = visible.contains(&a.team) && team.as_ref().is_none_or(|t| *t == a.team);
                        if !in_scope || seen.get(&a.id).is_some_and(|&v| a.version <= v) {
                            continue;
                        }
                        seen.insert(a.id, a.version);
                        let ev = Event::default().event("alert.upsert").data(api_alert(&a).to_string());
                        return Some((Ok(ev), (rx, ready, seen)));
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::info!(missed = n, "sse client lagged; closing for resync");
                        return None;
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        }
    })
}
