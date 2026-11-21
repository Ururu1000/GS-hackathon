//! A polled HTTP API as a source (`type: poll`, spec §5.1).
//!
//! ```yaml
//! id: poll:status
//! type: poll
//! url: https://status.example.com/api/incidents
//! interval: 30s             # default 30s, at least 1s
//! items: data.incidents     # optional: dot path to the array of items (default: the whole body)
//! id_path: id               # optional: each item's own id, for stable event ids
//! token_env: STATUS_TOKEN   # optional: env var holding a bearer token, never the token itself
//! ```
//!
//! Every `interval` Beacon GETs the URL (10 s timeout, 8 MiB body cap) and
//! publishes each item as an event. Its id is `<source>:<id_path value>`, or
//! `<source>:<content hash>` without `id_path`, so an item returned by every
//! poll is published once: the last [`SEEN_CAP`] ids are remembered, and the
//! matcher drops a repeated id after a restart too. A failed poll is logged and
//! retried at the next interval; nothing is published from it.

use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::kafka_source::{SourceSpec, publish_or_quarantine};
use crate::publisher::EventPublisher;

pub const SEEN_CAP: usize = 10_000;
const MAX_BODY: usize = 8 * 1024 * 1024;
/// Items published per poll, so one huge response can't flood `events.raw`.
const MAX_ITEMS: usize = 5_000;

fn interval(s: &SourceSpec) -> Result<Duration> {
    let raw = s.interval.as_deref().unwrap_or("30s");
    let d = humantime_serde::re::humantime::parse_duration(raw.trim()).with_context(|| format!("source `{}`: interval `{raw}` is not a duration like 30s", s.id))?;
    if d < Duration::from_secs(1) {
        bail!("source `{}`: interval must be at least 1s", s.id);
    }
    Ok(d)
}

fn valid_path(p: &str) -> bool {
    !p.is_empty() && p.split('.').all(|seg| !seg.is_empty())
}

pub fn validate(s: &SourceSpec) -> Result<()> {
    let url = s.url.as_deref().unwrap_or("");
    if !(url.starts_with("http://") || url.starts_with("https://")) || url.len() > 2048 {
        bail!("source `{}`: type poll needs an http(s) `url`", s.id);
    }
    interval(s)?;
    for (name, p) in [("items", &s.items), ("id_path", &s.id_path)] {
        if let Some(p) = p
            && !valid_path(p)
        {
            bail!("source `{}`: `{name}` must be a dot path like data.items", s.id);
        }
    }
    if let Some(var) = &s.token_env
        && (var.is_empty() || !var.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
    {
        bail!("source `{}`: `{var}` is not an environment variable name", s.id);
    }
    if !s.topics.is_empty() || s.bootstrap.is_some() || s.group.is_some() || s.security.is_some() || s.path.is_some() || s.checkpoint.is_some() {
        bail!("source `{}`: type poll takes only url, interval, items, id_path and token_env", s.id);
    }
    Ok(())
}

/// The value at a dot path (`data.items`, `meta.id`; numeric segments index arrays).
pub fn at_path<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(v, |cur, seg| match cur {
        Value::Object(m) => m.get(seg),
        Value::Array(a) => seg.parse::<usize>().ok().and_then(|i| a.get(i)),
        _ => None,
    })
}

/// The items one response carries: the array at `items`, or the whole body.
pub fn items_of(body: Value, items: Option<&str>) -> Result<Vec<Value>, String> {
    match items {
        None => Ok(vec![body]),
        Some(p) => match at_path(&body, p) {
            Some(Value::Array(a)) => Ok(a.clone()),
            Some(_) => Err(format!("`{p}` is not an array")),
            None => Err(format!("`{p}` is not in the response")),
        },
    }
}

/// Stable id for an item: its `id_path` value, else a hash of its content.
pub fn item_id(source: &str, item: &Value, id_path: Option<&str>) -> String {
    if let Some(p) = id_path
        && let Some(v) = at_path(item, p)
    {
        let s = match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        if !s.is_empty() && s.len() <= 200 {
            return format!("{source}:{s}");
        }
    }
    let digest = Sha256::digest(item.to_string().as_bytes());
    format!("{source}:{}", hex::encode(&digest[..12]))
}

/// The last [`SEEN_CAP`] ids, oldest dropped first.
#[derive(Default)]
pub struct Seen {
    order: VecDeque<String>,
    set: HashSet<String>,
}

impl Seen {
    pub fn contains(&self, id: &str) -> bool {
        self.set.contains(id)
    }

    pub fn insert(&mut self, id: String) {
        if self.set.insert(id.clone()) {
            self.order.push_back(id);
            if self.order.len() > SEEN_CAP
                && let Some(old) = self.order.pop_front()
            {
                self.set.remove(&old);
            }
        }
    }
}

async fn fetch(client: &reqwest::Client, url: &str, token: Option<&str>) -> Result<Value, String> {
    let mut req = client.get(url).header(reqwest::header::ACCEPT, "application/json");
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    let mut res = req.send().await.map_err(|e| format!("request failed: {e}"))?;
    if !res.status().is_success() {
        return Err(format!("HTTP {}", res.status()));
    }
    if res.content_length().is_some_and(|n| n as usize > MAX_BODY) {
        return Err(format!("response larger than {MAX_BODY} bytes"));
    }
    let mut body = Vec::new();
    while let Some(chunk) = res.chunk().await.map_err(|e| format!("reading the response: {e}"))? {
        body.extend_from_slice(&chunk);
        if body.len() > MAX_BODY {
            return Err(format!("response larger than {MAX_BODY} bytes"));
        }
    }
    serde_json::from_slice(&body).map_err(|e| format!("response is not JSON: {e}"))
}

/// One poll: fetch, split into items, publish the ones not seen yet.
/// Returns how many were published, or false-like `None` when shutting down.
pub async fn poll_once(spec: &SourceSpec, client: &reqwest::Client, token: Option<&str>, seen: &mut Seen, publisher: &EventPublisher, shutdown: &watch::Receiver<bool>) -> Option<usize> {
    let url = spec.url.as_deref().unwrap_or_default();
    let items = match fetch(client, url, token).await.and_then(|b| items_of(b, spec.items.as_deref())) {
        Ok(items) => items,
        Err(e) => {
            warn!(source = %spec.id, error = %e, "poll failed; retrying next interval");
            return Some(0);
        }
    };
    if items.len() > MAX_ITEMS {
        warn!(source = %spec.id, items = items.len(), "response has more items than one poll publishes; the rest wait for the next poll");
    }
    let mut published = 0;
    for item in items.into_iter().take(MAX_ITEMS) {
        let id = item_id(&spec.id, &item, spec.id_path.as_deref());
        if seen.contains(&id) {
            continue;
        }
        let parsed = if item.is_object() { Ok(item) } else { Err("item is not a JSON object".to_string()) };
        let payload = match &parsed {
            Ok(v) => v.to_string().into_bytes(),
            Err(_) => Vec::new(),
        };
        if !publish_or_quarantine(publisher, &spec.id, id.clone(), parsed, &payload, shutdown).await {
            return None;
        }
        seen.insert(id);
        published += 1;
    }
    Some(published)
}

pub async fn run_poll_source(spec: SourceSpec, publisher: EventPublisher, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let every = interval(&spec)?;
    let token = match &spec.token_env {
        Some(var) => Some(std::env::var(var).with_context(|| format!("source `{}`: environment variable {var} is not set", spec.id))?),
        None => None,
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::limited(3))
        .build()
        .context("building the HTTP client")?;
    let mut seen = Seen::default();
    info!(source = %spec.id, url = %spec.url.as_deref().unwrap_or_default(), interval_s = every.as_secs(), "poll source started");
    loop {
        match poll_once(&spec, &client, token.as_deref(), &mut seen, &publisher, &shutdown).await {
            None => return Ok(()),
            Some(n) if n > 0 => info!(source = %spec.id, published = n, "poll published new items"),
            Some(_) => {}
        }
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            _ = tokio::time::sleep(every) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(yaml: &str) -> SourceSpec {
        serde_saphyr::from_str(yaml).unwrap()
    }

    #[test]
    fn validation() {
        validate(&spec("id: poll:s\ntype: poll\nurl: https://x.example/api\ninterval: 5m\nitems: data.items\nid_path: id\ntoken_env: S_TOKEN\n")).unwrap();
        assert!(validate(&spec("id: poll:s\ntype: poll\n")).is_err(), "url required");
        assert!(validate(&spec("id: poll:s\ntype: poll\nurl: file:///etc/passwd\n")).is_err());
        assert!(validate(&spec("id: poll:s\ntype: poll\nurl: http://x\ninterval: 100ms\n")).is_err());
        assert!(validate(&spec("id: poll:s\ntype: poll\nurl: http://x\ninterval: soon\n")).is_err());
        assert!(validate(&spec("id: poll:s\ntype: poll\nurl: http://x\nitems: a..b\n")).is_err());
        assert!(validate(&spec("id: poll:s\ntype: poll\nurl: http://x\ntoken_env: \"has space\"\n")).is_err());
        assert!(validate(&spec("id: poll:s\ntype: poll\nurl: http://x\npath: /tmp/x\n")).is_err());
    }

    #[test]
    fn items_and_ids() {
        let body = json!({"data": {"items": [{"id": "INC-1", "sev": "high"}, {"id": 7}]}});
        let items = items_of(body.clone(), Some("data.items")).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(item_id("poll:s", &items[0], Some("id")), "poll:s:INC-1");
        assert_eq!(item_id("poll:s", &items[1], Some("id")), "poll:s:7");
        // Without id_path the id is a content hash: equal content, equal id.
        let a = item_id("poll:s", &json!({"x": 1}), None);
        assert_eq!(a, item_id("poll:s", &json!({"x": 1}), None));
        assert_ne!(a, item_id("poll:s", &json!({"x": 2}), None));
        assert_eq!(items_of(body.clone(), None).unwrap(), vec![body.clone()]);
        assert!(items_of(body.clone(), Some("data")).is_err());
        assert!(items_of(body, Some("nope")).is_err());
        assert_eq!(at_path(&json!({"a": [{"b": 3}]}), "a.0.b"), Some(&json!(3)));
    }

    #[test]
    fn seen_is_bounded() {
        let mut s = Seen::default();
        for i in 0..SEEN_CAP + 5 {
            s.insert(format!("id-{i}"));
        }
        assert!(!s.contains("id-0"), "oldest dropped");
        assert!(s.contains(&format!("id-{}", SEEN_CAP + 4)));
        assert_eq!(s.order.len(), SEEN_CAP);
    }
}
