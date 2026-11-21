//! DNS-rebinding guard: the API only answers requests whose `Host` it knows.
//!
//! The demo session has no login, so whoever can read `GET /api/session` gets
//! its CSRF token and acts as the (admin) demo user. The same-origin policy
//! keeps other sites out, unless an attacker's domain re-resolves to
//! `127.0.0.1` after the page loads: the browser then treats our API as that
//! domain's origin. Its requests still carry the attacker's name in `Host`,
//! so a `Host` allowlist stops them (`421 misdirected`).
//!
//! Allowed: `localhost` and `*.localhost`, any IP literal (a rebinding attack
//! needs a domain name), the host of `PUBLIC_URL`, and `API_ALLOWED_HOSTS`
//! (comma-separated names; `*` turns the guard off). A request without any
//! host (not a browser) passes.

use std::sync::Arc;

use axum::Router;
use axum::extract::Request;
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::error::ApiError;

#[derive(Debug, Clone)]
pub struct AllowedHosts {
    names: Arc<Vec<String>>,
    any: bool,
}

impl AllowedHosts {
    /// `names` are extra host names (ports and case ignored); `*` allows every host.
    pub fn new<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut any = false;
        let mut out = Vec::new();
        for n in names {
            let n = n.as_ref().trim();
            if n == "*" {
                any = true;
            } else if !n.is_empty() {
                out.push(normalize(hostname(n)));
            }
        }
        AllowedHosts { names: Arc::new(out), any }
    }

    /// From the environment: `PUBLIC_URL`'s host plus `API_ALLOWED_HOSTS`.
    pub fn from_env() -> Self {
        let public = std::env::var("PUBLIC_URL").ok().and_then(|u| url_host(&u));
        let extra = std::env::var("API_ALLOWED_HOSTS").unwrap_or_default();
        Self::new(public.into_iter().chain(extra.split(',').map(str::to_string)))
    }

    /// Whether a request with this `Host` header value is answered.
    pub fn allows(&self, host: &str) -> bool {
        if self.any {
            return true;
        }
        let name = normalize(hostname(host));
        if name.is_empty() {
            return false;
        }
        name == "localhost"
            || name.ends_with(".localhost")
            || name.trim_start_matches('[').trim_end_matches(']').parse::<std::net::IpAddr>().is_ok()
            || self.names.contains(&name)
    }
}

/// `host[:port]` → `host` (IPv6 literals keep their brackets).
fn hostname(host: &str) -> &str {
    let host = host.trim();
    if host.starts_with('[') {
        return host.find(']').map_or(host, |i| &host[..=i]);
    }
    match host.rsplit_once(':') {
        Some((h, port)) if port.chars().all(|c| c.is_ascii_digit()) => h,
        _ => host,
    }
}

fn normalize(name: &str) -> String {
    name.trim_end_matches('.').to_ascii_lowercase()
}

/// The host of an `http(s)://host[:port]/...` URL.
fn url_host(url: &str) -> Option<String> {
    let rest = url.split_once("://")?.1;
    let authority = rest.split(['/', '?', '#']).next()?;
    let authority = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let h = hostname(authority);
    (!h.is_empty()).then(|| h.to_string())
}

/// Wrap `app` so requests for an unknown `Host` get `421 misdirected`.
pub fn host_guard(app: Router, allowed: AllowedHosts) -> Router {
    app.layer(axum::middleware::from_fn(move |req: Request, next: Next| {
        let allowed = allowed.clone();
        async move {
            let host = req
                .headers()
                .get(header::HOST)
                .map(|v| v.to_str().unwrap_or("\u{0}").to_string())
                .or_else(|| req.uri().authority().map(|a| a.as_str().to_string()));
            match host {
                Some(h) if !allowed.allows(&h) => misdirected(),
                _ => next.run(req).await,
            }
        }
    }))
}

fn misdirected() -> Response {
    ApiError::new(StatusCode::MISDIRECTED_REQUEST, "misdirected", "unknown Host; set API_ALLOWED_HOSTS to serve this name").into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::get;
    use tower::ServiceExt;

    #[test]
    fn allows_loopback_ip_literals_and_configured_names() {
        let a = AllowedHosts::new(["Beacon.Example.com", "demo.local:8080"]);
        for ok in [
            "localhost",
            "localhost:8080",
            "LOCALHOST.",
            "app.localhost:5173",
            "127.0.0.1:8080",
            "192.168.1.20:8080",
            "[::1]:8080",
            "[::1]",
            "beacon.example.com",
            "beacon.example.com:443",
            "demo.local",
        ] {
            assert!(a.allows(ok), "{ok}");
        }
        for bad in ["evil.example", "evil.example:8080", "localhost.evil.example", "127.0.0.1.nip.io", "beacon.example.com.evil", "", ":8080", "\u{0}"] {
            assert!(!a.allows(bad), "{bad}");
        }
        assert!(AllowedHosts::new(["*"]).allows("anything.example"));
    }

    #[test]
    fn public_url_host() {
        assert_eq!(url_host("http://localhost:8080").as_deref(), Some("localhost"));
        assert_eq!(url_host("https://user@beacon.example.com/x?y").as_deref(), Some("beacon.example.com"));
        assert_eq!(url_host("http://[::1]:8080/").as_deref(), Some("[::1]"));
        assert_eq!(url_host("nope"), None);
    }

    #[tokio::test]
    async fn guard_rejects_rebound_names() {
        let app = host_guard(Router::new().route("/api/session", get(|| async { "ok" })), AllowedHosts::new(Vec::<String>::new()));
        let call = |host: Option<&'static str>| {
            let app = app.clone();
            async move {
                let mut req = Request::get("/api/session");
                if let Some(h) = host {
                    req = req.header(header::HOST, h);
                }
                app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap().status()
            }
        };
        assert_eq!(call(Some("attacker.example:8080")).await, StatusCode::MISDIRECTED_REQUEST);
        assert_eq!(call(Some("localhost:8080")).await, StatusCode::OK);
        assert_eq!(call(Some("127.0.0.1:8080")).await, StatusCode::OK);
        assert_eq!(call(None).await, StatusCode::OK);
    }
}
