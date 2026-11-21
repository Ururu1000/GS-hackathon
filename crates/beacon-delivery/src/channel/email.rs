//! Email channel: pooled async SMTP via lettre. M0 targets Mailpit on port 1025, no TLS.
//!
//! Hardening (delivery review):
//! - `SmtpConfig.tls` selects the transport: `None` (plaintext, the Mailpit default),
//!   `StartTls` (STARTTLS upgrade) or `Tls` (implicit TLS). Credentials (`username` with an
//!   optional `password`) are attached whenever they are configured.
//! - Plaintext SMTP to a non-local host is refused by [`EmailChannel::new`] unless the
//!   operator explicitly opts in with `allow_plaintext`: `Err("plaintext SMTP to a non-local
//!   host needs SmtpConfig::allow_plaintext")`. Local means `localhost`, `127.0.0.1`, `::1`
//!   or `mailpit` (see [`is_local_smtp_host`]); the decision lives in [`smtp_security`] so it
//!   is unit-testable without a server.
//! - The recipient address never appears in [`SendError`] messages: build failures report the
//!   parse error only, and transport error strings are scrubbed with
//!   [`mask_emails_in_text`]. `delivery_jobs.address` itself still stores the frozen
//!   destination; only errors (and the worker's log lines, via [`mask_email`]) are masked.
//!
//! TLS: the workspace `lettre` enables `tokio1-rustls-tls` (rustls + webpki roots), so
//! `StartTls` uses `starttls_relay` (the upgrade is required: no silent plaintext fallback)
//! and `Tls` uses implicit-TLS `relay`; configured credentials are attached in every mode.

use std::time::Duration;

use lettre::message::header::{ContentType, HeaderName, HeaderValue};
use lettre::message::{Mailbox, Message};
use lettre::transport::smtp::authentication::Credentials;
use lettre::transport::smtp::response::Response;
use lettre::{AsyncSmtpTransport, AsyncTransport, Tokio1Executor};

use crate::channel::{BoxFuture, Channel, OutboundMessage, Receipt, SendError};

/// How the SMTP connection is secured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SmtpTls {
    /// Plaintext SMTP. Only allowed for local hosts unless `allow_plaintext` is set.
    #[default]
    None,
    /// STARTTLS upgrade (submission port 587 unless `port` overrides). Needs a TLS backend.
    StartTls,
    /// Implicit TLS (submissions port 465 unless `port` overrides). Needs a TLS backend.
    Tls,
}

#[derive(Debug, Clone)]
pub struct SmtpConfig {
    pub host: String,
    pub port: u16,
    /// `From:` mailbox, e.g. `Beacon <beacon@example.test>`.
    pub from: String,
    /// Bound on connect + whole SMTP exchange.
    pub timeout: Duration,
    /// Transport security (default: plaintext for the local Mailpit relay).
    pub tls: SmtpTls,
    /// Optional SMTP username (`AUTH`), sent only over the configured transport.
    pub username: Option<String>,
    /// Optional SMTP password (empty string when `username` is set without one).
    pub password: Option<String>,
    /// Explicit opt-in to plaintext SMTP towards non-local hosts. Default `false`.
    pub allow_plaintext: bool,
}

impl Default for SmtpConfig {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 1025,
            from: "Beacon <beacon@example.test>".into(),
            timeout: Duration::from_secs(5),
            tls: SmtpTls::None,
            username: None,
            password: None,
            allow_plaintext: false,
        }
    }
}

/// Hosts treated as local for plaintext SMTP: loopback names/addresses and the compose
/// Mailpit service name. Comparison is case-insensitive; a trailing dot and `[...]` around
/// an IPv6 literal are tolerated.
pub fn is_local_smtp_host(host: &str) -> bool {
    let mut h = host.trim().to_ascii_lowercase();
    h = h.trim_end_matches('.').to_string();
    if let Some(inner) = h.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        h = inner.to_string();
    }
    h == "localhost" || h == "127.0.0.1" || h == "::1" || h == "mailpit"
}

/// Decide how to connect for `cfg`: the effective [`SmtpTls`] mode, or a refusal when
/// plaintext is requested towards a non-local host without the explicit opt-in. Pure
/// function (no I/O), unit-tested below.
pub fn smtp_security(cfg: &SmtpConfig) -> Result<SmtpTls, String> {
    match cfg.tls {
        SmtpTls::None if !is_local_smtp_host(&cfg.host) && !cfg.allow_plaintext => {
            Err("plaintext SMTP to a non-local host needs SmtpConfig::allow_plaintext".to_string())
        }
        mode => Ok(mode),
    }
}

/// Mask one email address for logs and error strings: `anna@example.test` becomes
/// `a***@example.test` (first character of the local part plus the whole domain).
/// Anything without exactly one `@` (or with an empty side) becomes `"***"`.
pub fn mask_email(address: &str) -> String {
    let mut parts = address.split('@');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(local), Some(domain), None) if !local.is_empty() && !domain.is_empty() => {
            let first = local.chars().next().expect("non-empty local part");
            format!("{first}***@{domain}")
        }
        _ => "***".to_string(),
    }
}

fn is_email_local_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '%' | '+' | '-')
}

fn is_email_domain_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_')
}

/// An email-shaped token: exactly one `@`, a non-empty local part, and a dotted domain
/// whose labels are non-empty (so `a@b`, `a@@b.io` and `not an@email` do not match, while
/// `anna@example.test` and `"anna@example.test",` cores do once punctuation is stripped).
fn looks_like_email(s: &str) -> bool {
    let mut parts = s.split('@');
    let (Some(local), Some(domain)) = (parts.next(), parts.next()) else {
        return false;
    };
    if parts.next().is_some() {
        return false;
    }
    if local.is_empty() || domain.is_empty() {
        return false;
    }
    if !local.chars().all(is_email_local_char) {
        return false;
    }
    if !domain.chars().all(is_email_domain_char) {
        return false;
    }
    if domain.starts_with('.') || domain.ends_with('.') || !domain.contains('.') {
        return false;
    }
    // No empty label (`a@b..io`).
    if domain.split('.').any(str::is_empty) {
        return false;
    }
    true
}

/// Mask every email-looking token inside free text (log lines that embed provider error
/// strings): `failed for anna@example.test: refused` becomes `failed for
/// a***@example.test: refused`. Whitespace-separated tokens keep their surrounding
/// punctuation (`<`, quotes, trailing commas); non-email text passes through unchanged.
pub fn mask_emails_in_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut first = true;
    for token in text.split_inclusive(char::is_whitespace) {
        if !first {
            // `split_inclusive` keeps the separator at the end of the previous token, so
            // nothing to add here; the flag only skips the (nonexistent) leading join.
        }
        first = false;
        // Split off trailing whitespace (kept verbatim) to inspect the word itself.
        let word_end = token
            .find(|c: char| c.is_whitespace())
            .unwrap_or(token.len());
        let (word, sep) = token.split_at(word_end);
        // Strip surrounding punctuation that is never part of an address.
        let core = word.trim_matches(|c: char| {
            !(is_email_local_char(c) || is_email_domain_char(c) || c == '@')
        });
        if looks_like_email(core) {
            let start = word.find(core).expect("trimmed core");
            out.push_str(&word[..start]);
            out.push_str(&mask_email(core));
            out.push_str(&word[start + core.len()..]);
        } else {
            out.push_str(word);
        }
        out.push_str(sep);
    }
    out
}

pub struct EmailChannel {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: Mailbox,
    timeout: Duration,
}

impl EmailChannel {
    pub fn new(cfg: SmtpConfig) -> Result<Self, String> {
        let from: Mailbox = cfg
            .from
            .parse()
            .map_err(|e| format!("invalid smtp from address {:?}: {e}", cfg.from))?;
        // Refuse silent plaintext towards remote hosts before building anything.
        let mode = smtp_security(&cfg)?;
        let base = match mode {
            SmtpTls::None => {
                // builder_dangerous = no TLS / STARTTLS (Mailpit on 1025). Pooling is on
                // by default.
                AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(cfg.host.clone())
            }
            // STARTTLS upgrade / implicit TLS via rustls with webpki roots (workspace lettre
            // feature `tokio1-rustls-tls`). `starttls_relay` requires the upgrade, so a server
            // that doesn't offer STARTTLS fails instead of silently downgrading.
            SmtpTls::StartTls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&cfg.host)
                .map_err(|e| format!("cannot build STARTTLS transport: {e}"))?,
            SmtpTls::Tls => AsyncSmtpTransport::<Tokio1Executor>::relay(&cfg.host)
                .map_err(|e| format!("cannot build TLS transport: {e}"))?,
        };
        let mut builder = base.port(cfg.port).timeout(Some(cfg.timeout));
        if let Some(username) = cfg.username.clone() {
            builder = builder.credentials(Credentials::new(
                username,
                cfg.password.clone().unwrap_or_default(),
            ));
        }
        let transport = builder.build();
        Ok(Self {
            transport,
            from,
            timeout: cfg.timeout,
        })
    }

    fn build_message(&self, msg: &OutboundMessage) -> Result<(Message, String), SendError> {
        // The recipient address never enters `SendError`: only the parse failure does.
        let to: Mailbox = msg
            .address
            .parse()
            .map_err(|e| SendError::Permanent(format!("invalid recipient address: {e}")))?;
        let message_id = format!("<{}@beacon>", msg.job_id);
        let mut email = Message::builder()
            .from(self.from.clone())
            .to(to)
            .subject(msg.subject.clone())
            .message_id(Some(message_id.clone()))
            .header(ContentType::TEXT_PLAIN)
            .body(msg.body.clone())
            .map_err(|e| SendError::Permanent(format!("cannot build message: {e}")))?;
        let headers = email.headers_mut();
        headers.insert_raw(HeaderValue::new(
            HeaderName::new_from_ascii_str("X-Beacon-Alert-Id"),
            msg.alert_id.to_string(),
        ));
        headers.insert_raw(HeaderValue::new(
            HeaderName::new_from_ascii_str("X-Beacon-Job-Id"),
            msg.job_id.to_string(),
        ));
        Ok((email, message_id))
    }
}

fn receipt_from(message_id: String, resp: &Response) -> Receipt {
    let message: Vec<&str> = resp.message().collect();
    Receipt {
        provider_message_id: message_id,
        detail: serde_json::json!({
            "smtp_code": resp.code().to_string().parse::<u16>().ok(),
            "smtp_message": message.join("\n"),
        }),
    }
}

impl Channel for EmailChannel {
    fn name(&self) -> &'static str {
        "email"
    }

    fn send<'a>(&'a self, msg: &'a OutboundMessage) -> BoxFuture<'a, Result<Receipt, SendError>> {
        Box::pin(async move {
            let (email, message_id) = self.build_message(msg)?;
            let outer = self.timeout + Duration::from_secs(1);
            match tokio::time::timeout(outer, self.transport.send(email)).await {
                Err(_elapsed) => Err(SendError::Ambiguous(format!(
                    "smtp send exceeded {}ms; server may have accepted the message",
                    outer.as_millis()
                ))),
                Ok(Ok(resp)) => Ok(receipt_from(message_id, &resp)),
                Ok(Err(e)) => {
                    // Scrubbed: a server reply must not echo the recipient into the job row.
                    let text = mask_emails_in_text(&e.to_string());
                    if e.is_permanent() {
                        Err(SendError::Permanent(text))
                    } else if e.is_transient() {
                        Err(SendError::Transient(text))
                    } else if e.is_timeout() {
                        Err(SendError::Ambiguous(text))
                    } else {
                        Err(SendError::Transient(text))
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_for(host: &str) -> SmtpConfig {
        SmtpConfig {
            host: host.to_string(),
            ..SmtpConfig::default()
        }
    }

    /// `EmailChannel::new` refusal message (the transport is not `Debug`, so no `expect_err`).
    fn new_err(cfg: SmtpConfig) -> String {
        match EmailChannel::new(cfg) {
            Ok(_) => panic!("expected EmailChannel::new to refuse"),
            Err(e) => e,
        }
    }

    #[test]
    fn local_hosts_allow_plaintext_by_default() {
        for host in [
            "localhost",
            "LOCALHOST",
            "  localhost  ",
            "localhost.",
            "127.0.0.1",
            "::1",
            "[::1]",
            "mailpit",
            "Mailpit",
            "MAILPIT",
        ] {
            assert!(is_local_smtp_host(host), "{host:?} must count as local");
            assert_eq!(smtp_security(&cfg_for(host)), Ok(SmtpTls::None), "{host:?}");
        }
        for host in [
            "",
            "example.test",
            "smtp.example.test",
            "mailpit.example.test",
            "localhost.example.test",
            "127.0.0.2",
            "127.1",
            "::2",
            "[::2]",
            "evil-localhost",
        ] {
            assert!(
                !is_local_smtp_host(host),
                "{host:?} must not count as local"
            );
        }
    }

    // Building a pooled transport spawns on drop, so this needs a Tokio runtime.
    #[tokio::test]
    async fn plaintext_to_remote_hosts_needs_the_explicit_opt_in() {
        let mut remote = cfg_for("smtp.example.test");
        assert_eq!(
            smtp_security(&remote),
            Err("plaintext SMTP to a non-local host needs SmtpConfig::allow_plaintext".to_string())
        );
        // So does the constructor: the exact refusal string is part of the contract.
        assert_eq!(
            new_err(cfg_for("smtp.example.test")),
            "plaintext SMTP to a non-local host needs SmtpConfig::allow_plaintext"
        );
        remote.allow_plaintext = true;
        assert_eq!(smtp_security(&remote), Ok(SmtpTls::None));
        // With the opt-in the transport builds (nothing is sent yet).
        assert!(
            EmailChannel::new(remote).is_ok(),
            "allow_plaintext opts a remote host back in"
        );
        // Local hosts never need the flag.
        assert!(EmailChannel::new(cfg_for("localhost")).is_ok());
        assert!(EmailChannel::new(cfg_for("mailpit")).is_ok());
    }

    #[tokio::test]
    async fn tls_modes_select_their_security_without_a_plaintext_fallback() {
        for (mode, host) in [
            (SmtpTls::StartTls, "localhost"),
            (SmtpTls::Tls, "localhost"),
            (SmtpTls::StartTls, "smtp.example.test"),
            (SmtpTls::Tls, "smtp.example.test"),
        ] {
            let cfg = SmtpConfig {
                tls: mode,
                ..cfg_for(host)
            };
            // The decision itself does not depend on the host: TLS is TLS.
            assert_eq!(smtp_security(&cfg), Ok(mode), "{mode:?} on {host}");
            // With the rustls backend compiled in, a TLS transport builds (no connection is
            // made until the first send).
            assert!(
                EmailChannel::new(cfg).is_ok(),
                "{mode:?} on {host} must build a TLS transport"
            );
        }
    }

    #[test]
    fn mask_email_keeps_the_first_letter_and_the_domain() {
        assert_eq!(mask_email("anna@example.test"), "a***@example.test");
        assert_eq!(mask_email("b@example.test"), "b***@example.test");
        assert_eq!(mask_email("anna+bounce@example.test"), "a***@example.test");
        for bad in ["", "no-at-sign", "@example.test", "anna@", "a@b@c.io", "@"] {
            assert_eq!(mask_email(bad), "***", "{bad:?}");
        }
    }

    #[test]
    fn mask_emails_in_text_scrubs_tokens_and_keeps_the_rest() {
        assert_eq!(
            mask_emails_in_text("refused for anna@example.test: 550"),
            "refused for a***@example.test: 550"
        );
        assert_eq!(
            mask_emails_in_text("to <anna@example.test>, cc bob@example.test ok"),
            "to <a***@example.test>, cc b***@example.test ok"
        );
        // Quoted, bracketed and parenthesised forms (typical SMTP replies).
        assert_eq!(
            mask_emails_in_text("invalid recipient address \"anna@example.test\": bad"),
            "invalid recipient address \"a***@example.test\": bad"
        );
        // Non-emails pass through unchanged: bare words, partial matches, no TLD.
        for plain in [
            "smtp 451 try later",
            "invalid recipient address: bad",
            "a@b has no domain dot",
            "not an@email address",
            "version 1@2 is not an email",
        ] {
            assert_eq!(mask_emails_in_text(plain), plain, "{plain:?}");
        }
        // The 451 regression message from the delivery suite survives masking.
        assert!(
            mask_emails_in_text("smtp 451 try later").contains("451"),
            "codes must survive scrubbing"
        );
    }
}
