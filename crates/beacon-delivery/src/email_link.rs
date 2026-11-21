//! Signed approval links for the email channel (SPEC §6.3, M7).
//!
//! An approval prompt email carries a link that decides through the SAME
//! `approvals::decide` state machine as the UI button and the SMS reply. The link is
//! authenticated by an HMAC-SHA256 signature over
//! `beacon-email-approval:v1:{approval_id}:{person_id}:{exp_unix}` — a domain-separated
//! message, distinct from the SMS reply callback's `"<timestamp>." + raw body`, so a
//! captured reply signature can never be replayed as a link and vice versa. `exp_unix` is
//! always the approval row's `expires_at` (unix seconds): the link dies with the request,
//! and the endpoint re-checks the row (pending, eligible, requester, single use) before
//! anything is decided — the signature only names WHO the link was issued to.
//!
//! The 4-digit code never enters an email (the email analogue of the code-at-rest rule in
//! `approvals`): the SMS prompt job carries `route_info.approval.code` because the SMS
//! text must name it, and the email prompt job's `route_info` simply has no `code` key.
//! The signature is the only secret in a link, and it is never logged.

use std::sync::Arc;

use hmac::{Hmac, Mac};
use sha2::Sha256;
use uuid::Uuid;

/// HMAC key + the public UI base for approval links. `Clone` (the key is shared behind an
/// `Arc`); the `Debug` impl never prints the secret, so a config that reaches a log line
/// leaks nothing.
#[derive(Clone)]
pub struct EmailLinkConfig {
    pub secret: Arc<Vec<u8>>,
    pub public_url: String,
}

impl std::fmt::Debug for EmailLinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmailLinkConfig")
            .field("secret", &"<redacted>")
            .field("public_url", &self.public_url)
            .finish()
    }
}

/// The exact bytes an approval link's signature covers.
fn signed_bytes(approval_id: Uuid, person_id: &str, exp_unix: i64) -> String {
    format!("beacon-email-approval:v1:{approval_id}:{person_id}:{exp_unix}")
}

/// Lowercase hex of HMAC-SHA256 over [`signed_bytes`].
pub fn sign(secret: &[u8], approval_id: Uuid, person_id: &str, exp_unix: i64) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret).expect("hmac-sha256 accepts any key length");
    mac.update(signed_bytes(approval_id, person_id, exp_unix).as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Constant-time verification of a [`sign`]ature: the comparison is `Mac::verify_slice`,
/// never a byte-by-byte early exit. A `sig_hex` that is not valid hex is simply `false`.
pub fn verify(
    secret: &[u8],
    approval_id: Uuid,
    person_id: &str,
    exp_unix: i64,
    sig_hex: &str,
) -> bool {
    let Ok(sig) = hex::decode(sig_hex) else {
        return false;
    };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret) else {
        return false;
    };
    mac.update(signed_bytes(approval_id, person_id, exp_unix).as_bytes());
    mac.verify_slice(&sig).is_ok()
}

/// Percent-encode `s` as one query component: RFC 3986 unreserved characters pass
/// through, everything else (including `+`, which is not a space in a query component)
/// becomes `%XX` with uppercase hex. The endpoint's query parser decodes the other side.
pub fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// `{public_url trimmed of trailing /}/api/approvals/email?a={id}&p={percent-encoded
/// person}&e={exp}&s={sig}` — the URL an approval prompt email carries.
pub fn link(cfg: &EmailLinkConfig, approval_id: Uuid, person_id: &str, exp_unix: i64) -> String {
    format!(
        "{}/api/approvals/email?a={}&p={}&e={}&s={}",
        cfg.public_url.trim_end_matches('/'),
        approval_id,
        percent_encode(person_id),
        exp_unix,
        sign(&cfg.secret, approval_id, person_id, exp_unix),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> EmailLinkConfig {
        EmailLinkConfig {
            secret: Arc::new(b"link-secret".to_vec()),
            public_url: "http://localhost:5173/".into(),
        }
    }

    #[test]
    fn sign_verify_roundtrip() {
        let id = Uuid::new_v4();
        let sig = sign(b"link-secret", id, "carol", 1_700_000_000);
        assert_eq!(sig.len(), 64, "sha256 hex");
        assert!(
            sig.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert!(verify(b"link-secret", id, "carol", 1_700_000_000, &sig));
    }

    #[test]
    fn tampering_any_field_fails() {
        let id = Uuid::new_v4();
        let sig = sign(b"link-secret", id, "carol", 1_700_000_000);
        // Wrong secret, wrong person, wrong expiry, wrong approval.
        assert!(!verify(b"other-secret", id, "carol", 1_700_000_000, &sig));
        assert!(!verify(b"link-secret", id, "bob", 1_700_000_000, &sig));
        assert!(!verify(b"link-secret", id, "carol", 1_700_000_001, &sig));
        assert!(!verify(
            b"link-secret",
            Uuid::new_v4(),
            "carol",
            1_700_000_000,
            &sig
        ));
        // A flipped signature byte fails too.
        let mut flipped = sig.clone();
        flipped.replace_range(0..1, if sig.starts_with('0') { "1" } else { "0" });
        assert_ne!(flipped, sig);
        assert!(!verify(
            b"link-secret",
            id,
            "carol",
            1_700_000_000,
            &flipped
        ));
    }

    #[test]
    fn bad_hex_is_false_not_an_error() {
        let id = Uuid::new_v4();
        for bad in ["", "zz", "0102not-hex", "0102030405", "0102 0304"] {
            assert!(
                !verify(b"link-secret", id, "carol", 1_700_000_000, bad),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn link_format_trims_the_base_and_encodes_the_person() {
        let id = Uuid::new_v4();
        let url = link(&cfg(), id, "bob", 1_700_000_000);
        let expected_prefix =
            format!("http://localhost:5173/api/approvals/email?a={id}&p=bob&e=1700000000&s=",);
        assert!(
            url.starts_with(&expected_prefix),
            "trailing / trimmed, fields in order: {url}"
        );
        let sig = url.strip_prefix(&expected_prefix).expect("sig suffix");
        assert!(verify(&cfg().secret, id, "bob", 1_700_000_000, sig));
        // A person id with reserved characters round-trips percent-encoded.
        let odd = "on call:night+shift";
        let url = link(&cfg(), id, odd, 5);
        assert!(
            url.contains(&format!("p={}", percent_encode(odd))),
            "person encoded: {url}"
        );
        assert!(percent_encode(odd).contains("%3A"));
        assert!(percent_encode(odd).contains("%2B"));
        assert!(!url.contains(' '), "no raw spaces in a url: {url}");
    }

    #[test]
    fn debug_never_prints_the_secret() {
        let text = format!("{:?}", cfg());
        assert!(text.contains("EmailLinkConfig"), "{text}");
        assert!(text.contains("http://localhost:5173/"), "{text}");
        assert!(!text.contains("link-secret"), "{text}");
    }
}
