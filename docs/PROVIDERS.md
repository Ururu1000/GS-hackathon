# Adding a real delivery provider

The brief lets external channels be simulated, as long as "adding a real provider would be a small, isolated change". This page shows how small that change is in Beacon. Email already uses real SMTP. SMS and phone calls go to the fake `gateway` role, and a real provider replaces only that last hop.

## What a channel is

A channel is one Rust trait, `beacon_delivery::channel::Channel` (`crates/beacon-delivery/src/channel.rs`):

```rust
pub trait Channel: Send + Sync {
    fn name(&self) -> &'static str;                        // "sms", "phone", "email", ...
    fn render(&self, alert: &AlertView, job: &DeliveryJob) -> OutboundMessage; // has a default
    fn send<'a>(&'a self, msg: &'a OutboundMessage) -> BoxFuture<'a, Result<Receipt, SendError>>;
}
```

`send` has two rules. It must not touch the database, and it must be bounded by a timeout. Everything else lives in shared code and stays the same for every provider:

| Shared, provider-independent | Where |
|---|---|
| Routing: rule route → people → channels (preferences, time windows, team defaults) | `router.rs`, `prefs.rs` |
| Job queue: leases with `FOR UPDATE SKIP LOCKED`, finalize fenced by `lease_token`, crash recovery | `worker.rs` |
| Retries: capped, jittered exponential backoff (1 s → 60 s), 8 attempts by default | `worker.rs`, `BackoffPolicy` |
| Per-channel circuit breaker with a single half-open probe | `breaker.rs` |
| One-hop fallback when a job dies (phone → sms, sms → email; a fallback never falls back again) | `worker.rs`, `config/delivery.yaml` |
| Escalation, throttles + digests, runbook approvals by SMS | `escalation.rs`, `throttle.rs`, `approvals.rs` |
| The provider's receipt stored on the job (`delivery_jobs.provider_receipt`) | `worker.rs` |

## The only thing a provider decides: how a failure is classified

`send` returns `Ok(Receipt)` or one of four `SendError`s. The worker does the rest:

| `SendError` | Use it for | What the worker does |
|---|---|---|
| `Transient(msg)` | connection refused, DNS, `5xx` | retry with backoff, and count toward the breaker |
| `RetryAfter(msg, d)` | `429` / `503` with `Retry-After` | retry no earlier than `d` (clamped to 10 min), and count toward the breaker |
| `Ambiguous(msg)` | timeout after the request was sent, `504` | retry, since the provider may already have accepted it (see idempotency below), and count toward the breaker |
| `Permanent(msg)` | bad number or address, other `4xx`, a `3xx` (wrong URL) | `dead` at once, plus the one-hop fallback. Never counts toward the breaker, because it says nothing about the provider |

A job also dies after its last attempt, which creates the fallback job too. `GatewayChannel::send_error_for_status` in `channel/gateway_client.rs` is a tested, pure example of this mapping.

**Idempotency.** `OutboundMessage.job_id` is stable across retries. Email sends it as the Message-ID (`<{job_id}@beacon>`), and the fake gateway takes it as `Idempotency-Key`, so a retried request is recognised and not stored twice. If the real provider supports an idempotency key, pass `job_id`. If it does not, an `Ambiguous` retry can duplicate a message. That is the at-least-once trade-off we document (SPEC §13); it never loses one.

## Option A: no Rust change at all

`GatewayChannel` already speaks a small HTTP contract, documented at the top of `channel/gateway_client.rs`:

```
POST {SMS_GATEWAY_URL}/provider/{sms|phone}
Idempotency-Key: <job_id>
Authorization: Bearer <GATEWAY_TOKEN>          (when set)
{"job_id", "alert_id", "recipient", "to", "text"} → 200 {"receipt_id", "duplicate"}
```

A thin adapter service that accepts this contract and forwards to Twilio, Vonage or a carrier API makes a real provider a deployment change. Point `SMS_GATEWAY_URL` at the adapter and stop running the `gateway` role. The status codes it returns map onto the table above.

## Option B: a native channel (about 60 lines)

1. **Implement `Channel`** in a new file, for example `crates/beacon-delivery/src/channel/twilio.rs`. Here is a sketch for an HTTP SMS API (illustrative, not compiled):

   ```rust
   pub struct TwilioSms { http: reqwest::Client, account: String, token: String, from: String }

   impl Channel for TwilioSms {
       fn name(&self) -> &'static str { "sms" }
       fn render(&self, alert: &AlertView, job: &DeliveryJob) -> OutboundMessage {
           // keep it short for SMS; GatewayChannel::render has the shared SMS format
           /* ... */
       }
       fn send<'a>(&'a self, msg: &'a OutboundMessage) -> BoxFuture<'a, Result<Receipt, SendError>> {
           Box::pin(async move {
               let res = self.http
                   .post(format!("https://api.twilio.com/2010-04-01/Accounts/{}/Messages.json", self.account))
                   .basic_auth(&self.account, Some(&self.token))
                   .form(&[("To", msg.address.as_str()), ("From", self.from.as_str()), ("Body", msg.body.as_str())])
                   .send().await
                   .map_err(|e| if e.is_timeout() { SendError::Ambiguous(e.to_string()) }
                                else { SendError::Transient(e.to_string()) })?;
               match res.status().as_u16() {
                   200 | 201 => { /* parse "sid" → Receipt { provider_message_id: sid, detail } */ }
                   429 | 503 => { /* RetryAfter if the header is present, else Transient */ }
                   400..=499 => { /* Permanent: bad number, unverified sender, ... */ }
                   _ => { /* Transient */ }
               }
           })
       }
   }
   ```

   Build the `reqwest::Client` with a timeout and with redirects disabled, as `GatewayChannel::new` does.

2. **Register it.** Add one arm to the `DELIVERY_CHANNELS` match in `crates/beacon/src/main.rs`, the delivery role. Read credentials from the environment (`TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`), never from YAML or the repo. Workers, the breaker and the fallback pick the channel up by name.

3. **Test it.** Unit-test the status → `SendError` mapping as a pure function, the way `send_error_for_status` is tested. The breaker, fallback and outage suites (`delivery_m4`) run against the `Channel` trait, so they need no change.

## Email

Email is already a real provider: SMTP through `lettre`. Pointing it at SES, SendGrid or Postmark is configuration only: `SMTP_HOST`, `SMTP_PORT`, `SMTP_TLS=starttls|tls`, `SMTP_USERNAME`, `SMTP_PASSWORD`, `MAIL_FROM`. Beacon refuses plaintext SMTP to a non-local host unless `SMTP_ALLOW_PLAINTEXT=1`.
