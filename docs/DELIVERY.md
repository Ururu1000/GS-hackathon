# Delivery

Delivery turns an alert into the right messages for the right people and follows up until someone acts. It covers routing, channel preferences, email/SMS/phone delivery, retries, escalation, throttling, runbooks, actions and approvals. Code: [`crates/beacon-delivery`](../crates/beacon-delivery). Config: `config/people`, `config/teams`, `config/delivery.yaml`, `config/runbooks`, `config/actions`.

> Status: `main` has the M0 part (PRs #5 and #7). PR #9 (preferences, the fake gateway, the breaker and fallback) is approved. PR #11 (escalation, throttles, runbooks, approvals and the security-audit fixes) is in final review.

## How a notification flows

```
aggregator (ingest/engine) ── one PG tx ──► alerts + outbox(kind=notify, unexpanded route)
router      claims notify rows (FOR UPDATE SKIP LOCKED); in ONE tx:
            expand team:/user:/oncall: → people → channels (precedence below)
            → per-(rule, person) throttle → delivery_jobs (idempotent)
            → freeze the runbook for the alert → start the escalation timer
workers     per channel: lease due jobs → re-check the alert is still open → send
            outside any tx → finalize fenced by the lease token
scheduler   fires due escalation steps (next on-call → team lead) unless acked/resolved
actions     runs queued runbook actions; approvals gate the risky ones
```

**Channel precedence** (SPEC §4.4): a route's explicit `channels:` (merged across routes), then the person's first matching preference rule, then the team's `default_channels`, then `[ui]` (no message; the feed shows it). The decision and its reason are frozen on each job (`route_info.decision`), so retries never re-route.

**Preferences** live in `config/people/<id>.yaml`. Rules match on `severity`, `service`, `team`, `rule` or `environment`, can carry a half-open local-time window, and have an `otherwise` list. Anna's example routes critical `payments-api` alerts to SMS in `[08:00, 20:00)` Europe/London and to email outside that window. Windows are evaluated in the rule's time zone, so they're DST-safe, and they can wrap past midnight.

## Reliability

- **Exactly one job per notification and channel:** the unique key is `(alert, recipient, channel, notify_seq, escalation_step)`. A replayed outbox row or a router crash never doubles a page.
- **Retries:** capped, jittered exponential backoff; a provider's `Retry-After` is honoured, clamped at 10 min. Errors are classed as transient, permanent or ambiguous.
- **Circuit breaker** per channel: 5 transient failures open it for 5 s. While it's open, no jobs are claimed. After the cooldown, one leased probe decides; its lease (`probe_until`) lets another worker take over if the probe's worker crashes.
- **Attempts during an outage:** only a job that never reached the provider gets its attempt back. A real failed send always uses its attempt and waits at least the cooldown. The failure before a job's last attempt waits at least `max(cooldown, backoff cap)` (60 s by default). A 60 s outage therefore drains with nothing lost, and a much longer one ends in `dead` plus the one-hop fallback.
- **One-hop fallback** (`config/delivery.yaml`: phone → sms → email): a job that finally fails creates exactly one job on the frozen fallback address, in the same transaction.
- **No pages for acked or resolved alerts:** each attempt re-reads the alert first.
- **Honest limits:** sends are at least once (a crash after SMTP accepted the message can resend; the Message-ID stays stable). Postgres is a single point of failure in the demo.

## Escalation, throttles, runbooks, approvals

- **Escalation** (`scheduler` role): the first notification starts a durable timer per alert. The chain is frozen then: the rest of the on-call rotation, then the team lead. Each step fires under a lease, and an ack or resolve cancels the rest. Critical alerts bypass throttles.
- **Throttles:** a token bucket per (rule, recipient) from the rule's `throttle: {max, per}`, debited once per recipient notification. Over the limit nothing is dropped: notifications are parked and one "N more alerts held back" digest is sent per channel when tokens return.
- **Runbooks** (`config/runbooks`): YAML steps (Appendix B), Markdown checklists, free text or a link. The runbook is selected per alert (the rule's `runbook:`, else the team's runbooks with matching `matches`), rendered from the alert's first event (`{{workflowId}}`) with secrets redacted, and frozen on the alert. Email carries the steps; SMS carries a short link.
- **Actions** (`actions` role): `http_get` to allowlisted targets (`config/actions/targets.yaml`, no redirects, size caps), `retry_workflow`, `restart_service`, `open_ticket`, `escalate`, `notify`. Each runs off the alert pipeline with a lease. Conditions such as "retry fails twice" unlock steps; `resolves: true` resolves the alert.
- **Approvals:** high-risk or `approval: required` steps need the team lead or the current on-call person. The requester can never approve their own request.
  - **Code:** 4 digits, stored only as a hash, valid 15 min, 5 SMS attempts, single-use. It's scrubbed from jobs and gateway receipts once the approval is decided or expires.
  - **Where to approve:** in the UI, or by replying `Y 1234` / `N 1234` on the fake phone. `ACK` acks the alert.

## Running it

Infrastructure: Postgres 16, Mailpit (SMTP 1025, UI 8025) and Redpanda for the ingest side, e.g. `docker compose -f deploy/docker-compose.yml up -d`. Then:

```sh
export DATABASE_URL=postgres://beacon:beacon@localhost:5432/beacon
export GATEWAY_TOKEN=demo-token REPLY_SECRET=demo-reply-secret
cargo run -p beacon -- --migrate \
  --role ingest,matcher,aggregator,relay,api,router,delivery,gateway,scheduler,actions,delivery-api
```

| Role | What it does |
|---|---|
| `router` | outbox `notify` rows → delivery jobs, throttles, runbook freeze, escalation timers |
| `delivery` | channel workers for `DELIVERY_CHANNELS` (default `email,sms,phone`) |
| `gateway` | fake SMS/phone provider + device-simulator API (`127.0.0.1:8070`) and the mock action targets |
| `scheduler` | escalation steps |
| `actions` | runbook action executions; sweeps expired approvals |
| `delivery-api` | standalone dev server for delivery's API (`127.0.0.1:8071`) until beacon-api mounts `beacon_delivery::api::router` |

| Setting | Default | Notes |
|---|---|---|
| `BEACON_CONFIG_DIR` | `config` | `people/` and `teams/` are required; startup fails without them |
| `SMTP_HOST` / `SMTP_PORT` / `MAIL_FROM` | `localhost` / `1025` / `Beacon <beacon@example.test>` | |
| `SMTP_TLS` | `none` | `starttls` or `tls` (rustls); plaintext to a non-local host needs `SMTP_ALLOW_PLAINTEXT=1` |
| `SMTP_USERNAME` / `SMTP_PASSWORD` | unset | |
| `GATEWAY_TOKEN` | unset (dev) | when set, every `/provider/*` and `/api/simulator/*` call needs `Authorization: Bearer`; without it the gateway only binds loopback |
| `REPLY_SECRET` | unset | HMAC key shared by the gateway and delivery for fake-phone replies (signed with a timestamp, ±300 s) |
| `SMS_GATEWAY_URL` / `PUBLIC_URL` | `http://localhost:8070` / `http://localhost:5173` | |
| `GATEWAY_MAX_STREAMS` | `64` | concurrent simulator SSE streams |
| `ESCALATE_AFTER_OVERRIDE_MS` | unset | demo knob: replaces every rule's `escalate_after` |
| `DELIVERY_WORKERS` / `DB_MAX_CONNECTIONS` / `SHUTDOWN_GRACE_SECS` | `1` / sized from roles / `20` | |
| `DELIVERY_API_DEMO_HEADER` | `0` | dev only: `1` lets `X-Beacon-Demo-Actor` pick the acting user on the standalone API |

## Demos and measured results

Measured on one laptop with `docker compose` and a debug build, except the latency row, which ran on GitHub Actions (release build).

| Scenario | Result |
|---|---|
| **60 s SMS gateway outage** (SPEC §9 c), `./scripts/demo_sms_outage.sh --events 50 --outage-secs 60` | 50/50 SMS sent, 0 failed, 0 fallbacks, 50 distinct gateway receipts, 0 duplicates, at most 2 attempts per job; the last SMS went out **3.8 s** after the gateway came back. Phone calls for the same events were unaffected (all sent within 0.5 s). |
| Same outage with a single SMS (`--events 1`), the worst case for retries | sent, 0 failed: it used all 8 attempts, and the final-attempt floor kept the last one after the outage (sent 33 s after recovery) |
| 1,000 emails over 8 workers into Mailpit (`DELIVERY_LOAD_TEST=1 cargo test -p beacon-delivery --test delivery_m0`) | each exactly once; **~47–163 msgs/s**, limited by Mailpit |
| Escalation (4 s override) | unacked alert: carol (next on call) paged by phone + SMS; acked alert: timer cancelled, nobody else paged |
| Throttle `{max: 2, per: 1m}`, 5 trades | 2 emails per person, 3 held back, then one digest each: "3 more alert(s) … were held back by throttling" |
| Settlement-timeout runbook (SPEC M5) | health check ok → `retry_workflow` fails twice → `escalate` unlocks → approval SMS to carol → her `Y <code>` approves → `counterparty-support` (gina) emailed with the runbook steps; full audit timeline; no approval code left at rest |
| **Ingest → delivery latency** (SPEC §9, M6), `./scripts/demo_latency.sh --events 500 --rate 50` on CI ([run 44867217055](https://github.com/Ururu1000/GS-hackathon/actions/runs/44867217055), commit `9140087`, ubuntu-latest, 2 vCPU, release build) | 500/500 events delivered, 0 missing, 0 dead. Ingest → fake-gateway receipt (sms + phone): **p50 862 ms, p90 1361 ms, p99 1751 ms, max 1970 ms**. Ingest → Mailpit email: **p50 904 ms, p90 1317 ms, p99 1542 ms, max 1578 ms**. Full table below. |

Run the settlement-timeout flow by hand against the stack above. The mock workflow engine is set to fail with `POST :8070/api/simulator/mock {"target":"workflows","mode":"fail"}` plus the gateway token. Post `samples/events/workflow-failed.json` to `:8090/ingest/webhook`, then run the steps with `POST :8071/api/alerts/<id>/steps/<s1|s2|s3>/run`, and answer the approval SMS through `POST :8070/api/simulator/reply`.

### Measuring ingest → delivery latency

`./scripts/demo_latency.sh` (SPEC §9, M6, runs `beacon-delivery/examples/latency_bench.rs`) drives the running stack with an **open-loop** load: it posts unique `workflow-failed` (→ `oncall:settlements-primary`, phone + sms through the fake gateway) and `trade-rejected` (→ `team:rates`, email through Mailpit) sample events on a fixed schedule — never waiting for a response before the next send — and reports count/p50/p90/p99/max in ms (nearest-rank percentiles) per metric:

- **ingest → gateway receipt**: `gateway_receipts.received_at − t0` for each alert's phone and sms jobs,
- **ingest → email receipt**: the Mailpit message `Created` time − t0, falling back to `delivery_jobs.sent_at` when the message cannot be found,
- **the internal split**: **ingest → alert** (`alerts.first_seen − t0`) and **alert → delivered** (`sent_at − first_seen`).

`t0` is the client clock just before each POST; `--warmup` events prime the caches and are excluded from the stats. The report carries its workload parameters (events, rate, mix, warmup, timestamp, git commit, host CPUs) and the command exits non-zero when any expected receipt is missing or any POST failed. CI-friendly example: `./scripts/demo_latency.sh --events 200 --rate 20`; add `--out results.json` for the same report as JSON.

Caveat: gateway receipts are timestamped by Postgres and email by Mailpit, so the numbers are only valid when the client, Postgres and Mailpit share a clock (same machine / NTP-synced) — the benchmark prints that warning with every run.

**Measured result.** One CI run, [run 44867217055](https://github.com/Ururu1000/GS-hackathon/actions/runs/44867217055): commit `9140087`, a GitHub `ubuntu-latest` runner (2 vCPU), release `beacon --role ingest,matcher,aggregator,relay,router,delivery,gateway` and the `deploy/docker-compose.yml` services all on that runner, so every timestamp shares one clock. Workload: 500 measured events plus 20 warmup at 50 events/s, mix `both` (250 sms/phone events and 250 email events). The `results.json` artifact holds every sample.

| metric (ms) | count | p50 | p90 | p99 | max |
|---|---:|---:|---:|---:|---:|
| ingest → gateway receipt (phone + sms jobs) | 500 | 862 | 1361 | 1751 | 1970 |
| ingest → email receipt (Mailpit, per recipient) | 500 | 904 | 1317 | 1542 | 1578 |
| client → ingest accept (`first_seen − t0`) | 500 | 0 | 1 | 4 | 7 |
| accept → delivered (`sent_at − first_seen`, per job) | 1000 | 902 | 1355 | 1694 | 1973 |

0 ingest errors, 0 missing alerts, receipts or Mailpit messages, 0 pending, dead or cancelled jobs, 0 fallbacks.

`alerts.first_seen` is stamped with the event's `ingested_at`, so the "ingest → alert" split measures only the client → ingest accept step. Nearly all of the ~0.9 s median is in the pipeline after that: Kafka → matcher → aggregator linger → outbox relay → router → polling delivery worker → provider.

Re-run it: Actions → `loadtest` → Run workflow (once this file is on `main`), or push any commit to a `ci-run/loadtest-<name>` branch.

## What is mocked

- **SMS and phone calls:** the `gateway` role stands in for a provider such as Twilio. It persists every message (`gateway_receipts`), deduplicates by job id, simulates outages (`down`, `lose_response`) and feeds the UI's device simulator. A real provider is one `Channel` implementation.
- **Email** is real SMTP; in the demo it goes to Mailpit.
- **Action targets** (workflow engine, service restart, tickets, health checks) are mock endpoints on the gateway, each switchable between `ok` and `fail`.
- **People, teams and preferences** are YAML loaded at startup; versioned Postgres config with hot reload is M2.
- **Not built yet:** signed email approval links (M7), a recipient-wide throttle (M7), and writable people/teams APIs.

## Security notes

- No approval code is ever logged or returned by an API. Codes exist only in the SMS itself until the approval is decided.
- Phone numbers and email addresses are masked in logs and error text.
- Delivery's API trusts the `Actor` set by beacon-api's session/CSRF layer: every POST must send `Content-Type: application/json` (415 otherwise, even with an empty body), and only members, the lead or an on-call of the alert's team may run steps.
- Approval requests are race-safe (one request and one set of prompt SMS per step), and an `ACK` reply only acks an alert that was actually delivered to that phone.
- `/api/replies` only accepts HMAC-signed, timestamped calls from the gateway.
- The gateway needs a token for everything once `GATEWAY_TOKEN` is set, and won't listen beyond loopback without one.

## Tests

```sh
export DATABASE_URL=postgres://beacon:beacon@localhost:5432/beacon   # Postgres + Mailpit running
cargo test -p beacon-delivery          # unit + integration (sqlx::test: a fresh DB per test)
DELIVERY_LOAD_TEST=1 cargo test -p beacon-delivery --test delivery_m0   # adds the 1,000-email Mailpit run
```

The integration suites are `delivery_m0` (router/worker/email), `delivery_m4` (breaker, fallback, outage), `gateway`, `escalation`, `throttle`, `runbooks`, `actions`, `approvals`, `api`, `security_gateway` and `security_api`.
