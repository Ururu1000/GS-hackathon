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

**Off-hours digest.** `otherwise` can also name `digest`: outside the window the notification is held instead of sent, and when the window next opens the person gets ONE email for everything held ("While you were off: 3 alerts, 1 resolved since"). The email lists each alert's *current* status. Critical alerts are never held: they go out as email at once. Escalation steps and runbook actions treat `digest` as email. `digest` is only valid in `otherwise`, because the window's start is the release time. Seed: Anna's warning line (`config/people/anna.yaml`) with the `payments-api-warnings` rule (`team:payments`, no explicit channels). Held rows live in `offhours_held`. The scheduler role releases them on each pass, and the alert timeline shows "held for …" and "sent … in their off-hours digest". Code: `crates/beacon-delivery/src/offhours.rs`.

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
  - **Email link (M7):** each eligible person with an email address also gets an approval email in Mailpit. It links to `{PUBLIC_URL}/api/approvals/email?a=<approval>&p=<person>&e=<expiry>&s=<signature>`, where the signature is HMAC-SHA256 under `REPLY_SECRET` over the approval, the person and the approval's own expiry. Opening the link only shows a confirm page, because mail scanners prefetch links. Approve or Reject on that page is a form POST that decides through the same path as the UI and the SMS reply, as that person, `via = email`. So the 15-minute expiry, single use, "lead or on-call, never the requester" and the 409 for an already-decided request all apply unchanged. A forged or altered link gets a 401 and uses up no SMS attempts. The email never contains the SMS code. Without `REPLY_SECRET` the email has no link and says to decide in Beacon.
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

Rows marked *CI* (and the latency rows) ran on a GitHub Actions `ubuntu-latest` runner (2 vCPU, release build, `.github/workflows/loadtest.yml`). Rows marked *Apple M5 Mac* were run by alexey on a 10-core Mac (release build, stack via `docker compose`). The other rows were measured on one laptop with `docker compose` and a debug build.

| Scenario | Result |
|---|---|
| **60 s SMS gateway outage** (SPEC §9 c), `./scripts/demo_sms_outage.sh --events 50 --outage-secs 60` | 50/50 SMS sent, 0 failed, 0 fallbacks, 50 distinct gateway receipts, 0 duplicates, at most 2 attempts per job; the last SMS went out **3.8 s** after the gateway came back. Phone calls for the same events were unaffected (all sent within 0.5 s). |
| *CI, on main* (`b8a4905`, [run 44868458383](https://github.com/Ururu1000/GS-hackathon/actions/runs/44868458383)): the same 60 s outage, 50 events | **PASS**: 50/50 SMS sent, 0 dead, 0 fallbacks, 50 distinct gateway receipts, 0 duplicates, at most 3 attempts per job; the last SMS went out **1.7 s** after the gateway came back |
| *Apple M5 Mac, 10 cores* (alexey; release build, main `6f01c42`): the same 60 s outage, 50 events | **PASS**: 50/50 SMS sent, 0 dead, 0 fallbacks, 50 distinct gateway receipts, 0 duplicates, at most 3 attempts per job; the last SMS went out **2.3 s** after recovery |
| *Apple M5 Mac, 10 cores* (alexey; release build, main `6f01c42`): the same outage with a single SMS (`--events 1`) | sent, 0 dead, 0 duplicates; all 8 attempts used; sent **28.8 s** after recovery (the final-attempt floor at work) |
| Same outage with a single SMS (`--events 1`), the worst case for retries | sent, 0 failed: it used all 8 attempts, and the final-attempt floor kept the last one after the outage (sent 33 s after recovery) |
| 1,000 emails over 8 workers into Mailpit (`DELIVERY_LOAD_TEST=1 cargo test -p beacon-delivery --test delivery_m0`) | each exactly once; **~47–163 msgs/s**, limited by Mailpit |
| Escalation (4 s override) | unacked alert: carol (next on call) paged by phone + SMS; acked alert: timer cancelled, nobody else paged |
| Throttle `{max: 2, per: 1m}`, 5 trades | 2 emails per person, 3 held back, then one digest each: "3 more alert(s) … were held back by throttling" |
| Settlement-timeout runbook (SPEC M5) | health check ok → `retry_workflow` fails twice → `escalate` unlocks → approval SMS to carol → her `Y <code>` approves → `counterparty-support` (gina) emailed with the runbook steps; full audit timeline; no approval code left at rest |
| **Ingest → delivery latency** (SPEC §9, M6), `./scripts/demo_latency.sh --events 500 --rate 50` on CI ([run 44867217055](https://github.com/Ururu1000/GS-hackathon/actions/runs/44867217055), commit `9140087`, ubuntu-latest, 2 vCPU, release build) | 500/500 events delivered, 0 missing, 0 dead. Ingest → fake-gateway receipt (sms + phone): **p50 862 ms, p90 1361 ms, p99 1751 ms, max 1970 ms**. Ingest → Mailpit email: **p50 904 ms, p90 1317 ms, p99 1542 ms, max 1578 ms**. Full table below. |
| *CI, on main* (`b8a4905`, [run 44868458383](https://github.com/Ururu1000/GS-hackathon/actions/runs/44868458383)): the same latency workload again | 500/500 delivered, 0 missing. Gateway: **p50 952 / p90 1225 / p99 1606 / max 1804 ms**. Email: **p50 936 / p90 1204 / p99 1391 / max 1556 ms** |
| *Apple M5 Mac, 10 cores* (alexey; release build, main `6f01c42`): `--events 500 --rate 50` | 500/500 delivered, 0 missing. Gateway: **p50 464 / p90 618 / p99 743 / max 765 ms**. Email: **p50 380 / p90 491 / p99 549 / max 618 ms** |
| *Apple M5 Mac, 10 cores* (alexey; release build, main `6f01c42`): `--events 2000 --rate 200` | 2000/2000 delivered, 0 missing, 0 dead. Gateway: **p50 653 / p90 833 / p99 959 / max 1085 ms**. Email: **p50 650 / p90 819 / p99 944 / max 1030 ms** |
| **Under beacon-loadgen** (*CI*, [run 44873549433](https://github.com/Ururu1000/GS-hackathon/actions/runs/44873549433), `9b6dd76`): 200 and 2000 ev/s for 30 s, 1000 keys per type | ingest accepted 203 / 2000 ev/s, 0 rejected; every job reconciled (0 missing). Ingest (first event per alert) → gateway receipt **p50 0.5 s / p99 1.3 s** at 200 ev/s, **p50 13.4 s / p99 23.2 s** at 2000 ev/s (41 s / 50 s with one router task) on 2 vCPU. Details under [Under beacon-loadgen](#under-beacon-loadgen-200-and-2000-eventss). |

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

### Under beacon-loadgen (200 and 2000 events/s)

The same latency under the team's load harness. `beacon-loadgen` (open loop, all four Appendix A event shapes) drives ingest, and then `scripts/delivery_latency.sh` (`beacon-delivery/examples/delivery_latency.rs`) reconciles that run's alerts. It finds them by loadgen's run id and leaves out its warm-up probe. Every alert's first-notification jobs are checked against gateway receipts (sms, phone) and Mailpit messages (email, `Message-ID <job_id@beacon>`). The tool waits until the router has processed every one of the run's `notify` outbox rows and every job is sent, and fails otherwise. Latencies are measured from `alerts.first_seen`, the ingest accept time of the alert's **first** event. Later duplicates of an alert are counted but not delivered again, so they aren't timed.

Workload (both rates): `beacon-loadgen --rate <R> --duration 30 --batch 100 --distinct-keys 1000 --match-ratio 0.8 --concurrency 256 --drain 15` over HTTP `POST /ingest/lg/batch`. Infrastructure: GitHub `ubuntu-latest` (2 vCPU), release build, `beacon --role ingest,matcher,aggregator,relay,router,delivery,gateway`, and `deploy/docker-compose.yml` (Redpanda, Postgres, Mailpit), all on that one runner. Routes: workflow failures go to on-call phone + sms, rejected trades to the rates team by email, Prometheus alerts to anna (email by her preferences), and pricing warnings to the UI only, so those aren't measured. Workflow: `.github/workflows/loadgen.yml`; the reports and JSON are in each run's `loadgen-results` artifact.

[Run 44873549433](https://github.com/Ururu1000/GS-hackathon/actions/runs/44873549433) (commit `9b6dd76`, four router tasks, `ROUTER_TASKS=4`, the default):

| rate | ingest | alerts | jobs reconciled | ingest (first event) → gateway receipt, sms / phone | ingest (first event) → Mailpit | of which: → job planned (sms) |
|---|---|---|---|---|---|---|
| **200 ev/s** | 6000 sent, 6000 accepted, **203 ev/s**, 0 rejected / failed | 2807 | 700 sms, 700 phone, 2090 email: all sent, 0 missing / dead / duplicate | p50 **0.5 s** / p99 **1.3 s**; phone p50 0.5 s / p99 1.3 s | p50 **0.5 s** / p99 **1.5 s** | p50 0.3 s / p99 0.9 s |
| **2000 ev/s** | 60000 sent, 60000 accepted, **2000 ev/s**, 0 rejected / failed | 4000 | 1000 sms, 1000 phone, 3000 email: all sent, 0 missing / dead / duplicate | p50 **13.4 s** / p99 **23.2 s**; phone p50 13.2 s / p99 23.0 s | p50 **14.7 s** / p99 **23.1 s** | p50 10.0 s / p99 17.9 s |

Before M7, with one router task ([run 44870310169](https://github.com/Ururu1000/GS-hackathon/actions/runs/44870310169), commit `e5d7522`, same workload): 2000 ev/s gateway sms p50 **41.2 s** / p99 **49.6 s**, Mailpit p50 41.0 s / p99 49.5 s, job planned p50 40.2 s / p99 48.4 s; 200 ev/s gateway sms p50 3.9 s / p99 5.3 s. Four router tasks without the batched job insert ([run 44872540962](https://github.com/Ururu1000/GS-hackathon/actions/runs/44872540962), 2000 ev/s only) gave job planned p50 23.2 s / p99 37.2 s and gateway sms p50 31.0 s / p99 39.8 s.

A second 200 ev/s sample, from [run 44869239778](https://github.com/Ururu1000/GS-hackathon/actions/runs/44869239778) (commit `631545c`, same workload, a different runner): 709 sms / 709 phone / 2140 email jobs, all sent. Gateway sms p50 **1.3 s** / p99 **3.0 s**, phone p50 1.1 s / p99 2.9 s, Mailpit p50 **1.4 s** / p99 **3.1 s**. Shared 2-vCPU runners differ that much from run to run. That run's 2000 ev/s figures are **not** used: the tool then stopped before the router had processed its outbox, so it measured only 30% of the notifications. The current tool waits and fails on unrouted rows.

What the numbers say:

- **Ingest keeps up**: 2000 ev/s accepted with 0 rejected. Alerts reach `alerts.updates` with first-alert p50 2.2 s / p99 13.6 s at 2000 ev/s (loadgen's own report), and p50 0.1 s / p99 0.9 s at 200 ev/s.
- **At 2000 ev/s everything shares 2 vCPU.** With 1000 keys per event type, all 4000 alerts open in the first seconds, a burst of 4000 notifications and 5000 jobs. One router task expanded about 100 notifications/s, so jobs were planned 40 s after ingest. Four router tasks (`ROUTER_TASKS`, M7) plus one `INSERT` per notification instead of one per job cut that to p50 10 s / p99 18 s. That is far from 4×, because the routers, the engine, Postgres, Redpanda and Mailpit share the same two cores. Part of the wait has moved downstream: alert creation itself reaches p99 13.6 s in this burst, and planned → sent is now about 3.5 s (it was about 1 s), so the delivery workers are the next limit. Nothing is lost: every job reconciles with exactly one receipt.
- **Several routers stay correct**: outbox rows are claimed `FOR UPDATE SKIP LOCKED`, every insert is idempotent, and each batch locks all the throttle buckets it may debit up front in one sorted order, so concurrent batches queue instead of deadlocking (a deadlock or serialization failure is retried after 10 ms anyway). `tests/router_concurrency.rs` checks four routers over 2000 alerts (each job exactly once) and four routers on one throttled recipient (never over the limit).
- On a 10-core machine the same pipeline delivers 200 ev/s with gateway p99 under 1 s (alexey's Mac rows above).

Re-run it: Actions → `loadgen` → Run workflow (inputs: rates, duration), or push to `ci-run/m6-latency`. By hand, against a running stack: `beacon-loadgen ... | tee lg.out`, then `scripts/delivery_latency.sh --run-id <the id in the report title>`.

## What is mocked

- **SMS and phone calls:** the `gateway` role stands in for a provider such as Twilio. It persists every message (`gateway_receipts`), deduplicates by job id, simulates outages (`down`, `lose_response`) and feeds the UI's device simulator. A real provider is one `Channel` implementation, or an adapter service with no Rust change: see [PROVIDERS.md](PROVIDERS.md).
- **Email** is real SMTP; in the demo it goes to Mailpit. A real relay (SES, SendGrid, ...) is configuration only.
- **Action targets** (workflow engine, service restart, tickets, health checks) are mock endpoints on the gateway, each switchable between `ok` and `fail`.
- **People, teams and preferences** come from `config/people` and `config/teams` (synthetic: `example.test` addresses, `+44 7700 900xxx` numbers). Edits through `PUT /api/people/{id}/preferences` and `PUT /api/teams/{id}` (the Preferences screen) are stored in Postgres (`directory_overrides`) and apply on the router's next pass, with no restart.
- **Not built:** a recipient-wide throttle (throttles are per rule and recipient). Approvals work by SMS code, in the UI, or through the signed email link.

## Security notes

- No approval code is ever logged or returned by an API. Codes exist only in the SMS itself until the approval is decided.
- Approval codes are 4 digits, stored only as a SHA-256 hash, unique per approver, single-use, valid for 15 minutes, with 5 attempts. Only the team lead or the current on-call may approve, never the person who asked, and the hash is scrubbed once the request is decided.
- Phone numbers and email addresses are masked in logs and error text.
- Delivery's API trusts the `Actor` set by beacon-api's session/CSRF layer: every POST must send `Content-Type: application/json` (415 otherwise, even with an empty body), and only members, the lead or an on-call of the alert's team may run steps.
- Approval requests are race-safe (one request and one set of prompt SMS per step), and an `ACK` reply only acks an alert that was actually delivered to that phone.
- `/api/replies` only accepts HMAC-signed, timestamped calls from the gateway.
- The gateway needs a token for everything once `GATEWAY_TOKEN` is set, and won't listen beyond loopback without one.
- Email refuses plaintext SMTP to a non-local host unless `SMTP_ALLOW_PLAINTEXT=1`; `SMTP_TLS=starttls|tls` for a real relay.
- How we used AI tools to build this, and how we kept data and secrets out of them: [AI_USAGE.md](AI_USAGE.md).

## Tests

```sh
export DATABASE_URL=postgres://beacon:beacon@localhost:5432/beacon   # Postgres + Mailpit running
cargo test -p beacon-delivery          # unit + integration (sqlx::test: a fresh DB per test)
DELIVERY_LOAD_TEST=1 cargo test -p beacon-delivery --test delivery_m0   # adds the 1,000-email Mailpit run
```

The integration suites are `delivery_m0` (router/worker/email), `delivery_m4` (breaker, fallback, outage), `gateway`, `escalation`, `throttle`, `runbooks`, `actions`, `approvals`, `email_approval`, `router_concurrency`, `api`, `security_gateway` and `security_api`.
