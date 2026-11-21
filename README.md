# GS-hackathon

Hackathon 2026 (Software Engineering track): **Flexible Alerting & Notification Service**.

A service that ingests events of any shape from many sources, matches them against team-defined rules, deduplicates and groups them, and delivers alerts on each person's preferred channel (Email, SMS, Phone, UI) with the matching runbook attached.

- Problem statement: [docs/PROBLEM_STATEMENT.md](docs/PROBLEM_STATEMENT.md)
- Delivery (routing, channels, escalation, throttles, runbooks, approvals, outage results): [docs/DELIVERY.md](docs/DELIVERY.md)
- Ingest, engine and API (part a), with every review and limit: [progress.md](progress.md); engine load tests: [loadtest/README.md](loadtest/README.md)
- Adding a real SMS/phone/email provider (what is mocked and how small the swap is): [docs/PROVIDERS.md](docs/PROVIDERS.md)
- How we used AI tools, and security/privacy: [docs/AI_USAGE.md](docs/AI_USAGE.md)

## Quick start

Needs Docker with Compose. Nothing else: the image builds the Rust binaries and the UI.

```sh
git clone https://github.com/Ururu1000/GS-hackathon && cd GS-hackathon
docker compose up --build -d      # first build takes a few minutes
docker compose ps -a              # beacon healthy, demo-seed exited 0
```

| What | Where |
|---|---|
| UI + API | http://localhost:8080 |
| Webhook ingest | `POST http://localhost:8090/ingest/{source}` |
| Email inbox (Mailpit) | http://localhost:8025 |

`demo-seed` sends the brief's sample events once: two through Kafka (`workflow-events`, `trade-events`) and three over the webhook. The feed shows four alerts (the unknown shape raises none until a rule matches it), and Mailpit gets three emails.

**Things to try**
1. **New schema at runtime.** Post any JSON to `:8090/ingest/demo`, with the JSON content type:
   `curl -H 'content-type: application/json' --data @event.json http://localhost:8090/ingest/demo`
   The Sources tab shows its shape; write a rule in the Rules tab, dry-run it in the test panel, save. The next event raises an alert, with no restart.
2. **Runbook + approval.** Open the settlement-workflow alert, run the steps, and ask for approval on *escalate*. The Phone simulator tab gets the approval SMS; reply `Y <code>`. Or open the approval email in Mailpit (:8025) and follow its signed link: the page shows the request, and *Approve* decides it (opening the link alone changes nothing). To make the retry step fail so escalate unlocks:
   `docker compose exec beacon curl -s -X POST -H 'Authorization: Bearer beacon-demo-gateway-token' -H 'content-type: application/json' -d '{"target":"workflows","mode":"fail"}' http://127.0.0.1:8070/api/simulator/mock`
3. **Channel outage.** In the Phone simulator tab, open **Gateway outage**, pick SMS and 60 s, and press *Take SMS down for 60 s*. Then trigger alerts. The SMS are held, the breaker opens, and every SMS arrives after recovery (at-least-once delivery; in our 60 s outage runs, 0 lost and 0 duplicates against the gateway's receipts). *End SMS outage now* stops it early.
4. **Preferences.** Change a person's channels in Preferences; the next notification follows them.

Stop with `docker compose down` (`-v` also deletes the data).

The demo session is a fixed admin user (`BEACON_DEMO_USER`, default `anna`). There is no real login, so every port binds to localhost only.

## What is mocked

Real: Kafka (Redpanda), Postgres, the HTTP/webhook, Kafka, file-tail and poll sources, SMTP delivery, and every Beacon service. Simulated, because they are external systems:

| What | Mock | Swapping in the real thing |
|---|---|---|
| Email provider | [Mailpit](http://localhost:8025) catches every message over plain SMTP | Config only: `SMTP_HOST`, `SMTP_PORT`, `SMTP_TLS`, `SMTP_USERNAME`/`SMTP_PASSWORD` |
| SMS and phone provider | The `gateway` role: a fake provider with the Phone simulator tab, receipts, outage switch and signed replies | One adapter behind the `Channel` trait; retries, breaker and fallback stay shared ([docs/PROVIDERS.md](docs/PROVIDERS.md)) |
| Runbook action targets | The gateway's `/mock/*` endpoints: health checks, workflow retry, service restart, tickets (`POST /api/simulator/mock` makes them fail) | Point the action at a real URL (`config/actions/targets.yaml` aliases) |
| Login | One fixed demo session (`BEACON_DEMO_USER`, default `anna`, an admin), so every port binds to localhost | An auth layer in front of `beacon-api`; handlers already take the `Actor` from the session |
| LLM schema suggestions | Off by default; `BEACON_LLM=mock` is a labelled heuristic | `BEACON_LLM=claude` with `ANTHROPIC_API_KEY` |
| On-call rotations and working hours | Read from `config/people` and `config/teams` (editable at runtime) | No calendar or paging-service integration |

## UI

React + TypeScript in [`ui/`](ui/), built to static assets. The dev server ships with a mock API, so it runs without the backend:

```sh
cd ui
npm install
npm run dev     # http://localhost:5173 (mock API in-process)
npm test        # unit + SSE fixture tests
npm run build   # static assets → ui/dist/
BEACON_API_URL=http://localhost:8080 npm run dev   # against a real backend
```

The API contract the UI depends on is in [`ui/contract/openapi.yaml`](ui/contract/openapi.yaml). More detail is in [`ui/README.md`](ui/README.md).

## Delivery

Routing, channels (email, SMS, phone), escalation, throttles, runbooks and approvals. The full reference is [docs/DELIVERY.md](docs/DELIVERY.md).

Roles (pass to `beacon --role`, comma-separated):

| Role | What it does |
|---|---|
| `router` | turns outbox `notify` rows into delivery jobs; applies throttles, freezes the runbook, starts escalation timers |
| `delivery` | channel workers (`DELIVERY_CHANNELS`, default `email,sms,phone`) |
| `gateway` | fake SMS/phone provider and the device simulator (`GATEWAY_ADDR`, default `127.0.0.1:8070`) |
| `scheduler` | escalation steps |
| `actions` | runbook actions; expires stale approvals |
| `delivery-api` | dev-only standalone server for delivery's API on `127.0.0.1:8071`. Not needed with `api`: the `api` role serves the same routes behind the session, and the device simulator when `gateway` runs in the same process |

Required settings beyond `DATABASE_URL`:

| Variable | Purpose |
|---|---|
| `GATEWAY_ADDR` | where the fake gateway listens; set `0.0.0.0:8070` inside a container |
| `GATEWAY_TOKEN` | bearer token shared by `gateway` and `delivery`; `api` adds it to the UI's simulator calls server-side. Unset is dev mode: the gateway then only binds loopback |
| `REPLY_SECRET` | HMAC key shared by `gateway` and `api` (or `delivery-api`) for signed SMS replies (`Y <code>`, `ACK`). Unset rejects every reply. `docker compose up` sets demo values for both secrets |

Demos, with `DATABASE_URL` set and infrastructure from `docker compose -f deploy/docker-compose.yml up -d`:

```sh
# 60 s SMS gateway outage: no lost or duplicate SMS. Needs the stack running
# (beacon --role ingest,matcher,aggregator,relay,router,delivery,gateway).
./scripts/demo_sms_outage.sh --events 50 --outage-secs 60

# 1,000 emails into Mailpit, each delivered exactly once (Postgres + Mailpit only).
DELIVERY_LOAD_TEST=1 cargo test -p beacon-delivery --test delivery_m0
```

## Measured performance

Everything below was measured, not estimated. Each row lists its hardware, workload and run. Every run uses a release build, with the client, Postgres, Redpanda and Mailpit on one machine so all timestamps share a clock. Percentiles are nearest-rank. Methods, caveats and the full tables are in [docs/DELIVERY.md](docs/DELIVERY.md#demos-and-measured-results).

**Hardware**
- *CI*: GitHub Actions `ubuntu-latest` (2 vCPU), [`.github/workflows/loadtest.yml`](.github/workflows/loadtest.yml).
- *Mac*: Apple M5 Mac with 10 cores, stack via `docker compose`.

### Throughput

The workload is `beacon-loadgen --duration 30 --batch 100 --distinct-keys 1000 --match-ratio 0.8 --concurrency 256`. It sends open-loop HTTP batches of all four sample event types, and 80% of the events match a rule. It ran on *CI* through `.github/workflows/loadgen.yml`. Each latency starts at server ingest of the first event for an alert key, not at every event. All figures are p50 / p99 in milliseconds. The per-channel tables and the bottleneck analysis are in [docs/DELIVERY.md](docs/DELIVERY.md#under-beacon-loadgen-200-and-2000-eventss).

| Target rate | Commit, run | Accepted | Processed | Ingest → alert | Ingest → gateway (sms) | Ingest → Mailpit | Delivered |
|---|---|---|---|---:|---:|---:|---|
| 200 ev/s | `9b6dd76`, [run 44873549433](https://github.com/Ururu1000/GS-hackathon/actions/runs/44873549433) | 6,000 of 6,000 (203 ev/s), 0 rejected | 2,807 alerts; drained 2.5 s after sending stopped | 138 / 852 | 502 / 1,346 | 502 / 1,483 | 700 SMS, 700 phone, 2,090 email; 0 missing, dead or duplicate |
| 200 ev/s | `e5d7522`, [run 44870310169](https://github.com/Ururu1000/GS-hackathon/actions/runs/44870310169) | 6,000 of 6,000 (203 ev/s), 0 rejected | 2,783 alerts; drained 2.5 s after sending stopped | 518 / 2,892 | 3,916 / 5,330 | 4,341 / 5,868 | 684 SMS, 684 phone, 2,129 email; 0 missing, dead or duplicate |
| 200 ev/s | `631545c`, [run 44869239778](https://github.com/Ururu1000/GS-hackathon/actions/runs/44869239778) | 6,000 of 6,000 (203 ev/s), 0 rejected | 2,806 alerts; drained 2.5 s after sending stopped | 281 / 1,690 | 1,286 / 3,045 | 1,387 / 3,107 | 709 SMS, 709 phone, 2,140 email; 0 missing, dead or duplicate |
| 2,000 ev/s | `9b6dd76`, [run 44873549433](https://github.com/Ururu1000/GS-hackathon/actions/runs/44873549433) | 60,000 of 60,000 (2,000 ev/s), 0 rejected | 4,000 alerts; alerts drained 2.5 s after sending stopped, every delivery done by then | 2,198 / 13,607 | 13,423 / 23,153 | 14,672 / 23,067 | 1,000 SMS, 1,000 phone, 3,000 email; 0 missing, dead or duplicate |
| 2,000 ev/s | `e5d7522`, [run 44870310169](https://github.com/Ururu1000/GS-hackathon/actions/runs/44870310169) | 60,000 of 60,000 (2,000 ev/s), 0 rejected | 4,000 alerts; alerts drained 6.5 s and deliveries 20.2 s after sending stopped | 3,904 / 8,495 | 41,214 / 49,649 | 41,010 / 49,514 | 1,000 SMS, 1,000 phone, 3,000 email; 0 missing, dead or duplicate |

Rows on `9b6dd76` run four router tasks (`ROUTER_TASKS=4`, the default since M7) with one job `INSERT` per notification; rows on `e5d7522` and `631545c` ran one router task. The two older 200 ev/s runs used the same code path and workload, and their delivery p50 differs threefold. That is the noise of a shared 2-vCPU runner, so read these as ranges, not exact figures.

**At 2,000 ev/s**, nothing was lost. With one routing task (`e5d7522`), delivery fell behind: one batch of 100 per transaction with per-row inserts handled about 100 notifications/s, below what this burst produces, so jobs were planned at p50 about 40 s and the last delivery went out 20.2 s after sending stopped. M7 runs four routing tasks on the same outbox (claims use `FOR UPDATE SKIP LOCKED`, inserts are idempotent, throttle buckets are locked in one order) and writes each notification's jobs in one `INSERT`. On the same 2-vCPU runner that cut ingest → gateway to p50 13.4 s / p99 23.2 s. It is not 4×, because routers, engine, Postgres, Redpanda and Mailpit share two cores, and alert creation itself now reaches p99 13.6 s in this burst; the delivery workers are the next limit (planned → sent is about 3.5 s, up from about 1 s).

### Ingest → delivery latency

The workload is `./scripts/demo_latency.sh`. It sends open-loop unique events, half routed to phone and SMS through the fake gateway and half to email through Mailpit. The figures are milliseconds from the client's POST to the gateway receipt or the Mailpit message.

| Workload | Hardware, commit, run | Delivered | Gateway p50 / p90 / p99 / max | Mailpit p50 / p90 / p99 / max |
|---|---|---|---:|---:|
| 500 events at 50 ev/s (+20 warm-up) | *CI*, main `b8a4905`, [run 44868458383](https://github.com/Ururu1000/GS-hackathon/actions/runs/44868458383) | 500/500, 0 missing | 952 / 1225 / 1606 / 1804 | 936 / 1204 / 1391 / 1556 |
| 500 events at 50 ev/s (+20 warm-up) | *CI*, `9140087`, [run 44867217055](https://github.com/Ururu1000/GS-hackathon/actions/runs/44867217055) | 500/500, 0 missing | 862 / 1361 / 1751 / 1970 | 904 / 1317 / 1542 / 1578 |
| 500 events at 50 ev/s | *Mac*, main `6f01c42` | 500/500, 0 missing | 464 / 618 / 743 / 765 | 380 / 491 / 549 / 618 |
| 2,000 events at 200 ev/s | *Mac*, main `6f01c42` | 2000/2000, 0 missing, 0 dead | 653 / 833 / 959 / 1085 | 650 / 819 / 944 / 1030 |

Client → ingest accept takes under 5 ms at p99. Nearly all of the median is in the pipeline after that: Kafka → matcher → aggregator → outbox relay → router → delivery worker → provider.

### Channel outage (SPEC §9 c)

The workload is `./scripts/demo_sms_outage.sh --events 50 --outage-secs 60`. The fake SMS gateway goes down for 60 s while alerts keep arriving.

| Hardware, commit, run | Result |
|---|---|
| *CI*, main `b8a4905`, [run 44868458383](https://github.com/Ururu1000/GS-hackathon/actions/runs/44868458383) | **PASS**: 50/50 SMS sent, 0 lost, 0 dead, 0 duplicates, at most 3 attempts per job, last SMS 1.7 s after recovery |
| *Mac*, main `6f01c42` | **PASS**: 50/50 SMS sent, 0 lost, 0 dead, 0 duplicates, at most 3 attempts per job, last SMS 2.3 s after recovery |
| *Mac*, main `6f01c42`, single SMS (`--events 1`), the worst case for retries | sent, 0 dead, 0 duplicates; all 8 attempts used, sent 28.8 s after recovery |

Email alone sustained ~47–163 messages/s into Mailpit (1,000 emails over 8 workers, each delivered exactly once; Mailpit is the limit).

**Re-run:** Actions → `loadtest` → Run workflow, or push to a `ci-run/loadtest-<name>` branch.

## Architecture

**Sources.** HTTP webhooks need no config. Other sources are YAML files in `config/sources/`:
- `type: kafka`: external topics, with optional TLS/SASL credentials taken from env vars.
- `type: file`: tail a JSON-lines file (`path`, `start`, optional `checkpoint`). Rotation and truncation are handled, and bad lines are quarantined.
- `type: poll`: GET an HTTP API every `interval`, with optional `items`, `id_path` and `token_env` (a bearer token from an env var). Each item is published once.

The demo ships `file:applog` (`config/sources/applog.yaml`), which tails `/tmp/beacon/app.jsonl` in the container. The command to append a line is in that file.

```
webhook :8090 ─┐
Kafka sources ─┤
file tail ─────┤
HTTP poll ─────┴─► events.raw ─► matcher ─► alerts.matches ─► aggregator ─► Postgres: alerts + outbox (one tx)
                                 (rules reload live from Postgres)                 │
                                         relay ◄── alert_update ──────────────────┤
                                           ▼                                       └── notify ─► router ─► delivery_jobs
                                  alerts.updates ─► api (SSE) ─► UI                              ▼
                                                                    email / sms / phone workers, escalation scheduler,
                                                                    runbook actions, approvals, audit log
```

One binary, `beacon`, runs any mix of roles (`--role ingest,matcher,...`); the compose demo runs them all in one container.

**Where state lives**
- **Redpanda (Kafka API):** `events.raw`, `alerts.matches`, `alerts.updates`. Matcher → aggregator is exactly-once: Kafka transactions plus fenced checkpoints in Postgres.
- **Postgres:** everything durable. Alerts, the outbox, rules, schema mappings, proposals, delivery jobs, escalation timers, throttles, approvals, runbook copies and the audit log.
- **In memory only:** dedup id windows (rebuilt from the log), SSE fan-out, the fake gateway's outage/mock switches.

**Scaling.** Every role is a replica set. Matchers and aggregators share Kafka consumer groups (aggregators fence by partition). Each API replica consumes `alerts.updates` in its own group for SSE. Router, delivery workers, scheduler and actions claim rows with `FOR UPDATE SKIP LOCKED` and leases. A crashed worker's jobs are picked up when their lease expires.

**Single points of failure, by design for the hackathon:** one Postgres and one broker. Everything else can run several copies. Measured limits are in [Measured performance](#measured-performance); at 2,000 ev/s on 2 vCPU, alert creation and the delivery workers set the latency now that routing runs four tasks.

## Security and privacy

- **Ports:** every port binds to `127.0.0.1`. The demo session has no login, and writes need its `X-CSRF-Token`.
- **DNS rebinding:** the API answers only `localhost`, IP literals, `PUBLIC_URL`'s host and `API_ALLOWED_HOSTS`, and any other `Host` gets `421`. A page on another domain that re-resolves to `127.0.0.1` can't read the session or its CSRF token.
- **Webhook ingest:** `POST /ingest/*` must be `Content-Type: application/json`, so a page the user visits can't post events across sites. Set `INGEST_TOKEN` to require `Authorization: Bearer <token>`. Without it the ingest logs a warning when it listens beyond loopback.
- **Redaction:** secret-looking fields are redacted before they reach alerts, samples or notifications. Delivery addresses and provider errors are masked in alert detail.
- **Logs:** no message text, SMS codes or approval challenges. Phone numbers are masked.
- **Approvals:** codes are stored hashed, single-use, expire after 15 min, and allow 5 attempts. Nobody approves their own request. The approval email carries a link signed with `REPLY_SECRET` (HMAC over the approval, the approver and the expiry) instead of the code. Opening it only shows a confirm page, because mail scanners prefetch links. The *Approve*/*Reject* POST then goes through the same checks as the SMS reply, and a forged link burns no attempts.
- **SMS replies:** they reach the API signed with `REPLY_SECRET` (HMAC with a timestamp). `GATEWAY_TOKEN` stays on the server, and the API adds it to the UI's simulator calls.
- **Demo caveat:** the device simulator is a fake phone for the demo. Any session can read the SMS it holds, approval codes included, and reply as any number. A real SMS provider replaces it.
- **Limits:** bodies, rule sizes, regexes, templates, SSE streams and pending proposals are all capped, and every request is validated.
- **LLM (off by default):** `BEACON_LLM=mock` (a labelled name-based heuristic) or `claude` (needs `ANTHROPIC_API_KEY`) enables `POST /api/schemas/suggest`, a draft mapping for an unknown event shape. The model never sees the event, only its paths, value types and short redacted examples, and it has no tools. Its answer can only pick paths that exist in the sample and the approved transforms. The server builds and validates the mapping and previews it against the sample, and an admin publishes it through the normal path. Use synthetic data with a live model.

