# GS-hackathon

Hackathon 2026 (Software Engineering track): **Flexible Alerting & Notification Service**.

A service that ingests events of any shape from many sources, matches them against team-defined rules, deduplicates and groups them, and delivers alerts on each person's preferred channel (Email, SMS, Phone, UI) with the matching runbook attached.

- Problem statement: [docs/PROBLEM_STATEMENT.md](docs/PROBLEM_STATEMENT.md)
- Delivery (routing, channels, escalation, throttles, runbooks, approvals, outage results): [docs/DELIVERY.md](docs/DELIVERY.md)

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

The workload is `beacon-loadgen --duration 30 --batch 100 --distinct-keys 1000 --match-ratio 0.8 --concurrency 256`. It sends open-loop HTTP batches of all four sample event types, and 80% of the events match a rule. It ran on *CI* at commit `631545c` ([run 44869239778](https://github.com/Ururu1000/GS-hackathon/actions/runs/44869239778), `.github/workflows/loadgen.yml`). Each latency starts at server ingest of the first event for an alert key, not at every event. All figures are p50 / p99 in milliseconds.

| Target rate | Accepted | Processed | Ingest → alert | Ingest → gateway (sms) | Ingest → Mailpit |
|---|---|---|---:|---:|---:|
| 200 ev/s | 6,000 of 6,000 (203 ev/s), 0 rejected | 2,806 alerts; backlog drained 2.5 s after sending stopped | 281 / 1,690 | 1,286 / 3,045 | 1,387 / 3,107 |
| 2,000 ev/s | 60,000 of 60,000 (1,996 ev/s), 0 rejected | 4,000 alerts; drained 5.5 s after sending stopped (about 1,690 ev/s end to end) | 3,949 / 12,526 | not met, see below | not met, see below |

At 200 ev/s, delivery kept up: 709 SMS, 709 phone calls and 2,140 emails were sent, with 0 missing, 0 dead and 0 duplicates.

At 2,000 ev/s, ingest and alerting kept up on the 2-vCPU runner, but routing did not. When the measurement ran, 2,101 of the 3,000 alerts that notify someone still had no delivery jobs (the 1,000 pricing alerts show only in the UI). The jobs that did exist were planned at p50 24.8 s, and they reached the gateway at p50 28.5 s. The tool that measures delivery stopped before routing finished, so this run gives no valid delivery percentile at 2,000 ev/s. The fix is tracked on the board.

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

## Status

Work in progress. Setup instructions and architecture will be added here.
