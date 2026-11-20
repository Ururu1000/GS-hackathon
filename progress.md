# Progress: ingest, engine, API and deploy (part a)

Owner: **alexey**. The team split is in [docs/SPEC.md](docs/SPEC.md) §12:
- **(a)** ingest, matcher, dedup, load test, plus the API layer and `docker compose`. This document covers (a).
- **(b)** delivery: ilyaarch.
- **(c)** UI: erop-laptop.

Last updated: 2026-11-21.

## Status at a glance

| Area | State | Where |
|---|---|---|
| Event ingest (HTTP webhook + Kafka source) | Done | PR #8 |
| Schema detection, condition tree, matcher | Done | PR #8 |
| Dedup aggregator (alerts + outbox in one transaction) | Done | PR #8 |
| Live-feed relay (`alerts.updates`) | Done | PR #8 |
| Load generator + measured numbers | Done | PR #8, [loadtest/README.md](loadtest/README.md) |
| Notify payload for delivery (`context`, `event`) | Done | PR #12 |
| REST + SSE API for the UI | Done | PR #12 |
| Shared ack/resolve transitions for delivery | Done | PR #12 |
| Rules in Postgres, live reload, rules / test panel / schemas API | Done | PR #13 |
| One-command `docker compose up --build` | Built, end-to-end run pending | PR #13 |
| Mount delivery M5b routes (runbooks, approvals, simulator) | Next | waits for #11 in the stack |
| Kafka-mode load test, exact counts under replay (M1) | Later | |

PR merge order: #5 → #8 → #7 → #9 → #11 → #12 → #13. Each PR is stacked on the previous one.

## What the pipeline does

```
POST /ingest/{source} ─┐
Kafka source topics ───┴─► events.raw ─► matcher ─► alerts.matches ─► aggregator ─► Postgres alerts + outbox
                                          (rules reload live)                         │
                                                                     relay ◄──────────┤ alert_update
                                                                       │               └► notify ─► delivery (b)
                                                                       ▼
                                                     alerts.updates ─► beacon-api ─► SSE ─► UI (c)
```

- **Any JSON shape is accepted.**
  - Known shapes are recognised by YAML mappings in `config/schemas/`.
  - Unknown shapes get `schema = unknown:<signature>` and can still be matched on `raw.*`.
- **Rules use a typed condition tree:** `eq … regex`, `all`/`any`/`not`. A missing value compares false.
- **Dedup:** the same incident inside the window is one alert with a counter. 500 duplicates give 1 alert with count 500. A recovery signal (e.g. Prometheus `resolved`) closes the alert.
- **The four example rules** from the brief ship as seed config, each with an acceptance test.

## Measured performance

One laptop (Apple M5), everything local. Latency is measured from ingest to the alert update on `alerts.updates`.

| Rate | Errors | p50 | p99 |
|---|---|---|---|
| 5,000 events/s | 0 | 45.6 ms | 53.8 ms |
| 10,000 events/s | 0 | 41.8 ms | 67.2 ms |
| 20,000 events/s | 0 | 84.2 ms | 167.2 ms |

The target was ≥ 20,000 events/s with p99 < 250 ms. The reports and the workload parameters are in [loadtest/](loadtest/).

## API for the UI (`crates/beacon-api`, port 8080)

- **Feed:** `GET /api/session`, `GET /api/alerts` (filters, keyset paging), `GET /api/alerts/{id}` (deliveries, timeline).
- **Ack and resolve:** `POST /api/alerts/{id}/ack|resolve`. A 409 is returned only when the status really changed underneath you. Repeating a request is idempotent.
- **Live updates:** `GET /api/stream` (SSE) sends a snapshot, then live upserts. Stale versions are dropped. When the feed lags or Kafka drops, the stream closes so the browser resyncs.
- **Rules:**
  - `GET/POST /api/rules` and `GET/PUT/DELETE /api/rules/{id}`, with revisions, 409 conflicts and 422 details
  - `POST /api/rules/test`: a dry run with a per-condition breakdown and the rendered card
- **Schemas and sources:** `GET /api/events/recent`, `/api/schemas`, `/api/schemas/discovered`, `/api/sources`.
- **Static UI:** the built UI is served from the same origin. Every error uses the contract's `ApiError` shape.

## Run it

```bash
docker compose up --build        # whole demo: UI + API http://localhost:8080, ingest :8090, inbox :8025
```

For development:

```bash
docker compose -f deploy/docker-compose.yml up -d --wait
DATABASE_URL=postgres://beacon:beacon@localhost:5432/beacon \
  cargo run --release -p beacon -- --migrate --role ingest,matcher,aggregator,relay,api,router,delivery
```

## Reviews and testing

- **After each milestone:** an independent tester agent ran the tests, an end-to-end run and a code review. Every finding was fixed or explained.
- **PR #8:** reviewed in a loop with GPT-6.2-Sol (4 rounds).
  - Fixed:
    - recovery events arriving before their alert
    - payload keys named `id`/`source`
    - oversized matches
    - quarantine acks
    - request gating
    - load-generator correctness
  - Still open: rare cross-batch reorderings (see below).
- **Security review of PR #8 (Cursor agent):** 7 fixes:
  - NUL bytes stalling Postgres
  - a body-read timeout
  - generic 503s
  - bounded signature and regex sizes
  - money formatting
  - localhost-only ports
- **Tests:** 70+ Rust tests across core, ingest, engine and API, many of them against a real Postgres.

## Known limitations

- **Pipeline delivery is at-least-once.** A crash replay or a repeated `Idempotency-Key` can bump a count twice. Exact counts are M1.
- **Rare event reorderings can merge incidents.** An older fire arriving after a newer one, across batches, merges two incidents into one. The open/resolved state stays correct; only history is affected.
- **Schema mappings are read-only through the API for now.** They come from `config/schemas`, and edits arrive with the schemas write endpoints.
- **The demo session is a fixed user** (`BEACON_DEMO_USER`, default `anna`). There is no real authentication yet, so the API listens on localhost by default.

## Next

1. Verify `docker compose up --build` end to end and record the command count and time for the README.
2. Merge the PR stack, then mount delivery M5b: runbooks, approvals, the SMS/phone simulator.
3. Schema write endpoints (`POST /api/schemas`, `/api/schemas/preview`), the recipient feed, and occurrence charts.
4. Kafka-mode load test, and exact counts under replay (M1).
5. README, the "what is mocked" list, and the demo video script.
