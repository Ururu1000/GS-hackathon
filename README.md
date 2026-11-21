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
| `delivery-api` | dev-only standalone server for delivery's API on `127.0.0.1:8071` |

Required settings beyond `DATABASE_URL`:

| Variable | Purpose |
|---|---|
| `GATEWAY_ADDR` | where the fake gateway listens; set `0.0.0.0:8070` inside a container |
| `GATEWAY_TOKEN` | bearer token shared by `gateway` and `delivery`. Unset is dev mode: the gateway then only binds loopback |
| `REPLY_SECRET` | HMAC key shared by `gateway` and `delivery-api` for signed SMS replies (`Y <code>`, `ACK`). Unset rejects every reply |

Demos, with `DATABASE_URL` set and infrastructure from `docker compose -f deploy/docker-compose.yml up -d`:

```sh
# 60 s SMS gateway outage: no lost or duplicate SMS. Needs the stack running
# (beacon --role ingest,matcher,aggregator,relay,router,delivery,gateway).
./scripts/demo_sms_outage.sh --events 50 --outage-secs 60

# 1,000 emails into Mailpit, each delivered exactly once (Postgres + Mailpit only).
DELIVERY_LOAD_TEST=1 cargo test -p beacon-delivery --test delivery_m0
```

## Status

Work in progress. Setup instructions, architecture and load-test results will be added here.
