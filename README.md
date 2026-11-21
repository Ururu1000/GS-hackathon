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

## Status

Work in progress. Setup instructions, architecture and load-test results will be added here.
