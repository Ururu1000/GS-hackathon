# Beacon UI

React + TypeScript (Vite), built to static assets in `ui/dist/`. Spec: [§7 screens, §8 API](../docs/SPEC.md).

## Commands

Needs Node ≥ 22.18 with npm (the mock runs `.ts` files with Node's built-in type stripping).

```sh
cd ui
npm install
npm run dev        # http://localhost:5173, with the mock API in-process (no backend needed)
npm test           # reducer, SSE fixture, rule editor, preference and mock-engine tests (vitest)
npm run build      # typecheck + static build → ui/dist/
```

Against a real backend, the dev server proxies `/api` and `/ingest`:

```sh
BEACON_API_URL=http://localhost:8080 npm run dev
```

Standalone mock (for backend folks, or to serve a build): `npm run mock` serves the mock API on `:8787` (`PORT=` to change) and, if `ui/dist/` exists, the built UI too. `npm run build && npm run mock` gives a full demo on one port.

Device simulator calls (inbox, stream, replies, outage) also go through the API, which adds the gateway token server-side. Set `VITE_MAILPIT_URL` if the Mailpit inbox uses a port other than 8025. The mock implements the new source, schema, runbook import, and SMS reply endpoints. Live deployments need the corresponding backend routes. The recipient filter and runbook import appear when the session advertises support.

### Serving the build

`ui/dist/` is plain static files with relative asset URLs and hash routing (`#/alerts/<id>`), so any static server or the API itself can serve it from any path, with no SPA fallback needed. The UI expects the API on the same origin (`/api/...`). Set `VITE_API_BASE` at build time to point it elsewhere.

## API contract

- [`contract/openapi.yaml`](contract/openapi.yaml): the endpoints the UI uses (session, feed page, detail, ack/resolve with expected version, SSE stream; rules CRUD + `POST /api/rules/test`; people, teams and preferences; proposals; runbook step run/check and approvals), with conventions, the SSE protocol and the config-write rules (`expected_revision` → `409 revision_conflict`, `422 invalid` with `details`) in its description.
- [`src/api/types.ts`](src/api/types.ts): the TypeScript mirror. Keep the two in sync.
- [`contract/fixtures/stream-basic.sse`](contract/fixtures/stream-basic.sse): an annotated SSE session (snapshot, insert, counter bump, stale duplicate, ack, removal, blocked resurrection, unknown event) with the client state it must end in, in `stream-basic.expected.json`. `npm test` replays it through the UI's reducer. The backend can replay it too.

## Screens

| Route | What it does |
|---|---|
| `#/` · `#/alerts/<id>` | Live feed and alert detail: ack/resolve, the runbook with filled-in steps (manual checklist, action buttons, approve/reject for risky steps), deliveries, history, and **Suggest a change** |
| `#/rules` · `#/rules/<id>` | Rules with 24 h fire counts; editor with a visual condition builder (nested AND/OR/NOT, typed values) and a YAML tab over one draft, plus a live test panel (per-condition ✓/✗, rendered card, template warnings) that writes nothing. New rules start from templates (workflow failures, Prometheus alerts, business events) |
| `#/sources` | Sources and activity, discovery samples, rule drafts from events, and schema mapping preview and save |
| `#/runbooks` | Import structured YAML, Markdown, a link, or plain text. Edit the preview checklist and save it for selection in the rule editor |
| `#/simulator` | Live SMS and phone receipts, a Mailpit link, and approval replies where the gateway supports them |
| `#/preferences` | Per person: ordered "first match wins" lines (severity, service, team → channels, optional hours + time zone, otherwise) with a **Try it** check. Per team: members, lead, on-call rotations, default channels |
| `#/proposals` | Suggested rule changes with a YAML diff; accept publishes against the base revision (stale if the rule moved on) |

## Mock API (`mock/`)

An in-memory stand-in for the backend. It generates the Appendix A events (workflow failures, Prometheus alerts, rejected trades) plus pricing-engine log warnings and deployments, detects their schema, and evaluates the seed rules (the same YAML schema as `config/rules`) with a toy engine (`mock/engine.ts`: condition tree with the spec's missing-value semantics, `{{ }}` templates, validation). Rules edited in the UI change what the feed does. Matches are deduped by key and streamed with the snapshot protocol. It also accepts `POST /ingest/{source}` with any JSON, so you can curl a sample event and watch it appear.

Also in memory: people and teams (deliveries are planned from routes + preferences when an alert opens), the Appendix B runbooks with step state, approvals and fake actions (`mock/runbooks.ts`), and proposals (one auto-suggested). The real engine, delivery and config store are authoritative; writes to people and teams exist only in the mock until config moves to Postgres (spec §4.6, M2).

To demo approval, open a settlement alert, run **Retry workflow** twice, and request approval on Escalate. Open **Devices**, select Carol's approval SMS, and reply with `Y` followed by its code. The simulator records Carol as the approver. The alert detail shows the decision and execution. To test recovery via a successful retry instead, set `MOCK_RETRY_OK=1` before starting the mock.

| Env | Default | Effect |
|---|---|---|
| `MOCK_SEED` | `60` | Alerts seeded at startup (try `5000`) |
| `MOCK_RATE` | `2` | Events per second (try `200`) |
| `MOCK_STALE` | `0.03` | Chance to re-send an old version after an upsert (exercises the version check) |
| `MOCK_RESNAPSHOT_S` | `0` | Close every stream every N seconds (simulates a rebalance: reconnect + fresh snapshot) |
| `MOCK_RETRY_OK` | `0` | `1` = the mock workflow engine accepts retries |

`POST /api/__mock/burst?n=500` ingests a burst at once.

## Layout

```
src/api/        contract types + fetch client (CSRF header on writes; 409 returned as a value)
src/feed/       feedStore.ts (SSE snapshot protocol reducer, pure), useFeedStream.ts (EventSource +
                per-frame batching), FeedList (virtualized, scroll anchoring, "new alerts" pill), filters
src/alert/      AlertDetail, Runbook (steps, actions, approvals), shared badges
src/rules/      RulesList, RuleEditor (builder + YAML over one draft), ConditionBuilder, TestPanel,
                sections (recipients/presentation/noise, shared with Suggest a change), draft.ts (pure helpers)
src/prefs/      PreferencesScreen (people + teams), resolve.ts (channel choice, shared with the mock)
src/proposals/  ProposalsScreen (diffs, accept/reject), SuggestChange (from an alert)
src/lib/        router (hash routes), diff, time, icons
mock/           dev mock API: mock.ts (HTTP + SSE), engine.ts (toy rule engine), runbooks.ts, scenario.ts
contract/       OpenAPI + SSE fixtures
```
