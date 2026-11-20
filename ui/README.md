# Beacon UI

React + TypeScript (Vite), built to static assets in `ui/dist/`. Spec: [§7 screens, §8 API](../docs/SPEC.md).

## Commands

Needs Node ≥ 22.18 with npm (the mock runs `.ts` files with Node's built-in type stripping).

```sh
cd ui
npm install
npm run dev        # http://localhost:5173, with the mock API in-process (no backend needed)
npm test           # reducer + SSE fixture tests (vitest)
npm run build      # typecheck + static build → ui/dist/
```

Against a real backend, the dev server proxies `/api` and `/ingest`:

```sh
BEACON_API_URL=http://localhost:8080 npm run dev
```

Standalone mock (for backend folks, or to serve a build): `npm run mock` serves the mock API on `:8787` (`PORT=` to change) and, if `ui/dist/` exists, the built UI too. `npm run build && npm run mock` gives a full demo on one port.

### Serving the build

`ui/dist/` is plain static files with relative asset URLs and hash routing (`#/alerts/<id>`), so any static server or the API itself can serve it from any path, with no SPA fallback needed. The UI expects the API on the same origin (`/api/...`). Set `VITE_API_BASE` at build time to point it elsewhere.

## API contract

- [`contract/openapi.yaml`](contract/openapi.yaml): the endpoints the UI uses (session, feed page, detail, ack/resolve with expected version, SSE stream), with conventions and the SSE protocol in its description.
- [`src/api/types.ts`](src/api/types.ts): the TypeScript mirror. Keep the two in sync.
- [`contract/fixtures/stream-basic.sse`](contract/fixtures/stream-basic.sse): an annotated SSE session (snapshot, insert, counter bump, stale duplicate, ack, removal, blocked resurrection, unknown event) with the client state it must end in, in `stream-basic.expected.json`. `npm test` replays it through the UI's reducer. The backend can replay it too.

## Mock API (`mock/`)

An in-memory stand-in for the backend. It generates the Appendix A events (workflow failures, Prometheus alerts, rejected trades) plus pricing-engine log warnings and deployments, matches them with toy versions of the seed rules, dedups them by key, and streams the result with the snapshot protocol. It also accepts `POST /ingest/{source}` with any JSON, so you can curl a sample event and watch it appear.

| Env | Default | Effect |
|---|---|---|
| `MOCK_SEED` | `60` | Alerts seeded at startup (try `5000`) |
| `MOCK_RATE` | `2` | Events per second (try `200`) |
| `MOCK_STALE` | `0.03` | Chance to re-send an old version after an upsert (exercises the version check) |
| `MOCK_RESNAPSHOT_S` | `0` | Close every stream every N seconds (simulates a rebalance: reconnect + fresh snapshot) |

`POST /api/__mock/burst?n=500` ingests a burst at once.

## Layout

```
src/api/        contract types + fetch client (CSRF header on writes; 409 returned as a value)
src/feed/       feedStore.ts (SSE snapshot protocol reducer, pure), useFeedStream.ts (EventSource +
                per-frame batching), FeedList (virtualized, scroll anchoring, "new alerts" pill), filters
src/alert/      AlertDetail, shared badges (severity label, status, live dedup counter)
mock/           dev mock API
contract/       OpenAPI + SSE fixtures
```
