# Spec — Flexible Alerting & Notification Service

Status: **draft v0.2** · Source brief: [PROBLEM_STATEMENT.md](PROBLEM_STATEMENT.md)

- **Builds on** the board's `proposal.md` (alexey/claude): the Variant A backbone, plus the rule playground and both AI features.
- **v0.2** merges a design review by 8 component reviewers. Their reports are in [docs/reviews/2026-11-20-sol/](reviews/2026-11-20-sol/).
- **§13 is the cut line**: what we build and what we claim in the demo.

Working name: **Beacon** (placeholder).

---

## 1. Goals and non-goals

### Goals (what the judges score)

| Criterion | What we do about it |
|---|---|
| Flexibility | New source instance / schema / rule / preference / runbook / **HTTP action** = config. A new transport, channel provider or action protocol = one trait impl registered by name. Rules and schema mappings are versioned data, reloaded live. |
| Scalability | Stateless ingest + matcher. Partition-owned stateful aggregation and recipient routing with fenced checkpoints in Postgres. Delivery and action jobs are leased from Postgres. Every role scales horizontally; the limits are stated in §2.4. |
| UX | Rule editor with a **side-effect-free live test panel** (paste any JSON, see each condition's result and the rendered alert). Alerts show the filled-in runbook and one-click actions. `docker compose up` → working demo. |
| Throughput / latency | Built-in open-loop load generator. Separate histograms for first-alert-in-browser, counter freshness and gateway acceptance. Results come with their workload parameters. Measured, not estimated. |
| Innovation | Unknown-schema onboarding ("create a rule from this event") with no deploy and no LLM needed. LLM-proposed mappings and runbook steps. Post-incident runbook diffs. |

### Non-goals

- Real SMS/phone/email providers. These are simulated; email is real SMTP into a local Mailpit.
- Multi-tenant auth / SSO. Named users and teams with a user picker. The identity boundary is noted as needed before live integrations.
- A general-purpose workflow engine. Actions are a small registry; no arbitrary branching.
- Database HA in the demo (see §2.4).

---

## 2. Architecture

```
 sources ─► ingest (HTTP webhook, external Kafka; stateless)          202 only after broker ack
              │
              ▼  events.raw            key: event id
         matcher (stateless)           normalize + match; Kafka consume→produce transaction
              │
              ▼  alerts.matches        key: rule + dedup tuple   (grouped rules: rule + group tuple)
         aggregator (partition owner)  dedup / group windows / quiet timers
              │   one fenced PG tx: alert mutations + intents + window state + next offset
              ▼
         Postgres ── outbox relay ──►  notifications   key: recipient id
              │                   └─►  alerts.updates  key: alert id (broadcast to every API replica)
              │
         recipient router (partition owner)  freeze routing, per-(rule,recipient) + per-recipient throttle
              │   one fenced PG tx: bucket state + delivery jobs + next offset
              ▼
         delivery_jobs (PG) ◄── channel workers (leases, SKIP LOCKED): retry, breaker, DLQ, fallback
              │                          ├─ email  → lettre → Mailpit
              │                          ├─ sms    → fake gateway (persists receipts, accepts replies)
              │                          └─ phone  → fake gateway (TTS script)
         action_executions (PG) ◄── action workers (leases): runbook actions after approval
         timers (PG) ◄── scheduler (SKIP LOCKED): escalation steps

         API replicas (axum): REST + SSE. Each consumes alerts.updates in its own group → per-scope fan-out → UI
```

All roles live in one Rust binary with role flags: `beacon --role ingest,matcher,aggregator,router,delivery,actions,scheduler,api`. The demo runs everything in one process. The scale demo runs roles as separate containers with replicas.

### 2.1 Why this shape

- **Matching is stateless**, so any matcher replica handles any event.
- **Stateful work is owned, not shared.** Matches are keyed by what the state is about: `rule + dedup tuple`, or for grouped rules `rule + group tuple`, so Pricing's "one alert per instrument" lands on one owner. Notifications are keyed by recipient. Kafka partitioning (24 fixed partitions) gives each replica exclusive ownership of its keys. There is no distributed lock and no shared hot key.
- **No Redis.** Single-key ownership makes a shared window store unnecessary. A Redis `INCR/EXPIRE` store also could not give the atomic checkpoint contract in §2.2, so there is no `WindowStore` abstraction for swapping backends.
- **Retries live in Postgres, not in Kafka.** A worker sleeping on a failed record would block healthy jobs on the same partition. Channel workers lease due jobs from an indexed `delivery_jobs` table instead. A channel outage just leaves jobs pending.

### 2.2 State, ownership and exactness

**Fenced checkpoints (aggregator and router).**
1. **On assignment:** take the PG checkpoint row for `(pipeline, topic, partition)`, bump its **ownership epoch**, restore the state, and seek to the stored **next offset**. Kafka auto-commit and auto-store are off.
2. **Every flush (including timer-only flushes):** lock the row and verify the epoch and expected checkpoint. Then commit dirty window and bucket state, deadlines, alert/audit mutations, outbox intents and the new next offset in **one transaction**.
3. **On revoke:** stop dispatching and discard uncommitted local state. A stale owner's write fails the epoch check.
4. **On an ambiguous commit:** reload and seek before reapplying anything. If the stored offset has fallen out of Kafka retention, stop with a visible recovery error.

The result: **exactly-once effects per consumed offset.**

**Matcher replay.**
- The matcher publishes matches and commits its input offsets in one **Kafka transaction**. Downstream consumers read with `read_committed`. Each match carries a stable `match_id = (event_id, rule_id)` and, separately, the config revision.
- Fallback if transactions fail the M0 spike: at-least-once matching, with `match_id` deduplicated durably by the aggregator over the retention horizon.
- Ingest retries keep the same event id.

**Alert identity and dedup generations.**
- Dedup is a **sliding inactivity window** over `last_seen`, with a persisted generation.
- A partial unique index on `alerts(rule_id, fingerprint) WHERE dedup_active AND status IN ('open','acked')` uses the same predicate as the `ON CONFLICT` target.
- Acked alerts keep counting without re-paging.
- When the window expires, the old generation retires (`dedup_active = false`) without being resolved, and the next match starts a new generation. Resolution stays a separate lifecycle (§6.4).

**Write budget.**
- First alerts are written promptly. Duplicate count bumps are coalesced per dirty alert and flushed every **200 ms** (configurable) as set-based batch writes.
- No raw events in Postgres: they stay in Kafka retention. Alerts keep the first N sample refs plus the latest one.
- No audit entry per duplicate.
- Discovery samples for the playground and unknown schemas go to a bounded, TTL'd, broker-backed store with a global entry/byte budget. That keeps them visible from every API replica, unlike a per-replica memory ring.

**Delivery idempotency.**
- `delivery_jobs` has a unique `(alert_id, recipient_id, channel, notify_seq)`. `notify_seq` is allocated under the alert lock, and the job id is propagated everywhere.
- Workers claim jobs in a short transaction (`FOR UPDATE SKIP LOCKED`, a lease deadline and a fresh token), send outside the lock, and finalize only if they still hold that token.
- The fake gateway deduplicates by job id and persists a receipt before replying. SMTP is **at-least-once**: a crash after provider acceptance can send twice. We state this, and Message-ID is stable.
- Each worker re-reads alert eligibility right before every attempt, so there are no pages for acked or resolved alerts. A send already in flight when an ack arrives is a known race.

### 2.3 Horizontal scaling per role

| Role | Scaling | Notes |
|---|---|---|
| ingest | stateless, N replicas | Bounded body size and in-flight requests. `429 + Retry-After` on saturation, `503` on broker failure or an ambiguous timeout. |
| matcher | up to the `events.raw` partition count | One immutable config snapshot per event (detectors, mappings, compiled CEL, index, templates). JSON parsed once. Bounded queues, so bursts show up as broker lag. |
| aggregator | up to 24 (partition count) | A hot fingerprint is serial on its owner, which is cheap (in-memory increment, coalesced write). Group buckets flush on deadline even when idle. |
| router | up to 24 | Routing is resolved once and frozen on the intent, so retries never change recipients mid-handover. Both throttle scopes are debited atomically, once per recipient intent. |
| delivery | N, per provider | Bounded concurrency per provider. Circuit breaker per endpoint with one leased probe after cooldown. Deferred jobs don't consume attempts. |
| actions | N | API returns `202` after persisting the intent. Bounded timeouts, response size and attempts. |
| scheduler | N, no leader | Claims due escalation rows with `SKIP LOCKED`, then locks and rechecks the alert's status and step before acting. |
| api | N | Each replica gets every `alerts.updates` record, so broker cost scales with the replica count. A per-scope subscriber registry with bounded buffers; slow sockets are dropped and resnapshotted. |

### 2.4 Limits, stated honestly

- **Postgres is the write bottleneck and a SPOF.** Every new alert and its delivery jobs converge on one primary. Rough rates at 20k events/s:

  | Share of events that open a new alert | New alerts/s | Delivery-job inserts/s |
  |---|---|---|
  | 1 % | ~200 | ~400 |
  | 100 % | 20k | 40k |

  The second row is not credible on a laptop, so we publish results per workload (§9). During a PG outage, durable processing pauses and Kafka absorbs the backlog within retention. HA (Patroni) improves availability, not write capacity.
- **The broker is a single node in compose.** A 3-node Redpanda cluster is standard; nothing in the code assumes one node.
- **Partition count is fixed** (24) for the demo. Changing it remaps keys and needs a drain or migration.
- **One key is serial.** A single fingerprint, group or recipient never parallelizes.

---

## 3. Tech stack

Versions were verified on docs.rs/crates.io during review. Pin them in `Cargo.lock` along with the toolchain.

| Concern | Choice | Notes |
|---|---|---|
| Language | Rust stable (≥ 1.89 for `serde-saphyr`), Cargo workspace | |
| Async / HTTP | `tokio 1.54`, `axum 0.8.9`, `tower 0.5` | REST + SSE on one port; finite deadlines except on SSE |
| Broker | Redpanda via `rdkafka 0.39` (`cmake-build`) | M0 must verify: native compose build, advertised listeners, transactional produce on the pinned image. JetStream would be a **different backend design** (no partition ownership/seek model), not a drop-in swap. |
| DB | Postgres 16, `sqlx =0.8.6` (`postgres, runtime-tokio, migrate, json`) | Typed runtime queries and checked-in migrations, so the build doesn't need a DB. Dedicated `PgListener` connection. |
| Rules | typed condition tree → `cel-interpreter 0.10` | `Program::compile`/`execute` verified. Semantics in §4.3. |
| Templates | `minijinja 2.x` | Strict undefined, registered filters (`money`), output/render budget, context-aware escaping |
| Config | Postgres-backed versioned snapshot + `notify 8.2` importer for `config/` + `arc-swap 1.8` | §4.6 |
| YAML / Markdown | `serde-saphyr 2.2`, `pulldown-cmark 0.13` | |
| Time zones | `chrono-tz 0.10` | Anna's window, with an injected clock for tests |
| Email | `lettre 0.11` (pooled async SMTP, timeout) → Mailpit | Mailpit's API is used for inbox checks in tests |
| SMS / phone / HTTP actions | `reqwest 0.12` → built-in fake gateway / mock targets | |
| Metrics | `metrics` + `metrics-exporter-prometheus 0.18`, `hdrhistogram 7.6` | Buckets include a 250 ms boundary. Merge raw HDR histograms across replicas. |
| LLM (optional) | Claude API (`claude-sonnet-5-5`) behind a trait; off by default; labelled mock fallback | §6.6 |
| UI | **React + TypeScript** (decided 2026-11-20, §3.1), built as static assets | Native form controls, virtualized list, code editor for YAML/JSON |
| Packaging | `docker compose up` | Pulls pinned prebuilt images, including built UI assets. A build override is for local edits. Health checks, one idempotent bootstrap job (migrations, topics, seed), and a smoke event that reaches the UI. |

### 3.1 UI: decided React/TS (GPUI web considered)

**Decision (erop-laptop, 2026-11-20): React/TS now, with no GPUI spike.** The UI carries much of the UX score, and the gates below were too much risk to take on in the hackathon. The analysis is kept for reference.

State of GPUI web as of the review:
- `gpui-pre-web 0.3.13` is a published snapshot of Zed's `gpui_web`.
- Longbridge's `gpui-component 0.10.0` has a working WASM web gallery pinned to `gpui-pre-web =0.3.12`, so a known-good dependency graph exists.
- Nobody has compiled it locally or run it in a browser yet.

**Spike (timeboxed to 3 h, using the gpui-component 0.10 graph).** GPUI is chosen only if **all** of these pass on the demo laptop:

1. A clean locked release build, with static assets packaged separately from the native Cargo build, served from compose. It runs in Chromium and Firefox; WebGL2 is tested if it is advertised as a fallback.
2. Keyboard-only rule edit → test → save, and ack/resolve. Focus is visible and controls have accessible names.
3. Multiline YAML/JSON paste, text selection, clipboard, IME, and clickable runbook links.
4. 5,000 alerts plus 200 updates/s: p95 frame time under 33 ms while scrolling, and bounded queues.
5. Reconnecting to a different API replica mid-burst converges to the PG state.

**If any gate fails at the deadline, React/TS.** It has native controls, a statically built client and predictable virtual-list and editor integration. Second choice is axum + htmx for the simple forms. Leptos only if someone already ships it.

The UI is a thin client either way. All logic (validation, test/preview, rendering) is behind the API in §8, which is specified with OpenAPI and SSE fixtures so either frontend can be built against it.

---

## 4. Data model

### 4.1 Event envelope (internal)

```json
{
  "id": "evt_01J...",             // stable: source id + topic/partition/offset, or a ULID kept across HTTP retries
  "source": "kafka:workflows",     // transport source instance (≠ producer/service)
  "ingested_at": "2026-10-23T09:14:02.400Z",
  "schema": "workflow.failed",     // detected, or "unknown:<structural signature>"
  "fields": {                      // typed canonical fields from the mapping (may be partial)
    "type": "WORKFLOW_FAILED", "severity": "error", "environment": "prod",
    "service": "order-settlement", "occurred_at": "2026-10-23T09:14:02.331Z"
  },
  "raw": { "...": "original payload, untouched" }
}
```

Rules filter on `fields.*` **or** any `raw.*` path. Paths use a restricted dot syntax for common cases (`raw.error.code`) and JSON Pointer for arbitrary keys and array indices.

### 4.2 Schema mappings and unknown schemas (config, not code)

```yaml
# config/schemas/alertmanager.yaml
schema: prometheus.alert
priority: 50
detect:                                  # typed tests, optionally scoped to a source
  source: http:alertmanager              # optional
  all:
    - { path: alertname, op: exists }
    - { path: status, op: in, value: [firing, resolved] }
fields:                                  # typed extraction, not string templates
  type:        { path: alertname }
  severity:    { path: labels.severity, transform: lower, enum: severity }
  service:     { path: labels.service }
  occurred_at: { path: startsAt, as: rfc3339 }
  resolved:    { path: status, equals: resolved }     # boolean; drives auto-resolve
```

- **Detection is deterministic.** An explicit source mapping wins. Otherwise the highest-priority detector whose discriminator tests pass. A tie becomes `unknown` with a diagnostic, because shape alone cannot tell apart events that share a structure.
- **Failed optional conversions** leave the field absent and record a diagnostic.
- **Unknown schemas (the judges' surprise event).**
  1. The event is accepted, kept, and immediately matchable on `raw.*`. Its `schema` is a versioned structural signature: sorted escaped paths, array positions normalized, values ignored. The signature is only used for grouping in discovery, since optional keys split shapes and equal shapes collide.
  2. The UI shows the sample under *New schemas*.
  3. Pick or paste it, then **Create rule from this event**: paths are pre-filled, you dry-run, then publish.
  4. **Resubmit the sample** (with a fresh id) to see the alert fire.

  None of this needs a mapping or the LLM. "Map this schema", and the LLM mapping proposal (§6.6), are optional extras on top.

### 4.3 Rule

```yaml
# config/rules/rates-rejected-trades.yaml
id: rates-rejected-trades
team: rates
enabled: true
when:
  all:
    - { path: raw.meta.schema, op: eq, value: "trade.rejected.v1" }
    - { path: raw.payload.desk, op: eq, value: rates }
    - { path: raw.payload.notional, op: gt, value: 10000000 }
severity: warning
route:
  - to: team:rates                       # team:, user:, oncall:<schedule>
    channels: [email, ui]                # optional explicit restriction (see §4.4 precedence)
present:
  title: "Rejected {{ raw.payload.currency }} {{ raw.payload.notional | money }} trade {{ raw.payload.tradeId }}"
  body: "Reason: {{ raw.payload.reason }}"
  fields: [raw.payload.desk, raw.payload.reason, raw.meta.producer]
  color: amber                           # semantic token; the UI always shows a severity label too
dedup:
  key: [raw.payload.tradeId]             # typed path tuple; independent of `present`
  window: 10m                            # sliding inactivity window
group:                                   # optional digest
  by: []
  window: 0
throttle: { max: 20, per: 1m }           # per (rule, recipient), enforced on the recipient owner
runbook: trade-rejected
auto_resolve:
  recover_when: { path: fields.resolved, op: eq, value: true }   # typed recovery signal, same incident key
  quiet_for: 30m
escalate_after: 10m
```

**Condition semantics (one compiler for publish and preview).**
- **Operators:** `eq, neq, gt, gte, lt, lte, in, not_in, contains, starts_with, regex, exists, not_exists`, nested under `all` / `any` / `not`.
- The tree is lowered to CEL, with paths and literals bound as typed variables (never string-spliced). Results must be Boolean.
- **Missing values:** missing ≠ JSON `null`. Comparisons on a missing or incompatible value are **false**, including `neq` and `not_in`. `not` negates that result. `exists` is true for a present `null`.
- **Regex** is precompiled; a malformed one is rejected at publish.
- **Errors:** a runtime error is reported against that rule and other rules keep evaluating.
- **Limits:** payload size and expression complexity are bounded.
- **Hand-written CEL** (`when: { cel: "..." }`) is **deferred**. If added, it gets an AST allowlist and the same semantics, and opens in an advanced text editor.

**Identity.**
- Dedup and group keys are typed path tuples, never derived from display fields, so editing a template can't split an incident.
- A missing key field produces a diagnostic rather than silently merging alerts.
- Changing a rule's identity or window gets a new rule version; existing groups drain under the old config.

**Rule pre-indexing is conservative.**
- Only necessary equalities in top-level conjunctions (`schema`, `fields.type`, `source`) are indexed. An OR is indexed only if every arm has a safe selector.
- Everything else goes in a fallback bucket. Candidates are unioned and the full predicate always runs.
- CI checks indexed results against an exhaustive scan.

**Appendix A seed rules.** All four ship as seed config, each with an acceptance test:

| Example | Config, and what the test asserts |
|---|---|
| Settlements | `WORKFLOW_FAILED` + `raw.workflowName = order-settlement` + `fields.environment = prod` → on-call phone + SMS. Body has the step and error; red presentation. |
| Pricing | Log mapping gives `service = pricing-engine` and a numeric severity rank, so `≥ WARN` matches. Instrument extracted. `group.by: [instrument]`, keyed by rule + instrument, one UI digest per **UTC 5-minute bucket** on processing time, with a grace period and a late-match metric. Missing instrument is tested separately. |
| Rates | Exact `trade.rejected.v1`, `desk = rates`, numeric `notional > 10,000,000`. Equality at the threshold must not match. Email + UI with the rendered title. |
| Anna | `critical` + `payments-api` → `user:anna`, no explicit channels. Her preference chooses SMS in `[08:00, 20:00)` Europe/London at the first routing decision, otherwise email. The choice is frozen across retries. Tests cover both boundaries and both sides of a DST change. |

### 4.4 Recipients, preferences, escalation

```yaml
# config/people/anna.yaml
user: anna
contact: { email: anna@example.test, phone: "+44 7700 900123" }
preferences:                                # first match wins
  - match: { severity: critical, service: payments-api }
    channels: [sms]
    when: { between: "08:00-20:00", tz: Europe/London }    # half-open [08:00, 20:00)
    otherwise: [email]
  - match: { severity: warning }
    channels: [email]
  - default: [ui]
```

**Routing precedence.**
1. Expand team and on-call targets into users, and deduplicate users reached by several routes.
2. For each user, take the first matching personal preference, then the team default, then the rule's fallback channels.
3. A rule's explicit `channels:` restricts the choice.
4. The decision is made **once at dispatch planning** and frozen on the intent. Escalation steps re-evaluate.

**Escalation** is a durable state machine: `(alert_id, step, due_at, intent_id)` rows.
- The clock starts at the alert's first notification.
- The scheduler claims due rows, locks the alert, and acts only if the alert is still `open` and still on the expected step. It then enqueues the next on-call person, then the team lead, and advances the step in one transaction.
- Critical escalations bypass throttling.
- Suppressed or failed sends still count as "notified" for the clock.
- MVP has one bounded chain.

### 4.5 Tables (Postgres)

| Table | Purpose |
|---|---|
| `config_revisions`, `rules`, `schemas`, `runbooks`, `people`, `teams` | Versioned JSONB under relational ids and revisions. Compare-and-set on edit. |
| `alerts` | `id, rule_id, rule_revision, fingerprint, generation, dedup_active, status[open\|acked\|resolved], severity, title, body, fields, color, count, first_seen, last_seen, version, runbook_revision, ack_by, resolved_by, resolved_reason` |
| `alert_events` | Sampled event refs (first N + latest), with a broker locator while retained |
| `checkpoints` | `(pipeline, topic, partition) → epoch, next_offset, state blob` |
| `outbox` | Intents to publish (`notifications`, `alerts.updates`), with lease + token |
| `delivery_jobs` | Unique `(alert_id, recipient_id, channel, notify_seq)`. State, attempts, `due_at`, lease, receipt, fallback-of. |
| `approvals` | Pending requests: frozen params, eligible approvers, challenge hashes, expiry, state |
| `action_executions` | Stable execution id, frozen intent, lease, outcome (`ok \| failed \| uncertain`) |
| `timers` | Escalation steps |
| `audit_log` | Transitions only (actor, channel, alert/step/version, execution id, redacted outcome). No raw bodies, no secrets. |

Feeds are indexed by team + recency, and audit by alert + time. Terminal outbox and job rows have bounded retention.

### 4.6 Configuration authority and hot reload

- **Postgres is the authority.** API edits and YAML imports go through **one validator** and a compare-and-set on the revision.
- A single debounced importer watches `config/` (rescanning on rename) and reports conflicts. Config, the new ruleset revision and a `NOTIFY` are committed in one transaction.
- **Replicas:**
  1. Subscribe first, then load a consistent snapshot.
  2. Compile the whole snapshot and swap it atomically (`ArcSwap`).
  3. Reconcile on reconnect and periodically; `NOTIFY` is only a hint.
  4. On an invalid revision, keep the last good one.
  5. Expose each replica's revision lag.
- **Matches carry their config revision.** Digests render with the revision that opened them, and an existing alert keeps its opening presentation while count and status update live.

---

## 5. Pipeline behaviour

1. **Ingest.** `Source` adapters receive a bounded async publish handle and get back the broker's delivery result.
   - **Kafka sources** commit the upstream offset only after a durable publish.
   - **HTTP** returns `202 {id}` only after the broker ack.
   - Permanent decode errors go to an ingest quarantine.
   - A new source *instance* is a YAML file: id, bootstrap/auth, topics, group, first-start offset policy. A new *transport* is a registered adapter. Source config is read at startup in the MVP.
2. **Normalize + match.**
   - Load the config snapshot, detect the schema, map fields, select candidate rules, evaluate them.
   - Emit `Match{match_id, rule_id, revision, key, event_ref + context projection}`. Presentation is rendered later.
   - Publish in a Kafka transaction with the input offsets.
3. **Aggregate** (partition owner, fenced commit).
   - *Dedup:* same key within the sliding window → `count += 1`, `last_seen`. The UI gets a counter update; recipients are not re-paged, except on a severity escalation or a configured re-notify interval.
   - *Group:* the bucket persists its deadline, flushes when due even if idle, and emits one digest.
   - *Quiet timer:* evaluated by the owner after it has processed earlier buffered matches. It locks the alert and checks generation and `last_seen` before resolving.
   - *Recovery signals* route to the same incident key.
4. **Route + throttle** (recipient owner, fenced commit). Resolve and freeze routing (§4.4). Debit the `(rule, recipient)` and recipient-wide buckets. Write delivery jobs. Excess is folded into a "N more suppressed" digest that is emitted when quota returns; nothing is silently dropped.
5. **Render per channel** from an immutable presentation/runbook snapshot taken when the alert opens:
   - **UI and email:** the full body plus the filled-in steps.
   - **SMS:** a bounded title, an absolute short link (configured public URL), and an approval challenge if one is needed.
   - **Phone:** a short TTS script.
6. **Deliver** (§2.2).
   - **Retries:** capped, jittered backoff. Errors are classed as transient, permanent or ambiguous, and provider retry delays are honoured.
   - **Breaker:** per provider endpoint.
   - **Terminal failure:** the DLQ state and **one** deterministic fallback job are recorded atomically, with a one-hop limit and an eligibility check at send time. Terminal jobs stay replayable under their original id.
   - **UI delivery** is the committed alert state plus a versioned `alerts.updates` record. It is never cancelled by paging eligibility.

---

## 6. Runbooks

### 6.1 Formats → one internal model

| Format | Handling |
|---|---|
| Structured YAML (Appendix B) | Adapter maps the flat action params and the `matches` equality map into typed params and the shared predicate form. |
| Markdown | Rendered safely (no raw HTML or unsafe URLs). List items become manual checklist steps; phrases like "needs approval" are shown as **hints**, not enforced. |
| Link (wiki/Confluence) | Stored with an optional user label. No server-side fetch. |
| Free text | Kept verbatim. Optionally, the LLM proposes manual steps or references to registered actions (§6.6). |
| **Anything else** (the judges' surprise runbook) | Manual import: choose the format explicitly (plain text is always possible) and preview. The original is kept, and an editable checklist is created. |

**Model.** `Runbook{id, revision, matches?, steps: [Step{id, kind: manual|condition|action, text, action?, params, approval}]}`.
- Revisions are immutable and step ids are stable.
- Only designated template fields are rendered, from the pinned event context, with strict missing-field handling and contextual escaping.
- Invalid actions are disabled with a visible reason.

**Selection.** An explicit rule binding comes first. Otherwise, team-scoped candidates by explicit priority, then stable id. Natural-language conditions are guidance only and never drive execution.
- **Appendix B example:** "retry fails twice" counts recorded terminal retry failures on the current alert instance, and the escalate step becomes available after two of them.

### 6.2 Actions

- **The `Action` trait:** name, typed params schema, risk, and `execute(ctx) -> Outcome`. Built-ins:
  - `escalate`, which re-routes the alert to another team
  - `notify`
  - `retry_workflow`, `restart_service` and `open_ticket`, which call mock targets. The mock workflow engine can be switched to fail or succeed, so "retry fails twice → escalate" is demonstrable.
- **Config-only HTTP actions.** A named entry gives the fixed target alias, method, typed params, request template, credential reference, timeout and risk. A new HTTP integration is YAML, and only custom protocols need Rust.
  - Path and query values are encoded, and JSON fields are serialized.
  - Redirects are disabled.
  - Event values never reach the origin, the credentials or the auth headers.
  - Entries are validated at publish and again before execution.
- **Risk sets a minimum approval level.** The registry records the action definition version used.
- **Execution happens off the alert pipeline.**
  1. Run or approve persists an `action_execution` with a stable id and frozen params.
  2. The API returns `202`.
  3. Action workers lease the row, execute asynchronously and persist the result with lease fencing.
  - **Retries:** the same execution id is sent to idempotent targets on every attempt.
  - **Uncertain outcomes:** a crash or timeout against a non-idempotent target is marked `uncertain` and goes to manual reconciliation.
  - Delayed work (`wait`) is persisted with a due time; the general `wait` action is deferred.

### 6.3 Approvals (one state machine, every channel)

A pending approval freezes the alert instance, runbook revision, step, action version and rendered params, and lists the eligible approvers. Approving it is **one atomic transaction**: consume the request, append to the audit log, insert the execution intent. The same request can be approved from three places:

- **UI:** approve or reject, showing the rendered request with secrets redacted.
- **SMS reply:** `Reply "Y 4821"`. The challenge expires, is bound to the sender and attempt-limited, and arrives through an authenticated fake-gateway callback (`/api/replies`).
- **Signed email link:** an HMAC over request, decision, approver, nonce and expiry. **GET shows a confirmation page and only POST consumes it**, so link scanners can't approve.

Before claiming execution, the current alert state is revalidated. Rejection, expiry and cancellation are audited as transitions.

### 6.4 Resolution

An alert generation closes when one of these happens:
- a user resolves it
- a typed recovery signal arrives (`auto_resolve.recover_when`), routed to the same incident key and partition
- `quiet_for` elapses (owner-evaluated, §5.3)
- a runbook step marked `resolves: true` succeeds

Every close is a conditional transition on that generation's row, so a stale timer or an old action result can't close a newer occurrence. The close's audit entry and UI update are written in the same transaction.

### 6.5 Proposals (innovation)

- **Alert improvement:** *Suggest change* on an alert (too noisy → throttle, wrong team → reroute, missing field → template) creates a typed proposal against the rule's base revision. The rule owner accepts it, which is an ordinary config publish. The system also auto-suggests, e.g. "this rule fired 400× this week, 95% acked with no action → suggest grouping".
- **Post-incident runbook update:** on resolve, the audit trail becomes a **redacted diff** against the runbook's base revision. It needs re-review if that revision changed in the meantime.

### 6.6 LLM trust boundary

- **No tools, no credentials.** The model gets nothing it could act with. Input is bounded and synthetic, or explicitly allowlisted and redacted (free text and audit notes can carry PII even after field masking).
- **Untrusted text both ways.** Inputs and outputs are inert data, which is the prompt-injection story for the video.
- **Output is a typed proposal.** It is validated by the server and accepted only after an authorized person reviews a preview:
  - Mapping proposals may only pick existing paths and approved transforms, and must pass a preview against the sample.
  - Generated steps may only reference registered actions, and inherit their risk and approval policy.
- **Kept off the hot path.** Proposal jobs are deduplicated per unknown shape or incident revision, and rate-limited.
- **Off in the demo by default.** The demo shows the unseen-schema and unseen-runbook flows with the LLM disabled. Any live LLM call uses synthetic data, and mock suggestions are labelled as such.

---

## 7. UI (screens)

1. **Live feed:** alerts for the selected team or user, pushed over SSE.
   - **Display:** severity label plus the rule's colour token, and a live dedup counter.
   - **Controls:** filters for status, severity and rule. Keyboard `a` ack, `r` resolve.
   - **Paging and updates:** a bounded recent page in stable keyset order `(first_seen, id)`, with independent history paging. Virtualized rows, client updates batched per frame, scroll position preserved, and a "new alerts" indicator.
2. **Alert detail:** the alert card exactly as the feed shows it, highlighted fields, an occurrence sparkline, the runbook with filled-in steps and action buttons (approval-gated ones marked), delivery status per recipient and channel, and the audit timeline. Details refetch on invalidation and after reconnect.
3. **Rules:** a list with fire counts.
   - **One authoritative draft:** a versioned rule schema shared by the visual builder (nested groups, typed values), the YAML tab and the API. YAML is parsed before it replaces the visual draft; invalid text stays editable with line/column errors.
   - **Live test panel:** run against pasted JSON or an authorized recent sample. It shows per-node match / no-match / skipped / error and template warnings, and makes **zero writes and zero dispatches**. Requests are debounced and stale responses discarded.
   - **Starting points:** seeded with the three Appendix A samples and guided defaults.
4. **Sources & schemas:** configured sources and their throughput, *New schemas* with samples, "Create rule from this event", and the mapping editor.
5. **Preferences:** per user and team, severity → channels, the time-window editor, and the on-call rotation.
6. **Device simulator:** a fake phone showing incoming SMS and calls (reply to approve), plus a link to the Mailpit inbox.
7. **Proposals:** pending rule and runbook proposals with diffs.

"New team in under 5 minutes": pick a template ("workflow failures", "Prometheus alerts", "business events") → choose a channel → done. This flow appears in the video.

---

## 8. API

The API is specified in **OpenAPI**, plus SSE event fixtures. Those cover the feed projection, detail, mutations, status transitions and error bodies, along with timestamp format, enums, filter/cursor semantics and expected-version conflicts (`409`) on edits and transitions. Auth is a same-origin cookie (EventSource-compatible), with CSRF protection on writes. Authorization is applied consistently to list, stream, detail and mutations.

```
POST   /ingest/{source}                    raw JSON (any shape) → 202 {id} | 429 | 503
GET    /api/alerts?team=&status=&cursor=   feed page (keyset)
GET    /api/alerts/{id}                    detail + runbook + delivery + timeline
POST   /api/alerts/{id}/ack | /resolve     expected version → 409 on conflict
POST   /api/alerts/{id}/steps/{step}/run   → 202 {execution_id} | 409 approval required
POST   /api/approvals/{id}/approve | /reject
GET    /a/approve/{token}                  confirmation page; POST consumes
GET/POST/PUT/DELETE /api/rules[/{id}]      CRUD, compare-and-set revision
POST   /api/rules/test                     {draft rule, event} → node diagnostics + rendered card (no side effects)
GET    /api/schemas/discovered             unknown-schema samples
GET/POST /api/schemas
GET/PUT /api/people/{id}/preferences  ·  GET/PUT /api/teams/{id}
GET/POST /api/runbooks  ·  POST /api/runbooks/import   explicit format + preview
GET    /api/rules/{id}/stats               fire counts, occurrence series
POST   /api/replies                        authenticated fake-gateway callback
GET    /api/proposals  ·  POST /api/proposals  ·  POST /api/proposals/{id}/accept
GET    /api/simulator/stream               mock-device events (SSE)
GET    /api/stream?team=                   SSE: snapshot | alert.upsert | alert.remove | delivery.updated | approval.requested
GET    /metrics · /ready                   Prometheus · per-role readiness
```

**SSE protocol (MVP: a snapshot on every reconnect, no `Last-Event-ID`).**
1. The API replica waits for its `alerts.updates` consumer-group assignment.
2. On connect, it subscribes and buffers first, then reads the authorized feed page from PG.
3. It sends a `snapshot` event that replaces the client's page.
4. It drains the buffer as absolute upserts and removals, dropping any per-alert `version` that is older than or equal to what the client has.
5. Rows not in the snapshot are validated against the authorized PG projection before insert, so a delayed update can't resurrect a stale row. Removal versions are cached for the connection, bounded.
6. On buffer overflow, a consumer gap or a rebalance, the stream restarts with a fresh snapshot. A client can reconnect to **any** replica.
7. Heartbeats run and proxy buffering is off.

True `Last-Event-ID` replay needs a durable, commit-ordered feed log, which is a later feature.

---

## 9. Load testing

- **`beacon-loadgen`** (Rust, **open-loop**): replays the Appendix A samples with randomized fields over HTTP and directly to Kafka. Duplicate ratio, distinct keys, match ratio, recipient fanout and skew are all configurable.
- **Every result includes its workload:** hardware, Docker limits, release build, payload size, rule count, match ratio, unique-alert ratio, jobs per alert, connected browsers, accepted vs processed rate, backlog slope and drain time, and errors.
- **Separate histograms, each with p50/p99:**
  - **first alert:** ingest receipt → browser SSE receipt, correlated by id
  - **counter freshness:** a duplicate → the updated count in the browser (bounded by the flush interval)
  - **group lateness:** a bucket deadline → digest emitted
  - **gateway acceptance:** ingest → fake gateway receipt
  - **email:** ingest → Mailpit
- **Scenarios:**

  | | Scenario | Must show |
  |---|---|---|
  | a | 1-process baseline, steady run | |
  | b | 3 matchers + 3 aggregators + 2 routers over 24 partitions; also high-cardinality and hot-key variants with per-partition lag | |
  | c | SMS gateway down for 60 s | every expected SMS job reconciled against persisted gateway receipts: 0 lost, duplicates counted |
  | d | 500 dupes/s on one fingerprint | 1 alert, exact count |
  | e | kill an aggregator mid-run | exact counts, group deadlines kept, outbox invariants hold |
  | f | force SSE reconnect to another API replica during writes | the client converges to PG |
  | g | slow action targets | the alert pipeline's p99 is unaffected |

- **Output:** `loadtest/results/*.md` plus a Grafana screenshot. A minimal load test runs at **M1** so bottlenecks show up early.

**Target (to beat, not a promise):** ≥ 20k raw events/s with a selective / duplicate-heavy workload, and first-alert p99 < 250 ms. We publish what we actually measure, including the high-fanout run where Postgres will be the limit.

---

## 10. Repo layout

```
crates/
  beacon-core/      envelope, paths, condition tree → CEL, templates, schema mapping, config snapshot
  beacon-ingest/    Source trait + http, kafka adapters
  beacon-engine/    matcher, aggregator, router/throttle, checkpoints
  beacon-delivery/  Channel trait + registry, job leasing, retry/breaker, email/sms/phone, fake gateway
  beacon-runbook/   format adapters, Action trait + HTTP-config actions, approvals, action workers
  beacon-api/       axum REST + SSE, OpenAPI
  beacon/           the binary (role flags; wires adapters)
  beacon-loadgen/
ui/                 React/TS frontend, built separately as static assets
config/             sources/ schemas/ rules/ people/ teams/ runbooks/ actions/  (seed = Appendix A/B)
deploy/             docker-compose.yml (pinned images), compose.build.yml, compose.scale.yml, grafana/
loadtest/
docs/
```

Dependencies point one way: transport-neutral types live in `core`, adapters are wired in the binary, and services are injected into the API.

---

## 11. Milestones

| # | Deliverable | Done when |
|---|---|---|
| M0 | **Walking skeleton (simple on purpose):** webhook + Kafka ingest → typed condition tree → simple dedup (count bump on the open alert) → PG alerts → SSE feed in the React UI + Mailpit email, with the load-test harness running. **In parallel:** the rdkafka compose build. | an Appendix A event reaches the browser and Mailpit via `docker compose up`; the first measured numbers are recorded |
| M1 | Make the skeleton correct: **fenced aggregator** checkpoints, the SSE snapshot protocol, the matcher exactly-once path (transactions or match-id dedup) | a worker killed before/after commit leaves counts exact |
| M2 | Versioned config + importer + hot reload, rule editor + test panel, unknown-schema flow | all 4 seed rules pass; judge-style unknown JSON → rule → alert with no deploy |
| M3 | Dedup generations, grouping buckets, quiet timers | 500 dupes → 1 alert / count 500; Pricing digest per 5-min bucket |
| M4 | Router + throttles, preferences + Anna, delivery jobs + retries/breaker/fallback, fake gateway, Mailpit, escalation (one chain) | 60 s outage: 0 lost, reconciled against receipts |
| M5 | Runbooks: YAML + Markdown/free text/link display + manual import, one config HTTP action, approval state machine (UI + SMS) | settlement-timeout flow end to end with audit |
| M6 | Full load test suite + metrics | numbers with workload parameters in README |
| M7 | Stretch: signed email approvals, proposals, LLM mapping/runbook proposals, post-incident diffs | |
| M8 | README, "what is mocked", security/AI notes, video | |

---

## 12. Open questions

- ~~UI stack~~ **Decided: React/TS** (§3.1).
- ~~Dedup expiry~~ **Decided:** expiry retires the dedup generation (`dedup_active = false`) and leaves the incident open until it is resolved.
- **Matcher exactly-once.** Kafka transactions (preferred) or match-id dedup, decided at M0.
- **Team split** (proposed by alexey/claude): (a) ingest + matcher + dedup/aggregator, (b) delivery: router, delivery_jobs, channel mocks, escalation/approvals, runbooks, (c) UI + playground + rule builder. **erop-laptop takes (c).** (a) and (b) are still to be assigned between alexey and ilyaarch.
- **LLM in the demo:** off by default, synthetic data only, stated in the video.

---

## 13. Cut line: what we build vs. what we claim

The review pushed toward production-grade guarantees. For the hackathon, judges score a working demo and measured numbers above design depth, so the order is: **get M0 working end to end first.** Kafka transactions, dedup generations, action leases and the router come only after that.

**M0 (walking skeleton) has only these:**
- webhook + Kafka ingest
- a typed condition tree
- simple dedup
- Postgres alerts
- SSE feed
- Mailpit email
- the load-test harness running

M0 has none of these: fencing, transactions, generations, throttles, runbooks.

**Build and demo (must work):**
- fenced aggregator checkpoints
- the SSE snapshot protocol
- the delivery-job queue with retries and fallback
- the unknown-schema → rule flow
- all 4 seed rules
- the Appendix B YAML runbook with one HTTP action and UI + SMS approval
- load scenarios a–e

**Build if time allows (M7):**
- the full recipient-wide throttle (otherwise per-(rule, recipient) only, using the same PG bucket rows)
- signed email approvals
- proposals and LLM features
- scenarios f–g

**Claim only what we tested:**
- Exactly-once counts per consumed offset.
- At-least-once external sends, with SMTP duplicates possible.
- Single Postgres and a single broker as failure domains.
- Throughput numbers tied to their workload.
