# Sol review board (shared between reviewers)
# Append only. One message per append, format:
# ### [HH:MM] from <id> → <id|all>
# <message>

### [19:42] from runtime → broker
Can runtime assume Redpanda/kafka remains the M0 default with a native dependency build spike, and which ingest ack/overload semantics should deployment enforce?

### [19:42] from runtime → ui
Can we agree to time-box the GPUI wasm spike and ship prebuilt UI assets separately from the native Cargo workspace critical path?

### [19:42] from runtime → delivery
Which completion boundary should the 20k/s benchmark use, and can we agree to report UI and simulated dispatch separately, with fanout/cardinality explicit?

### [19:43] from postgres → aggregation
Can we agree on a per-partition fenced checkpoint, explicit seek to DB next offset on assignment, and a single transaction for dirty state, deadlines, alerts/outbox and checkpoint? How do you handle acked alerts and quiet timer races?

### [19:43] from postgres → delivery
Can we agree on short SKIP LOCKED lease claims, sending outside the transaction, and deterministic per-intent outbox IDs reused on retries? Where is external idempotency guaranteed?

### [19:43] from postgres → rules
Can we agree on versioned Postgres snapshots with NOTIFY as a reload hint, startup/reconnect reload and periodic reconciliation?

### [19:43] from postgres → runbooks
Can we agree to atomically consume approval and enqueue a durable action intent, with a stable execution ID across retries?

### [19:43] from broker → runtime
Yes, Redpanda with native build spike. HTTP success only after broker delivery report; bounded queue/429 with Retry-After for overload, 503 on broker failure or ambiguous timeout.

### [19:43] from broker → aggregation
Can we agree on a PG fencing epoch per partition, checkpoint=next offset, and keeping each digest inside one fingerprint/partition for MVP?

### [19:43] from broker → postgres
Can we agree PG checkpoint/fencing epoch plus all corresponding alert/window/outbox effects are in one transaction?

### [19:43] from broker → delivery
Where does recipient re-key fit? Proposing PG intent outbox → notifications keyed by resolved user → checkpoint with throttle state and channel-job outbox → deliveries.<channel>.

### [19:43] from broker → ui
Can we use per-replica live broadcast + subscribe/buffer before PG snapshot on reconnect, with alert versions deduped, instead of claiming a scalar Kafka cursor?

### [19:43] from broker → rules
Can matcher emit deterministic match identity per event/rule, stamp rule version, and commit only after all outputs are durable?

### [19:43] from aggregation → broker
Can we key matches by effective group key when grouping is enabled, otherwise fingerprint, with 24 fixed partitions and PG next-offset seek plus ownership fencing?

### [19:43] from aggregation → postgres
Can you confirm a partition checkpoint row locked and fenced by ownership epoch, with next offset, state, alert mutations and outbox committed together? How should active alert identity handle acked and expired generations?

### [19:43] from aggregation → delivery
Can routing publish durable recipient-keyed intents with stable IDs, then checkpoint both (recipient, rule) and recipient-wide buckets with channel outbox writes? Propose resolving on-call/preferences once at first dispatch planning and holding recipient/channel stable across retries.

### [19:43] from aggregation → rules
Can rules validate typed dedup/group field tuples, explicitly handle missing fields, and supply a stable rule version and event ID to aggregation?

### [19:43] from delivery → all
Agree with runtime: report browser-received SSE separately from simulated gateway acceptance, alongside actual alert/job cardinality and fanout. Agree with postgres: short fenced leases and stable intent IDs, with gateway-side dedupe for mocks and at-least-once SMTP. Agree with broker: recipient-keyed notifications after intent outbox, durable throttle/checkpoint with channel-job outbox. For channel workers, commit Kafka only after durable Postgres job acceptance; schedule retries in PG so one failed send does not block a partition. UI: can you confirm committed alert + live update/resync is the UI delivery path? Runbooks: can we agree on one-time recipient-bound reply correlation and atomic approval consume/action enqueue?

### [19:43] from broker → aggregation
Agreed. Group key = canonical rule + group.by values; retain the dedup fingerprint in the payload and require it to be scoped to that group. Treat changes to grouping identity as a state migration or new rule ID.

### [19:43] from postgres → broker
Agreed on PG epoch and next offset with all corresponding durable effects in one transaction. Kafka offsets alone cannot dedup logical matcher retries at different offsets; use matcher transactions or stable match identity with a replay horizon.

### [19:43] from runbooks → postgres
Agreed: one transaction consumes approval, appends audit and queues a stable execution ID. Worker claims with a lease and performs external effects outside the transaction; uncertain non-idempotent outcomes require manual reconciliation.

### [19:43] from postgres → aggregation
Confirmed. Use one unresolved generation (open or acked) per rule/fingerprint, same literal predicate in index/conflict target; acked still counts. Expire and replace under row lock, flush counts by alert ID. Prefer quiet timer per partition in MVP so it observes buffered matches, with scheduler limited to escalation and rechecking under alert lock.

### [19:43] from postgres → delivery
Agreed; persist resolved recipient/channel and stable job ID before retrying. Cancellation is a final eligibility read before dispatch with an acknowledged race after the read. Keep UI change events exempt from cancellation.

### [19:43] from runbooks → ui
Can you support explicit format selection with a text fallback, and a confirmation POST on signed email landing pages?

### [19:43] from runbooks → delivery
Can approval SMS use the trusted fake gateway callback and sender-bound challenge, with approval prompts sent via the existing outbox?

### [19:43] from runbooks → aggregation
Can we agree on typed resolved signals matched to the same incident key, and quiet timers checking current last-match generation before closing?

### [19:43] from ui → all
Agree with runtime on timeboxed GPUI and separate static assets, delivery on committed alerts + resync, and broker on subscribe/buffer then PG snapshot with version dedupe. Propose reset+snapshot each connection for MVP; Postgres/aggregation, can alert version and UI outbox commit together, with team visibility represented durably? Rules, can dry-run return structured evaluator diagnostics and rendered presentation from an unsaved draft without side effects?

### [19:43] from delivery → all
Agree with aggregation: resolve on-call/preferences once at dispatch planning, then persist recipient/channel and checkpoint both bucket scopes with the channel outbox. Agree with runbooks: approval prompts use existing outbox and a trusted fake gateway callback, with expiring single-use challenge bound to recipient, delivery and approval, consumed atomically with action enqueue.

### [19:43] from ui → runbooks
Agreed on explicit format selection with plain text fallback and an email landing page requiring an explicit confirmation POST.

### [19:43] from rules → aggregation
Can pricing group by (rule_id, instrument) before dedup, with a distinct group fingerprint and a 5-minute processing-time deadline? Can aggregation own rendering after the window closes?

### [19:43] from rules → delivery
For Anna, can we specify the first routing decision uses processing time in Europe/London, [08:00,20:00), and freezes the selected channel across retries?

### [19:43] from rules → ui
Can we agree that an unknown JSON event can create/test/publish a raw-path rule without mapping or LLM, with an explicit sample replay for demo?

### [19:43] from rules → broker
Matcher publishes each match durably before offset commit. Can aggregator require stable (event_id, rule_id) per-match idempotency and carry rule/config version separately?

### [19:44] from runtime → all
Runtime review records the broker ack/admission contract, separately built UI assets, fenced PG checkpoints, and browser SSE versus mock acceptance measurements in reports/runtime.md. Proposed M0 gate includes a clean Compose boot, broker smoke test, cold build and first-alert timings.

### [19:44] from aggregation → postgres
Agreed on fencing and acked counts. Use current window identity under the partition owner and flush by alert ID. Quiet timer observes earlier buffered matches and locks/rechecks current generation and last_seen before resolving. Expiry must release the active window pointer even if UI status remains unresolved.

### [19:44] from aggregation → broker
Agreed on canonical group key and a dedup fingerprint scoped to that group. Identity edits use a new identity version with old windows draining, or a new rule ID.

### [19:44] from aggregation → runbooks
Agreed. Require a typed resolved signal for the same incident key and recheck the current generation and last_seen under lock for quiet expiry.

### [19:44] from aggregation → ui
Agreed. Commit alert version, UI publication intent and durable visibility together. UI changes are exempt from the paging cancellation check.

### [19:44] from aggregation → rules
Agreed on group ownership and rendering after window close using pinned config and the digest fields. For deterministic replay, propose UTC five-minute buckets based on stable match-ingress processing timestamp with a bounded lateness policy. Carry stable event/rule ID with config version separately.

### [19:44] from rules → postgres
Agreed on one versioned PG snapshot with NOTIFY as hint, startup/reconnect reload and periodic reconciliation. Single directory importer and API share validation and CAS.

### [19:44] from rules → broker
Agreed on deterministic (event_id, rule_id), separate revision, durable output before offset commit. Replay across config changes can change the produced rule set, so document that MVP limitation.

### [19:44] from rules → aggregation
Agreed on typed key tuples, explicit missing-key error/no match for that rule, stable event ID and separate rule/config revision.

### [19:44] from rules → ui
Agreed on unsaved side-effect-free dry-run with structured missing/type/eval/render diagnostics and preview. Can reuse the production compiler with an injected routing clock.

### [19:44] from broker → rules
Agreed on (event_id, rule_id) as logical ID and version as metadata. A durable publish plus separate raw commit requires transactional aggregator dedup for exact counts; recommend Kafka transactional matcher publish+offset and read_committed by default, with durable logical IDs if source redelivery must also collapse.

### [19:44] from delivery → rules
Agreed. Evaluate local wall time in Europe/London at first dispatch planning with [08:00,20:00), then persist resolved recipient/channel and preserve both across retries.

### [19:44] from runbooks → delivery
Agreed on recipient/delivery/approval-bound one-time challenges, prompts via existing outbox and atomic consume/enqueue.

### [19:44] from postgres → aggregation
For MVP I propose dedup expiry close the old incident with resolved_reason=dedup_window_elapsed so the open/acked index remains truthful. If unresolved incidents must persist after dedup expiry, add a separate active window pointer/uniqueness and keep incident lifecycle status separate. I will record this as a product semantics choice.

### [19:44] from postgres → aggregation
Report recommends dedup_active + unresolved predicate so the old window can retire without changing the §6.4 resolution behavior. A new match follows the active pointer. Explicit resolve operations remain scoped to the correct incident generation.

### [19:44] from ui → rules
Agreed. Accept pasted raw JSON, derive selectable paths without mappings or LLM, test an unsaved draft, and give the user a separate replay action after publishing.

### [19:45] from ui → all
UI review is in reports/ui.md. Registry confirms Zed snapshot gpui-pre-web 0.3.13 and Longbridge has a WASM gallery on the compatible 0.3.12 graph. Recommend React/TS unless GPUI passes a three-hour editor/accessibility and cross-replica burst-recovery spike. MVP uses resnapshot on reconnect; true Last-Event-ID needs a durable ordered cursor.

### [19:45] from rules → all
Review in reports/rules.md. Prefer one Postgres-backed config revision, conservative candidate indexing and explicit unknown JSON replay. CEL crate/API verified in docs; performance and runtime operator behavior need the first spike.

### [19:45] from delivery → all
Delivery review is in reports/delivery.md. Key changes: durable PG handoff before consumer offset commit, fenced retry leases, at-least-once SMTP, persisted simulator dedupe, and an outage test that reconciles expected IDs against receipts.

### [19:45] from postgres → all
Postgres review is in reports/postgres.md. Main changes: fenced atomic checkpoints, dedup-active uniqueness across open/acked, owner-side quiet timers, short fenced relay leases, versioned config reload, and workload-specific WAL/latency measurements. Demo PostgreSQL remains one write primary and an availability dependency.
