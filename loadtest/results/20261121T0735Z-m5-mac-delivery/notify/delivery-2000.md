# Delivery latency: `m6-delivery-2000-m5` (run `01a1fd0cffbc`)

## Workload

beacon-loadgen --rate 2000 --duration 30 --batch 100 --distinct-keys 1000 --match-ratio 0.8 --concurrency 256 --drain 15 --scenario m6-delivery-2000-m5 --out results/notify/loadgen-2000

| fact | value |
|---|---|
| git commit | `35650c5` |
| host CPUs | 10 |
| generated at (UTC) | 2026-11-21T07:41:23.050Z |
| drain | 0.0 s — every notification routed, nothing unsent |
| selection | run id `01a1fd0cffbc` in alerts.title/body/fields |

Alerts matched: 4000 (1000 without sms/phone/email jobs)

| rule | alerts |
|---|---:|
| anna-payments-critical | 1000 |
| pricing-engine-warnings | 1000 |
| rates-rejected-trades | 1000 |
| settlements-workflow-failed | 1000 |

## Reconciliation

Jobs: the alerts' first notification only (lowest `notify_seq` per alert, `escalation_step = 0`), channels sms/phone/email.

| channel | expected | sent | unsent | dead | missing | duplicates | attempts>1 |
|---|---:|---:|---:|---:|---:|---:|---:|
| sms | 1000 | 1000 | 0 | 0 | 0 | 0 | 0 |
| phone | 1000 | 1000 | 0 | 0 | 0 | 0 | 0 |
| email | 3000 | 3000 | 0 | 0 | 0 | n/a | 0 |

missing = sent but no gateway receipt (sms, phone) or no Mailpit message (email); unsent = `sent_at IS NULL` and status not dead (pending, in_flight, cancelled); duplicates = SUM(`gateway_receipts.duplicates`).

## Latency (ms)

| metric | channel | count | p50 | p90 | p99 | max |
|---|---|---:|---:|---:|---:|---:|
| gateway acceptance: ingest (first event per alert) → receipt (`received_at − first_seen`) | sms | 1000 | 3153 | 5174 | 6341 | 6540 |
| gateway acceptance: ingest (first event per alert) → receipt (`received_at − first_seen`) | phone | 1000 | 3159 | 5171 | 6341 | 6540 |
| email in Mailpit: ingest (first event per alert) → message (`Created − first_seen`) | email | 3000 | 3419 | 5617 | 6410 | 6520 |
| internal: ingest (first event per alert) → job planned (`created_at − first_seen`) | sms | 1000 | 2318 | 4269 | 5864 | 6013 |
| internal: ingest (first event per alert) → job planned (`created_at − first_seen`) | phone | 1000 | 2318 | 4269 | 5864 | 6013 |
| internal: ingest (first event per alert) → job planned (`created_at − first_seen`) | email | 3000 | 2325 | 4515 | 5913 | 6013 |
| internal: ingest (first event per alert) → provider ack (`sent_at − first_seen`) | sms | 1000 | 3161 | 5187 | 6347 | 6545 |
| internal: ingest (first event per alert) → provider ack (`sent_at − first_seen`) | phone | 1000 | 3173 | 5186 | 6347 | 6546 |
| internal: ingest (first event per alert) → provider ack (`sent_at − first_seen`) | email | 3000 | 3424 | 5625 | 6415 | 6530 |

Clock assumption: `first_seen` is written by the Beacon processes, `received_at`/`sent_at` by Postgres, `Created` by Mailpit, so the numbers are valid only when they share a host clock (same machine or NTP).

RESULT: PASS — every sms/phone/email job sent; nothing missing, unsent or dead
