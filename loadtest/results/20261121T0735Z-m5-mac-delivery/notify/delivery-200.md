# Delivery latency: `m6-delivery-200-m5` (run `01a1fd0c8028`)

## Workload

beacon-loadgen --rate 200 --duration 30 --batch 100 --distinct-keys 1000 --match-ratio 0.8 --concurrency 256 --drain 15 --scenario m6-delivery-200-m5 --out results/notify/loadgen-200

| fact | value |
|---|---|
| git commit | `35650c5` |
| host CPUs | 10 |
| generated at (UTC) | 2026-11-21T07:40:49.971Z |
| drain | 0.0 s — every notification routed, nothing unsent |
| selection | run id `01a1fd0c8028` in alerts.title/body/fields |

Alerts matched: 2777 (699 without sms/phone/email jobs)

| rule | alerts |
|---|---:|
| anna-payments-critical | 703 |
| pricing-engine-warnings | 699 |
| rates-rejected-trades | 710 |
| settlements-workflow-failed | 665 |

## Reconciliation

Jobs: the alerts' first notification only (lowest `notify_seq` per alert, `escalation_step = 0`), channels sms/phone/email.

| channel | expected | sent | unsent | dead | missing | duplicates | attempts>1 |
|---|---:|---:|---:|---:|---:|---:|---:|
| sms | 665 | 665 | 0 | 0 | 0 | 0 | 0 |
| phone | 665 | 665 | 0 | 0 | 0 | 0 | 0 |
| email | 2123 | 2123 | 0 | 0 | 0 | n/a | 0 |

missing = sent but no gateway receipt (sms, phone) or no Mailpit message (email); unsent = `sent_at IS NULL` and status not dead (pending, in_flight, cancelled); duplicates = SUM(`gateway_receipts.duplicates`).

## Latency (ms)

| metric | channel | count | p50 | p90 | p99 | max |
|---|---|---:|---:|---:|---:|---:|
| gateway acceptance: ingest (first event per alert) → receipt (`received_at − first_seen`) | sms | 665 | 180 | 205 | 220 | 225 |
| gateway acceptance: ingest (first event per alert) → receipt (`received_at − first_seen`) | phone | 665 | 178 | 206 | 221 | 224 |
| email in Mailpit: ingest (first event per alert) → message (`Created − first_seen`) | email | 2123 | 197 | 237 | 268 | 276 |
| internal: ingest (first event per alert) → job planned (`created_at − first_seen`) | sms | 665 | 115 | 121 | 129 | 129 |
| internal: ingest (first event per alert) → job planned (`created_at − first_seen`) | phone | 665 | 115 | 121 | 129 | 129 |
| internal: ingest (first event per alert) → job planned (`created_at − first_seen`) | email | 2123 | 115 | 121 | 129 | 129 |
| internal: ingest (first event per alert) → provider ack (`sent_at − first_seen`) | sms | 665 | 185 | 211 | 226 | 231 |
| internal: ingest (first event per alert) → provider ack (`sent_at − first_seen`) | phone | 665 | 185 | 212 | 226 | 229 |
| internal: ingest (first event per alert) → provider ack (`sent_at − first_seen`) | email | 2123 | 201 | 241 | 271 | 279 |

Clock assumption: `first_seen` is written by the Beacon processes, `received_at`/`sent_at` by Postgres, `Created` by Mailpit, so the numbers are valid only when they share a host clock (same machine or NTP).

RESULT: PASS — every sms/phone/email job sent; nothing missing, unsent or dead
