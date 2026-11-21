# Delivery latency: `m6-delivery-5000-m5` (run `01a1fd0d80f4`)

## Workload

beacon-loadgen --rate 5000 --duration 30 --batch 100 --distinct-keys 1000 --match-ratio 0.8 --concurrency 256 --drain 15 --scenario m6-delivery-5000-m5 --out results/notify/loadgen-5000

| fact | value |
|---|---|
| git commit | `35650c5` |
| host CPUs | 10 |
| generated at (UTC) | 2026-11-21T07:41:56.182Z |
| drain | 0.1 s — every notification routed, nothing unsent |
| selection | run id `01a1fd0d80f4` in alerts.title/body/fields |

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
| gateway acceptance: ingest (first event per alert) → receipt (`received_at − first_seen`) | sms | 1000 | 5682 | 8255 | 9092 | 9245 |
| gateway acceptance: ingest (first event per alert) → receipt (`received_at − first_seen`) | phone | 1000 | 5681 | 8267 | 9040 | 9187 |
| email in Mailpit: ingest (first event per alert) → message (`Created − first_seen`) | email | 3000 | 5594 | 8441 | 9345 | 9548 |
| internal: ingest (first event per alert) → job planned (`created_at − first_seen`) | sms | 1000 | 4360 | 6636 | 7592 | 7692 |
| internal: ingest (first event per alert) → job planned (`created_at − first_seen`) | phone | 1000 | 4360 | 6636 | 7592 | 7692 |
| internal: ingest (first event per alert) → job planned (`created_at − first_seen`) | email | 3000 | 4219 | 6575 | 7632 | 7712 |
| internal: ingest (first event per alert) → provider ack (`sent_at − first_seen`) | sms | 1000 | 5696 | 8263 | 9123 | 9277 |
| internal: ingest (first event per alert) → provider ack (`sent_at − first_seen`) | phone | 1000 | 5696 | 8274 | 9081 | 9222 |
| internal: ingest (first event per alert) → provider ack (`sent_at − first_seen`) | email | 3000 | 5599 | 8447 | 9352 | 9555 |

Clock assumption: `first_seen` is written by the Beacon processes, `received_at`/`sent_at` by Postgres, `Created` by Mailpit, so the numbers are valid only when they share a host clock (same machine or NTP).

RESULT: PASS — every sms/phone/email job sent; nothing missing, unsent or dead
