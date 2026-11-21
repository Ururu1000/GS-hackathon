# Delivery latency: `m6-delivery-200-m5` (run `01a1fd098460`)

## Workload

beacon-loadgen --rate 200 --duration 30 --batch 100 --distinct-keys 1000 --match-ratio 0.8 --concurrency 256 --drain 15 --scenario m6-delivery-200-m5 --out results/main/loadgen-200

| fact | value |
|---|---|
| git commit | `963b4b4` |
| host CPUs | 10 |
| generated at (UTC) | 2026-11-21T07:37:35.083Z |
| drain | 0.1 s — every notification routed, nothing unsent |
| selection | run id `01a1fd098460` in alerts.title/body/fields |

Alerts matched: 2783 (699 without sms/phone/email jobs)

| rule | alerts |
|---|---:|
| anna-payments-critical | 707 |
| pricing-engine-warnings | 699 |
| rates-rejected-trades | 692 |
| settlements-workflow-failed | 685 |

## Reconciliation

Jobs: the alerts' first notification only (lowest `notify_seq` per alert, `escalation_step = 0`), channels sms/phone/email.

| channel | expected | sent | unsent | dead | missing | duplicates | attempts>1 |
|---|---:|---:|---:|---:|---:|---:|---:|
| sms | 685 | 685 | 0 | 0 | 0 | 0 | 0 |
| phone | 685 | 685 | 0 | 0 | 0 | 0 | 0 |
| email | 2091 | 2091 | 0 | 0 | 0 | n/a | 0 |

missing = sent but no gateway receipt (sms, phone) or no Mailpit message (email); unsent = `sent_at IS NULL` and status not dead (pending, in_flight, cancelled); duplicates = SUM(`gateway_receipts.duplicates`).

## Latency (ms)

| metric | channel | count | p50 | p90 | p99 | max |
|---|---|---:|---:|---:|---:|---:|
| gateway acceptance: ingest (first event per alert) → receipt (`received_at − first_seen`) | sms | 685 | 402 | 569 | 757 | 781 |
| gateway acceptance: ingest (first event per alert) → receipt (`received_at − first_seen`) | phone | 685 | 396 | 560 | 721 | 755 |
| email in Mailpit: ingest (first event per alert) → message (`Created − first_seen`) | email | 2091 | 433 | 615 | 793 | 833 |
| internal: ingest (first event per alert) → job planned (`created_at − first_seen`) | sms | 685 | 193 | 314 | 413 | 413 |
| internal: ingest (first event per alert) → job planned (`created_at − first_seen`) | phone | 685 | 193 | 314 | 413 | 413 |
| internal: ingest (first event per alert) → job planned (`created_at − first_seen`) | email | 2091 | 193 | 314 | 413 | 478 |
| internal: ingest (first event per alert) → provider ack (`sent_at − first_seen`) | sms | 685 | 406 | 580 | 768 | 789 |
| internal: ingest (first event per alert) → provider ack (`sent_at − first_seen`) | phone | 685 | 400 | 573 | 734 | 763 |
| internal: ingest (first event per alert) → provider ack (`sent_at − first_seen`) | email | 2091 | 436 | 619 | 800 | 836 |

Clock assumption: `first_seen` is written by the Beacon processes, `received_at`/`sent_at` by Postgres, `Created` by Mailpit, so the numbers are valid only when they share a host clock (same machine or NTP).

RESULT: PASS — every sms/phone/email job sent; nothing missing, unsent or dead
