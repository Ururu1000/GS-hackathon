# Delivery latency: `m6-delivery-2000-m5` (run `01a1fd0a067d`)

## Workload

beacon-loadgen --rate 2000 --duration 30 --batch 100 --distinct-keys 1000 --match-ratio 0.8 --concurrency 256 --drain 15 --scenario m6-delivery-2000-m5 --out results/main/loadgen-2000

| fact | value |
|---|---|
| git commit | `963b4b4` |
| host CPUs | 10 |
| generated at (UTC) | 2026-11-21T07:38:08.198Z |
| drain | 0.0 s — every notification routed, nothing unsent |
| selection | run id `01a1fd0a067d` in alerts.title/body/fields |

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
| gateway acceptance: ingest (first event per alert) → receipt (`received_at − first_seen`) | sms | 1000 | 8362 | 10031 | 10492 | 10671 |
| gateway acceptance: ingest (first event per alert) → receipt (`received_at − first_seen`) | phone | 1000 | 8471 | 9954 | 10684 | 10797 |
| email in Mailpit: ingest (first event per alert) → message (`Created − first_seen`) | email | 3000 | 8793 | 10982 | 11478 | 11668 |
| internal: ingest (first event per alert) → job planned (`created_at − first_seen`) | sms | 1000 | 7090 | 9256 | 9845 | 9995 |
| internal: ingest (first event per alert) → job planned (`created_at − first_seen`) | phone | 1000 | 7090 | 9256 | 9845 | 9995 |
| internal: ingest (first event per alert) → job planned (`created_at − first_seen`) | email | 3000 | 7162 | 9256 | 9895 | 10046 |
| internal: ingest (first event per alert) → provider ack (`sent_at − first_seen`) | sms | 1000 | 8370 | 10044 | 10502 | 10678 |
| internal: ingest (first event per alert) → provider ack (`sent_at − first_seen`) | phone | 1000 | 8479 | 9965 | 10704 | 10813 |
| internal: ingest (first event per alert) → provider ack (`sent_at − first_seen`) | email | 3000 | 8802 | 10992 | 11494 | 11674 |

Clock assumption: `first_seen` is written by the Beacon processes, `received_at`/`sent_at` by Postgres, `Created` by Mailpit, so the numbers are valid only when they share a host clock (same machine or NTP).

RESULT: PASS — every sms/phone/email job sent; nothing missing, unsent or dead
