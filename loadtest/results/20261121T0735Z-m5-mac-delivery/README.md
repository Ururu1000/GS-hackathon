# Ingest → delivery on a 10-core Apple M5 Mac: main vs delivery/latency-notify

Run 2026-11-21 07:35–07:42 UTC on one 10-core Apple M5 (24 GB). Every service shares one clock.

| | main | delivery/latency-notify (PR #49) |
|---|---|---|
| commit | `963b4b4` | `35650c5` |
| change | 250 ms polling | router + workers wake on LISTEN/NOTIFY; breaker hot-row fix |

**Setup, the same for both** (as in `.github/workflows/loadgen.yml`):
- **Infrastructure:** Postgres 16, Redpanda and Mailpit via `deploy/docker-compose.yml` (infra services only), with fresh volumes for each commit.
- **Beacon:** native release build: `beacon --migrate --role ingest,matcher,aggregator,relay,router,delivery,gateway`.
- **Measurement tools:** `latency_bench`, `sms_outage_demo` and `delivery_latency` are all built from `35650c5` and used for both commits. That `latency_bench` waits for its warm-up to drain.
- **Order:** the same on both commits: latency runs, then the SMS outage, then loadgen at 200, 2,000 and 5,000 ev/s on the same stack.

Every run is **PASS**: 0 missing, 0 dead, 0 duplicates, 0 ingest errors, 0 rejected (429).

## Steady load: `latency_bench` (unique events, mix sms + phone + email)

| run | metric | main p50 / p90 / p99 / max (ms) | notify p50 / p90 / p99 / max (ms) |
|---|---|---|---|
| 500 @ 50/s | ingest → gateway (sms + phone) | 383 / 550 / 664 / 747 | **130 / 225 / 242 / 246** |
| 500 @ 50/s | ingest → Mailpit | 392 / 536 / 647 / 708 | **136 / 179 / 253 / 270** |
| 2000 @ 200/s | ingest → gateway (sms + phone) | 506 / 749 / 1194 / 1380 | **197 / 285 / 361 / 406** |
| 2000 @ 200/s | ingest → Mailpit | 529 / 775 / 998 / 1107 | **210 / 293 / 388 / 427** |

## SMS gateway outage: 50 events, 60 s outage

| | main | notify |
|---|---|---|
| sent / dead / fallbacks | 50 / 0 / 0 | 50 / 0 / 0 |
| distinct receipts / duplicates | 50 / 0 | 50 / 0 |
| max attempts | 2 | 2 |
| outage end → last SMS sent | 2.8 s | 2.1 s |

## Bursts: `beacon-loadgen --duration 30 --batch 100 --distinct-keys 1000 --match-ratio 0.8 --concurrency 256 --drain 15`

The first event of each alert is matched to its sms/phone/email jobs. At 2,000 and 5,000 ev/s that is 1000 sms + 1000 phone + 3000 email jobs per run. Ingest → alert is loadgen's intended send → any update on `alerts.updates`.

| rate (accepted) | metric | main p50 / p99 (ms) | notify p50 / p99 (ms) |
|---|---|---|---|
| 200 (203/s) | ingest → alert update | 118 / 264 | 119 / 133 |
| 200 | ingest → gateway (sms) | 402 / 757 | **180 / 220** |
| 200 | ingest → Mailpit | 433 / 793 | **197 / 268** |
| 2000 (2002/s) | ingest → alert update | 369 / 6742 | 204 / 4330 |
| 2000 | ingest → job planned (sms) | 7090 / 9845 | 2318 / 5864 |
| 2000 | ingest → gateway (sms) | 8362 / 10492 | **3153 / 6341** |
| 2000 | ingest → Mailpit | 8793 / 11478 | **3419 / 6410** |
| 5000 (5000/s) | ingest → alert update | 284 / 7655 | 259 / 4907 |
| 5000 | ingest → job planned (sms) | 7333 / 11075 | 4360 / 7592 |
| 5000 | ingest → gateway (sms) | 8569 / 13067 | **5682 / 9092** |
| 5000 | ingest → Mailpit | 9094 / 13123 | **5594 / 9345** |

All runs drained within 0.1 s of loadgen's 15 s drain window.

## Reading

- **Steady state:** notify cuts ingest → delivery by about 3× (200/s: gateway p99 1194 → 361 ms). The gap is the 250 ms poll, paid at the router and again at the worker.
- **Bursts:** the burst lag is real on this hardware, not only CI starvation. On notify at 2,000 ev/s, delivery is p50 3.2 s / p99 6.3 s.
  - About two thirds of that is before a job exists: ingest → job planned is p50 2.3 s.
  - Part of it is already in the engine: alert-update p99 is 4.3 s at 2,000 ev/s. With 1000 distinct keys at 80% match, the first seconds of the burst are all new-alert inserts.
- **Targets proposed on the board:**
  - delivery p99 < 1 s at 200 ev/s steady: met on notify (gateway p99 220–361 ms);
  - < 5 s in the 2,000 ev/s burst: not met (p99 6.3 s).
- **Reproducibility:** each subdirectory has the raw reports (`*.md`, `*.txt`) and JSON (`*.json`).
