# Load testing

`beacon-loadgen` is an **open-loop** generator. It schedules requests on a clock rather than waiting for the previous one to finish, so a slow server can't hide its latency. It sends Appendix A-shaped events (workflow failures, rejected trades, pricing log lines, Prometheus alerts) with randomized keys.

**What is measured:** latency runs from each event's *intended* send time until the alert carrying that event id is published on `alerts.updates`. That covers ingest, matching, dedup, the Postgres commit and the relay. Browser SSE delivery is measured separately once the API layer exists.

## Run

```bash
docker compose -f deploy/docker-compose.yml up -d --wait
cargo build --release -p beacon -p beacon-loadgen
DATABASE_URL=postgres://beacon:beacon@localhost:5432/beacon \
  MATCHER_WORKERS=4 AGGREGATOR_WORKERS=4 RELAY_WORKERS=2 \
  ./target/release/beacon --migrate --role ingest,matcher,aggregator,relay &
./target/release/beacon-loadgen --rate 20000 --duration 20 --batch 200 --distinct-keys 5000
```

Each run writes `results/<timestamp>-<scenario>.md` with every workload parameter.

Useful knobs:

| Flag | Meaning |
|---|---|
| `--rate` | Target events per second |
| `--batch` | Events per HTTP request (`POST /ingest/{source}/batch`) |
| `--distinct-keys` | Incident keys per event type. Fewer keys means more duplicates folded into each alert. |
| `--match-ratio` | Share of events that match a rule |
| `--scenario` | Label used in the report name |

The generator first sends a warm-up probe through the whole pipeline. That way Kafka consumer-group joins are not counted as latency.

## M0 results (one laptop, everything local)

Setup:
- **Host:** Apple M5. Redpanda (2 cores) and Postgres 16 run in Colima (4 vCPU, 8 GB).
- **Beacon:** a release build with 4 matchers, 4 aggregators and 2 relays.

| Target rate | Accepted | p50 | p99 | max |
|---|---|---|---|---|
| 5,000 ev/s | 5,007 ev/s, 0 errors | 45.6 ms | 53.8 ms | 69.2 ms |
| 10,000 ev/s | 10,006 ev/s, 0 errors | 41.8 ms | 67.2 ms | 108.0 ms |
| 20,000 ev/s | 19,999 ev/s, 0 errors | 84.2 ms | 167.2 ms | 240.5 ms |

- **Workload:** 200 events per request, 5,000 keys per event type and an 80% match ratio. These are any-update latencies; full reports are in `results/`.
- **Caveats:**
  - Delivery roles were not running in these runs.
  - The UI/SSE hop is not included yet.

## Scenario e: kill the engine mid-run (exact counts)

Setup:
- Ingest, relay and API run in one process.
- Matcher and aggregator run in a second process: 4 matcher workers and 4 aggregator workers.
- 10,000 events/s for 25 s, keys namespaced per run.
- About 10 s into the run, the matcher/aggregator process is killed with `kill -9` and restarted 2 s later.

| Measure | Value |
|---|---|
| Events accepted (202) | 250,000 |
| Matching events accepted | 199,999 |
| Sum of alert counts after drain | **200,000** (199,999 + 1 warm-up probe) |
| Lost or double-counted | **0** |
| Processing gap after the kill | about 10 s (open Kafka transaction timeout) |

How this works:
- **Matcher:** it produces matches and commits its `events.raw` offsets in one Kafka transaction.
- **Aggregator:** it advances an epoch-fenced checkpoint in the same Postgres transaction as its alert writes, and skips anything below the checkpoint on replay.

Latency percentiles in this run include the crash and the recovery, so they are not comparable with the steady-state rows above.

## Exactly-once vs at-least-once at 20,000 events/s

The matcher runs exactly-once by default: it uses Kafka transactions, plus the fenced aggregator checkpoints above. Each matcher worker blocks on a transaction commit every `MATCHER_TXN_FLUSH_MS` (default 50 ms), so it needs more workers to keep up.

These runs used the same laptop as the first table, while another load test was running on it, so the absolute numbers are higher than in that table. Compare the rows with each other.

| Mode | Matcher workers | p50 | p99 | max |
|---|---|---|---|---|
| At-least-once (`MATCHER_TRANSACTIONS=false`) | 4 | 165 ms | 321 ms | 487 ms |
| **Exactly-once (default)**, 50 ms commits | 8 | 329 ms | **487 ms** | 607 ms |
| Exactly-once, 100 ms commits | 12 | 429 ms | 673 ms | 871 ms |
| Exactly-once, 20 ms commits | 4 | 334 ms | 2,894 ms | 3,396 ms (backlog) |

The trade-off: exactly-once costs about 170 ms of p99 at this rate. Here the results are latency from intended send until the alert update, as above.

## Scenario d: 500 duplicates per second on one key

`--event-types workflow --distinct-keys 1 --match-ratio 1 --rate 500 --duration 20`: every event is the same workflow failure.

| Measure | Value |
|---|---|
| Events accepted | 10,000 |
| Alerts for that key | **1** |
| Alert count | **10,000** (exact) |
| Count update latency (p50 / p99) | 165.6 ms / 204.3 ms |

## Kafka source (second source type)

`--via kafka --topic workflow-events --event-types workflow --rate 5000 --duration 15`.

Events go to the external `workflow-events` topic, which the `kafka:workflows` source in `config/sources/` consumes. They are not posted to the webhook.

| Measure | Value |
|---|---|
| Produced (acked) | 75,000 at 5,005 events/s, 0 failed |
| First alert, ingest receipt → first update (p50 / p99) | 146.7 ms / 284.4 ms |

This run measured only first-alert latency (server clock), and that clock starts when the Kafka source reads the event, so it leaves out the time an event waits in `workflow-events`. Since `security/pr17-fixes`, loadgen correlates each event by the topic/partition/offset in the broker ack, so later Kafka runs also report "any update" from the intended send time, including that wait. Each report now records its transport, topic and event types.
