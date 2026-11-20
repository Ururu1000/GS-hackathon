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
