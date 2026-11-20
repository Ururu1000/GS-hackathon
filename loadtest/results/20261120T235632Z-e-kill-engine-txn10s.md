# Load test `e-kill-engine-txn10s` (01a1fb637f2e)

## Workload

| Parameter | Value |
|---|---|
| Target rate | 10000 events/s |
| Duration | 25 s |
| Events per request | 200 |
| Distinct keys per event type | 2000 |
| Match ratio | 0.8 |
| Max concurrent requests | 256 |
| Host | Apple M5 |
| Ingest URL | http://127.0.0.1:8090 |

## Throughput

| Metric | Value |
|---|---|
| Sent | 250000 |
| Accepted (202) | 250000 |
| Matching events accepted (expected sum of alert counts) | 199999 |
| Rejected (429) | 0 |
| Failed | 0 |
| Accepted rate | 9999 events/s |
| Shed by client (over concurrency budget) | 0 events |
| Alert updates observed | 167459 |
| Distinct alerts observed | 8000 |
| Drain after send stopped | 3.0 s |

## Latency (ms), intended send → alert on `alerts.updates`

| Measure | Samples | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| Any update (count bump or new alert) | 167459 | 481.3 | 4067.3 | 9388.0 | 11468.8 |
| First alert: ingest receipt → first update (server clock) | 8000 | 236.3 | 312.8 | 373.8 | 446.7 |

Samples are the events whose id is the alert's `last_event_id` when an update is published; duplicates folded into the same batch are not individually timed.
