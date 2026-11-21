# Load test `m6-delivery-5000-m5` (01a1fd0a87d1)

## Workload

| Parameter | Value |
|---|---|
| Transport | HTTP `POST http://127.0.0.1:8090/ingest/lg/batch` |
| Event types | all |
| Target rate | 5000 events/s |
| Duration | 30 s |
| Events per batch | 100 |
| Distinct keys per event type | 1000 |
| Match ratio | 0.8 |
| Max concurrent batches | 256 |
| Host | Apple M5 |

## Throughput

| Metric | Value |
|---|---|
| Sent | 150000 |
| Accepted (HTTP 202 or broker ack) | 150000 |
| Matching events accepted (expected sum of alert counts) | 120195 |
| Rejected (429) | 0 |
| Failed | 0 |
| Accepted rate | 5002 events/s |
| Shed by client (over concurrency budget) | 0 events |
| Alert updates observed | 89066 |
| Distinct alerts observed | 4000 |
| Drain after send stopped | 2.5 s |

## Latency (ms), intended send → alert on `alerts.updates`

| Measure | Samples | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| Any update (count bump or new alert) | 89016 | 283.9 | 2418.7 | 7655.4 | 8699.9 |
| First alert: ingest receipt → first update (server clock) | 4000 | 2085.9 | 2730.0 | 8421.4 | 8691.7 |

Samples are the events whose id is the alert's `last_event_id` when an update is published; duplicates folded into the same batch are not individually timed.
