# Load test `m6-delivery-2000-m5` (01a1fd0a067d)

## Workload

| Parameter | Value |
|---|---|
| Transport | HTTP `POST http://127.0.0.1:8090/ingest/lg/batch` |
| Event types | all |
| Target rate | 2000 events/s |
| Duration | 30 s |
| Events per batch | 100 |
| Distinct keys per event type | 1000 |
| Match ratio | 0.8 |
| Max concurrent batches | 256 |
| Host | Apple M5 |

## Throughput

| Metric | Value |
|---|---|
| Sent | 60000 |
| Accepted (HTTP 202 or broker ack) | 60000 |
| Matching events accepted (expected sum of alert counts) | 47973 |
| Rejected (429) | 0 |
| Failed | 0 |
| Accepted rate | 2002 events/s |
| Shed by client (over concurrency budget) | 0 events |
| Alert updates observed | 38915 |
| Distinct alerts observed | 4000 |
| Drain after send stopped | 2.5 s |

## Latency (ms), intended send → alert on `alerts.updates`

| Measure | Samples | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| Any update (count bump or new alert) | 38915 | 368.6 | 2695.2 | 6742.0 | 8077.3 |
| First alert: ingest receipt → first update (server clock) | 4000 | 1755.1 | 7241.7 | 8011.8 | 8138.8 |

Samples are the events whose id is the alert's `last_event_id` when an update is published; duplicates folded into the same batch are not individually timed.
