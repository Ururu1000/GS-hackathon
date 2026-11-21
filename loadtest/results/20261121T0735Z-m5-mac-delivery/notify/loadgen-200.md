# Load test `m6-delivery-200-m5` (01a1fd0c8028)

## Workload

| Parameter | Value |
|---|---|
| Transport | HTTP `POST http://127.0.0.1:8090/ingest/lg/batch` |
| Event types | all |
| Target rate | 200 events/s |
| Duration | 30 s |
| Events per batch | 100 |
| Distinct keys per event type | 1000 |
| Match ratio | 0.8 |
| Max concurrent batches | 256 |
| Host | Apple M5 |

## Throughput

| Metric | Value |
|---|---|
| Sent | 6000 |
| Accepted (HTTP 202 or broker ack) | 6000 |
| Matching events accepted (expected sum of alert counts) | 4824 |
| Rejected (429) | 0 |
| Failed | 0 |
| Accepted rate | 203 events/s |
| Shed by client (over concurrency budget) | 0 events |
| Alert updates observed | 4778 |
| Distinct alerts observed | 2777 |
| Drain after send stopped | 2.5 s |

## Latency (ms), intended send → alert on `alerts.updates`

| Measure | Samples | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| Any update (count bump or new alert) | 4778 | 119.1 | 125.1 | 132.6 | 135.4 |
| First alert: ingest receipt → first update (server clock) | 2777 | 116.5 | 122.8 | 131.1 | 133.4 |

Samples are the events whose id is the alert's `last_event_id` when an update is published; duplicates folded into the same batch are not individually timed.
