# Load test `m6-delivery-200-m5` (01a1fd098460)

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
| Matching events accepted (expected sum of alert counts) | 4815 |
| Rejected (429) | 0 |
| Failed | 0 |
| Accepted rate | 203 events/s |
| Shed by client (over concurrency budget) | 0 events |
| Alert updates observed | 4770 |
| Distinct alerts observed | 2783 |
| Drain after send stopped | 2.5 s |

## Latency (ms), intended send → alert on `alerts.updates`

| Measure | Samples | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| Any update (count bump or new alert) | 4770 | 117.5 | 174.8 | 263.9 | 364.5 |
| First alert: ingest receipt → first update (server clock) | 2783 | 115.1 | 153.3 | 249.5 | 363.5 |

Samples are the events whose id is the alert's `last_event_id` when an update is published; duplicates folded into the same batch are not individually timed.
