# Load test `a-kafka-compose` (01a1fba10fb7)

## Workload

| Parameter | Value |
|---|---|
| Transport | Kafka topic `workflow-events` on 127.0.0.1:19092, read by a Kafka source |
| Event types | workflow |
| Target rate | 3000 events/s |
| Duration | 15 s |
| Events per batch | 100 |
| Distinct keys per event type | 1000 |
| Match ratio | 0.8 |
| Max concurrent batches | 256 |
| Host | Apple M5 |

## Throughput

| Metric | Value |
|---|---|
| Sent | 45000 |
| Accepted (HTTP 202 or broker ack) | 45000 |
| Matching events accepted (expected sum of alert counts) | 35951 |
| Rejected (429) | 0 |
| Failed | 0 |
| Accepted rate | 3006 events/s |
| Shed by client (over concurrency budget) | 0 events |
| Alert updates observed | 11820 |
| Distinct alerts observed | 1000 |
| Drain after send stopped | 20.1 s |

## Latency (ms), intended send → alert on `alerts.updates`

| Measure | Samples | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| Any update (count bump or new alert) | 11820 | 14966.8 | 27033.6 | 29720.6 | 30031.9 |
| First alert: ingest receipt → first update (server clock) | 1000 | 116.2 | 188.8 | 250.6 | 269.6 |

Samples are the events whose id is the alert's `last_event_id` when an update is published; duplicates folded into the same batch are not individually timed.
Over Kafka, "any update" includes the time the event waited in the source topic; "first alert" starts when the Kafka source read it.
