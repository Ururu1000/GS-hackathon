#!/usr/bin/env bash
# Ingest → delivery latency benchmark (SPEC §9, M6), scripted front end for the Rust
# example `beacon-delivery/examples/latency_bench.rs`.
#
# Posts unique WORKFLOW_FAILED (rule settlements-workflow-failed → phone + sms via
# the fake gateway) and trade.rejected.v1 (rule rates-rejected-trades → email via
# Mailpit) sample events through the ingest webhook on a fixed open-loop schedule —
# never waiting for a response before the next send — then reports, per metric,
# count/p50/p90/p99/max in ms (nearest-rank percentiles):
#   ingest → gateway receipt   gateway_receipts.received_at - t0 (phone + sms jobs)
#   ingest → email receipt     Mailpit message Created - t0 (falls back to sent_at)
#   ingest → alert             alerts.first_seen - t0
#   alert → delivered          delivery_jobs.sent_at - first_seen
# with the workload parameters (events, rate, mix, warmup, timestamp, git commit,
# host CPUs) and any errors/missing receipts.
#
# Requirements: the full stack already running and reachable:
#   beacon --role ingest,matcher,aggregator,relay,router,delivery,gateway
# plus Postgres, Redpanda and Mailpit (deploy/docker-compose.yml). Nothing is wiped;
# this run's events are identified by a random tag embedded in their ids, so dedup
# never merges them.
#
# Environment:
#   DATABASE_URL    required, e.g. postgres://beacon:beacon@localhost:5432/beacon
#   INGEST_URL      default http://localhost:8090
#   GATEWAY_URL     default http://localhost:8070 (pre-flight read only)
#   MAILPIT_URL     default http://localhost:8025
#   GATEWAY_TOKEN   optional bearer for the gateway pre-flight read
#
# Usage: scripts/demo_latency.sh [--events N] [--rate R] [--mix sms|email|both]
#                                [--warmup W] [--timeout S] [--out PATH]
# Examples:
#   scripts/demo_latency.sh                            # 500 events at 50/s, mix both
#   scripts/demo_latency.sh --events 200 --rate 20     # CI-friendly
#   scripts/demo_latency.sh --mix email --out run.json
#
# NOTE: the numbers are only valid when the client, Postgres and Mailpit share a
# clock (same machine / NTP).
#
# Exits 0 only if every expected receipt arrived and no ingest POST failed.
set -euo pipefail
cd "$(dirname "$0")/.."
exec cargo run -q --release -p beacon-delivery --example latency_bench -- "$@"
