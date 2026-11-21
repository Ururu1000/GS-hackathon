#!/usr/bin/env bash
# Delivery latency reconciler (SPEC §9, M6), scripted front end for the Rust example
# `beacon-delivery/examples/delivery_latency.rs`.
#
# Runs AFTER a beacon-loadgen run, against the same stack: reads only Postgres and
# Mailpit (it sends no events), waits for the run's delivery jobs to drain, measures
# ingest → gateway receipt / Mailpit message / job planned / provider ack per channel,
# and reconciles every job against the fake gateway's receipts.
#
# Requirements: the beacon-loadgen run finished and the stack still reachable:
#   beacon --role ingest,matcher,aggregator,relay,router,delivery,gateway
# plus Postgres and Mailpit. Nothing is wiped; the run is identified by the 12-hex
# run id that beacon-loadgen prints in its report title.
#
# Environment:
#   DATABASE_URL   required, e.g. postgres://beacon:beacon@localhost:5432/beacon
#   MAILPIT_URL    default http://localhost:8025
#
# Example:
#   beacon-loadgen --rate 5000 --duration 30 | tee lg.out
#   scripts/delivery_latency.sh --run-id "$(grep -o '([0-9a-f]\{12\})' lg.out | tr -d '()' | head -1)" \
#       --scenario a-baseline \
#       --workload "beacon-loadgen --rate 5000 --duration 30" \
#       --out loadtest/results/delivery-a-baseline.md --json delivery-a-baseline.json
#
# Passes every argument through; see `--help` for the full list. Exits 0 only if
# nothing is unsent, dead or missing; 1 if the reconciliation found a shortfall;
# 2 on a usage error, an empty selection or an unreachable stack.
set -euo pipefail
cd "$(dirname "$0")/.."
exec cargo run -q --release -p beacon-delivery --example delivery_latency -- "$@"
