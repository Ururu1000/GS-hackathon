#!/usr/bin/env bash
# SMS outage demo (SPEC §9 scenario c), scripted front end for the Rust example
# `beacon-delivery/examples/sms_outage_demo.rs`.
#
# Starts a simulated sms outage at the fake gateway, posts WORKFLOW_FAILED events
# through the ingest webhook, watches the sms delivery jobs drain, and reconciles
# them against the gateway's persisted receipts.
#
# Requirements: the full stack already running and reachable:
#   beacon --role ingest,matcher,aggregator,relay,router,delivery,gateway
# plus Kafka and Postgres. Nothing is wiped; the run's alerts are identified by a
# random tag embedded in every workflowId.
#
# Environment:
#   DATABASE_URL   required, e.g. postgres://beacon:beacon@localhost:5432/beacon
#   INGEST_URL     default http://localhost:8090
#   GATEWAY_URL    default http://localhost:8070
#
# Usage: scripts/demo_sms_outage.sh [--events N] [--outage-secs S]
# Exits 0 only if every expected sms job was found and sent, nothing died, and
# every job reconciles against exactly one gateway receipt.
set -euo pipefail
cd "$(dirname "$0")/.."
exec cargo run -q -p beacon-delivery --example sms_outage_demo -- "$@"
