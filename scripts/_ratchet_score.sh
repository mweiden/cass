#!/usr/bin/env bash
# Ratchet harness wrapper. Treated as evaluation harness — DO NOT modify
# during a ratchet run; doing so invalidates cross-iteration comparison.
#
# Builds the cass server image (so source changes take effect), runs
# scripts/perf_compare.sh --cass-only, then averages the 95th-percentile
# latency (in ms) across every cass_t*.log file (READ + WRITE lines) and
# prints a single line:
#
#     score: <avg_p95_ms>
#
# Exit non-zero on any pipeline failure so ratchet records a crash.
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose build >/dev/null 2>&1
./scripts/perf_compare.sh --cass-only >/dev/null 2>&1

avg=$(cat perf-results/cass_t*.log \
  | grep '95th' \
  | tr -s ' ' \
  | cut -d' ' -f5 \
  | awk '{ total += $1; count++ } END { if (count == 0) { exit 1 } print total/count }')

printf 'score: %s\n' "$avg"
