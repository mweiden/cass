#!/usr/bin/env bash
# Ratchet harness wrapper. Treated as evaluation harness — DO NOT modify
# during a ratchet run; doing so invalidates cross-iteration comparison.
#
# Builds the cass server image (so source changes take effect), runs
# scripts/perf_compare.sh --cass-only, then emits two scores averaged
# across every cass_t*.log file (READ + WRITE lines):
#
#     Latency Score: <avg_p95_ms>
#     Operations Score: <avg_ops_per_sec>
#
# Configure ratchet with metric_name="Operations Score" (direction=maximize)
# to optimize throughput, or "Latency Score" (direction=minimize) for p95.
#
# Exit non-zero on any pipeline failure so ratchet records a crash.
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose build >/dev/null 2>&1
./scripts/perf_compare.sh --cass-only >/dev/null 2>&1

latency=$(cat perf-results/cass_t*.log \
  | grep '95th' \
  | tr -s ' ' \
  | cut -d' ' -f5 \
  | awk '{ total += $1; count++ } END { if (count == 0) { exit 1 } print total/count }')

ops=$(cat perf-results/cass_t*.log \
  | grep '^Op rate' \
  | tr -s ' ' \
  | cut -d' ' -f4 \
  | awk '{ total += $1; count++ } END { if (count == 0) { exit 1 } print total/count }')

printf 'Latency Score: %s\n' "$latency"
printf 'Operations Score: %s\n' "$ops"
