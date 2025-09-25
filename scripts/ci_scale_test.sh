#!/usr/bin/env bash
set -euo pipefail

# Simple scale smoke test used in CI. Spins up two cass nodes with
# read consistency set to ALL and drives a small amount of load using the
# perf_client example.

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CARGO_BIN=${CARGO_BIN:-cargo}
NODE1=${NODE1:-http://127.0.0.1:18080}
NODE2=${NODE2:-http://127.0.0.1:18081}
OPS=${OPS:-1000}
THREADS=${THREADS:-4}
INFLIGHT=${INFLIGHT:-4}
WAIT_SECS=${WAIT_SECS:-60}
STABILIZE_SECS=${STABILIZE_SECS:-10}
WARMUP_OPS=${WARMUP_OPS:-1}

cleanup() {
  local ec=$1
  trap - EXIT INT TERM

  for pid in ${SERVER_PIDS:-}; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done

  if [[ -n "${DATA_DIRS:-}" ]]; then
    # shellcheck disable=SC2086
    rm -rf ${DATA_DIRS}
  fi

  exit "$ec"
}
trap 'cleanup $?' EXIT
trap 'cleanup 1' INT TERM

wait_for_port() {
  local url=$1
  local host port
  host=${url#*://}
  port=${host##*:}
  host=${host%%:*}
  if [[ "$host" == "$port" ]]; then
    host=127.0.0.1
  fi
  if [[ ! "$port" =~ ^[0-9]+$ ]]; then
    echo "error: could not parse port from '$url'" >&2
    return 1
  fi

  python3 - "$host" "$port" "$WAIT_SECS" <<'PY'
import socket
import sys
import time

host = sys.argv[1]
port = int(sys.argv[2])
wait_secs = int(sys.argv[3])
end = time.time() + wait_secs

while time.time() < end:
    try:
        with socket.create_connection((host, port), timeout=1):
            sys.exit(0)
    except OSError:
        time.sleep(0.2)

print(f"timed out waiting for {host}:{port} after {wait_secs}s", file=sys.stderr)
sys.exit(1)
PY
}

pushd "$ROOT_DIR" >/dev/null

# Build the server binary and perf client once before launching anything.
$CARGO_BIN build --bin cass --example perf_client

export CASS_DISABLE_TRACING=${CASS_DISABLE_TRACING:-1}

CASS_BIN="$ROOT_DIR/target/debug/cass"
CLIENT_BIN="$ROOT_DIR/target/debug/examples/perf_client"
if [[ ! -x "$CASS_BIN" || ! -x "$CLIENT_BIN" ]]; then
  echo "error: expected built binaries at $CASS_BIN and $CLIENT_BIN" >&2
  exit 1
fi

data_dir1=$(mktemp -d)
data_dir2=$(mktemp -d)
DATA_DIRS="$data_dir1 $data_dir2"

# Launch both nodes with RF=2 and read consistency ALL so that every read
# consults both replicas.
"$CASS_BIN" \
  server \
  --node-addr "$NODE1" \
  --peer "$NODE2" \
  --data-dir "$data_dir1" \
  --rf 2 \
  --read-consistency all &
SERVER1_PID=$!

"$CASS_BIN" \
  server \
  --node-addr "$NODE2" \
  --peer "$NODE1" \
  --data-dir "$data_dir2" \
  --rf 2 \
  --read-consistency all &
SERVER2_PID=$!

SERVER_PIDS="$SERVER1_PID $SERVER2_PID"

wait_for_port "$NODE1"
wait_for_port "$NODE2"

# Give gossip a brief moment to propagate peers before firing load.
sleep "$STABILIZE_SECS"

# Ensure the schema is present on all replicas before the main workload.
for warm_node in "$NODE1" "$NODE2"; do
  "$CLIENT_BIN" \
    --node "$warm_node" \
    --ops "$WARMUP_OPS" \
    --threads 1 \
    --inflight 1
done

client_log=$(mktemp)
if ! "$CLIENT_BIN" \
  --node "$NODE1" \
  --ops "$OPS" \
  --threads "$THREADS" \
  --inflight "$INFLIGHT" \
  >"$client_log" 2>&1; then
  cat "$client_log"
  rm -f "$client_log"
  echo "error: perf_client exited with a failure status" >&2
  exit 1
fi

cat "$client_log"
if grep -qE '\[[A-Z]+\] [0-9]+ RPCs failed' "$client_log"; then
  rm -f "$client_log"
  echo "error: perf_client reported RPC failures" >&2
  exit 1
fi
rm -f "$client_log"

echo "Scale test completed successfully"
