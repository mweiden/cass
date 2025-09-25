#!/usr/bin/env bash
set -euo pipefail

# Generate a CPU flamegraph for the query endpoint by running the cass server
# under cargo-flamegraph while driving load with the examples/perf_client.
#
# Requirements:
# - cargo + rust toolchain
# - cargo-flamegraph (install with: cargo install flamegraph)
# - Linux: perf (e.g., sudo apt-get install linux-tools-common linux-tools-generic)
# - macOS: dtrace requires sudo and may require Developer Mode (or SIP relaxed)
#
# Usage:
#   scripts/flamegraph_query.sh [--]            # uses defaults
#
# Environment variables (override to customize):
#   NODE=http://127.0.0.1:8080   # gRPC node address
#   OPS=10000                    # total operations for perf_client
#   THREADS=32                   # concurrent client threads
#   OUTDIR=perf-results          # output directory for the flamegraph
#   EXTRA_SERVER_ARGS=""         # extra args passed to `cass server`
#   WAIT_SECS=180                # max seconds to wait for server readiness
#   PREBUILD=1                   # build release first to avoid profiling compile
#   ALLOW_PORT_IN_USE=0          # set to 1 to skip port-in-use preflight check

NODE=${NODE:-http://127.0.0.1:8080}
OPS=${OPS:-10}
THREADS=${THREADS:-32}
OUTDIR=${OUTDIR:-perf-results}
EXTRA_SERVER_ARGS=${EXTRA_SERVER_ARGS:-"--read-consistency one"}
WAIT_SECS=${WAIT_SECS:-180}

OS=$(uname -s || echo unknown)

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is required on PATH" >&2
  exit 1
fi

# cargo-flamegraph is invoked as `cargo flamegraph` but the binary is cargo-flamegraph
if ! command -v cargo-flamegraph >/dev/null 2>&1; then
  echo "error: cargo-flamegraph not found. Install with: cargo install flamegraph" >&2
  exit 1
fi

if [[ "$OS" == "Linux" ]]; then
  if ! command -v perf >/dev/null 2>&1; then
    echo "error: perf not found. Install linux perf tools and retry." >&2
    exit 1
  fi
fi

mkdir -p "$OUTDIR"

# Derive host/port from NODE for readiness probing
HOST=$(echo "$NODE" | sed -E 's#^[a-zA-Z]+://([^/:]+).*$#\1#')
PORT=$(echo "$NODE" | sed -E 's#.*:([0-9]+).*#\1#')
if ! [[ "$PORT" =~ ^[0-9]+$ ]]; then
  echo "error: could not parse port from NODE='$NODE'" >&2
  exit 1
fi
if [[ -z "$HOST" || "$HOST" == "$NODE" ]]; then
  HOST=127.0.0.1
fi

if (( ${ALLOW_PORT_IN_USE:-0} == 0 )); then
  # Bail out early if something is already listening on the chosen host:port
  if (echo > /dev/tcp/"$HOST"/"$PORT") >/dev/null 2>&1; then
    echo "error: $HOST:$PORT already has a listener. Stop it or set NODE to a different port." >&2
    echo "       Example: NODE=http://127.0.0.1:18080 EXTRA_SERVER_ARGS=\"--data-dir /tmp/cass-data-fg\" $0" >&2
    echo "       To force and skip this check, export ALLOW_PORT_IN_USE=1 (not recommended for profiling)." >&2
    exit 1
  fi
fi

OUT_SVG="$OUTDIR/query_flamegraph.svg"

echo "[flamegraph] output: $OUT_SVG"
echo "[flamegraph] node:   $NODE | ops=$OPS threads=$THREADS"

# Clean any previous default output to avoid confusion
rm -f flamegraph.svg perf.data perf.data.old >/dev/null 2>&1 || true

# Optionally prebuild to avoid long compile under the profiler
if (( ${PREBUILD:-1} == 1 )); then
  echo "[flamegraph] prebuilding release binary (for faster startup) ..."
  env CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release
fi

# Ensure cleanup of profiler and children on exit or interruption
STOP_WAIT_SECS=${STOP_WAIT_SECS:-20}

find_server_pid() {
  # Try to identify the cass server PID listening on $HOST:$PORT
  local pid=""
  if command -v lsof >/dev/null 2>&1; then
    pid=$(lsof -nP -iTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | awk 'NR>1{print $2; exit}') || pid=""
  fi
  if [[ -z "$pid" ]] && command -v ss >/dev/null 2>&1; then
    # ss output contains pid=... when run as root; try best-effort parse
    pid=$(ss -lptn 2>/dev/null | awk -v p=":$PORT" '$4 ~ p {print $0}' | sed -n 's/.*pid=\([0-9][0-9]*\).*/\1/p' | head -n1) || pid=""
  fi
  if [[ -z "$pid" ]] && command -v fuser >/dev/null 2>&1; then
    pid=$(fuser -n tcp "$PORT" 2>/dev/null | awk '{print $1; exit}') || pid=""
  fi
  # Fallback: look for our cass binary under cargo target (best-effort)
  if [[ -z "$pid" ]]; then
    pid=$(ps -Ao pid,command | grep -E "target/.*/cass( |$)" | grep -E "server( |$)" | awk '{print $1; exit}') || pid=""
  fi
  echo "$pid"
}

stop_profiler() {
  # Prefer to stop the profiler first so it can finalize output (dtrace/perf
  # dumps) even if the child keeps running briefly. If it doesn't exit, escalate.
  if [[ -z "${FG_PID:-}" ]] || ! kill -0 "$FG_PID" 2>/dev/null; then
    return 0
  fi

  PGID=$(ps -o pgid= -p "$FG_PID" | tr -d ' ' || true)
  if [[ -n "$PGID" ]]; then
    echo "[flamegraph] signaling profiler group (pgid $PGID) with INT ..."
    kill -INT -"$PGID" 2>/dev/null || true
  else
    echo "[flamegraph] signaling profiler (pid $FG_PID) with INT ..."
    kill -INT "$FG_PID" 2>/dev/null || true
  fi

  # Bounded wait for cargo-flamegraph to flush and exit
  local waited=0
  while kill -0 "$FG_PID" 2>/dev/null; do
    if [[ $waited -ge $STOP_WAIT_SECS ]]; then
      break
    fi
    sleep 1
    waited=$((waited+1))
  done

  if kill -0 "$FG_PID" 2>/dev/null; then
    # Profiler still running; try stopping the server child to let profiler finish
    local srv_pid="$(find_server_pid)"
    if [[ -n "$srv_pid" ]] && kill -0 "$srv_pid" 2>/dev/null; then
      echo "[flamegraph] signaling server (pid $srv_pid) with INT ..."
      kill -INT "$srv_pid" 2>/dev/null || true
    fi

    # Wait a bit more
    waited=0
    while kill -0 "$FG_PID" 2>/dev/null; do
      if [[ $waited -ge 5 ]]; then
        break
      fi
      sleep 1
      waited=$((waited+1))
    done
  fi

  if kill -0 "$FG_PID" 2>/dev/null; then
    echo "[flamegraph] profiler still running; escalating to TERM/KILL on process group ..."
    if [[ -n "$PGID" ]]; then
      kill -TERM -"$PGID" 2>/dev/null || true
      sleep 1 || true
      kill -KILL -"$PGID" 2>/dev/null || true
    else
      kill -TERM "$FG_PID" 2>/dev/null || true
      sleep 1 || true
      kill -KILL "$FG_PID" 2>/dev/null || true
    fi
  fi
  wait "$FG_PID" 2>/dev/null || true
}

cleanup() {
  local ec=$?
  stop_profiler || true
  return $ec
}
trap cleanup EXIT INT TERM

# Launch server under cargo flamegraph in background. Build command as an array
# to safely pass multi-word EXTRA_SERVER_ARGS (e.g., "--read-consistency one").
set -m
read -r -a EXTRA_ARR <<< "$EXTRA_SERVER_ARGS"
CMD=(cargo flamegraph --release -o "$OUT_SVG" -- server --node-addr "$NODE")
if [[ ${#EXTRA_ARR[@]} -gt 0 ]]; then
  CMD+=("${EXTRA_ARR[@]}")
fi
# Preserve optional debug symbols hint via env for this invocation
ENV_PREFIX=(env OS_ACTIVITY_MODE=disable CARGO_PROFILE_RELEASE_DEBUG=true)
"${ENV_PREFIX[@]}" "${CMD[@]}" &
FG_PID=$!

# Wait for the gRPC port to accept connections (best-effort, 30s timeout)
echo "[flamegraph] waiting for server at $HOST:$PORT (up to ${WAIT_SECS}s) ..."
tries=0
until (echo > /dev/tcp/"$HOST"/"$PORT") >/dev/null 2>&1; do
  sleep 1
  tries=$((tries+1))
  if [[ $tries -ge $WAIT_SECS ]]; then
    echo "warning: server not ready after ${WAIT_SECS}s; continuing anyway" >&2
    break
  fi
done

# Drive load using the example perf client
echo "[flamegraph] starting perf_client load ..."
cargo run --example perf_client -- --node "$NODE" --ops "$OPS" --threads "$THREADS"

# Give a small tail window to capture trailing activity
sleep 2

# Stop profiling process (trap will also handle abnormal exits)
stop_profiler || true

if [[ -f "$OUT_SVG" ]]; then
  echo "[flamegraph] wrote $OUT_SVG"
else
  # Fallback for older cargo-flamegraph that writes default name
  if [[ -f flamegraph.svg ]]; then
    echo "[flamegraph] expected $OUT_SVG, found default flamegraph.svg; moving ..."
    mv -f flamegraph.svg "$OUT_SVG"
    echo "[flamegraph] wrote $OUT_SVG"
  else
    echo "error: expected $OUT_SVG not found. Check stderr above for clues." >&2
    exit 1
  fi
fi

echo "[flamegraph] done"
