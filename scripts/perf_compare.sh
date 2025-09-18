#!/usr/bin/env bash
set -euox pipefail

# Perf compare harness: runs cass (docker compose) and Apache Cassandra (docker) and
# collects throughput/latency logs, then plots a comparison.
#
# Usage:
#   scripts/perf_compare.sh [--cass-only]
#
# Flags:
#   --cass-only   Skip the Apache Cassandra phase; only run cass and write cass_*.log.
#                 Useful when you already have cassandra_* logs in $OUTDIR.

OPS=${OPS:-5000}
OUTDIR=${OUTDIR:-perf-results}
CASS_NODE=${CASS_NODE:-http://localhost:8080}
THREADS=${THREADS:-54}
# Space-separated set of thread counts to test
THREADS_SET=${THREADS_SET:-"1 2 4 8 16 32 64"}
# If set (or --cass-only flag passed), skip Cassandra phase and only collect cass metrics
CASS_ONLY=${CASS_ONLY:-0}

# CLI flag: --cass-only
for arg in "$@"; do
  case "$arg" in
    --cass-only)
      CASS_ONLY=1
      shift || true
      ;;
  esac
done

cleanup() {
  # Tear down cass cluster if still up, but leave monitoring running
  docker compose stop cass1 cass2 cass3 cass4 cass5 >/dev/null 2>&1 || true
  docker compose rm -f cass1 cass2 cass3 cass4 cass5 >/dev/null 2>&1 || true

  # Remove any cass data
  rm -r /tmp/cass-data* || true

  # Clean up Cassandra Docker cluster and network
  docker rm -f cassA1 cassA2 cassA3 cassA4 cassA5 perf-cassandra >/dev/null 2>&1 || true
  docker network rm perf-cassandra-net >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

mkdir -p "$OUTDIR"

  # Remove any prexisting cass data
rm -r /tmp/cass-data* || true

echo "Starting cass cluster (5 nodes, rf=3)..."
docker compose up -d >/dev/null

# Give the nodes a moment to come up
sleep 10
for T in $THREADS_SET; do
  echo "Running cass perf_client with threads=$T ..."
  cargo run --example perf_client -- --node "$CASS_NODE" --ops "$OPS" --threads "$T" > "$OUTDIR/cass_t${T}.log"
done
# Capture Prometheus metrics from the prometheus node
curl -s http://localhost:9090/metrics > "$OUTDIR/cass_metrics.prom"
docker compose logs --no-color > perf-results/cass_server.log
docker compose stop cass1 cass2 cass3 cass4 cass5 >/dev/null 2>&1 || true
docker compose rm -f cass1 cass2 cass3 cass4 cass5 >/dev/null 2>&1 || true

start_cassandra_cluster() {
  echo "Starting Apache Cassandra cluster (5 nodes)..."
  docker network create perf-cassandra-net >/dev/null 2>&1 || true

  # Remove any previous nodes
  docker rm -f cassA1 cassA2 cassA3 cassA4 cassA5 >/dev/null 2>&1 || true

  # Helper to wait for UN count from seed
  wait_un_count() {
    local expected=$1
    local tries=0
    local max_tries=120  # ~10 minutes
    while true; do
      local count
      count=$(docker exec cassA1 nodetool status 2>/dev/null | awk '/^UN/ {c++} END{print c+0}') || true
      echo "UN count: ${count:-0}/${expected}"
      if [ "${count:-0}" -ge "$expected" ]; then
        break
      fi
      tries=$((tries+1))
      if [ "$tries" -ge "$max_tries" ]; then
        echo "Timed out waiting for $expected nodes to be UN" >&2
        return 1
      fi
      sleep 5
    done
  }

  # Start seed (smaller heap, fewer tokens for quicker startup)
  docker run -d --name cassA1 --hostname cassA1 --network perf-cassandra-net \
    -e CASSANDRA_CLUSTER_NAME=perf \
    -e CASSANDRA_SEEDS=cassA1 \
    -e CASSANDRA_NUM_TOKENS=16 \
    -e MAX_HEAP_SIZE=512M \
    -e HEAP_NEWSIZE=100M \
    -p 9042:9042 \
    cassandra:4.1 >/dev/null

  echo "Waiting for seed to be UN and CQL ready..."
  wait_un_count 1 || { docker logs --tail=200 cassA1 || true; return 1; }
  until docker exec cassA1 cqlsh -e 'DESCRIBE CLUSTER' >/dev/null 2>&1; do
    echo "waiting for cql on cassA1..."; sleep 5; done

  # Start non-seeds one by one and wait after each
  for n in 2 3 4 5; do
    echo "Starting cassA$n..."
    docker run -d --name cassA$n --hostname cassA$n --network perf-cassandra-net \
      -e CASSANDRA_CLUSTER_NAME=perf \
      -e CASSANDRA_SEEDS=cassA1 \
      -e CASSANDRA_NUM_TOKENS=16 \
      -e MAX_HEAP_SIZE=512M \
      -e HEAP_NEWSIZE=100M \
      cassandra:4.1 >/dev/null
    wait_un_count $n || { docker logs --tail=200 cassA$n || true; return 1; }
  done
}

if [[ "$CASS_ONLY" -eq 0 ]]; then
  start_cassandra_cluster

  # Load the user profile for analogous queries (INSERT/SELECT by primary key)
  docker cp scripts/cassandra_stress_profile.yaml cassA1:/tmp/profile.yaml

  # Ensure a clean keyspace
  docker exec cassA1 cqlsh -e "DROP KEYSPACE IF EXISTS perf_rf3;" || true

  for T in $THREADS_SET; do
    echo "Running cassandra-stress with threads=$T ..."
    # Writes with QUORUM consistency (built-in insert op)
    docker exec cassA1 /opt/cassandra/tools/bin/cassandra-stress \
      user profile=/tmp/profile.yaml 'ops(insert=1)' n=$OPS cl=QUORUM -node cassA1 -mode native cql3 \
      -rate threads=$T \
      > "$OUTDIR/cassandra_write_t${T}.log"

    # Reads with QUORUM consistency (primary key lookup)
    docker exec cassA1 /opt/cassandra/tools/bin/cassandra-stress \
      user profile=/tmp/profile.yaml 'ops(select1=1)' n=$OPS cl=QUORUM -node cassA1 -mode native cql3 \
      -rate threads=$T \
      > "$OUTDIR/cassandra_read_t${T}.log"
  done

  # Capture metrics from Cassandra
  docker exec cassA1 nodetool tpstats > "$OUTDIR/cassandra_metrics.log"
else
  echo "Skipping Cassandra phase (--cass-only); using existing cassandra_* logs in $OUTDIR if present."
fi

# Generate unified comparison plot across thread counts
PLOT_FEATURES=${PLOT_FEATURES:-plot-ttf}
if [[ -n "$PLOT_FEATURES" ]]; then
  cargo run --features "$PLOT_FEATURES" --example plot_perf -- "$OUTDIR/perf_comparison.png" "$THREADS_SET"
else
  cargo run --example plot_perf -- "$OUTDIR/perf_comparison.png" "$THREADS_SET"
fi

echo "Results stored under $OUTDIR"
