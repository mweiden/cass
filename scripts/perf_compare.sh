#!/usr/bin/env bash
set -euo pipefail

OPS=${OPS:-1000}
OUTDIR=${OUTDIR:-perf-results}
CASS_NODE=${CASS_NODE:-http://localhost:8080}

mkdir -p "$OUTDIR"

# Start cass (single node)
echo "Starting cass..."
docker compose up -d cass1 >/dev/null
# Give the node a moment to come up
sleep 5
cargo run --example perf_client -- --node "$CASS_NODE" --ops "$OPS" --metrics-endpoint http://localhost:9080/metrics --metrics-out "$OUTDIR/cass_metrics.prom" > "$OUTDIR/cass.log"
docker compose down >/dev/null

# Start Apache Cassandra
echo "Starting Apache Cassandra..."
docker run -d --name perf-cassandra -p 9042:9042 cassandra:4.1 >/dev/null
# Wait until Cassandra is ready
until docker exec perf-cassandra nodetool status >/dev/null 2>&1; do
  echo "waiting for cassandra..."
  sleep 5
done

docker exec perf-cassandra cassandra-stress write n=$OPS -mode native cql3 > "$OUTDIR/cassandra_write.log"
docker exec perf-cassandra cassandra-stress read n=$OPS -mode native cql3 > "$OUTDIR/cassandra_read.log"
docker exec perf-cassandra nodetool tpstats > "$OUTDIR/cassandra_metrics.log"
docker rm -f perf-cassandra >/dev/null

echo "Results stored under $OUTDIR"
