#!/usr/bin/env bash
set -euo pipefail

OPS=${OPS:-1000}
OUTDIR=${OUTDIR:-perf-results}
CASS_NODE=${CASS_NODE:-http://localhost:8080}

mkdir -p "$OUTDIR"

# Start a three-node cass cluster and Prometheus
echo "Starting cass cluster..."
docker compose up -d cass1 cass2 cass3 prometheus >/dev/null
# Give the nodes a moment to come up
sleep 10
cargo run --example perf_client -- --node "$CASS_NODE" --ops "$OPS" > "$OUTDIR/cass.log"
# Capture Prometheus metrics from the first node
docker compose exec cass1 curl -s http://localhost:9080/metrics > "$OUTDIR/cass_metrics.prom"
docker compose down >/dev/null

# Start Apache Cassandra
echo "Starting Apache Cassandra..."
docker run -d --name perf-cassandra -p 9042:9042 cassandra:4.1 >/dev/null
# Wait until Cassandra is ready
until docker exec perf-cassandra nodetool status >/dev/null 2>&1; do
  echo "waiting for cassandra..."
  sleep 5
done

docker exec perf-cassandra /opt/cassandra/tools/bin/cassandra-stress write n=$OPS -mode native cql3 > "$OUTDIR/cassandra_write.log"
docker exec perf-cassandra /opt/cassandra/tools/bin/cassandra-stress read n=$OPS -mode native cql3 > "$OUTDIR/cassandra_read.log"
docker exec perf-cassandra nodetool tpstats > "$OUTDIR/cassandra_metrics.log"
docker rm -f perf-cassandra >/dev/null

echo "Results stored under $OUTDIR"
