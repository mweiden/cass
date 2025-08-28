# Cass
[![Test Status](https://github.com/mweiden/cass/actions/workflows/ci.yml/badge.svg)](https://github.com/mweiden/cass/actions/workflows/ci.yml) [![codecov](https://codecov.io/gh/mweiden/cass/branch/main/graph/badge.svg)](https://codecov.io/gh/mweiden/cass)

Toy/experimental clone of [Apache Cassandra](https://en.wikipedia.org/wiki/Apache_Cassandra) written in Rust, mostly using [OpenAI Codex](https://chatgpt.com/codex).

## Features

- **gRPC API and CLI** for submitting SQL queries
- **Data Structure:** Stores data in a [log-structured merge tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree)
- **Storage:** Column-oriented SSTable placeholders with bloom filters and zone maps to speed up queries; persist to local or S3 AWS backends
- **Durability / Recovery:** Sharded write-ahead logs for durability and in-memory tables for parallel ingestion
- **Deployment:** Dockerfile and docker-compose for containerized deployment and local testing
- **Scalability:** Horizontally scalable
- **Gossip:** Cluster membership and liveness detection via gossip with health checks

## Design tradeoffs

Like Cassandra itself, `cass` is an [AP system](https://en.wikipedia.org/wiki/CAP_theorem):

- **Consistency:** Consistency is relaxed, last-write-wins conflict resolution
- **Availability:** always writable, tunably consistent, fault-tolerant through replication
- **Partition tolerance:** will continue to work even if parts of the cluster cannot communicate

## Query Syntax

The built-in SQL engine understands a small subset of SQL:

- `INSERT` of a `key`/`value` pair into a table
- `UPDATE` and `DELETE` statements targeting a single key
- `SELECT` with optional `WHERE` filters, `ORDER BY`, `GROUP BY`,
  `DISTINCT`, simple aggregate functions (`COUNT`, `MIN`, `MAX`, `SUM`)
  and `LIMIT`
- Table management statements such as `CREATE TABLE`, `DROP TABLE` and
  `SHOW TABLES`

Note on creating [partition and clustering keys](https://cassandra.apache.org/doc/4.0/cassandra/data_modeling/intro.html#partitions):
the first column in `PRIMARY KEY(...)` will be the partition key, subsequent columns will be indexed as clustering keys.

So for the example `id` will be the partition key and `c` will be a clustering key:

```
CREATE TABLE t (
   id int,
   c text,
   k int,
   v text,
   PRIMARY KEY (id,c)
);
```

## Development

```bash
cargo test            # run unit tests
cargo run -- server  # start the gRPC server on port 8080
```

### Contributing

Before submitting changes, ensure the code is formatted and tests pass:

```bash
cargo fmt
cargo test
```

The project uses idiomatic Rust patterns with small, focused functions. See the
module-level comments in `src/` for a high-level overview of the architecture.

## Storage Backends

The server supports both local filesystem storage and Amazon S3.

### Local

Local storage is the default. Specify a directory with `--data-dir`:

```bash
cargo run -- --data-dir ./data
```

### S3

To use S3, set AWS credentials in the environment and provide the bucket
name:

```bash
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  cargo run -- --storage s3 --bucket my-bucket
```

`AWS_REGION` controls the region (default `us-east-1`).


## Example / Docker Compose Cluster

With the server running you can insert and query data using gRPC. The provided `docker-compose.yml` starts a five-node cluster using local
storage with a replication factor of three where you can try this out.

Start the cluster:

```bash
docker compose up
```
Connect using the built-in REPL and run some queries:

```bash
$ cass repl http://localhost:8080

> CREATE TABLE orders (customer_id TEXT, order_id TEXT, order_date TEXT, PRIMARY KEY(customer_id, order_id))
CREATE TABLE 1 table
> INSERT INTO orders VALUES ('nike', 'abc123', '2025-08-25')
INSERT 1 row
> INSERT INTO orders VALUES ('nike', 'def456', '2025-08-26')
INSERT 1 row
> SELECT * FROM orders WHERE customer_id = 'nike'
  customer_id order_date  order_id
0 nike        2025-08-25  abc123
1 nike        2025-08-26  def456
(2 rows)
> SELECT COUNT(1) FROM orders WHERE customer_id = 'nike'
 count
0 2
(1 rows)
> SELECT * FROM orders WHERE customer_id = 'nike' AND order_id = 'abc123'
  customer_id order_date  order_id
0 nike        2025-08-25  abc123
(1 rows)
```

## Maintenance Commands

The CLI exposes helper commands useful during testing:

- `cass flush <node>` instructs the specified node to broadcast a flush to all
  peers.
- `cass panic <node>` forces the target node to report itself as unhealthy for
  60 seconds.

## Monitoring

Each node exposes Prometheus metrics on the gRPC port plus 1000 at
`/metrics` (for example, if the server listens on `8080`, metrics are
available on `9080`). The provided `docker-compose.yml` also starts
Prometheus and Grafana. After running

```bash
docker compose up
```

visit <http://localhost:3000> and sign in with the default
`admin`/`admin` credentials. The Grafana instance is preconfigured with the
Prometheus data source so you can explore metrics such as gRPC request
counts, peer health, RAM and CPU usage, and SSTable disk usage.

There is also a preconfigured dashboard with basic metrics from all instances. Screenshot below:

<img width="1257" height="821" alt="Screenshot 2025-08-17 at 11 48 28 PM" src="https://github.com/user-attachments/assets/cbaf71aa-c726-4c6a-a1eb-422060aecd0a" />

## Performance Benchmarking

The repository includes a simple harness for comparing write and read throughput of `cass` against a reference Apache Cassandra node.

```bash
scripts/perf_compare.sh         # runs both databases and stores metrics in ./perf-results
```

The script uses the example program `perf_client` to drive load against `cass` and `cassandra-stress` for the Apache Cassandra container. Prometheus metrics from `cass` and `nodetool` statistics from Cassandra are written to the `perf-results` directory for analysis.
