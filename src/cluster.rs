use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    io::Cursor,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::rpc::{
    FlushRequest, HealthRequest, MetaResult, MetaRow, MutationResult, QueryRequest, QueryResponse,
    ResultSet, Row as RpcRow, ShowTablesResult, cass_client::CassClient, query_response,
};
use futures::future::join_all;
use murmur3::murmur3_32;
use tonic::Request;

use crate::{
    Database, SqlEngine,
    query::{QueryError, QueryOutput},
    schema::decode_row,
    storage::StorageError,
};
use serde_json::{Value, json};
use sqlparser::ast::{ObjectType, Statement};
use sysinfo::Disks;
use tokio::{sync::RwLock, time::sleep};

fn output_to_proto(out: QueryOutput) -> QueryResponse {
    match out {
        QueryOutput::Mutation { op, unit, count } => QueryResponse {
            payload: Some(query_response::Payload::Mutation(MutationResult {
                op: op.to_string(),
                unit: unit.to_string(),
                count: count as u64,
            })),
        },
        QueryOutput::Rows(rows) => {
            let rpc_rows: Vec<RpcRow> = rows
                .into_iter()
                .map(|cols| RpcRow {
                    columns: cols.into_iter().collect(),
                })
                .collect();
            QueryResponse {
                payload: Some(query_response::Payload::Rows(ResultSet { rows: rpc_rows })),
            }
        }
        QueryOutput::Meta(rows) => {
            let rpc_rows: Vec<MetaRow> = rows
                .into_iter()
                .map(|(key, ts, value)| MetaRow { key, ts, value })
                .collect();
            QueryResponse {
                payload: Some(query_response::Payload::Meta(MetaResult { rows: rpc_rows })),
            }
        }
        QueryOutput::Tables(tables) => QueryResponse {
            payload: Some(query_response::Payload::Tables(ShowTablesResult { tables })),
        },
        QueryOutput::None => QueryResponse { payload: None },
    }
}

fn proto_to_output(resp: QueryResponse) -> QueryOutput {
    match resp.payload {
        Some(query_response::Payload::Mutation(m)) => QueryOutput::Mutation {
            op: m.op,
            unit: m.unit,
            count: m.count as usize,
        },
        Some(query_response::Payload::Rows(rs)) => QueryOutput::Rows(
            rs.rows
                .into_iter()
                .map(|r| r.columns.into_iter().collect())
                .collect(),
        ),
        Some(query_response::Payload::Meta(m)) => {
            QueryOutput::Meta(m.rows.into_iter().map(|r| (r.key, r.ts, r.value)).collect())
        }
        Some(query_response::Payload::Tables(t)) => QueryOutput::Tables(t.tables),
        None => QueryOutput::None,
    }
}

/// Simple cluster management and request coordination.
///
/// This implementation provides a very lightweight rendition of a
/// peer-to-peer ring with configurable replication.  Nodes are
/// identified by their base HTTP address (e.g. `http://127.0.0.1:8080`).
/// Each node owns a number of virtual nodes on the ring in order to
/// balance load.  Requests are replicated to the selected peers based on
/// a Murmur3 hash of the incoming statement.
pub struct Cluster {
    db: Arc<Database>,
    ring: BTreeMap<u32, String>,
    rf: usize,
    self_addr: String,
    health: Arc<RwLock<HashMap<String, Instant>>>,
    panic_until: Arc<RwLock<Option<Instant>>>,
}

impl Cluster {
    /// Create a new cluster coordinator.
    pub fn new(
        db: Arc<Database>,
        self_addr: String,
        mut peers: Vec<String>,
        vnodes: usize,
        rf: usize,
    ) -> Self {
        peers.push(self_addr.clone());
        let mut ring = BTreeMap::new();
        for node in peers.iter() {
            for v in 0..vnodes.max(1) {
                let token_key = format!("{}-{}", node, v);
                let mut cursor = Cursor::new(token_key.as_bytes());
                let token = murmur3_32(&mut cursor, 0).unwrap_or(0);
                ring.insert(token, node.clone());
            }
        }

        let mut initial = HashMap::new();
        for p in peers.iter() {
            if p != &self_addr {
                initial.insert(p.clone(), Instant::now());
            }
        }
        let health = Arc::new(RwLock::new(initial));
        let panic_until = Arc::new(RwLock::new(None));
        let gossip_peers = peers.clone();
        let gossip_addr = self_addr.clone();
        let gossip_health = health.clone();
        tokio::spawn(async move {
            let mut idx = 0usize;
            loop {
                if gossip_peers.is_empty() {
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                idx = (idx + 1) % gossip_peers.len();
                let peer = &gossip_peers[idx];
                if peer != &gossip_addr {
                    let ok = if let Ok(mut client) = CassClient::connect(peer.clone()).await {
                        client.health(Request::new(HealthRequest {})).await.is_ok()
                    } else {
                        false
                    };
                    let mut map = gossip_health.write().await;
                    if ok {
                        map.insert(peer.clone(), Instant::now());
                    } else {
                        map.insert(peer.clone(), Instant::now() - Duration::from_secs(9));
                    }
                }
                sleep(Duration::from_secs(1)).await;
            }
        });

        Self {
            db,
            ring,
            rf: rf.max(1),
            self_addr,
            health,
            panic_until,
        }
    }

    pub fn replicas_for(&self, key: &str) -> Vec<String> {
        let mut cursor = Cursor::new(key.as_bytes());
        let token = murmur3_32(&mut cursor, 0).unwrap_or(0);
        let mut reps = Vec::new();
        for (_k, node) in self.ring.range(token..) {
            if !reps.contains(node) {
                reps.push(node.clone());
            }
            if reps.len() == self.rf {
                return reps;
            }
        }
        for (_k, node) in &self.ring {
            if !reps.contains(node) {
                reps.push(node.clone());
            }
            if reps.len() == self.rf {
                break;
            }
        }
        reps
    }

    pub async fn is_alive(&self, node: &str) -> bool {
        if node == self.self_addr {
            return self.self_healthy().await;
        }
        let map = self.health.read().await;
        map.get(node)
            .map(|t| t.elapsed() < Duration::from_secs(8))
            .unwrap_or(false)
    }

    pub async fn peer_health(&self) -> Vec<(String, bool)> {
        let map = self.health.read().await;
        map.iter()
            .map(|(peer, t)| (peer.clone(), t.elapsed() < Duration::from_secs(8)))
            .collect()
    }

    pub fn health_info(&self) -> Value {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let tokens: Vec<u32> = self
            .ring
            .iter()
            .filter_map(|(tok, node)| (node == &self.self_addr).then_some(*tok))
            .collect();
        json!({
            "node": self.self_addr,
            "timestamp": now,
            "tokens": tokens,
        })
    }

    /// Artificially mark this node unhealthy for the provided duration.
    pub async fn panic_for(&self, dur: Duration) {
        let mut until = self.panic_until.write().await;
        *until = Some(Instant::now() + dur);
    }

    /// Return whether this node is currently healthy.
    ///
    /// A node is considered unhealthy if it is within a panic window
    /// or if its local storage has less than 5% free space remaining.
    pub async fn self_healthy(&self) -> bool {
        if let Some(until) = *self.panic_until.read().await {
            if Instant::now() < until {
                return false;
            }
        }

        if let Some(path) = self.db.storage().local_path() {
            let mut disks = Disks::new_with_refreshed_list();
            disks.refresh();
            if let Some(disk) = disks
                .list()
                .iter()
                .find(|d| path.starts_with(d.mount_point()))
            {
                let total = disk.total_space() as f64;
                let avail = disk.available_space() as f64;
                if total > 0.0 && avail / total < 0.05 {
                    return false;
                }
            }
        }
        true
    }

    /// Return the address of this node.
    pub fn self_addr(&self) -> &str {
        &self.self_addr
    }

    /// Flush the local memtable to disk.
    pub async fn flush_self(&self) -> Result<(), StorageError> {
        self.db.flush().await
    }

    /// Flush memtables on all nodes in the cluster.
    pub async fn flush_all(&self) -> Result<(), String> {
        let nodes: HashSet<String> = self.ring.values().cloned().collect();
        let tasks: Vec<_> = nodes
            .into_iter()
            .map(|node| {
                let self_addr = self.self_addr.clone();
                let db = self.db.clone();
                async move {
                    if node == self_addr {
                        db.flush().await.map_err(|e| e.to_string())
                    } else {
                        match CassClient::connect(node.clone()).await {
                            Ok(mut client) => client
                                .flush_internal(Request::new(FlushRequest {}))
                                .await
                                .map(|_| ())
                                .map_err(|e| e.to_string()),
                            Err(e) => Err(e.to_string()),
                        }
                    }
                }
            })
            .collect();
        let mut last_err = None;
        for res in join_all(tasks).await {
            if let Err(e) = res {
                last_err = Some(e);
            }
        }
        if let Some(e) = last_err {
            Err(e)
        } else {
            Ok(())
        }
    }

    /// Execute `sql` against the appropriate replicas.
    ///
    /// When `forwarded` is false the current node acts as the coordinator
    /// and forwards the statement to the replica nodes determined by the
    /// partition key.  Results from all replicas are unioned together and
    /// returned to the caller.  When `forwarded` is true the query is being
    /// handled on behalf of a peer and is executed locally without further
    /// replication.
    pub async fn execute(&self, sql: &str, forwarded: bool) -> Result<QueryResponse, QueryError> {
        let engine = SqlEngine::new();
        if forwarded {
            let (ts, real_sql) = if let Some(rest) = sql.strip_prefix("--ts:") {
                if let Some(pos) = rest.find('\n') {
                    let ts = rest[..pos].parse().unwrap_or(0);
                    (ts, &rest[pos + 1..])
                } else {
                    (0, sql)
                }
            } else {
                (0, sql)
            };
            let out = engine.execute_with_ts(&self.db, real_sql, ts, true).await?;
            return Ok(output_to_proto(out));
        }

        // Determine if the statement is a schema mutation that should be
        // broadcast to every node and whether it is a write.
        let mut broadcast = false;
        let mut is_write = false;
        let mut first_stmt: Option<Statement> = None;
        if let Ok(stmts) = engine.parse(sql) {
            if let Some(st) = stmts.first() {
                first_stmt = Some(st.clone());
            }
            broadcast = stmts.iter().all(|s| {
                matches!(
                    s,
                    Statement::CreateTable(_)
                        | Statement::Drop {
                            object_type: ObjectType::Table,
                            ..
                        }
                        | Statement::ShowTables { .. }
                )
            });
            is_write = stmts.iter().any(|s| {
                matches!(
                    s,
                    Statement::Insert(_)
                        | Statement::Update { .. }
                        | Statement::Delete(_)
                        | Statement::CreateTable(_)
                        | Statement::Drop {
                            object_type: ObjectType::Table,
                            ..
                        }
                )
            });
        }

        let ts = if is_write {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|_| std::time::Duration::from_secs(0))
                .as_micros() as u64
        } else {
            0
        };

        // Work out the target replica set.
        let mut replicas: HashSet<String> = HashSet::new();
        if broadcast {
            replicas.extend(self.ring.values().cloned());
        } else {
            let keys = match engine.partition_keys(&self.db, sql).await {
                Ok(k) => k,
                Err(e) => return Err(e),
            };
            for key in keys {
                for node in self.replicas_for(&key) {
                    replicas.insert(node);
                }
            }
            if replicas.is_empty() {
                replicas.insert(self.self_addr.clone());
            }
        }

        let mut replicas: Vec<String> = replicas.into_iter().collect();
        let mut healthy: Vec<String> = Vec::new();
        for node in replicas.drain(..) {
            if self.is_alive(&node).await {
                healthy.push(node);
            }
        }
        if !broadcast && healthy.len() < self.rf {
            return Err(QueryError::Other("not enough healthy replicas".into()));
        }

        // Collect results from all replicas, performing last-write-wins per key.
        let mut rows: BTreeMap<String, (u64, String)> = BTreeMap::new();
        let mut table_set: BTreeSet<String> = BTreeSet::new();
        let mut arr_rows: Vec<BTreeMap<String, String>> = Vec::new();
        let mut last_err: Option<QueryError> = None;
        let mut row_count: u64 = 0;

        let sql_string = sql.to_string();
        let tasks: Vec<_> = healthy
            .into_iter()
            .map(|node| {
                let db = self.db.clone();
                let self_addr = self.self_addr.clone();
                let sql_clone = sql_string.clone();
                async move {
                    if node == self_addr {
                        let engine = SqlEngine::new();
                        engine.execute_with_ts(&db, &sql_clone, ts, true).await
                    } else {
                        let payload = if ts > 0 {
                            format!("--ts:{}\n{}", ts, sql_clone.clone())
                        } else {
                            sql_clone.clone()
                        };
                        match CassClient::connect(node.clone()).await {
                            Ok(mut client) => client
                                .internal(Request::new(QueryRequest { sql: payload }))
                                .await
                                .map(|resp| proto_to_output(resp.into_inner()))
                                .map_err(|e| QueryError::Other(e.to_string())),
                            Err(e) => Err(QueryError::Other(e.to_string())),
                        }
                    }
                }
            })
            .collect();

        for resp in join_all(tasks).await {
            match resp {
                Ok(QueryOutput::Meta(meta_rows)) => {
                    for (key, ts, val) in meta_rows {
                        match rows.get(&key) {
                            Some((cur_ts, _)) if *cur_ts >= ts => {}
                            _ => {
                                rows.insert(key, (ts, val));
                            }
                        }
                    }
                }
                Ok(QueryOutput::Mutation { count, .. }) => {
                    row_count = row_count.max(count as u64);
                }
                Ok(QueryOutput::Tables(tables)) => {
                    for t in tables {
                        table_set.insert(t);
                    }
                }
                Ok(QueryOutput::Rows(r)) => arr_rows.extend(r),
                Ok(QueryOutput::None) => {}
                Err(e) => last_err = Some(e),
            }
        }

        if is_write {
            let count = match first_stmt {
                Some(Statement::CreateTable(_))
                | Some(Statement::Drop {
                    object_type: ObjectType::Table,
                    ..
                }) => 1,
                _ => row_count as usize,
            };
            let (op, unit) = match first_stmt {
                Some(Statement::Insert(_)) => ("INSERT", "row"),
                Some(Statement::Update { .. }) => ("UPDATE", "row"),
                Some(Statement::Delete(_)) => ("DELETE", "row"),
                Some(Statement::CreateTable(_)) => ("CREATE TABLE", "table"),
                Some(Statement::Drop {
                    object_type: ObjectType::Table,
                    ..
                }) => ("DROP TABLE", "table"),
                _ => ("UNKNOWN", ""),
            };
            Ok(output_to_proto(QueryOutput::Mutation {
                op: op.to_string(),
                unit: unit.to_string(),
                count,
            }))
        } else if let Some(Statement::ShowTables { .. }) = first_stmt {
            let tables: Vec<String> = table_set.into_iter().collect();
            Ok(output_to_proto(QueryOutput::Tables(tables)))
        } else if !rows.is_empty() || !arr_rows.is_empty() {
            for (_k, (_ts, val)) in rows {
                if !val.is_empty() {
                    let map = decode_row(val.as_bytes());
                    arr_rows.push(map);
                }
            }
            Ok(output_to_proto(QueryOutput::Rows(arr_rows)))
        } else if let Some(err) = last_err {
            Err(err)
        } else {
            Ok(output_to_proto(QueryOutput::Rows(Vec::new())))
        }
    }
}
