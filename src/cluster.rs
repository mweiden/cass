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
    schema::{TableSchema, decode_row},
    storage::StorageError,
};
use serde_json::{Value, json};
use sqlparser::ast::{Expr, ObjectName, ObjectType, SelectItem, SetExpr, Statement, TableFactor};
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
    read_consistency: usize,
    self_addr: String,
    health: Arc<RwLock<HashMap<String, Instant>>>,
    panic_until: Arc<RwLock<Option<Instant>>>,
    hints: Arc<RwLock<HashMap<String, Vec<(u64, String)>>>>,
}

struct QueryMeta {
    broadcast: bool,
    is_write: bool,
    is_count: bool,
    first_stmt: Option<Statement>,
    ns: Option<String>,
}

impl Cluster {
    /// Create a new cluster coordinator.
    pub fn new(
        db: Arc<Database>,
        self_addr: String,
        mut peers: Vec<String>,
        vnodes: usize,
        rf: usize,
        read_consistency: usize,
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
            read_consistency: read_consistency.max(1),
            self_addr,
            health,
            panic_until,
            hints: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn replicas_for(&self, key: &str) -> Vec<String> {
        let mut cursor = Cursor::new(key.as_bytes());
        let token = murmur3_32(&mut cursor, 0).unwrap_or(0);
        let mut reps = Vec::new();
        let mut seen = HashSet::new();
        for (_k, node) in self.ring.range(token..).chain(self.ring.range(..)) {
            if seen.insert(node.clone()) {
                reps.push(node.clone());
                if reps.len() == self.rf {
                    break;
                }
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
            let (ts, real_sql) = Self::parse_forwarded(sql);
            let out = engine.execute_with_ts(&self.db, real_sql, ts, true).await?;
            return Ok(output_to_proto(out));
        }

        let meta = Self::analyze_sql(&engine, sql);
        let ts = Self::timestamp_for(meta.is_write);
        let replicas = self.target_replicas(&engine, sql, meta.broadcast).await?;
        let healthy = self.healthy_nodes(replicas.clone()).await;
        let unhealthy: Vec<String> = replicas
            .iter()
            .filter(|n| !healthy.contains(n))
            .cloned()
            .collect();
        if !meta.broadcast {
            if meta.is_write {
                if healthy.is_empty() {
                    return Err(QueryError::Other("no healthy replicas".into()));
                }
                if !unhealthy.is_empty() {
                    self.store_hints(&unhealthy, sql.to_string(), ts).await;
                }
            } else if healthy.len() < self.read_consistency {
                return Err(QueryError::Other("not enough healthy replicas".into()));
            }
        }
        let results = self
            .run_on_nodes(healthy.clone(), sql.to_string(), ts)
            .await;
        if !meta.is_write && !meta.broadcast {
            self.read_repair(&meta, &results, replicas, unhealthy).await;
        }
        self.merge_results(results.into_iter().map(|(_, r)| r).collect(), meta)
    }

    fn parse_forwarded(sql: &str) -> (u64, &str) {
        if let Some(rest) = sql.strip_prefix("--ts:") {
            if let Some(pos) = rest.find('\n') {
                let ts = rest[..pos].parse().unwrap_or(0);
                return (ts, &rest[pos + 1..]);
            }
        }
        (0, sql)
    }

    fn analyze_sql(engine: &SqlEngine, sql: &str) -> QueryMeta {
        let mut meta = QueryMeta {
            broadcast: false,
            is_write: false,
            is_count: false,
            first_stmt: None,
            ns: None,
        };
        if let Ok(stmts) = engine.parse(sql) {
            if let Some(st) = stmts.first() {
                meta.first_stmt = Some(st.clone());
                meta.ns = match st {
                    Statement::Insert(insert) => {
                        if let sqlparser::ast::TableObject::TableName(name) = &insert.table {
                            Self::object_name_to_ns(name)
                        } else {
                            None
                        }
                    }
                    Statement::Update { table, .. } => Self::table_factor_to_ns(&table.relation),
                    Statement::Delete(delete) => {
                        use sqlparser::ast::FromTable;
                        let table = match &delete.from {
                            FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
                        };
                        if table.len() == 1 {
                            Self::table_factor_to_ns(&table[0].relation)
                        } else {
                            None
                        }
                    }
                    Statement::Query(q) => {
                        if let SetExpr::Select(s) = &*q.body {
                            if !s.from.is_empty() {
                                Self::table_factor_to_ns(&s.from[0].relation)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
            }
            if let Some(Statement::Query(q)) = meta.first_stmt.as_ref() {
                if let SetExpr::Select(s) = &*q.body {
                    if s.projection.len() == 1 {
                        if let SelectItem::UnnamedExpr(Expr::Function(func)) = &s.projection[0] {
                            if func.name.to_string().eq_ignore_ascii_case("count") {
                                meta.is_count = true;
                            }
                        }
                    }
                }
            }
            meta.broadcast = stmts.iter().all(|s| {
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
            meta.is_write = stmts.iter().any(|s| {
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
        meta
    }

    fn timestamp_for(write: bool) -> u64 {
        if write {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|_| Duration::from_secs(0))
                .as_micros() as u64
        } else {
            0
        }
    }

    async fn target_replicas(
        &self,
        engine: &SqlEngine,
        sql: &str,
        broadcast: bool,
    ) -> Result<Vec<String>, QueryError> {
        if broadcast {
            return Ok(self.ring.values().cloned().collect());
        }
        let keys = engine.partition_keys(&self.db, sql).await?;
        let mut replicas: HashSet<String> = HashSet::new();
        for key in keys {
            for node in self.replicas_for(&key) {
                replicas.insert(node);
            }
        }
        if replicas.is_empty() {
            replicas.insert(self.self_addr.clone());
        }
        Ok(replicas.into_iter().collect())
    }

    async fn healthy_nodes(&self, replicas: Vec<String>) -> Vec<String> {
        let mut healthy = Vec::new();
        for node in replicas {
            if self.is_alive(&node).await {
                self.apply_hints(&node).await;
                healthy.push(node);
            }
        }
        healthy
    }

    async fn run_on_nodes(
        &self,
        nodes: Vec<String>,
        sql: String,
        ts: u64,
    ) -> Vec<(String, Result<QueryOutput, QueryError>)> {
        let tasks: Vec<_> = nodes
            .into_iter()
            .map(|node| {
                let db = self.db.clone();
                let self_addr = self.self_addr.clone();
                let sql_clone = sql.clone();
                async move {
                    let res = if node == self_addr {
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
                    };
                    (node, res)
                }
            })
            .collect();
        join_all(tasks).await
    }

    async fn store_hints(&self, nodes: &[String], sql: String, ts: u64) {
        if nodes.is_empty() {
            return;
        }
        let mut map = self.hints.write().await;
        for n in nodes {
            map.entry(n.clone()).or_default().push((ts, sql.clone()));
        }
    }

    async fn apply_hints(&self, node: &str) {
        let hints = {
            let mut map = self.hints.write().await;
            map.remove(node)
        };
        if let Some(hints) = hints {
            for (ts, sql) in hints {
                let res = self
                    .run_on_nodes(vec![node.to_string()], sql.clone(), ts)
                    .await;
                if res.first().map(|(_, r)| r.is_err()).unwrap_or(true) {
                    let mut map = self.hints.write().await;
                    map.entry(node.to_string()).or_default().push((ts, sql));
                    break;
                }
            }
        }
    }

    async fn read_repair(
        &self,
        meta: &QueryMeta,
        results: &[(String, Result<QueryOutput, QueryError>)],
        replicas: Vec<String>,
        unhealthy: Vec<String>,
    ) {
        let ns = match &meta.ns {
            Some(ns) => ns.clone(),
            None => return,
        };
        let Some(schema) = Self::get_schema(&self.db, &ns).await else {
            return;
        };
        let mut latest: BTreeMap<String, (u64, String)> = BTreeMap::new();
        for (_node, res) in results.iter() {
            if let Ok(QueryOutput::Meta(rows)) = res {
                for (k, ts, v) in rows {
                    match latest.get(k) {
                        Some((cur, _)) if *cur >= *ts => {}
                        _ => {
                            latest.insert(k.clone(), (*ts, v.clone()));
                        }
                    }
                }
            }
        }
        let healthy: Vec<String> = replicas
            .into_iter()
            .filter(|n| !unhealthy.contains(n))
            .collect();
        for (key, (ts, val)) in latest {
            let mut row_map = decode_row(val.as_bytes());
            for (col, part) in schema.key_columns().iter().zip(key.split('|')) {
                row_map.insert(col.clone(), part.to_string());
            }
            let cols = schema.columns.clone();
            let vals = cols
                .iter()
                .map(|c| format!("'{}'", row_map.get(c).cloned().unwrap_or_default()))
                .collect::<Vec<_>>()
                .join(", ");
            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES ({});",
                ns,
                cols.join(", "),
                vals
            );
            if !unhealthy.is_empty() {
                self.store_hints(&unhealthy, insert_sql.clone(), ts).await;
            }
            let _ = self.run_on_nodes(healthy.clone(), insert_sql, ts).await;
        }
    }

    async fn get_schema(db: &Database, table: &str) -> Option<TableSchema> {
        db.get_ns("_schemas", table).await.and_then(|v| {
            let (_, data) = Self::split_ts(&v);
            serde_json::from_slice(data).ok()
        })
    }

    fn split_ts(bytes: &[u8]) -> (u64, &[u8]) {
        if bytes.len() < 8 {
            return (0, bytes);
        }
        let mut ts_bytes = [0u8; 8];
        ts_bytes.copy_from_slice(&bytes[..8]);
        (u64::from_be_bytes(ts_bytes), &bytes[8..])
    }

    fn object_name_to_ns(name: &ObjectName) -> Option<String> {
        name.0
            .last()
            .and_then(|p| p.as_ident())
            .map(|i| i.value.to_lowercase())
    }

    fn table_factor_to_ns(tf: &TableFactor) -> Option<String> {
        match tf {
            TableFactor::Table { name, .. } => Self::object_name_to_ns(name),
            _ => None,
        }
    }

    fn merge_results(
        &self,
        results: Vec<Result<QueryOutput, QueryError>>,
        meta: QueryMeta,
    ) -> Result<QueryResponse, QueryError> {
        let mut rows: BTreeMap<String, (u64, String)> = BTreeMap::new();
        let mut table_set: BTreeSet<String> = BTreeSet::new();
        let mut arr_rows: Vec<BTreeMap<String, String>> = Vec::new();
        let mut last_err: Option<QueryError> = None;
        let mut row_count: u64 = 0;
        let mut count_val: Option<u64> = None;

        for resp in results {
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
                Ok(QueryOutput::Rows(r)) => {
                    if meta.is_count {
                        if count_val.is_none() {
                            if let Some(c) = r.get(0).and_then(|m| m.get("count")) {
                                count_val = c.parse::<u64>().ok();
                            }
                        }
                    } else {
                        arr_rows.extend(r);
                    }
                }
                Ok(QueryOutput::None) => {}
                Err(e) => last_err = Some(e),
            }
        }

        if meta.is_write {
            let count = match meta.first_stmt {
                Some(Statement::CreateTable(_))
                | Some(Statement::Drop {
                    object_type: ObjectType::Table,
                    ..
                }) => 1,
                _ => row_count as usize,
            };
            let (op, unit) = match meta.first_stmt {
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
            return Ok(output_to_proto(QueryOutput::Mutation {
                op: op.to_string(),
                unit: unit.to_string(),
                count,
            }));
        }

        if matches!(meta.first_stmt, Some(Statement::ShowTables { .. })) {
            let tables: Vec<String> = table_set.into_iter().collect();
            return Ok(output_to_proto(QueryOutput::Tables(tables)));
        }

        if meta.is_count {
            let total = count_val.unwrap_or(0);
            let mut row = BTreeMap::new();
            row.insert("count".to_string(), total.to_string());
            return Ok(output_to_proto(QueryOutput::Rows(vec![row])));
        }

        if !rows.is_empty() || !arr_rows.is_empty() {
            for (_k, (_ts, val)) in rows {
                if !val.is_empty() {
                    let map = decode_row(val.as_bytes());
                    arr_rows.push(map);
                }
            }
            return Ok(output_to_proto(QueryOutput::Rows(arr_rows)));
        }

        if let Some(err) = last_err {
            Err(err)
        } else {
            Ok(output_to_proto(QueryOutput::Rows(Vec::new())))
        }
    }
}
