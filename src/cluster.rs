use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    convert::TryInto,
    io::Cursor,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::rpc::{
    FlushRequest, HealthRequest, LwtCommitRequest, LwtPrepareRequest, LwtProposeRequest,
    LwtReadRequest, MetaResult, MetaRow, MutationResult, QueryRequest, QueryResponse, ResultSet,
    Row as RpcRow, ShowTablesResult, cass_client::CassClient, query_response,
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
use sqlparser::ast::{
    AssignmentTarget, Expr, ObjectName, ObjectType, SelectItem,
    SetExpr, Statement, TableFactor,
};
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
    read_cl: ConsistencyLevel,
    self_addr: String,
    health: Arc<RwLock<HashMap<String, Instant>>>,
    panic_until: Arc<RwLock<Option<Instant>>>,
    hints: Arc<RwLock<HashMap<String, Vec<(u64, String)>>>>,
    lwt: Arc<RwLock<HashMap<String, PaxosSlot>>>,
}

struct QueryMeta {
    broadcast: bool,
    is_write: bool,
    is_count: bool,
    first_stmt: Option<Statement>,
    ns: Option<String>,
    is_lwt: bool,
}

struct PaxosSlot {
    promised: u64,
    accepted_ballot: u64,
    accepted_value: Vec<u8>,
}

/// Server-level consistency options similar to Cassandra.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ConsistencyLevel {
    One,
    Quorum,
    All,
}

impl ConsistencyLevel {
    fn required(self, rf: usize) -> usize {
        match self {
            Self::One => 1,
            Self::Quorum => rf / 2 + 1,
            Self::All => rf.max(1),
        }
    }
}

impl Cluster {
    /// Create a new cluster coordinator.
    pub fn new(
        db: Arc<Database>,
        self_addr: String,
        peers: Vec<String>,
        vnodes: usize,
        rf: usize,
        read_consistency: usize,
    ) -> Self {
        Self::new_with_consistency(db, self_addr, peers, vnodes, rf, read_consistency)
    }

    /// Create a cluster with explicit LWT consistency.
    pub fn new_with_consistency(
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

        let read_cl = if read_consistency <= 1 {
            ConsistencyLevel::One
        } else if read_consistency >= rf.max(1) {
            ConsistencyLevel::All
        } else {
            ConsistencyLevel::Quorum
        };

        Self {
            db,
            ring,
            rf: rf.max(1),
            read_cl,
            self_addr,
            health,
            panic_until,
            hints: Arc::new(RwLock::new(HashMap::new())),
            lwt: Arc::new(RwLock::new(HashMap::new())),
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
        if meta.is_lwt {
            // Execute Cassandra-style LWT via Paxos across replicas
            return self.execute_lwt(&engine, sql).await;
        }
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
            } else if healthy.len() < self.read_cl.required(self.rf) {
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
            is_lwt: false,
        };
        let parsed_sql = engine.base_sql(sql);
        if let Ok(stmts) = engine.parse(&parsed_sql) {
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
        // Heuristic: presence of trailing " IF ..." indicates an LWT on
        // supported statements (INSERT/UPDATE). Base SQL strips it.
        let has_if = engine.find_trailing_if_index(sql).is_some();
        if has_if {
            if let Some(st) = &meta.first_stmt {
                meta.is_lwt = matches!(st, Statement::Insert(_) | Statement::Update { .. });
            }
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

    /// Execute a lightweight transaction (INSERT ... IF NOT EXISTS or UPDATE ... IF col=val)
    /// across replicas using a Paxos-style protocol.
    async fn execute_lwt(
        &self,
        engine: &SqlEngine,
        sql: &str,
    ) -> Result<QueryResponse, QueryError> {
        // Determine replicas for the partition
        let replicas = self.target_replicas(engine, sql, false).await?;
        let healthy = self.healthy_nodes(replicas.clone()).await;
        let rf = self.rf.max(1);
        let required = ConsistencyLevel::Quorum.required(rf);
        if healthy.len() < required {
            return Err(QueryError::Other("not enough healthy replicas".into()));
        }

        // Parse statement and condition
        let base = engine.base_sql(sql);
        let mut stmts = engine.parse(&base)?;
        if stmts.len() != 1 {
            return Err(QueryError::Unsupported);
        }
        let stmt = stmts.pop().unwrap();

        // Extract conditional part
        let mut lwt_not_exists = false;
        let mut lwt_equals: BTreeMap<String, String> = BTreeMap::new();
        if let Some(idx) = engine.find_trailing_if_index(sql) {
            let cond_str = sql[idx + 2..].trim_start();
            if cond_str.eq_ignore_ascii_case("not exists") {
                lwt_not_exists = true;
            } else {
                // Parse simple equality conjunctions via a synthetic WHERE clause
                let dialect = sqlparser::dialect::GenericDialect {};
                let cond_sql = format!("SELECT * FROM tmp WHERE {}", cond_str);
                if let Ok(mut s) = sqlparser::parser::Parser::parse_sql(&dialect, &cond_sql) {
                    if let Some(Statement::Query(q)) = s.pop() {
                        if let SetExpr::Select(select) = *q.body {
                            if let Some(expr) = select.selection {
                                lwt_equals = Self::where_to_map(&expr);
                            }
                        }
                    }
                }
            }
        }

        // Compute namespace, key, and the row mutation intent (for INSERT or UPDATE)
        let (ns, key, assignments, insert_row_values) = match &stmt {
            Statement::Insert(insert) => {
                let ns = match &insert.table {
                    sqlparser::ast::TableObject::TableName(name) => {
                        Self::object_name_to_ns(name).ok_or(QueryError::Unsupported)?
                    }
                    _ => return Err(QueryError::Unsupported),
                };
                let schema = Self::get_schema(&self.db, &ns)
                    .await
                    .ok_or(QueryError::Unsupported)?;
                let source = insert.source.clone().ok_or(QueryError::Unsupported)?;
                let values = match *source.body {
                    SetExpr::Values(v) => v,
                    _ => return Err(QueryError::Unsupported),
                };
                if values.rows.len() != 1 {
                    return Err(QueryError::Unsupported);
                }
                let cols: Vec<String> = if !insert.columns.is_empty() {
                    insert
                        .columns
                        .iter()
                        .map(|c| c.value.to_lowercase())
                        .collect()
                } else {
                    schema.columns.clone()
                };
                let row_exprs = &values.rows[0];
                if cols.len() != row_exprs.len() {
                    return Err(QueryError::Unsupported);
                }
                let mut row_map: BTreeMap<String, String> = BTreeMap::new();
                for (c, e) in cols.iter().zip(row_exprs.iter()) {
                    if let Some(v) = Self::expr_to_string(e) {
                        row_map.insert(c.clone(), v);
                    }
                }
                // Build full primary key string
                let key = Self::build_key_from_map(&schema, &row_map)?;
                (ns, key, None, Some((schema, row_map)))
            }
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                let ns =
                    Self::table_factor_to_ns(&table.relation).ok_or(QueryError::Unsupported)?;
                let schema = Self::get_schema(&self.db, &ns)
                    .await
                    .ok_or(QueryError::Unsupported)?;
                let where_expr = selection.as_ref().ok_or(QueryError::Unsupported)?;
                let cond_map = Self::where_to_map(where_expr);
                let key = Self::build_key_from_map(&schema, &cond_map)?;
                (ns, key, Some((schema, assignments.clone())), None)
            }
            _ => return Err(QueryError::Unsupported),
        };

        // Choose a ballot and timestamp for the mutation
        let ts = Self::timestamp_for(true);
        let salt = {
            let mut c = std::io::Cursor::new(self.self_addr.as_bytes());
            murmur3_32(&mut c, 0).unwrap_or(0) as u64
        };
        let ballot = (ts << 16) | (salt & 0xffff);

        // Prepare phase
        let mut promised = 0usize;
        let mut max_accepted_ballot = 0u64;
        let mut max_accepted_value: Vec<u8> = Vec::new();
        for node in &healthy {
            if node == &self.self_addr {
                let (ok, acc_b, acc_v) = self.lwt_prepare(&ns, &key, ballot).await;
                if ok {
                    promised += 1;
                    if acc_b > max_accepted_ballot {
                        max_accepted_ballot = acc_b;
                        max_accepted_value = acc_v;
                    }
                }
            } else if let Ok(mut client) = CassClient::connect(node.clone()).await {
                if let Ok(resp) = client
                    .lwt_prepare(tonic::Request::new(LwtPrepareRequest {
                        namespace: ns.clone(),
                        key: key.clone(),
                        ballot,
                    }))
                    .await
                {
                    let r = resp.into_inner();
                    if r.promised {
                        promised += 1;
                        if r.ballot > max_accepted_ballot {
                            max_accepted_ballot = r.ballot;
                            max_accepted_value = r.value;
                        }
                    }
                }
            }
        }
        if promised < required {
            return Err(QueryError::Other("not enough healthy replicas".into()));
        }

        // Read phase
        let mut read_ballot = max_accepted_ballot;
        let mut read_value = max_accepted_value.clone();
        for node in &healthy {
            if node == &self.self_addr {
                let (b, v) = self.lwt_read(&ns, &key).await;
                if b > read_ballot
                    || (b == 0 && read_ballot == 0 && read_value.is_empty() && !v.is_empty())
                {
                    read_ballot = b;
                    read_value = v;
                }
            } else if let Ok(mut client) = CassClient::connect(node.clone()).await {
                if let Ok(resp) = client
                    .lwt_read(tonic::Request::new(LwtReadRequest {
                        namespace: ns.clone(),
                        key: key.clone(),
                    }))
                    .await
                {
                    let r = resp.into_inner();
                    if r.ballot > read_ballot
                        || (r.ballot == 0
                            && read_ballot == 0
                            && read_value.is_empty()
                            && !r.value.is_empty())
                    {
                        read_ballot = r.ballot;
                        read_value = r.value;
                    }
                }
            }
        }

        // Evaluate condition and build proposed value
        let mut applied = false;
        let proposed_value: Vec<u8> = match (&stmt, lwt_not_exists) {
            (Statement::Insert(_), true) => {
                // Check if row exists
                let data = Self::split_ts(&read_value).1;
                if data.is_empty() {
                    applied = true;
                    let (_schema, row_map) = insert_row_values.unwrap();
                    let mut buf = ts.to_be_bytes().to_vec();
                    buf.extend_from_slice(&crate::schema::encode_row(&row_map));
                    buf
                } else {
                    Vec::new()
                }
            }
            (Statement::Update { .. }, _) => {
                let data = Self::split_ts(&read_value).1;
                let mut current = crate::schema::decode_row(data);
                if !lwt_equals.is_empty() {
                    let success = lwt_equals
                        .iter()
                        .all(|(k, v)| current.get(k).map(|val| val == v).unwrap_or(false));
                    if !success {
                        applied = false;
                        Vec::new()
                    } else {
                        applied = true;
                        // apply assignments
                        if let Some((schema, assigns)) = assignments {
                            for assign in assigns {
                                if let AssignmentTarget::ColumnName(name) = &assign.target {
                                    if let Some(id) = name.0.first().and_then(|p| p.as_ident()) {
                                        let col = id.value.to_lowercase();
                                        if schema.partition_keys.contains(&col)
                                            || schema.clustering_keys.contains(&col)
                                        {
                                            continue;
                                        }
                                        if let Some(val) = Self::expr_to_string(&assign.value) {
                                            current.insert(col, val);
                                        }
                                    }
                                }
                            }
                        }
                        let mut buf = ts.to_be_bytes().to_vec();
                        buf.extend_from_slice(&crate::schema::encode_row(&current));
                        buf
                    }
                } else {
                    // Unsupported UPDATE condition form
                    return Err(QueryError::Unsupported);
                }
            }
            _ => return Err(QueryError::Unsupported),
        };

        // Propose phase
        let mut accepted = 0usize;
        for node in &healthy {
            if node == &self.self_addr {
                if self
                    .lwt_propose(&ns, &key, ballot, proposed_value.clone())
                    .await
                {
                    accepted += 1;
                }
            } else if let Ok(mut client) = CassClient::connect(node.clone()).await {
                if let Ok(resp) = client
                    .lwt_propose(tonic::Request::new(LwtProposeRequest {
                        namespace: ns.clone(),
                        key: key.clone(),
                        ballot,
                        value: proposed_value.clone(),
                    }))
                    .await
                {
                    if resp.into_inner().accepted {
                        accepted += 1;
                    }
                }
            }
        }
        if accepted < required {
            return Err(QueryError::Other("not enough healthy replicas".into()));
        }

        // Commit phase (best effort to all healthy replicas)
        for node in &healthy {
            if node == &self.self_addr {
                self.lwt_commit(&ns, &key, proposed_value.clone()).await;
            } else if let Ok(mut client) = CassClient::connect(node.clone()).await {
                let _ = client
                    .lwt_commit(tonic::Request::new(LwtCommitRequest {
                        namespace: ns.clone(),
                        key: key.clone(),
                        value: proposed_value.clone(),
                    }))
                    .await;
            }
        }

        // Build LWT response row
        let mut row = BTreeMap::new();
        row.insert(
            "[applied]".to_string(),
            if applied { "true" } else { "false" }.to_string(),
        );
        if !applied && !lwt_equals.is_empty() {
            let data = Self::split_ts(&read_value).1;
            let current = crate::schema::decode_row(data);
            for (k, _) in lwt_equals.iter() {
                if let Some(v) = current.get(k) {
                    row.insert(k.clone(), v.clone());
                }
            }
        }
        Ok(output_to_proto(QueryOutput::Rows(vec![row])))
    }

    fn expr_to_string(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Value(v) => match &v.value {
                sqlparser::ast::Value::SingleQuotedString(s) => Some(s.clone()),
                sqlparser::ast::Value::Number(n, _) => Some(n.clone()),
                _ => None,
            },
            _ => None,
        }
    }

    fn where_to_map(expr: &Expr) -> BTreeMap<String, String> {
        fn collect(e: &Expr, out: &mut BTreeMap<String, String>) {
            match e {
                Expr::BinaryOp { left, op, right } => {
                    if *op == sqlparser::ast::BinaryOperator::And {
                        collect(left, out);
                        collect(right, out);
                    } else if *op == sqlparser::ast::BinaryOperator::Eq {
                        if let Expr::Identifier(id) = &**left {
                            if let Some(val) = Cluster::expr_to_string(right) {
                                out.insert(id.value.to_lowercase(), val);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        let mut map = BTreeMap::new();
        collect(expr, &mut map);
        map
    }

    fn build_key_from_map(
        schema: &TableSchema,
        map: &BTreeMap<String, String>,
    ) -> Result<String, QueryError> {
        let mut parts = Vec::new();
        for col in schema.key_columns() {
            if let Some(v) = map.get(&col) {
                parts.push(v.clone());
            } else {
                return Err(QueryError::Unsupported);
            }
        }
        Ok(parts.join("|"))
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

    /// Record failed writes as hints for later delivery.
    async fn store_hints(&self, nodes: &[String], sql: String, ts: u64) {
        if nodes.is_empty() {
            return;
        }
        let mut map = self.hints.write().await;
        for n in nodes {
            map.entry(n.clone()).or_default().push((ts, sql.clone()));
        }
    }

    /// Attempt to deliver any stored hints to `node`.
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

    /// Handle the prepare phase of a Paxos-style lightweight transaction.
    ///
    /// Returns whether the promise was made along with any previously
    /// accepted ballot and value.
    pub async fn lwt_prepare(&self, ns: &str, key: &str, ballot: u64) -> (bool, u64, Vec<u8>) {
        let composite = format!("{}:{}", ns, key);
        let mut map = self.lwt.write().await;
        let slot = map.entry(composite).or_insert(PaxosSlot {
            promised: 0,
            accepted_ballot: 0,
            accepted_value: Vec::new(),
        });
        if ballot > slot.promised {
            slot.promised = ballot;
            (true, slot.accepted_ballot, slot.accepted_value.clone())
        } else {
            (false, slot.promised, slot.accepted_value.clone())
        }
    }

    /// Record a proposed value for the given ballot if the promise still holds.
    pub async fn lwt_propose(&self, ns: &str, key: &str, ballot: u64, value: Vec<u8>) -> bool {
        let composite = format!("{}:{}", ns, key);
        let mut map = self.lwt.write().await;
        let slot = map.entry(composite).or_insert(PaxosSlot {
            promised: 0,
            accepted_ballot: 0,
            accepted_value: Vec::new(),
        });
        if ballot >= slot.promised {
            slot.promised = ballot;
            slot.accepted_ballot = ballot;
            slot.accepted_value = value;
            true
        } else {
            false
        }
    }

    /// Read the latest accepted value for a lightweight transaction key.
    pub async fn lwt_read(&self, ns: &str, key: &str) -> (u64, Vec<u8>) {
        let composite = format!("{}:{}", ns, key);
        let maybe = {
            let map = self.lwt.read().await;
            map.get(&composite)
                .map(|s| (s.accepted_ballot, s.accepted_value.clone()))
        };
        if let Some((ballot, val)) = maybe {
            if ballot > 0 && !val.is_empty() {
                return (ballot, val);
            }
        }
        let val = self.db.get_ns(ns, key).await.unwrap_or_default();
        (0, val)
    }

    /// Commit the chosen value to durable storage and clear any in-memory state.
    pub async fn lwt_commit(&self, ns: &str, key: &str, value: Vec<u8>) {
        if !value.is_empty() {
            if value.len() >= 8 {
                let ts = u64::from_be_bytes(value[..8].try_into().unwrap_or([0; 8]));
                let data = value[8..].to_vec();
                self.db.insert_ns_ts(ns, key.to_string(), data, ts).await;
            } else {
                self.db.insert_ns(ns, key.to_string(), value).await;
            }
        }
        let composite = format!("{}:{}", ns, key);
        self.lwt.write().await.remove(&composite);
    }

    /// Reconcile divergent replicas by sending the freshest values to healthy nodes
    /// and hinting any that are down.
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

    /// Retrieve the [`TableSchema`] for `table` from the internal schema store.
    async fn get_schema(db: &Database, table: &str) -> Option<TableSchema> {
        db.get_ns("_schemas", table).await.and_then(|v| {
            let (_, data) = Self::split_ts(&v);
            serde_json::from_slice(data).ok()
        })
    }

    /// Split the leading 8-byte timestamp from a buffer, returning the timestamp
    /// and the remaining bytes.
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
            if !arr_rows.is_empty() {
                // conditional writes return status rows instead of mutation counts
                return Ok(output_to_proto(QueryOutput::Rows(vec![
                    arr_rows[0].clone(),
                ])));
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;
    use crate::storage::local::LocalStorage;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::time::Duration;

    async fn test_cluster(self_addr: &str, peers: Vec<String>) -> Cluster {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(LocalStorage::new(dir.path()));
        let db = Arc::new(Database::new(storage, "wal.log").await.unwrap());
        Cluster::new(db, self_addr.to_string(), peers, 1, 1, 1)
    }

    #[test]
    fn proto_to_output_round_trip() {
        let resp = output_to_proto(QueryOutput::Mutation {
            op: "INSERT".into(),
            unit: "rows".into(),
            count: 1,
        });
        if let QueryOutput::Mutation { op, unit, count } = proto_to_output(resp) {
            assert_eq!(op, "INSERT");
            assert_eq!(unit, "rows");
            assert_eq!(count, 1);
        } else {
            panic!("expected mutation");
        }
        let resp = output_to_proto(QueryOutput::Rows(vec![BTreeMap::from([(
            "k".into(),
            "v".into(),
        )])]));
        if let QueryOutput::Rows(rs) = proto_to_output(resp) {
            assert_eq!(rs[0].get("k"), Some(&"v".to_string()));
        } else {
            panic!("expected rows");
        }

        let resp = output_to_proto(QueryOutput::Meta(vec![("key".into(), 1, "val".into())]));
        if let QueryOutput::Meta(m) = proto_to_output(resp) {
            assert_eq!(m[0].0, "key");
            assert_eq!(m[0].1, 1);
            assert_eq!(m[0].2, "val");
        } else {
            panic!("expected meta");
        }

        let resp = output_to_proto(QueryOutput::Tables(vec!["t".into()]));
        if let QueryOutput::Tables(t) = proto_to_output(resp) {
            assert_eq!(t, vec!["t".to_string()]);
        } else {
            panic!("expected tables");
        }

        let resp = output_to_proto(QueryOutput::None);
        assert!(matches!(proto_to_output(resp), QueryOutput::None));
    }

    #[tokio::test]
    async fn peer_health_reports_status() {
        let peer1 = "http://127.0.0.1:9001".to_string();
        let peer2 = "http://127.0.0.1:9002".to_string();
        let cluster =
            test_cluster("http://127.0.0.1:9000", vec![peer1.clone(), peer2.clone()]).await;

        {
            let mut map = cluster.health.write().await;
            map.insert(peer1.clone(), Instant::now());
            map.insert(peer2.clone(), Instant::now() - Duration::from_secs(10));
        }

        let mut status = cluster.peer_health().await;
        status.sort();
        assert_eq!(status, vec![(peer1, true), (peer2, false)]);
    }

    #[tokio::test]
    async fn analyze_sql_extracts_delete_ns() {
        let engine = SqlEngine::new();
        let meta = Cluster::analyze_sql(&engine, "DELETE FROM tbl WHERE id='1'");
        assert_eq!(meta.ns, Some("tbl".into()));
        assert!(meta.is_write);
    }

    #[tokio::test]
    async fn apply_hints_preserves_on_failure() {
        let peer = "http://127.0.0.1:9201".to_string();
        let cluster = test_cluster("http://127.0.0.1:9200", vec![peer.clone()]).await;
        cluster
            .store_hints(&[peer.clone()], "INSERT INTO t (id) VALUES ('a')".into(), 1)
            .await;
        assert!(cluster.hints.read().await.get(&peer).is_some());
        cluster.apply_hints(&peer).await;
        assert!(cluster.hints.read().await.get(&peer).is_some());
    }
}
