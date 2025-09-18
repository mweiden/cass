//! Minimal SQL execution engine for the key-value store.

use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, ColumnOption, DataType, Delete, Expr, FromTable,
    Insert, LimitClause, ObjectName, ObjectType, OrderBy, Query, Select, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use once_cell::sync::Lazy;
use tokio::sync::RwLock;

use crate::{
    Database,
    schema::{TableSchema, decode_row, encode_row},
};

/// Structured results produced by the SQL engine.
pub enum QueryOutput {
    /// Mutation summary with operation, unit, and affected count.
    Mutation {
        op: String,
        unit: String,
        count: usize,
    },
    /// Row set for queries like SELECT.
    Rows(Vec<BTreeMap<String, String>>),
    /// Internal representation of rows with key and timestamp for replication.
    Meta(Vec<(String, u64, String)>),
    /// List of table names for SHOW TABLES.
    Tables(Vec<String>),
    /// No result to return.
    None,
}

/// Condition for Cassandra-style lightweight transactions.
///
/// Currently only supports checking for absence of a row or comparing column
/// equality. Additional variants can be added in the future as the SQL dialect
/// expands.
#[derive(Clone)]
pub(crate) enum LwtCondition {
    /// Proceed only if the targeted row does not yet exist.
    NotExists,
    /// Proceed only if the specified columns equal the provided values.
    Equals(BTreeMap<String, String>),
}

#[derive(Clone)]
pub struct ParsedQuery {
    base_sql: String,
    statements: Vec<Statement>,
    lwt_condition: Option<LwtCondition>,
}

impl ParsedQuery {
    pub fn statements(&self) -> &[Statement] {
        &self.statements
    }

    pub fn into_statements(self) -> Vec<Statement> {
        self.statements
    }

    pub(crate) fn lwt_condition(&self) -> Option<&LwtCondition> {
        self.lwt_condition.as_ref()
    }

    pub fn is_lwt(&self) -> bool {
        self.lwt_condition.is_some()
    }

    pub fn base_sql(&self) -> &str {
        &self.base_sql
    }
}

type SchemaCache = Arc<RwLock<HashMap<(usize, String), Arc<TableSchema>>>>;

static SCHEMA_CACHE: Lazy<SchemaCache> = Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

fn schema_cache_key(db: &Database, table: &str) -> (usize, String) {
    (db.instance_id(), table.to_string())
}

/// Split the leading 8-byte timestamp from a buffer, returning the timestamp and
/// the remaining slice.
pub fn split_ts(bytes: &[u8]) -> (u64, &[u8]) {
    if bytes.len() < 8 {
        return (0, bytes);
    }
    let ts = u64::from_be_bytes(bytes[..8].try_into().unwrap_or([0; 8]));
    (ts, &bytes[8..])
}

/// Errors that can occur when parsing or executing a query.
#[derive(thiserror::Error, Debug)]
pub enum QueryError {
    /// The input SQL could not be parsed.
    #[error("parse: {0}")]
    Parse(#[from] sqlparser::parser::ParserError),
    /// The query is syntactically valid but not supported by the engine.
    #[error("unsupported query")]
    Unsupported,
    /// Any other internal or I/O error.
    #[error("{0}")]
    Other(String),
}

/// Simple SQL engine capable of executing a subset of SQL statements.
pub struct SqlEngine {
    dialect: GenericDialect,
}

impl SqlEngine {
    /// Create a new [`SqlEngine`].
    pub fn new() -> Self {
        Self {
            dialect: GenericDialect {},
        }
    }

    /// Parse `sql` into a list of statements.
    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>, sqlparser::parser::ParserError> {
        Parser::parse_sql(&self.dialect, sql)
    }

    /// Return the index of a trailing `IF` clause that is not inside quotes/comments.
    pub fn find_trailing_if_index(&self, sql: &str) -> Option<usize> {
        let bytes = sql.as_bytes();
        let mut i = 0usize;
        let mut in_squote = false;
        let mut in_dquote = false;
        let mut in_line_comment = false;
        let mut last_if: Option<usize> = None;
        while i < bytes.len() {
            let b = bytes[i];
            if in_line_comment {
                if b == b'\n' {
                    in_line_comment = false;
                }
                i += 1;
                continue;
            }
            if in_squote {
                if b == b'\'' {
                    // handle escaped single quote by doubling
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    in_squote = false;
                    i += 1;
                    continue;
                }
                i += 1;
                continue;
            }
            if in_dquote {
                if b == b'"' {
                    // handle escaped double quote by doubling
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    in_dquote = false;
                    i += 1;
                    continue;
                }
                i += 1;
                continue;
            }
            // not in quotes/comments
            if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                in_line_comment = true;
                i += 2;
                continue;
            }
            if b == b'\'' {
                in_squote = true;
                i += 1;
                continue;
            }
            if b == b'"' {
                in_dquote = true;
                i += 1;
                continue;
            }

            // check for IF token with word boundaries (ASCII only)
            // case-insensitive match for 'IF'
            if (b == b'I' || b == b'i') && i + 1 < bytes.len() {
                let b2 = bytes[i + 1];
                if b2 == b'F' || b2 == b'f' {
                    // check boundaries: prev non-alnum underscore, next non-alnum underscore
                    let prev = if i == 0 { b' ' } else { bytes[i - 1] };
                    let next = if i + 2 < bytes.len() {
                        bytes[i + 2]
                    } else {
                        b' '
                    };
                    let is_word = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
                    if !is_word(prev) && !is_word(next) {
                        last_if = Some(i);
                    }
                }
            }
            i += 1;
        }
        last_if
    }

    /// Return the base SQL with any trailing lightweight-transaction predicate removed.
    ///
    /// For example, strips the trailing `IF NOT EXISTS` or `IF col='val'` from
    /// INSERT/UPDATE statements so the remaining SQL can be parsed normally for
    /// analysis, routing, and planning.
    pub fn base_sql<'a>(&self, sql: &'a str) -> String {
        let trimmed = sql.trim();
        // Only consider stripping a trailing IF clause for statements that
        // actually support LWT (INSERT/UPDATE). This avoids breaking valid
        // constructs like `CREATE TABLE IF NOT EXISTS ...`.
        let lower = trimmed
            .chars()
            .take(10)
            .collect::<String>()
            .to_ascii_lowercase();
        let is_lwt_stmt = lower.starts_with("insert") || lower.starts_with("update");
        if is_lwt_stmt {
            if let Some(idx) = self.find_trailing_if_index(sql) {
                return sql[..idx].trim().to_string();
            }
        }
        trimmed.to_string()
    }

    /// Execute `sql` against the provided [`Database`].
    pub async fn execute(&self, db: &Database, sql: &str) -> Result<QueryOutput, QueryError> {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_micros() as u64;
        self.execute_with_ts(db, sql, ts, false).await
    }

    fn extract_lwt_condition(&self, sql: &str) -> Option<LwtCondition> {
        if let Some(idx) = self.find_trailing_if_index(sql) {
            let cond_str = sql[idx + 2..].trim_start();
            if cond_str.eq_ignore_ascii_case("not exists") {
                return Some(LwtCondition::NotExists);
            }
            let cond_sql = format!("SELECT * FROM tmp WHERE {}", cond_str);
            if let Ok(mut stmts) = Parser::parse_sql(&self.dialect, &cond_sql) {
                if let Some(Statement::Query(q)) = stmts.pop() {
                    if let SetExpr::Select(select) = *q.body {
                        if let Some(expr) = select.selection {
                            let map = where_to_map(&expr);
                            return Some(LwtCondition::Equals(map));
                        }
                    }
                }
            }
        }
        None
    }

    pub fn parse_query(&self, sql: &str) -> Result<ParsedQuery, QueryError> {
        let base_sql = self.base_sql(sql);
        let lwt_condition = self.extract_lwt_condition(sql);
        let statements = self.parse(&base_sql)?;
        Ok(ParsedQuery {
            base_sql,
            statements,
            lwt_condition,
        })
    }

    /// Execute `sql` with a supplied timestamp. When `meta` is true, results
    /// will include the row key and mutation timestamp.
    pub async fn execute_with_ts(
        &self,
        db: &Database,
        sql: &str,
        ts: u64,
        meta: bool,
    ) -> Result<QueryOutput, QueryError> {
        let parsed = self.parse_query(sql)?;
        self.execute_with_parsed(db, &parsed, ts, meta).await
    }

    pub async fn execute_with_parsed(
        &self,
        db: &Database,
        parsed: &ParsedQuery,
        ts: u64,
        meta: bool,
    ) -> Result<QueryOutput, QueryError> {
        let mut result = QueryOutput::None;
        let lwt_cond = parsed.lwt_condition();
        for stmt in parsed.statements().iter().cloned() {
            result = self.execute_stmt(db, stmt, ts, meta, lwt_cond).await?;
        }
        Ok(result)
    }

    /// Execute a single parsed SQL [`Statement`].
    async fn execute_stmt(
        &self,
        db: &Database,
        stmt: Statement,
        ts: u64,
        meta: bool,
        cond: Option<&LwtCondition>,
    ) -> Result<QueryOutput, QueryError> {
        match stmt {
            Statement::Insert(insert) => self.exec_insert(db, insert, ts, cond).await,
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                self.exec_update(db, table, assignments, selection, ts, cond)
                    .await
            }
            Statement::Delete(delete) => {
                let count = self.exec_delete(db, delete, ts).await?;
                Ok(QueryOutput::Mutation {
                    op: "DELETE".to_string(),
                    unit: "row".to_string(),
                    count,
                })
            }
            Statement::CreateTable(ct) => {
                let ns = object_name_to_ns(&ct.name).ok_or(QueryError::Unsupported)?;
                if db.get_ns("_tables", &ns).await.is_some() {
                    return Err(QueryError::Unsupported);
                }
                let schema = schema_from_create(&ct).ok_or(QueryError::Unsupported)?;
                save_schema(db, &ns, &schema).await;
                register_table(db, &ns).await;
                Ok(QueryOutput::Mutation {
                    op: "CREATE TABLE".to_string(),
                    unit: "table".to_string(),
                    count: 1,
                })
            }
            Statement::ShowTables { .. } => self.exec_show_tables(db).await,
            Statement::Drop {
                object_type: ObjectType::Table,
                names,
                ..
            } => {
                if let Some(name) = names.first() {
                    let ns = object_name_to_ns(name).ok_or(QueryError::Unsupported)?;
                    db.clear_ns(&ns).await;
                    db.delete_ns("_tables", &ns).await;
                    db.delete_ns("_schemas", &ns).await;
                    invalidate_schema_entry(db, &ns).await;
                    Ok(QueryOutput::Mutation {
                        op: "DROP TABLE".to_string(),
                        unit: "table".to_string(),
                        count: 1,
                    })
                } else {
                    Err(QueryError::Unsupported)
                }
            }
            Statement::Truncate { table_names, .. } => {
                if let Some(target) = table_names.first() {
                    let ns = object_name_to_ns(&target.name).ok_or(QueryError::Unsupported)?;
                    db.clear_ns(&ns).await;
                    Ok(QueryOutput::Mutation {
                        op: "TRUNCATE".to_string(),
                        unit: "table".to_string(),
                        count: 1,
                    })
                } else {
                    Err(QueryError::Unsupported)
                }
            }
            Statement::Query(q) => self.exec_query(db, q, meta).await,
            _ => Err(QueryError::Unsupported),
        }
    }

    /// Handle an `INSERT` statement inserting a single key/value pair.
    async fn exec_insert(
        &self,
        db: &Database,
        insert: Insert,
        ts: u64,
        cond: Option<&LwtCondition>,
    ) -> Result<QueryOutput, QueryError> {
        let ns = match &insert.table {
            sqlparser::ast::TableObject::TableName(name) => {
                object_name_to_ns(name).ok_or(QueryError::Unsupported)?
            }
            _ => return Err(QueryError::Unsupported),
        };
        let schema = lookup_schema(db, &ns)
            .await
            .ok_or(QueryError::Unsupported)?;
        let schema_ref = schema.as_ref();
        let source = insert.source.ok_or(QueryError::Unsupported)?;
        let values = match *source.body {
            SetExpr::Values(v) => v,
            _ => return Err(QueryError::Unsupported),
        };
        // Determine column order for the insert.
        let cols: Vec<String> = if !insert.columns.is_empty() {
            insert
                .columns
                .iter()
                .map(|c| c.value.to_lowercase())
                .collect()
        } else {
            schema_ref.columns.clone()
        };

        if let Some(LwtCondition::NotExists) = cond {
            if values.rows.len() != 1 {
                return Err(QueryError::Unsupported);
            }
            let (key, data) =
                build_row(schema_ref, &cols, &values.rows[0]).ok_or(QueryError::Unsupported)?;
            let applied = db.insert_ns_if_absent_ts(&ns, key, data, ts).await;
            let mut row = BTreeMap::new();
            row.insert("[applied]".to_string(), applied.to_string());
            Ok(QueryOutput::Rows(vec![row]))
        } else {
            let mut count = 0;
            for row in values.rows {
                let (key, data) =
                    build_row(schema_ref, &cols, &row).ok_or(QueryError::Unsupported)?;
                db.insert_ns_ts(&ns, key, data, ts).await;
                count += 1;
            }
            Ok(QueryOutput::Mutation {
                op: "INSERT".to_string(),
                unit: "row".to_string(),
                count,
            })
        }
    }

    /// Handle an `UPDATE` statement that sets the value for a single key.
    async fn exec_update(
        &self,
        db: &Database,
        table: TableWithJoins,
        assignments: Vec<Assignment>,
        selection: Option<Expr>,
        ts: u64,
        cond: Option<&LwtCondition>,
    ) -> Result<QueryOutput, QueryError> {
        let ns = table_factor_to_ns(&table.relation).ok_or(QueryError::Unsupported)?;
        let schema = lookup_schema(db, &ns)
            .await
            .ok_or(QueryError::Unsupported)?;
        let schema_ref = schema.as_ref();
        // schema-aware path using the WHERE clause for the key
        let where_expr = selection.ok_or(QueryError::Unsupported)?;
        let cond_map = where_to_map(&where_expr);
        let key = build_single_key(schema_ref, &cond_map).ok_or(QueryError::Unsupported)?;
        let mut row_map = if let Some(bytes) = db.get_ns(&ns, &key).await {
            let (_, data) = split_ts(&bytes);
            decode_row(data)
        } else {
            BTreeMap::new()
        };
        if let Some(lwt) = cond {
            match lwt {
                LwtCondition::NotExists => {
                    if !row_map.is_empty() {
                        let mut row = BTreeMap::new();
                        row.insert("[applied]".to_string(), "false".to_string());
                        return Ok(QueryOutput::Rows(vec![row]));
                    }
                }
                LwtCondition::Equals(expected) => {
                    let success = expected
                        .iter()
                        .all(|(k, v)| row_map.get(k).map(|val| val == v).unwrap_or(false));
                    if !success {
                        let mut row = BTreeMap::new();
                        row.insert("[applied]".to_string(), "false".to_string());
                        for (k, _) in expected.iter() {
                            if let Some(val) = row_map.get(k) {
                                row.insert(k.clone(), val.clone());
                            }
                        }
                        return Ok(QueryOutput::Rows(vec![row]));
                    }
                }
            }
        }
        for assign in assignments {
            if let AssignmentTarget::ColumnName(name) = assign.target {
                if let Some(id) = name.0.first().and_then(|p| p.as_ident()) {
                    let col = id.value.to_lowercase();
                    if schema_ref.partition_keys.contains(&col)
                        || schema_ref.clustering_keys.contains(&col)
                    {
                        continue; // skip key columns
                    }
                    let val = expr_to_string(&assign.value).ok_or(QueryError::Unsupported)?;
                    row_map.insert(col, val);
                }
            }
        }
        let data = encode_row(&row_map);
        db.insert_ns_ts(&ns, key, data, ts).await;
        if cond.is_some() {
            let mut row = BTreeMap::new();
            row.insert("[applied]".to_string(), "true".to_string());
            Ok(QueryOutput::Rows(vec![row]))
        } else {
            Ok(QueryOutput::Mutation {
                op: "UPDATE".to_string(),
                unit: "row".to_string(),
                count: 1,
            })
        }
    }

    /// Handle a `DELETE` statement for a single key.
    async fn exec_delete(
        &self,
        db: &Database,
        delete: Delete,
        ts: u64,
    ) -> Result<usize, QueryError> {
        let table = match &delete.from {
            FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
        };
        if table.len() != 1 {
            return Err(QueryError::Unsupported);
        }
        let ns = table_factor_to_ns(&table[0].relation).ok_or(QueryError::Unsupported)?;
        let schema = lookup_schema(db, &ns)
            .await
            .ok_or(QueryError::Unsupported)?;
        // schema-aware deletion using key columns
        let expr = delete.selection.ok_or(QueryError::Unsupported)?;
        let cond_map = where_to_map(&expr);
        let key = build_single_key(schema.as_ref(), &cond_map).ok_or(QueryError::Unsupported)?;
        // record tombstone with timestamp
        db.insert_ns_ts(&ns, key, Vec::new(), ts).await;
        Ok(1)
    }

    /// Execute the inner query of a [`Statement::Query`].
    async fn exec_query(
        &self,
        db: &Database,
        q: Box<Query>,
        meta: bool,
    ) -> Result<QueryOutput, QueryError> {
        match *q.body {
            SetExpr::Select(select) => {
                self.exec_select(db, *select, q.order_by, q.limit_clause, meta)
                    .await
            }
            _ => Err(QueryError::Unsupported),
        }
    }

    /// Execute a `SELECT` statement returning at most one row/value.
    async fn exec_select(
        &self,
        db: &Database,
        select: Select,
        _order: Option<OrderBy>,
        _limit: Option<LimitClause>,
        meta: bool,
    ) -> Result<QueryOutput, QueryError> {
        if select.from.len() != 1 {
            return Err(QueryError::Unsupported);
        }
        let ns = table_factor_to_ns(&select.from[0].relation).ok_or(QueryError::Unsupported)?;
        let schema = lookup_schema(db, &ns)
            .await
            .ok_or(QueryError::Unsupported)?;
        let schema_ref = schema.as_ref();

        // handle COUNT(*) with optional WHERE filtering
        if select.projection.len() == 1 {
            if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] {
                if func.name.to_string().eq_ignore_ascii_case("count") {
                    let selection = select.selection.clone();
                    return self.exec_count(db, &ns, schema_ref, selection).await;
                }
            }
        }

        self.exec_select_schema(db, &ns, schema_ref, select, meta)
            .await
    }

    /// Execute a `COUNT(*)` query with optional equality filters.
    async fn exec_count(
        &self,
        db: &Database,
        ns: &str,
        schema: &TableSchema,
        selection: Option<Expr>,
    ) -> Result<QueryOutput, QueryError> {
        let cond_map = if let Some(expr) = selection {
            where_to_map(&expr)
        } else {
            BTreeMap::new()
        };
        let mut count = 0;
        if let Some(key) = build_single_key(schema, &cond_map) {
            if let Some(bytes) = db.get_ns(ns, &key).await {
                let (_, data) = split_ts(&bytes);
                if !data.is_empty() {
                    let mut row_map = decode_row(data);
                    for col in schema.key_columns() {
                        if let Some(v) = cond_map.get(&col) {
                            row_map.insert(col, v.clone());
                        }
                    }
                    if cond_map
                        .iter()
                        .all(|(c, v)| row_map.get(c).map_or(false, |val| val == v))
                    {
                        count = 1;
                    }
                }
            }
        } else {
            // Scan the namespace which now includes both in-memory and on-disk rows.
            for (k, bytes) in db.scan_ns(ns).await.into_iter() {
                let (_, data) = split_ts(&bytes);
                if data.is_empty() {
                    continue;
                }
                let mut row_map = decode_row(data);
                for (col, part) in schema.key_columns().iter().zip(k.split('|')) {
                    row_map.insert(col.clone(), part.to_string());
                }
                if cond_map
                    .iter()
                    .all(|(c, v)| row_map.get(c).map_or(false, |val| val == v))
                {
                    count += 1;
                }
            }
        }
        let mut row = BTreeMap::new();
        row.insert("count".to_string(), count.to_string());
        Ok(QueryOutput::Rows(vec![row]))
    }

    /// Simplified `SELECT` handler for schema-aware tables.
    async fn exec_select_schema(
        &self,
        db: &Database,
        ns: &str,
        schema: &TableSchema,
        select: Select,
        meta: bool,
    ) -> Result<QueryOutput, QueryError> {
        let Select {
            projection,
            selection,
            ..
        } = select;
        let (cols, wildcard) = parse_projection(projection)?;

        let cond_multi = if let Some(expr) = selection {
            where_to_multi_map(&expr)
        } else {
            BTreeMap::new()
        };
        let key_cols = schema.key_columns();
        let mut prefix_len = 0;
        for col in &key_cols {
            if cond_multi.contains_key(col) {
                prefix_len += 1;
            } else {
                break;
            }
        }
        if prefix_len == 0 {
            return Err(QueryError::Unsupported);
        }

        let mut out_rows: Vec<BTreeMap<String, String>> = Vec::new();
        let mut meta_rows: Vec<(String, u64, String)> = Vec::new();

        if prefix_len == key_cols.len() {
            let keys = build_keys(&key_cols, &cond_multi);
            for key in keys {
                if let Some(row_bytes) = db.get_ns(ns, &key).await {
                    let (ts, data) = split_ts(&row_bytes);
                    if data.is_empty() {
                        if meta {
                            meta_rows.push((key.clone(), ts, String::new()));
                        }
                        continue;
                    }
                    let mut row_map = decode_row(data);
                    for (col, part) in key_cols.iter().zip(key.split('|')) {
                        row_map.insert(col.clone(), part.to_string());
                    }
                    let sel_map = project_row(&row_map, &cols, wildcard);
                    if meta {
                        let val = String::from_utf8_lossy(&encode_row(&sel_map)).into_owned();
                        meta_rows.push((key.clone(), ts, val));
                    } else {
                        out_rows.push(sel_map);
                    }
                }
            }
        } else {
            let prefix_cols = &key_cols[..prefix_len];
            let prefixes = build_keys(prefix_cols, &cond_multi);
            // Expanded scan returns rows from both the memtable and on-disk tables
            // which are then filtered by the prefix conditions.
            for (k, bytes) in db.scan_ns(ns).await.into_iter() {
                for prefix in &prefixes {
                    if k == *prefix || k.starts_with(&format!("{}|", prefix)) {
                        let (ts, data) = split_ts(&bytes);
                        if data.is_empty() {
                            if meta {
                                meta_rows.push((k.clone(), ts, String::new()));
                            }
                            break;
                        }
                        let mut row_map = decode_row(data);
                        for (col, part) in key_cols.iter().zip(k.split('|')) {
                            row_map.insert(col.clone(), part.to_string());
                        }
                        if cond_multi
                            .iter()
                            .all(|(c, v)| row_map.get(c).map_or(false, |val| v.contains(val)))
                        {
                            let sel_map = project_row(&row_map, &cols, wildcard);
                            if meta {
                                let val =
                                    String::from_utf8_lossy(&encode_row(&sel_map)).into_owned();
                                meta_rows.push((k.clone(), ts, val));
                            } else {
                                out_rows.push(sel_map);
                            }
                        }
                        break;
                    }
                }
            }
        }

        if meta {
            if meta_rows.is_empty() {
                Ok(QueryOutput::None)
            } else {
                Ok(QueryOutput::Meta(meta_rows))
            }
        } else {
            Ok(QueryOutput::Rows(out_rows))
        }
    }

    /// Return a list of registered table names.
    async fn exec_show_tables(&self, db: &Database) -> Result<QueryOutput, QueryError> {
        let mut tables: Vec<String> = db
            .scan_ns("_tables")
            .await
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        tables.sort();
        Ok(QueryOutput::Tables(tables))
    }

    /// Extract partition key values from `sql` for routing and replication.
    pub async fn partition_keys(
        &self,
        db: &Database,
        sql: &str,
    ) -> Result<Vec<String>, QueryError> {
        let parsed = self.parse_query(sql)?;
        self.partition_keys_parsed(db, &parsed).await
    }

    /// Extract partition key values from a previously parsed query.
    pub async fn partition_keys_parsed(
        &self,
        db: &Database,
        parsed: &ParsedQuery,
    ) -> Result<Vec<String>, QueryError> {
        let mut keys = Vec::new();
        let mut needs_key = false;
        for stmt in parsed.statements() {
            match stmt {
                Statement::Insert(insert) => {
                    needs_key = true;
                    let ns = match &insert.table {
                        sqlparser::ast::TableObject::TableName(name) => {
                            object_name_to_ns(name).ok_or(QueryError::Unsupported)?
                        }
                        _ => return Err(QueryError::Unsupported),
                    };
                    let schema = lookup_schema(db, &ns)
                        .await
                        .ok_or(QueryError::Unsupported)?;
                    let schema_ref = schema.as_ref();
                    let source = insert.source.as_ref().ok_or(QueryError::Unsupported)?;
                    let values = match &*source.body {
                        SetExpr::Values(v) => v,
                        _ => return Err(QueryError::Unsupported),
                    };
                    let cols: Vec<String> = if !insert.columns.is_empty() {
                        insert
                            .columns
                            .iter()
                            .map(|c| c.value.to_lowercase())
                            .collect()
                    } else {
                        schema_ref.columns.clone()
                    };
                    for row in &values.rows {
                        if let Some(key) = extract_partition_key(schema_ref, &cols, row) {
                            keys.push(key);
                        }
                    }
                }
                Statement::Update {
                    table, selection, ..
                } => {
                    needs_key = true;
                    let ns = table_factor_to_ns(&table.relation).ok_or(QueryError::Unsupported)?;
                    let schema = lookup_schema(db, &ns)
                        .await
                        .ok_or(QueryError::Unsupported)?;
                    if let Some(expr) = selection.as_ref() {
                        let map = where_to_multi_map(expr);
                        keys.extend(build_keys(&schema.partition_keys, &map));
                    }
                }
                Statement::Delete(delete) => {
                    needs_key = true;
                    let table = match &delete.from {
                        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
                    };
                    if table.len() != 1 {
                        return Err(QueryError::Unsupported);
                    }
                    let ns =
                        table_factor_to_ns(&table[0].relation).ok_or(QueryError::Unsupported)?;
                    let schema = lookup_schema(db, &ns)
                        .await
                        .ok_or(QueryError::Unsupported)?;
                    if let Some(expr) = delete.selection.as_ref() {
                        let map = where_to_multi_map(expr);
                        keys.extend(build_keys(&schema.partition_keys, &map));
                    }
                }
                Statement::Query(q) => {
                    needs_key = true;
                    if let SetExpr::Select(select) = &*q.body {
                        if select.from.len() != 1 {
                            return Err(QueryError::Unsupported);
                        }
                        let ns = table_factor_to_ns(&select.from[0].relation)
                            .ok_or(QueryError::Unsupported)?;
                        let schema = lookup_schema(db, &ns)
                            .await
                            .ok_or(QueryError::Unsupported)?;
                        if let Some(expr) = select.selection.as_ref() {
                            let map = where_to_multi_map(expr);
                            keys.extend(build_keys(&schema.partition_keys, &map));
                        }
                    }
                }
                _ => {}
            }
        }
        if needs_key && keys.is_empty() {
            return Err(QueryError::Other(
                "partition key must be fully specified".into(),
            ));
        }
        Ok(keys)
    }
}

/// Register a table name in the internal catalog if it does not already exist.
async fn register_table(db: &Database, table: &str) {
    if db.get_ns("_tables", table).await.is_none() {
        db.insert_ns("_tables", table.to_string(), Vec::new()).await;
    }
}

async fn cache_schema_entry(db: &Database, table: &str, schema: Arc<TableSchema>) {
    let key = schema_cache_key(db, table);
    let mut guard = SCHEMA_CACHE.write().await;
    guard.insert(key, schema);
}

async fn invalidate_schema_entry(db: &Database, table: &str) {
    let key = schema_cache_key(db, table);
    let mut guard = SCHEMA_CACHE.write().await;
    guard.remove(&key);
}

/// Retrieve the schema for a table if it exists, caching successful lookups.
pub(crate) async fn lookup_schema(db: &Database, table: &str) -> Option<Arc<TableSchema>> {
    if let Some(schema) = {
        let cache = SCHEMA_CACHE.read().await;
        cache.get(&schema_cache_key(db, table)).cloned()
    } {
        return Some(schema);
    }
    let raw = db.get_ns("_schemas", table).await?;
    let (_, data) = split_ts(&raw);
    let parsed = serde_json::from_slice(data).ok()?;
    let schema = Arc::new(parsed);
    cache_schema_entry(db, table, Arc::clone(&schema)).await;
    Some(schema)
}

/// Persist a schema definition for a table.
async fn save_schema(db: &Database, table: &str, schema: &TableSchema) {
    if let Ok(data) = serde_json::to_vec(schema) {
        db.insert_ns("_schemas", table.to_string(), data).await;
        cache_schema_entry(db, table, Arc::new(schema.clone())).await;
    }
}

/// Build a [`TableSchema`] from a parsed `CREATE TABLE` statement.
fn schema_from_create(ct: &sqlparser::ast::CreateTable) -> Option<TableSchema> {
    let columns: Vec<String> = ct
        .columns
        .iter()
        .map(|c| c.name.value.to_lowercase())
        .collect();
    if columns.is_empty() {
        return None;
    }
    // Determine primary key columns.
    let mut pk: Vec<String> = Vec::new();
    let mut ck: Vec<String> = Vec::new();
    for constr in &ct.constraints {
        if let sqlparser::ast::TableConstraint::PrimaryKey { columns, .. } = constr {
            for (i, ic) in columns.iter().enumerate() {
                if let Expr::Identifier(id) = &ic.column.expr {
                    if i == 0 {
                        pk.push(id.value.to_lowercase());
                    } else {
                        ck.push(id.value.to_lowercase());
                    }
                }
            }
        }
    }
    // If no table constraint primary key, look for column options.
    if pk.is_empty() {
        for col in &ct.columns {
            for opt in &col.options {
                if let ColumnOption::Unique {
                    is_primary: true, ..
                } = opt.option
                {
                    pk.push(col.name.value.to_lowercase());
                }
            }
        }
    }
    if pk.is_empty() {
        return None;
    }
    Some(TableSchema::new(pk, ck, columns))
}

/// Convert a simple expression into a string value.
fn expr_to_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => Some(s.clone()),
            Value::Number(n, _) => Some(n.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// Parse the projection list of a `SELECT` into column names and optional casts.
fn parse_projection(
    projection: Vec<SelectItem>,
) -> Result<(Vec<(String, Option<DataType>)>, bool), QueryError> {
    let mut cols = Vec::new();
    let mut wildcard = false;
    for item in projection {
        match item {
            SelectItem::Wildcard(_) => {
                wildcard = true;
                break;
            }
            SelectItem::UnnamedExpr(Expr::Identifier(id)) => {
                cols.push((id.value.to_lowercase(), None));
            }
            SelectItem::UnnamedExpr(Expr::Cast {
                expr, data_type, ..
            }) => {
                if let Expr::Identifier(id) = *expr {
                    cols.push((id.value.to_lowercase(), Some(data_type)));
                } else {
                    return Err(QueryError::Unsupported);
                }
            }
            _ => return Err(QueryError::Unsupported),
        }
    }
    Ok((cols, wildcard))
}

/// Project a row map down to the requested columns, applying simple casts.
fn project_row(
    row_map: &BTreeMap<String, String>,
    cols: &[(String, Option<DataType>)],
    wildcard: bool,
) -> BTreeMap<String, String> {
    if wildcard {
        return row_map.clone();
    }
    let mut sel_map = BTreeMap::new();
    for (col, cast) in cols {
        if let Some(val) = row_map.get(col) {
            let mut v = val.clone();
            if let Some(dt) = cast {
                if let Some(cv) = cast_simple(val, dt) {
                    v = cv;
                }
            }
            sel_map.insert(col.clone(), v);
        }
    }
    sel_map
}

/// Convert a WHERE clause into a map of column -> possible values, supporting `IN` lists.
fn where_to_multi_map(expr: &Expr) -> BTreeMap<String, Vec<String>> {
    fn collect(e: &Expr, out: &mut BTreeMap<String, Vec<String>>) {
        match e {
            Expr::BinaryOp { left, op, right } => {
                if *op == BinaryOperator::And {
                    collect(left, out);
                    collect(right, out);
                } else if *op == BinaryOperator::Eq {
                    if let Expr::Identifier(id) = &**left {
                        if let Some(val) = expr_to_string(right) {
                            out.entry(id.value.to_lowercase()).or_default().push(val);
                        }
                    }
                }
            }
            Expr::InList { expr, list, .. } => {
                if let Expr::Identifier(id) = &**expr {
                    let vals: Vec<String> = list.iter().filter_map(expr_to_string).collect();
                    if !vals.is_empty() {
                        out.entry(id.value.to_lowercase()).or_default().extend(vals);
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

/// Extract the partition key from an insert row.
fn extract_partition_key(schema: &TableSchema, cols: &[String], row: &[Expr]) -> Option<String> {
    if cols.len() != row.len() {
        return None;
    }
    let mut parts = Vec::new();
    for (col, expr) in cols.iter().zip(row.iter()) {
        if schema.partition_keys.contains(col) {
            let val = expr_to_string(expr)?;
            parts.push(val);
        }
    }
    if parts.len() == schema.partition_keys.len() {
        Some(parts.join("|"))
    } else {
        None
    }
}

/// Build the full key and encoded row data from an insert row.
fn build_row(schema: &TableSchema, cols: &[String], row: &[Expr]) -> Option<(String, Vec<u8>)> {
    if cols.len() != row.len() {
        return None;
    }
    let mut key_parts = Vec::new();
    let mut data_map = BTreeMap::new();
    for (col, expr) in cols.iter().zip(row.iter()) {
        let val = expr_to_string(expr)?;
        if schema.partition_keys.contains(col) || schema.clustering_keys.contains(col) {
            key_parts.push(val.clone());
        } else {
            data_map.insert(col.clone(), val);
        }
    }
    Some((key_parts.join("|"), encode_row(&data_map)))
}

/// Build partition key strings from column values, generating all combinations.
fn build_keys(pk_cols: &[String], map: &BTreeMap<String, Vec<String>>) -> Vec<String> {
    if pk_cols.is_empty() {
        return Vec::new();
    }
    let mut lists = Vec::new();
    for col in pk_cols {
        if let Some(vals) = map.get(col) {
            lists.push(vals.clone());
        } else {
            return Vec::new();
        }
    }
    fn expand(idx: usize, lists: &[Vec<String>], cur: &mut Vec<String>, out: &mut Vec<String>) {
        if idx == lists.len() {
            out.push(cur.join("|"));
            return;
        }
        for v in &lists[idx] {
            cur.push(v.clone());
            expand(idx + 1, lists, cur, out);
            cur.pop();
        }
    }
    let mut out = Vec::new();
    expand(0, &lists, &mut Vec::new(), &mut out);
    out
}

/// Build a single partition key from a map of column -> value.
/// Returns `None` if any key column is missing.
fn build_single_key(schema: &TableSchema, map: &BTreeMap<String, String>) -> Option<String> {
    let mut parts = Vec::new();
    for col in schema.key_columns() {
        if let Some(v) = map.get(&col) {
            parts.push(v.clone());
        } else {
            return None;
        }
    }
    Some(parts.join("|"))
}

/// Convert a simple WHERE clause into a map of column -> value.
fn where_to_map(expr: &Expr) -> BTreeMap<String, String> {
    fn collect(e: &Expr, out: &mut BTreeMap<String, String>) {
        match e {
            Expr::BinaryOp { left, op, right } => {
                if *op == BinaryOperator::And {
                    collect(left, out);
                    collect(right, out);
                } else if *op == BinaryOperator::Eq {
                    if let Expr::Identifier(id) = &**left {
                        if let Some(val) = expr_to_string(right) {
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

/// Convert an AST [`ObjectName`] into a lowercase namespace string.
pub fn object_name_to_ns(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.to_lowercase())
}

/// Extract a namespace from a [`TableFactor`].
pub fn table_factor_to_ns(tf: &TableFactor) -> Option<String> {
    match tf {
        TableFactor::Table { name, .. } => object_name_to_ns(name),
        _ => None,
    }
}

/// Perform a basic cast of `val` to the specified [`DataType`].
pub fn cast_simple(val: &str, data_type: &DataType) -> Option<String> {
    match data_type {
        DataType::Int(_)
        | DataType::Integer(_)
        | DataType::BigInt(_)
        | DataType::SmallInt(_)
        | DataType::Unsigned => val.parse::<i64>().ok().map(|i| i.to_string()),
        DataType::Text | DataType::Varchar(_) | DataType::Char(_) => Some(val.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        SqlEngine,
        storage::{Storage, local::LocalStorage},
    };
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn schema_cache_scoped_per_database() {
        let tmp1 = tempdir().unwrap();
        let tmp2 = tempdir().unwrap();
        let storage1: Arc<dyn Storage> = Arc::new(LocalStorage::new(tmp1.path()));
        let storage2: Arc<dyn Storage> = Arc::new(LocalStorage::new(tmp2.path()));
        let db1 = Database::new(storage1, "wal1.log").await.unwrap();
        let db2 = Database::new(storage2, "wal2.log").await.unwrap();
        let engine = SqlEngine::new();

        engine
            .execute(
                &db1,
                "CREATE TABLE users (id TEXT, val TEXT, PRIMARY KEY(id))",
            )
            .await
            .unwrap();
        engine
            .execute(
                &db2,
                "CREATE TABLE users (id TEXT, ts TEXT, val TEXT, PRIMARY KEY(id, ts))",
            )
            .await
            .unwrap();

        let schema1 = lookup_schema(&db1, "users").await.unwrap();
        let schema2 = lookup_schema(&db2, "users").await.unwrap();
        assert_eq!(schema1.key_columns(), vec!["id".to_string()]);
        assert_eq!(
            schema2.key_columns(),
            vec!["id".to_string(), "ts".to_string()]
        );

        invalidate_schema_entry(&db1, "users").await;
        invalidate_schema_entry(&db2, "users").await;
    }
}
