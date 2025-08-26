//! Minimal SQL execution engine for the key-value store.

use std::collections::BTreeMap;
use std::convert::TryInto;
use std::time::{SystemTime, UNIX_EPOCH};

use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, ColumnOption, DataType, Delete, Expr, FromTable,
    Insert, LimitClause, ObjectName, ObjectType, OrderBy, Query, Select, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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

fn split_ts(bytes: &[u8]) -> (u64, &[u8]) {
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

    /// Execute `sql` against the provided [`Database`].
    pub async fn execute(&self, db: &Database, sql: &str) -> Result<QueryOutput, QueryError> {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_micros() as u64;
        self.execute_with_ts(db, sql, ts, false).await
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
        let stmts = self.parse(sql)?;
        let mut result = QueryOutput::None;
        for stmt in stmts {
            result = self.execute_stmt(db, stmt, ts, meta).await?;
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
    ) -> Result<QueryOutput, QueryError> {
        match stmt {
            Statement::Insert(insert) => {
                let count = self.exec_insert(db, insert, ts).await?;
                Ok(QueryOutput::Mutation {
                    op: "INSERT".to_string(),
                    unit: "row".to_string(),
                    count,
                })
            }
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                let count = self
                    .exec_update(db, table, assignments, selection, ts)
                    .await?;
                Ok(QueryOutput::Mutation {
                    op: "UPDATE".to_string(),
                    unit: "row".to_string(),
                    count,
                })
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
                    Ok(QueryOutput::Mutation {
                        op: "DROP TABLE".to_string(),
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
    ) -> Result<usize, QueryError> {
        let ns = match &insert.table {
            sqlparser::ast::TableObject::TableName(name) => {
                object_name_to_ns(name).ok_or(QueryError::Unsupported)?
            }
            _ => return Err(QueryError::Unsupported),
        };
        let schema = get_schema(db, &ns).await.ok_or(QueryError::Unsupported)?;
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
            schema.columns.clone()
        };

        let mut count = 0;
        for row in values.rows {
            let (key, data) = build_row(&schema, &cols, &row).ok_or(QueryError::Unsupported)?;
            db.insert_ns_ts(&ns, key, data, ts).await;
            count += 1;
        }
        Ok(count)
    }

    /// Handle an `UPDATE` statement that sets the value for a single key.
    async fn exec_update(
        &self,
        db: &Database,
        table: TableWithJoins,
        assignments: Vec<Assignment>,
        selection: Option<Expr>,
        ts: u64,
    ) -> Result<usize, QueryError> {
        let ns = table_factor_to_ns(&table.relation).ok_or(QueryError::Unsupported)?;
        let schema = get_schema(db, &ns).await.ok_or(QueryError::Unsupported)?;
        // schema-aware path
        let cond = selection.ok_or(QueryError::Unsupported)?;
        let cond_map = where_to_map(&cond);
        let key = build_single_key(&schema, &cond_map).ok_or(QueryError::Unsupported)?;
        let mut row_map = if let Some(bytes) = db.get_ns(&ns, &key).await {
            let (_, data) = split_ts(&bytes);
            decode_row(data)
        } else {
            BTreeMap::new()
        };
        for assign in assignments {
            if let AssignmentTarget::ColumnName(name) = assign.target {
                if let Some(id) = name.0.first().and_then(|p| p.as_ident()) {
                    let col = id.value.to_lowercase();
                    if schema.partition_keys.contains(&col) || schema.clustering_keys.contains(&col)
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
        Ok(1)
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
        let schema = get_schema(db, &ns).await.ok_or(QueryError::Unsupported)?;
        // schema-aware deletion using key columns
        let expr = delete.selection.ok_or(QueryError::Unsupported)?;
        let cond_map = where_to_map(&expr);
        let key = build_single_key(&schema, &cond_map).ok_or(QueryError::Unsupported)?;
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
        let schema = get_schema(db, &ns).await.ok_or(QueryError::Unsupported)?;

        // handle COUNT(*) with optional WHERE filtering
        if select.projection.len() == 1 {
            if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] {
                if func.name.to_string().eq_ignore_ascii_case("count") {
                    let selection = select.selection.clone();
                    return self.exec_count(db, &ns, &schema, selection).await;
                }
            }
        }

        self.exec_select_schema(db, &ns, &schema, select, meta)
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
            let rows = db.scan_ns(ns).await;
            for (k, bytes) in rows {
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
            let rows = db.scan_ns(ns).await;
            for (k, bytes) in rows {
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
        let stmts = self.parse(sql)?;
        let mut keys = Vec::new();
        let mut needs_key = false;
        for stmt in stmts {
            match stmt {
                Statement::Insert(insert) => {
                    needs_key = true;
                    let ns = match &insert.table {
                        sqlparser::ast::TableObject::TableName(name) => {
                            object_name_to_ns(name).ok_or(QueryError::Unsupported)?
                        }
                        _ => return Err(QueryError::Unsupported),
                    };
                    let schema = get_schema(db, &ns).await.ok_or(QueryError::Unsupported)?;
                    let source = insert.source.ok_or(QueryError::Unsupported)?;
                    let values = match *source.body {
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
                        schema.columns.clone()
                    };
                    for row in values.rows {
                        if let Some(key) = extract_partition_key(&schema, &cols, &row) {
                            keys.push(key);
                        }
                    }
                }
                Statement::Update {
                    table, selection, ..
                } => {
                    needs_key = true;
                    let ns = table_factor_to_ns(&table.relation).ok_or(QueryError::Unsupported)?;
                    let schema = get_schema(db, &ns).await.ok_or(QueryError::Unsupported)?;
                    if let Some(expr) = selection {
                        let map = where_to_multi_map(&expr);
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
                    let schema = get_schema(db, &ns).await.ok_or(QueryError::Unsupported)?;
                    if let Some(expr) = delete.selection {
                        let map = where_to_multi_map(&expr);
                        keys.extend(build_keys(&schema.partition_keys, &map));
                    }
                }
                Statement::Query(q) => {
                    needs_key = true;
                    if let SetExpr::Select(select) = *q.body {
                        if select.from.len() != 1 {
                            return Err(QueryError::Unsupported);
                        }
                        let ns = table_factor_to_ns(&select.from[0].relation)
                            .ok_or(QueryError::Unsupported)?;
                        let schema = get_schema(db, &ns).await.ok_or(QueryError::Unsupported)?;
                        if let Some(expr) = select.selection {
                            let map = where_to_multi_map(&expr);
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

/// Retrieve the schema for a table if it exists.
async fn get_schema(db: &Database, table: &str) -> Option<TableSchema> {
    db.get_ns("_schemas", table).await.and_then(|v| {
        let (_, data) = split_ts(&v);
        serde_json::from_slice(data).ok()
    })
}

/// Persist a schema definition for a table.
async fn save_schema(db: &Database, table: &str, schema: &TableSchema) {
    if let Ok(data) = serde_json::to_vec(schema) {
        db.insert_ns("_schemas", table.to_string(), data).await;
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
fn object_name_to_ns(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.to_lowercase())
}

/// Extract a namespace from a [`TableFactor`].
fn table_factor_to_ns(tf: &TableFactor) -> Option<String> {
    match tf {
        TableFactor::Table { name, .. } => object_name_to_ns(name),
        _ => None,
    }
}

/// Perform a basic cast of `val` to the specified [`DataType`].
fn cast_simple(val: &str, data_type: &DataType) -> Option<String> {
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
    use sqlparser::ast::{DataType, Expr, Ident, ObjectName, ObjectNamePart, TableFactor, Value};

    #[test]
    fn split_ts_handles_short_buffers() {
        let buf = [1u8, 2u8, 3u8];
        let (ts, rest) = super::split_ts(&buf);
        assert_eq!(ts, 0);
        assert_eq!(rest, &buf);
    }

    #[test]
    fn split_ts_parses_timestamp_and_rest() {
        let ts_val: u64 = 42;
        let mut buf = ts_val.to_be_bytes().to_vec();
        buf.extend_from_slice(b"hello");
        let (ts, rest) = super::split_ts(&buf);
        assert_eq!(ts, ts_val);
        assert_eq!(rest, b"hello");
    }

    #[test]
    fn object_name_to_ns_extracts_last_segment_lowercase() {
        let name = ObjectName(vec![
            ObjectNamePart::Identifier(Ident::new("Foo")),
            ObjectNamePart::Identifier(Ident::new("Bar")),
        ]);
        assert_eq!(super::object_name_to_ns(&name), Some("bar".to_string()));
    }

    #[test]
    fn object_name_to_ns_returns_none_for_empty() {
        let name = ObjectName(vec![]);
        assert!(super::object_name_to_ns(&name).is_none());
    }

    #[test]
    fn table_factor_to_ns_handles_table_and_non_table() {
        let table = TableFactor::Table {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("users"))]),
            alias: None,
            args: None,
            with_hints: vec![],
            version: None,
            with_ordinality: false,
            partitions: vec![],
            json_path: None,
            sample: None,
            index_hints: vec![],
        };
        assert_eq!(super::table_factor_to_ns(&table), Some("users".to_string()));

        let func = TableFactor::TableFunction {
            expr: Expr::Value(Value::Number("1".into(), false).into()),
            alias: None,
        };
        assert!(super::table_factor_to_ns(&func).is_none());
    }

    #[test]
    fn cast_simple_covers_types_and_errors() {
        assert_eq!(
            super::cast_simple("123", &DataType::Int(None)),
            Some("123".to_string())
        );
        assert!(super::cast_simple("abc", &DataType::Int(None)).is_none());
        assert_eq!(
            super::cast_simple("hi", &DataType::Text),
            Some("hi".to_string())
        );
        assert!(super::cast_simple("t", &DataType::Boolean).is_none());
    }
}
