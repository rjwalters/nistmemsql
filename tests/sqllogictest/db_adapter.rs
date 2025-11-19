//! Database adapter for SQLLogicTest runner.

use std::{
    cell::RefCell,
    collections::HashSet,
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use vibesql_executor::{cache::{QueryResultCache, QuerySignature}, SelectExecutor};
use vibesql_parser::Parser;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use vibesql_storage::Database;
use tokio::time::timeout;
use vibesql_types::SqlValue;

use super::{execution::TestError, formatting::format_sql_value};

/// Safely truncate a string to a maximum byte length, respecting UTF-8 character boundaries.
/// Returns the truncated string with "..." appended if truncation occurred.
fn truncate_sql(sql: &str, max_bytes: usize) -> String {
    if sql.len() <= max_bytes {
        return sql.to_string();
    }

    // Find the last valid UTF-8 character boundary at or before max_bytes
    let mut boundary = max_bytes;
    while boundary > 0 && !sql.is_char_boundary(boundary) {
        boundary -= 1;
    }

    format!("{}...", &sql[..boundary])
}

// Thread-local Database pool for reuse across test files within the same worker thread.
// This avoids the overhead of creating a new Database for each test file (622 files in full suite).
// Each worker thread gets its own cached Database that is reset between files.
thread_local! {
    static DB_POOL: RefCell<Option<Database>> = RefCell::new(None);
}

/// Get a reset Database from the thread-local pool.
/// First call creates a new Database, subsequent calls reuse and reset the existing one.
/// Uses take/replace pattern to avoid cloning overhead.
fn get_pooled_database() -> Database {
    DB_POOL.with(|pool| {
        let mut pool_ref = pool.borrow_mut();
        match pool_ref.take() {
            Some(mut db) => {
                // Reuse existing database after resetting it (no clone)
                db.reset();
                db
            }
            None => {
                // First use - create new database with MySQL mode (default)
                // The SQLLogicTest suite was generated from MySQL 8 and expects MySQL semantics
                // including decimal division (INTEGER / INTEGER → DECIMAL)
                vibesql_storage::Database::new()
            }
        }
    })
}

/// Statement timing statistics
#[derive(Debug, Default)]
struct StatementTimings {
    count: usize,
    total_duration: Duration,
    max_duration: Duration,
    max_sql: Option<String>,
}

impl StatementTimings {
    fn record(&mut self, duration: Duration, sql: &str) {
        self.count += 1;
        self.total_duration += duration;
        if duration > self.max_duration {
            self.max_duration = duration;
            // Store truncated SQL for the slowest statement
            self.max_sql = Some(truncate_sql(sql, 100));
        }
    }
}

pub struct VibeSqlDB {
    db: Database,
    query_count: usize,
    verbose: bool,
    worker_id: Option<usize>,
    current_file: Option<String>,
    file_start_time: Option<Instant>,
    query_timeout_ms: u64,
    timed_out_queries: usize,
    result_cache: Arc<QueryResultCache>,
    cache_enabled: bool,
    cache_hits: usize,
    cache_misses: usize,
    // Statement timing instrumentation
    timing_enabled: bool,
    slow_query_threshold_ms: u64,
    insert_timings: StatementTimings,
    create_index_timings: StatementTimings,
    select_timings: StatementTimings,
    other_timings: StatementTimings,
}

impl VibeSqlDB {
    pub fn new() -> Self {
        let verbose = env::var("SQLLOGICTEST_VERBOSE")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);

        let worker_id = env::var("SQLLOGICTEST_WORKER_ID").ok().and_then(|s| s.parse().ok());

        let query_timeout_ms = env::var("SQLLOGICTEST_QUERY_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(500); // Default: 500ms per query

        // Query result cache: enabled by default, can be disabled via env var
        let cache_enabled = env::var("SQLLOGICTEST_CACHE_ENABLED")
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true); // Enabled by default

        let cache_size = env::var("SQLLOGICTEST_CACHE_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10000); // Default: 10,000 entries

        // Statement timing: enabled by default, configurable
        let timing_enabled = env::var("SQLLOGICTEST_TIMING")
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true); // Enabled by default

        let slow_query_threshold_ms = env::var("SQLLOGICTEST_SLOW_THRESHOLD_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000); // Default: 1000ms (1 second)

        Self {
            db: get_pooled_database(),
            query_count: 0,
            verbose,
            worker_id,
            current_file: None,
            file_start_time: None,
            query_timeout_ms,
            timed_out_queries: 0,
            result_cache: Arc::new(QueryResultCache::new(cache_size)),
            cache_enabled,
            cache_hits: 0,
            cache_misses: 0,
            timing_enabled,
            slow_query_threshold_ms,
            insert_timings: StatementTimings::default(),
            create_index_timings: StatementTimings::default(),
            select_timings: StatementTimings::default(),
            other_timings: StatementTimings::default(),
        }
    }

    #[allow(dead_code)]
    fn start_test_file(&mut self, file_path: &str) {
        self.current_file = Some(file_path.to_string());
        self.file_start_time = Some(Instant::now());
        self.query_count = 0;

        if let Some(worker_id) = self.worker_id {
            eprintln!("[Worker {}] Starting: {}", worker_id, file_path);
        } else {
            eprintln!("Starting: {}", file_path);
        }
    }

    #[allow(dead_code)]
    fn finish_test_file(&self, result: &Result<(), TestError>) {
        if let (Some(file_path), Some(start_time)) = (&self.current_file, &self.file_start_time) {
            let elapsed = start_time.elapsed();
            let elapsed_secs = elapsed.as_secs_f64();

            match result {
                Ok(_) => {
                    if let Some(worker_id) = self.worker_id {
                        eprintln!("[Worker {}] ✓ {} ({:.2}s)", worker_id, file_path, elapsed_secs);
                    } else {
                        eprintln!("✓ {} ({:.2}s)", file_path, elapsed_secs);
                    }
                }
                Err(e) => {
                    if let Some(worker_id) = self.worker_id {
                        eprintln!(
                            "[Worker {}] ✗ {} ({:.2}s): {}",
                            worker_id, file_path, elapsed_secs, e
                        );
                    } else {
                        eprintln!("✗ {} ({:.2}s): {}", file_path, elapsed_secs, e);
                    }
                }
            }
        }
    }

    /// Format result rows for SQLLogicTest
    /// Converts storage rows to the format expected by sqllogictest-rs
    fn format_result_rows(
        &self,
        rows: &[vibesql_storage::Row],
        types: Vec<DefaultColumnType>,
    ) -> Result<DBOutput<DefaultColumnType>, TestError> {
        let formatted_rows: Vec<Vec<String>> = rows
            .iter()
            .map(|row| {
                row.values
                    .iter()
                    .enumerate()
                    .map(|(col_idx, val)| format_sql_value(val, types.get(col_idx)))
                    .collect()
            })
            .collect();

        Ok(DBOutput::Rows { types, rows: formatted_rows })
    }

    fn execute_sql(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, TestError> {
        use std::time::Instant;

        let profile_queries = env::var("SQLLOGICTEST_PROFILE").is_ok();
        let total_start = if profile_queries { Some(Instant::now()) } else { None };

        let parse_start = if profile_queries { Some(Instant::now()) } else { None };
        let stmt = Parser::parse_sql(sql)
            .map_err(|e| TestError::Execution(format!("Parse error: {:?}", e)))?;
        let parse_time = parse_start.map(|s| s.elapsed());

        match stmt {
            vibesql_ast::Statement::Select(select_stmt) => {
                // Try cache first if enabled
                if self.cache_enabled {
                    let signature = QuerySignature::from_sql(sql);
                    if let Some((cached_rows, _schema)) = self.result_cache.get(&signature) {
                        self.cache_hits += 1;
                        return self.format_query_result(cached_rows);
                    }
                    self.cache_misses += 1;
                }

                // Cache miss or cache disabled - execute query
                let exec_start = if profile_queries { Some(Instant::now()) } else { None };
                let executor = SelectExecutor::new(&self.db);
                let rows = executor
                    .execute(&select_stmt)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                let exec_time = exec_start.map(|s| s.elapsed());

                // Log profiling info if enabled
                if let (Some(parse_elapsed), Some(exec_elapsed), Some(total_elapsed)) =
                   (parse_time, exec_time, total_start.map(|s| s.elapsed())) {
                    if exec_elapsed.as_millis() > 10 {  // Only log queries >10ms
                        let sql_preview = truncate_sql(sql, 80);
                        eprintln!("🔍 Query #{}: parse={:.2}ms, exec={:.2}ms, total={:.2}ms | {}",
                            self.query_count,
                            parse_elapsed.as_secs_f64() * 1000.0,
                            exec_elapsed.as_secs_f64() * 1000.0,
                            total_elapsed.as_secs_f64() * 1000.0,
                            sql_preview);
                    }
                }

                // Cache the result if cache is enabled
                if self.cache_enabled {
                    use vibesql_catalog::{ColumnSchema, TableSchema};
                    use vibesql_executor::schema::CombinedSchema;

                    let signature = QuerySignature::from_sql(sql);

                    // Create a simple schema from the result rows
                    let schema = if let Some(first_row) = rows.first() {
                        let columns: Vec<ColumnSchema> = first_row.values.iter().enumerate().map(|(i, val)| {
                            ColumnSchema {
                                name: format!("col{}", i),
                                data_type: val.get_type(),
                                nullable: val.is_null(),
                                default_value: None,
                            }
                        }).collect();
                        let table_schema = TableSchema::new("result".to_string(), columns);
                        CombinedSchema::from_table("result".to_string(), table_schema)
                    } else {
                        // Empty result - create empty schema
                        let table_schema = TableSchema::new("result".to_string(), vec![]);
                        CombinedSchema::from_table("result".to_string(), table_schema)
                    };

                    // Extract table names from SELECT statement for cache invalidation
                    let tables = self.extract_table_names(&select_stmt);

                    self.result_cache.insert(signature, rows.clone(), schema, tables);
                }

                self.format_query_result(rows)
            }
            vibesql_ast::Statement::CreateTable(create_stmt) => {
                vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::Insert(insert_stmt) => {
                // Invalidate cache for this table
                if self.cache_enabled {
                    self.result_cache.invalidate_table(&insert_stmt.table_name);
                }

                let rows_affected =
                    vibesql_executor::InsertExecutor::execute(&mut self.db, &insert_stmt)
                        .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            vibesql_ast::Statement::Update(update_stmt) => {
                // Invalidate cache for this table
                if self.cache_enabled {
                    self.result_cache.invalidate_table(&update_stmt.table_name);
                }

                let rows_affected =
                    vibesql_executor::UpdateExecutor::execute(&update_stmt, &mut self.db)
                        .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            vibesql_ast::Statement::Delete(delete_stmt) => {
                // Invalidate cache for this table
                if self.cache_enabled {
                    self.result_cache.invalidate_table(&delete_stmt.table_name);
                }

                let rows_affected =
                    vibesql_executor::DeleteExecutor::execute(&delete_stmt, &mut self.db)
                        .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            vibesql_ast::Statement::DropTable(drop_stmt) => {
                // Invalidate cache for this table
                if self.cache_enabled {
                    self.result_cache.invalidate_table(&drop_stmt.table_name);
                }

                vibesql_executor::DropTableExecutor::execute(&drop_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::AlterTable(alter_stmt) => {
                vibesql_executor::AlterTableExecutor::execute(&alter_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateSchema(create_schema_stmt) => {
                vibesql_executor::SchemaExecutor::execute_create_schema(&create_schema_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropSchema(drop_schema_stmt) => {
                vibesql_executor::SchemaExecutor::execute_drop_schema(&drop_schema_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::SetSchema(set_schema_stmt) => {
                vibesql_executor::SchemaExecutor::execute_set_schema(&set_schema_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::SetCatalog(set_stmt) => {
                vibesql_executor::SchemaExecutor::execute_set_catalog(&set_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::SetNames(set_stmt) => {
                vibesql_executor::SchemaExecutor::execute_set_names(&set_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::SetVariable(set_var_stmt) => {
                vibesql_executor::SchemaExecutor::execute_set_variable(&set_var_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::SetTimeZone(set_stmt) => {
                vibesql_executor::SchemaExecutor::execute_set_time_zone(&set_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::Grant(grant_stmt) => {
                vibesql_executor::GrantExecutor::execute_grant(&grant_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::Revoke(revoke_stmt) => {
                vibesql_executor::RevokeExecutor::execute_revoke(&revoke_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateRole(create_role_stmt) => {
                vibesql_executor::RoleExecutor::execute_create_role(&create_role_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropRole(drop_role_stmt) => {
                vibesql_executor::RoleExecutor::execute_drop_role(&drop_role_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateDomain(create_domain_stmt) => {
                vibesql_executor::DomainExecutor::execute_create_domain(&create_domain_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropDomain(drop_domain_stmt) => {
                vibesql_executor::DomainExecutor::execute_drop_domain(&drop_domain_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateType(create_type_stmt) => {
                vibesql_executor::TypeExecutor::execute_create_type(&create_type_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropType(drop_type_stmt) => {
                vibesql_executor::TypeExecutor::execute_drop_type(&drop_type_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateAssertion(create_assertion_stmt) => {
                vibesql_executor::advanced_objects::execute_create_assertion(
                    &create_assertion_stmt,
                    &mut self.db,
                )
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropAssertion(drop_assertion_stmt) => {
                vibesql_executor::advanced_objects::execute_drop_assertion(
                    &drop_assertion_stmt,
                    &mut self.db,
                )
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateView(create_view_stmt) => {
                vibesql_executor::advanced_objects::execute_create_view(&create_view_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropView(drop_view_stmt) => {
                vibesql_executor::advanced_objects::execute_drop_view(&drop_view_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateIndex(create_index_stmt) => {
                vibesql_executor::IndexExecutor::execute(&create_index_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropIndex(drop_index_stmt) => {
                vibesql_executor::IndexExecutor::execute_drop(&drop_index_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::Analyze(analyze_stmt) => {
                vibesql_executor::AnalyzeExecutor::execute(&analyze_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::Reindex(reindex_stmt) => {
                vibesql_executor::IndexExecutor::execute_reindex(&reindex_stmt, &self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::CreateTrigger(create_trigger_stmt) => {
                vibesql_executor::TriggerExecutor::create_trigger(&mut self.db, &create_trigger_stmt)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::DropTrigger(drop_trigger_stmt) => {
                vibesql_executor::TriggerExecutor::drop_trigger(&mut self.db, &drop_trigger_stmt)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            // Unimplemented statements return success for now
            vibesql_ast::Statement::BeginTransaction(_)
            | vibesql_ast::Statement::Commit(_)
            | vibesql_ast::Statement::Rollback(_)
            | vibesql_ast::Statement::Savepoint(_)
            | vibesql_ast::Statement::RollbackToSavepoint(_)
            | vibesql_ast::Statement::ReleaseSavepoint(_)
            | vibesql_ast::Statement::SetTransaction(_)
            | vibesql_ast::Statement::CreateSequence(_)
            | vibesql_ast::Statement::DropSequence(_)
            | vibesql_ast::Statement::AlterSequence(_)
            | vibesql_ast::Statement::CreateCollation(_)
            | vibesql_ast::Statement::DropCollation(_)
            | vibesql_ast::Statement::CreateCharacterSet(_)
            | vibesql_ast::Statement::DropCharacterSet(_)
            | vibesql_ast::Statement::CreateTranslation(_)
            | vibesql_ast::Statement::DropTranslation(_)
            | vibesql_ast::Statement::DeclareCursor(_)
            | vibesql_ast::Statement::OpenCursor(_)
            | vibesql_ast::Statement::Fetch(_)
            | vibesql_ast::Statement::CloseCursor(_)
            | vibesql_ast::Statement::CreateProcedure(_)
            | vibesql_ast::Statement::DropProcedure(_)
            | vibesql_ast::Statement::CreateFunction(_)
            | vibesql_ast::Statement::DropFunction(_)
            | vibesql_ast::Statement::Call(_)
            | vibesql_ast::Statement::TruncateTable(_)
            | vibesql_ast::Statement::ShowTables(_)
            | vibesql_ast::Statement::ShowDatabases(_)
            | vibesql_ast::Statement::ShowColumns(_)
            | vibesql_ast::Statement::ShowIndex(_)
            | vibesql_ast::Statement::ShowCreateTable(_)
            | vibesql_ast::Statement::Describe(_)
            | vibesql_ast::Statement::AlterTrigger(_) => Ok(DBOutput::StatementComplete(0)),
        }
    }

    fn format_query_result(
        &self,
        rows: Vec<vibesql_storage::Row>,
    ) -> Result<DBOutput<DefaultColumnType>, TestError> {
        if rows.is_empty() {
            return Ok(DBOutput::Rows { types: vec![], rows: vec![] });
        }

        let types: Vec<DefaultColumnType> = rows[0]
            .values
            .iter()
            .map(|val| match val {
                SqlValue::Integer(_)
                | SqlValue::Smallint(_)
                | SqlValue::Bigint(_)
                | SqlValue::Unsigned(_) => DefaultColumnType::Integer,
                SqlValue::Float(_)
                | SqlValue::Real(_)
                | SqlValue::Double(_)
                | SqlValue::Numeric(_) => DefaultColumnType::FloatingPoint,
                SqlValue::Varchar(_)
                | SqlValue::Character(_)
                | SqlValue::Date(_)
                | SqlValue::Time(_)
                | SqlValue::Timestamp(_)
                | SqlValue::Interval(_) => DefaultColumnType::Text,
                SqlValue::Boolean(_) => DefaultColumnType::Integer,
                SqlValue::Null => DefaultColumnType::Any,
            })
            .collect();

        self.format_result_rows(&rows, types)
    }

    /// Detect statement type from SQL for timing categorization
    fn detect_statement_type(&self, sql: &str) -> &str {
        let sql_upper = sql.trim().to_uppercase();
        if sql_upper.starts_with("INSERT") {
            "INSERT"
        } else if sql_upper.starts_with("CREATE INDEX") || sql_upper.starts_with("CREATE UNIQUE INDEX") {
            "CREATE_INDEX"
        } else if sql_upper.starts_with("SELECT") {
            "SELECT"
        } else {
            "OTHER"
        }
    }

    /// Extract table names from a SELECT statement for cache invalidation
    fn extract_table_names(&self, select: &vibesql_ast::SelectStmt) -> HashSet<String> {
        let mut tables = HashSet::new();

        // Extract from FROM clause
        if let Some(ref from) = select.from {
            self.extract_table_names_from_from(from, &mut tables);
        }

        tables
    }

    fn extract_table_names_from_from(
        &self,
        from: &vibesql_ast::FromClause,
        tables: &mut HashSet<String>,
    ) {
        match from {
            vibesql_ast::FromClause::Table { name, .. } => {
                // Handle schema.table format
                let table_name = if let Some(pos) = name.rfind('.') {
                    &name[pos + 1..]
                } else {
                    name
                };
                tables.insert(table_name.to_string());
            }
            vibesql_ast::FromClause::Join { left, right, .. } => {
                self.extract_table_names_from_from(left, tables);
                self.extract_table_names_from_from(right, tables);
            }
            vibesql_ast::FromClause::Subquery { query, .. } => {
                // Recursively extract from subquery
                let subquery_tables = self.extract_table_names(query);
                tables.extend(subquery_tables);
            }
        }
    }
}

#[async_trait]
impl AsyncDB for VibeSqlDB {
    type Error = TestError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.query_count += 1;

        // Log query progress if verbose mode enabled
        if self.verbose {
            let log_interval = env::var("SQLLOGICTEST_LOG_QUERY_INTERVAL")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100);

            if self.query_count % log_interval == 0 {
                eprintln!("  Query {}: {}", self.query_count, truncate_sql(sql, 60));
            }
        }

        // Start timing if enabled
        let stmt_start = if self.timing_enabled { Some(Instant::now()) } else { None };
        let stmt_type = if self.timing_enabled { Some(self.detect_statement_type(sql).to_string()) } else { None };

        // Execute query with per-query timeout
        let timeout_duration = Duration::from_millis(self.query_timeout_ms);
        let result = match timeout(timeout_duration, self.execute_sql_async(sql)).await {
            Ok(result) => result,
            Err(_) => {
                self.timed_out_queries += 1;
                eprintln!(
                    "⏱️  Query timeout ({}ms): Query {}: {}",
                    self.query_timeout_ms, self.query_count, truncate_sql(sql, 80)
                );

                // Log timeout stats if verbose
                if self.verbose {
                    eprintln!("  Total timed out queries so far: {}", self.timed_out_queries);
                }

                // Skip the timed-out query and continue
                Ok(DBOutput::Rows { types: vec![], rows: vec![] })
            }
        };

        // Record timing if enabled
        if let (Some(start), Some(stype)) = (stmt_start, stmt_type) {
            let elapsed = start.elapsed();

            // Record in appropriate bucket
            match stype.as_str() {
                "INSERT" => self.insert_timings.record(elapsed, sql),
                "CREATE_INDEX" => self.create_index_timings.record(elapsed, sql),
                "SELECT" => self.select_timings.record(elapsed, sql),
                _ => self.other_timings.record(elapsed, sql),
            }

            // Log slow queries
            let elapsed_ms = elapsed.as_millis() as u64;
            if elapsed_ms >= self.slow_query_threshold_ms {
                eprintln!(
                    "[SLOW QUERY] {:.3}s - {} - {}",
                    elapsed.as_secs_f64(),
                    stype,
                    truncate_sql(sql, 100)
                );
            }
        }

        result
    }

    async fn shutdown(&mut self) {
        // Log statement timing summary
        if self.timing_enabled {
            let total_stmts = self.insert_timings.count
                + self.create_index_timings.count
                + self.select_timings.count
                + self.other_timings.count;

            if total_stmts > 0 {
                eprintln!("\n📊 Statement Timing Summary:");
                eprintln!("  Total statements: {}", total_stmts);

                let total_time = self.insert_timings.total_duration
                    + self.create_index_timings.total_duration
                    + self.select_timings.total_duration
                    + self.other_timings.total_duration;
                eprintln!("  Total time: {:.2}s", total_time.as_secs_f64());

                // INSERT statistics
                if self.insert_timings.count > 0 {
                    let avg_ms = self.insert_timings.total_duration.as_millis() as f64
                        / self.insert_timings.count as f64;
                    eprintln!("\n  INSERT statements: {}", self.insert_timings.count);
                    eprintln!("    Total time: {:.2}s", self.insert_timings.total_duration.as_secs_f64());
                    eprintln!("    Average: {:.2}ms", avg_ms);
                    eprintln!("    Slowest: {:.2}s", self.insert_timings.max_duration.as_secs_f64());
                    if let Some(ref sql) = self.insert_timings.max_sql {
                        eprintln!("      SQL: {}", sql);
                    }
                }

                // CREATE INDEX statistics
                if self.create_index_timings.count > 0 {
                    let avg_ms = self.create_index_timings.total_duration.as_millis() as f64
                        / self.create_index_timings.count as f64;
                    eprintln!("\n  CREATE INDEX statements: {}", self.create_index_timings.count);
                    eprintln!("    Total time: {:.2}s", self.create_index_timings.total_duration.as_secs_f64());
                    eprintln!("    Average: {:.2}ms", avg_ms);
                    eprintln!("    Slowest: {:.2}s", self.create_index_timings.max_duration.as_secs_f64());
                    if let Some(ref sql) = self.create_index_timings.max_sql {
                        eprintln!("      SQL: {}", sql);
                    }
                }

                // SELECT statistics
                if self.select_timings.count > 0 {
                    let avg_ms = self.select_timings.total_duration.as_millis() as f64
                        / self.select_timings.count as f64;
                    eprintln!("\n  SELECT queries: {}", self.select_timings.count);
                    eprintln!("    Total time: {:.2}s", self.select_timings.total_duration.as_secs_f64());
                    eprintln!("    Average: {:.2}ms", avg_ms);
                    eprintln!("    Slowest: {:.2}s", self.select_timings.max_duration.as_secs_f64());
                    if let Some(ref sql) = self.select_timings.max_sql {
                        eprintln!("      SQL: {}", sql);
                    }
                }

                // OTHER statistics
                if self.other_timings.count > 0 {
                    let avg_ms = self.other_timings.total_duration.as_millis() as f64
                        / self.other_timings.count as f64;
                    eprintln!("\n  Other statements: {}", self.other_timings.count);
                    eprintln!("    Total time: {:.2}s", self.other_timings.total_duration.as_secs_f64());
                    eprintln!("    Average: {:.2}ms", avg_ms);
                    eprintln!("    Slowest: {:.2}s", self.other_timings.max_duration.as_secs_f64());
                    if let Some(ref sql) = self.other_timings.max_sql {
                        eprintln!("      SQL: {}", sql);
                    }
                }

                eprintln!();
            }
        }

        // Log final timeout statistics
        if self.timed_out_queries > 0 {
            eprintln!("📊 Query Timeout Summary:");
            eprintln!("  Total queries executed: {}", self.query_count);
            eprintln!("  Queries that timed out: {}", self.timed_out_queries);
            eprintln!("  Timeout per query: {}ms", self.query_timeout_ms);
        }

        // Log cache statistics if cache was enabled
        if self.cache_enabled {
            let total_cache_requests = self.cache_hits + self.cache_misses;
            if total_cache_requests > 0 {
                let hit_rate = (self.cache_hits as f64 / total_cache_requests as f64) * 100.0;
                eprintln!("📊 Query Result Cache Summary:");
                eprintln!("  Cache hits: {}", self.cache_hits);
                eprintln!("  Cache misses: {}", self.cache_misses);
                eprintln!("  Hit rate: {:.2}%", hit_rate);
                eprintln!("  Cache size: {} / {} entries",
                    self.result_cache.stats().size,
                    self.result_cache.max_size());
            }
        }
    }
}

impl VibeSqlDB {
    /// Execute SQL asynchronously (wrapper for query execution)
    async fn execute_sql_async(
        &mut self,
        sql: &str,
    ) -> Result<DBOutput<DefaultColumnType>, TestError> {
        self.execute_sql(sql)
    }
}

impl Drop for VibeSqlDB {
    fn drop(&mut self) {
        // Return database to thread-local pool for reuse
        // Only return if pool is empty to avoid conflicts with multiple instances
        DB_POOL.with(|pool| {
            let mut pool_ref = pool.borrow_mut();
            if pool_ref.is_none() {
                *pool_ref = Some(std::mem::take(&mut self.db));
            }
        });
    }
}
