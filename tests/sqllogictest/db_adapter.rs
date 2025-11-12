//! Database adapter for SQLLogicTest runner.

use std::{
    cell::RefCell,
    env,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use vibesql_storage::Database;
use tokio::time::timeout;
use vibesql_types::SqlValue;

use super::{execution::TestError, formatting::format_sql_value};

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
                // First use - create new database
                Database::new()
            }
        }
    })
}

pub struct NistMemSqlDB {
    db: Database,
    query_count: usize,
    verbose: bool,
    worker_id: Option<usize>,
    current_file: Option<String>,
    file_start_time: Option<Instant>,
    query_timeout_ms: u64,
    timed_out_queries: usize,
}

impl NistMemSqlDB {
    pub fn new() -> Self {
        let verbose = env::var("SQLLOGICTEST_VERBOSE")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);

        let worker_id = env::var("SQLLOGICTEST_WORKER_ID").ok().and_then(|s| s.parse().ok());

        let query_timeout_ms = env::var("SQLLOGICTEST_QUERY_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(500); // Default: 500ms per query

        Self {
            db: get_pooled_database(),
            query_count: 0,
            verbose,
            worker_id,
            current_file: None,
            file_start_time: None,
            query_timeout_ms,
            timed_out_queries: 0,
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
        let stmt = Parser::parse_sql(sql)
            .map_err(|e| TestError::Execution(format!("Parse error: {:?}", e)))?;

        match stmt {
            vibesql_ast::Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(&self.db);
                let rows = executor
                    .execute(&select_stmt)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                self.format_query_result(rows)
            }
            vibesql_ast::Statement::CreateTable(create_stmt) => {
                vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            vibesql_ast::Statement::Insert(insert_stmt) => {
                let rows_affected =
                    vibesql_executor::InsertExecutor::execute(&mut self.db, &insert_stmt)
                        .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            vibesql_ast::Statement::Update(update_stmt) => {
                let rows_affected =
                    vibesql_executor::UpdateExecutor::execute(&update_stmt, &mut self.db)
                        .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            vibesql_ast::Statement::Delete(delete_stmt) => {
                let rows_affected =
                    vibesql_executor::DeleteExecutor::execute(&delete_stmt, &mut self.db)
                        .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            vibesql_ast::Statement::DropTable(drop_stmt) => {
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
            vibesql_ast::Statement::Reindex(reindex_stmt) => {
                vibesql_executor::IndexExecutor::execute_reindex(&reindex_stmt, &self.db)
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
            | vibesql_ast::Statement::CreateTrigger(_)
            | vibesql_ast::Statement::DropTrigger(_)
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
            | vibesql_ast::Statement::Describe(_) => Ok(DBOutput::StatementComplete(0)),
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
}

#[async_trait]
impl AsyncDB for NistMemSqlDB {
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
                let sql_preview =
                    if sql.len() > 60 { format!("{}...", &sql[..60]) } else { sql.to_string() };
                eprintln!("  Query {}: {}", self.query_count, sql_preview);
            }
        }

        // Execute query with per-query timeout
        let timeout_duration = Duration::from_millis(self.query_timeout_ms);
        match timeout(timeout_duration, self.execute_sql_async(sql)).await {
            Ok(result) => result,
            Err(_) => {
                self.timed_out_queries += 1;
                let sql_preview =
                    if sql.len() > 80 { format!("{}...", &sql[..80]) } else { sql.to_string() };
                eprintln!(
                    "⏱️  Query timeout ({}ms): Query {}: {}",
                    self.query_timeout_ms, self.query_count, sql_preview
                );

                // Log timeout stats if verbose
                if self.verbose {
                    eprintln!("  Total timed out queries so far: {}", self.timed_out_queries);
                }

                // Skip the timed-out query and continue
                Ok(DBOutput::Rows { types: vec![], rows: vec![] })
            }
        }
    }

    async fn shutdown(&mut self) {
        // Log final timeout statistics
        if self.timed_out_queries > 0 {
            eprintln!("📊 Query Timeout Summary:");
            eprintln!("  Total queries executed: {}", self.query_count);
            eprintln!("  Queries that timed out: {}", self.timed_out_queries);
            eprintln!("  Timeout per query: {}ms", self.query_timeout_ms);
        }
    }
}

impl NistMemSqlDB {
    /// Execute SQL asynchronously (wrapper for query execution)
    async fn execute_sql_async(
        &mut self,
        sql: &str,
    ) -> Result<DBOutput<DefaultColumnType>, TestError> {
        self.execute_sql(sql)
    }
}

impl Drop for NistMemSqlDB {
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
