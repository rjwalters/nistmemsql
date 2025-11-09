//! Database adapter for SQLLogicTest runner.

use std::{
    env,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use executor::SelectExecutor;
use md5::{Digest, Md5};
use parser::Parser;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use storage::Database;
use tokio::time::timeout;
use types::SqlValue;

use super::{
    execution::TestError,
    formatting::{format_sql_value, format_sql_value_canonical},
};

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
            db: Database::new(),
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

    /// Format and optionally hash result rows
    /// Returns either the full result set or an MD5 hash for large results (>8 values)
    fn format_result_rows(
        &self,
        rows: &[storage::Row],
        types: Vec<DefaultColumnType>,
    ) -> Result<DBOutput<DefaultColumnType>, TestError> {
        let formatted_rows: Vec<Vec<String>> = rows
            .iter()
            .map(|row| {
                row.values
                    .iter()
                    .enumerate()
                    .map(|(idx, val)| format_sql_value(val, types.get(idx)))
                    .collect()
            })
            .collect();

        // Count total values before flattening
        let total_values: usize = formatted_rows.iter().map(|r| r.len()).sum();

        // For hashing, we need to sort and join the original rows using canonical format
        if total_values > 8 {
            let mut hasher = Md5::new();
            // Use canonical format for hashing (no .000 suffix for integers)
            let canonical_rows: Vec<Vec<String>> = rows
                .iter()
                .map(|row| {
                    row.values
                        .iter()
                        .enumerate()
                        .map(|(idx, val)| format_sql_value_canonical(val, types.get(idx)))
                        .collect()
                })
                .collect();

            let mut sort_keys: Vec<_> = canonical_rows.iter().map(|row| row.join(" ")).collect();
            sort_keys.sort();
            for key in &sort_keys {
                hasher.update(key);
                hasher.update("\n");
            }
            let hash = format!("{:x}", hasher.finalize());
            let hash_string = format!("{} values hashing to {}", total_values, hash);
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text],
                rows: vec![vec![hash_string]],
            })
        } else {
            // Flatten multi-column results: each value becomes its own row
            // This is required for SQLLogicTest format where each value is on separate rows
            let mut flattened_rows: Vec<Vec<String>> = Vec::new();
            let mut flattened_types: Vec<DefaultColumnType> = Vec::new();

            // Get the type for single values (they're all treated as individual rows now)
            if !types.is_empty() {
                flattened_types = vec![types[0].clone(); total_values];
            }

            for row in formatted_rows {
                for val in row {
                    flattened_rows.push(vec![val]);
                }
            }

            Ok(DBOutput::Rows { types: flattened_types, rows: flattened_rows })
        }
    }

    fn execute_sql(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, TestError> {
        let stmt = Parser::parse_sql(sql)
            .map_err(|e| TestError::Execution(format!("Parse error: {:?}", e)))?;

        match stmt {
            ast::Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(&self.db);
                let rows = executor
                    .execute(&select_stmt)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                self.format_query_result(rows)
            }
            ast::Statement::CreateTable(create_stmt) => {
                executor::CreateTableExecutor::execute(&create_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::Insert(insert_stmt) => {
                let rows_affected =
                    executor::InsertExecutor::execute(&mut self.db, &insert_stmt)
                        .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            ast::Statement::Update(update_stmt) => {
                let rows_affected =
                    executor::UpdateExecutor::execute(&update_stmt, &mut self.db)
                        .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            ast::Statement::Delete(delete_stmt) => {
                let rows_affected =
                    executor::DeleteExecutor::execute(&delete_stmt, &mut self.db)
                        .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            ast::Statement::DropTable(drop_stmt) => {
                executor::DropTableExecutor::execute(&drop_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::AlterTable(alter_stmt) => {
                executor::AlterTableExecutor::execute(&alter_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateSchema(create_schema_stmt) => {
                executor::SchemaExecutor::execute_create_schema(&create_schema_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropSchema(drop_schema_stmt) => {
                executor::SchemaExecutor::execute_drop_schema(&drop_schema_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::SetSchema(set_schema_stmt) => {
                executor::SchemaExecutor::execute_set_schema(&set_schema_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::SetCatalog(set_stmt) => {
                executor::SchemaExecutor::execute_set_catalog(&set_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::SetNames(set_stmt) => {
                executor::SchemaExecutor::execute_set_names(&set_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::SetTimeZone(set_stmt) => {
                executor::SchemaExecutor::execute_set_time_zone(&set_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::Grant(grant_stmt) => {
                executor::GrantExecutor::execute_grant(&grant_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::Revoke(revoke_stmt) => {
                executor::RevokeExecutor::execute_revoke(&revoke_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateRole(create_role_stmt) => {
                executor::RoleExecutor::execute_create_role(&create_role_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropRole(drop_role_stmt) => {
                executor::RoleExecutor::execute_drop_role(&drop_role_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateDomain(create_domain_stmt) => {
                executor::DomainExecutor::execute_create_domain(&create_domain_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropDomain(drop_domain_stmt) => {
                executor::DomainExecutor::execute_drop_domain(&drop_domain_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateType(create_type_stmt) => {
                executor::TypeExecutor::execute_create_type(&create_type_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropType(drop_type_stmt) => {
                executor::TypeExecutor::execute_drop_type(&drop_type_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateAssertion(create_assertion_stmt) => {
                executor::advanced_objects::execute_create_assertion(
                    &create_assertion_stmt,
                    &mut self.db,
                )
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropAssertion(drop_assertion_stmt) => {
                executor::advanced_objects::execute_drop_assertion(
                    &drop_assertion_stmt,
                    &mut self.db,
                )
                .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateView(create_view_stmt) => {
                executor::advanced_objects::execute_create_view(&create_view_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropView(drop_view_stmt) => {
                executor::advanced_objects::execute_drop_view(&drop_view_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateIndex(create_index_stmt) => {
                executor::IndexExecutor::execute(&create_index_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropIndex(drop_index_stmt) => {
                executor::IndexExecutor::execute_drop(&drop_index_stmt, &mut self.db)
                    .map_err(|e| TestError::Execution(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            // Unimplemented statements return success for now
            ast::Statement::BeginTransaction(_)
            | ast::Statement::Commit(_)
            | ast::Statement::Rollback(_)
            | ast::Statement::Savepoint(_)
            | ast::Statement::RollbackToSavepoint(_)
            | ast::Statement::ReleaseSavepoint(_)
            | ast::Statement::SetTransaction(_)
            | ast::Statement::CreateSequence(_)
            | ast::Statement::DropSequence(_)
            | ast::Statement::AlterSequence(_)
            | ast::Statement::CreateCollation(_)
            | ast::Statement::DropCollation(_)
            | ast::Statement::CreateCharacterSet(_)
            | ast::Statement::DropCharacterSet(_)
            | ast::Statement::CreateTranslation(_)
            | ast::Statement::DropTranslation(_)
            | ast::Statement::CreateTrigger(_)
            | ast::Statement::DropTrigger(_)
            | ast::Statement::DeclareCursor(_)
            | ast::Statement::OpenCursor(_)
            | ast::Statement::Fetch(_)
            | ast::Statement::CloseCursor(_) => Ok(DBOutput::StatementComplete(0)),
        }
    }

    fn format_query_result(
        &self,
        rows: Vec<storage::Row>,
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
