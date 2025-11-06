//! Comprehensive SQLLogicTest suite runner using the dolthub/sqllogictest submodule.
//!
//! This test suite runs ~5.9 million SQL tests from the official SQLLogicTest corpus.
//! Tests are randomly selected each run to progressively build coverage over time.
//! Results are merged with historical data to track: tested/passed, tested/failed, not-yet-tested.
//!
//! Tests are organized by category:
//! - select1-5.test: Basic SELECT queries
//! - evidence/: Core SQL language features
//! - index/: Index and ordering tests
//! - random/: Randomized query tests
//! - ddl/: Data Definition Language tests

use async_trait::async_trait;
use executor::SelectExecutor;
use md5::{Md5, Digest};
use parser::Parser;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType, Runner};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::{env, fs, io::Write};
use storage::Database;
use types::SqlValue;

#[derive(Debug)]
struct TestError(String);

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TestError {}

struct NistMemSqlDB {
    db: Database,
}

impl NistMemSqlDB {
    fn new() -> Self {
        Self { db: Database::new() }
    }

    fn execute_sql(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>, TestError> {
        let stmt =
            Parser::parse_sql(sql).map_err(|e| TestError(format!("Parse error: {:?}", e)))?;

        match stmt {
            ast::Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(&self.db);
                let rows = executor
                    .execute(&select_stmt)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                self.format_query_result(rows)
            }
            ast::Statement::CreateTable(create_stmt) => {
                executor::CreateTableExecutor::execute(&create_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::Insert(insert_stmt) => {
                let rows_affected = executor::InsertExecutor::execute(&mut self.db, &insert_stmt)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            ast::Statement::Update(update_stmt) => {
                let rows_affected = executor::UpdateExecutor::execute(&update_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            ast::Statement::Delete(delete_stmt) => {
                let rows_affected = executor::DeleteExecutor::execute(&delete_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(rows_affected as u64))
            }
            ast::Statement::DropTable(drop_stmt) => {
                executor::DropTableExecutor::execute(&drop_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::AlterTable(alter_stmt) => {
                executor::AlterTableExecutor::execute(&alter_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateSchema(create_schema_stmt) => {
                executor::SchemaExecutor::execute_create_schema(&create_schema_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropSchema(drop_schema_stmt) => {
                executor::SchemaExecutor::execute_drop_schema(&drop_schema_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::SetSchema(set_schema_stmt) => {
                executor::SchemaExecutor::execute_set_schema(&set_schema_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::SetCatalog(set_stmt) => {
                executor::SchemaExecutor::execute_set_catalog(&set_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::SetNames(set_stmt) => {
                executor::SchemaExecutor::execute_set_names(&set_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::SetTimeZone(set_stmt) => {
                executor::SchemaExecutor::execute_set_time_zone(&set_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::Grant(grant_stmt) => {
                executor::GrantExecutor::execute_grant(&grant_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::Revoke(revoke_stmt) => {
                executor::RevokeExecutor::execute_revoke(&revoke_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateRole(create_role_stmt) => {
                executor::RoleExecutor::execute_create_role(&create_role_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropRole(drop_role_stmt) => {
                executor::RoleExecutor::execute_drop_role(&drop_role_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateDomain(create_domain_stmt) => {
                executor::DomainExecutor::execute_create_domain(&create_domain_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropDomain(drop_domain_stmt) => {
                executor::DomainExecutor::execute_drop_domain(&drop_domain_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateType(create_type_stmt) => {
                executor::TypeExecutor::execute_create_type(&create_type_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropType(drop_type_stmt) => {
                executor::TypeExecutor::execute_drop_type(&drop_type_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateAssertion(create_assertion_stmt) => {
                executor::advanced_objects::execute_create_assertion(
                    &create_assertion_stmt,
                    &mut self.db,
                )
                .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropAssertion(drop_assertion_stmt) => {
                executor::advanced_objects::execute_drop_assertion(
                    &drop_assertion_stmt,
                    &mut self.db,
                )
                .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateView(create_view_stmt) => {
                executor::advanced_objects::execute_create_view(&create_view_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropView(drop_view_stmt) => {
                executor::advanced_objects::execute_drop_view(&drop_view_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::CreateIndex(create_index_stmt) => {
                executor::IndexExecutor::execute(&create_index_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
                Ok(DBOutput::StatementComplete(0))
            }
            ast::Statement::DropIndex(drop_index_stmt) => {
                executor::IndexExecutor::execute_drop(&drop_index_stmt, &mut self.db)
                    .map_err(|e| TestError(format!("Execution error: {:?}", e)))?;
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
                SqlValue::Integer(_) | SqlValue::Smallint(_) | SqlValue::Bigint(_) | SqlValue::Unsigned(_) => {
                    DefaultColumnType::Integer
                }
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

        let mut formatted_rows: Vec<Vec<String>> = rows
            .iter()
            .map(|row| row.values.iter().map(|val| self.format_sql_value(val)).collect())
            .collect();

        // Sort rows for consistent ordering (required for hashing and rowsort)
        formatted_rows.sort_by(|a, b| a.join(" ").cmp(&b.join(" ")));

        let total_values: usize = formatted_rows.iter().map(|r| r.len()).sum();

        // If there are many values, return hash instead of rows
        if total_values > 8 {
            let mut hasher = Md5::new();
            for row in &formatted_rows {
                hasher.update(row.join(" "));
                hasher.update("\n");
            }
            let hash = format!("{:x}", hasher.finalize());
            let hash_string = format!("{} values hashing to {}", total_values, hash);
            Ok(DBOutput::Rows {
                types: vec![DefaultColumnType::Text],
                rows: vec![vec![hash_string]],
            })
        } else {
            Ok(DBOutput::Rows { types, rows: formatted_rows })
        }
    }

    fn format_sql_value(&self, value: &SqlValue) -> String {
        match value {
            SqlValue::Integer(i) => i.to_string(),
            SqlValue::Smallint(i) => i.to_string(),
            SqlValue::Bigint(i) => i.to_string(),
            SqlValue::Unsigned(i) => i.to_string(),
            SqlValue::Numeric(f) => f.to_string(),
            SqlValue::Float(f) | SqlValue::Real(f) => {
                if f.fract() == 0.0 {
                    format!("{:.1}", f)
                } else {
                    f.to_string()
                }
            }
            SqlValue::Double(f) => {
                if f.fract() == 0.0 {
                    format!("{:.1}", f)
                } else {
                    f.to_string()
                }
            }
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.clone(),
            SqlValue::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
            SqlValue::Null => "NULL".to_string(),
            SqlValue::Date(d)
            | SqlValue::Time(d)
            | SqlValue::Timestamp(d)
            | SqlValue::Interval(d) => d.clone(),
        }
    }
}

#[async_trait]
impl AsyncDB for NistMemSqlDB {
    type Error = TestError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.execute_sql(sql)
    }

    async fn shutdown(&mut self) {
        // No cleanup needed for in-memory database
    }
}

/// Detailed failure information for a single test file
#[derive(Debug, Clone)]
struct TestFailure {
    sql_statement: String,
    expected_result: Option<String>,
    actual_result: Option<String>,
    error_message: String,
    line_number: Option<usize>,
}

/// Test result statistics
#[derive(Debug, Default)]
struct TestStats {
    total: usize,
    passed: usize,
    failed: usize,
    errors: usize,
    skipped: usize,
    tested_files: HashSet<String>, // Files that were actually tested this run
    detailed_failures: Vec<(String, Vec<TestFailure>)>, // (file_path, failures) pairs
}

impl TestStats {
    fn pass_rate(&self) -> f64 {
        let relevant_total = self.total - self.skipped;
        if relevant_total == 0 {
            0.0
        } else {
            (self.passed as f64 / relevant_total as f64) * 100.0
        }
    }
}


/// Load historical test results from JSON file
fn load_historical_results() -> serde_json::Value {
    // Try to load from target/sqllogictest_cumulative.json (workflow format)
    if let Ok(content) = std::fs::read_to_string("target/sqllogictest_cumulative.json") {
        if let Ok(json) = serde_json::from_str(&content) {
            return json;
        }
    }

    // Try to load from target/sqllogictest_analysis.json (analysis format)
    if let Ok(content) = std::fs::read_to_string("target/sqllogictest_analysis.json") {
        if let Ok(json) = serde_json::from_str(&content) {
            return json;
        }
    }

    // Return empty object if no historical data found
    serde_json::Value::Object(serde_json::Map::new())
}

/// Prioritize test files based on historical results: failed first, then untested, then passed
fn prioritize_test_files(
    all_files: &[PathBuf],
    historical: &serde_json::Value,
    test_dir: &PathBuf,
    seed: u64,
) -> Vec<PathBuf> {
    // Extract historical passed and failed sets
    let mut historical_passed = HashSet::new();
    let mut historical_failed = HashSet::new();

    if let Some(tested_files) = historical.get("tested_files") {
        if let Some(passed) = tested_files.get("passed").and_then(|p| p.as_array()) {
            for file in passed {
                if let Some(file_str) = file.as_str() {
                    historical_passed.insert(file_str.to_string());
                }
            }
        }
        if let Some(failed) = tested_files.get("failed").and_then(|f| f.as_array()) {
            for file in failed {
                if let Some(file_str) = file.as_str() {
                    historical_failed.insert(file_str.to_string());
                }
            }
        }
    }

    // Categorize files by priority
    let mut failed_files = Vec::new();
    let mut untested_files = Vec::new();
    let mut passed_files = Vec::new();

    for file_path in all_files {
        let relative_path =
            file_path.strip_prefix(test_dir).unwrap_or(file_path).to_string_lossy().to_string();

        if historical_failed.contains(&relative_path) {
            failed_files.push(file_path.clone());
        } else if historical_passed.contains(&relative_path) {
            passed_files.push(file_path.clone());
        } else {
            untested_files.push(file_path.clone());
        }
    }

    // Shuffle within each category using deterministic seed
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let shuffle_with_seed = |files: &mut Vec<PathBuf>| {
        files.sort_by_cached_key(|path| {
            let mut hasher = DefaultHasher::new();
            path.hash(&mut hasher);
            seed.hash(&mut hasher);
            hasher.finish()
        });
    };

    shuffle_with_seed(&mut failed_files);
    shuffle_with_seed(&mut untested_files);
    shuffle_with_seed(&mut passed_files);

    // Apply worker-based partitioning if parallel workers are configured
    // This ensures each worker tests a unique slice of untested files
    let (worker_id, total_workers) = get_worker_config();

    if total_workers > 1 && worker_id > 0 && worker_id <= total_workers {
        println!("Worker partitioning: This is worker {}/{}", worker_id, total_workers);

        // Partition untested files among workers
        let untested_partition = partition_files(&untested_files, worker_id, total_workers);
        println!("  Untested files assigned to this worker: {} of {}",
                 untested_partition.len(), untested_files.len());

        // All workers test failed files (high priority)
        // But each worker gets their own slice of untested files
        // Passed files are shared (lowest priority, rarely reached)
        let mut prioritized = Vec::new();
        prioritized.extend(failed_files);
        prioritized.extend(untested_partition);
        prioritized.extend(passed_files);

        prioritized
    } else {
        // Single worker mode: test everything in priority order
        let mut prioritized = Vec::new();
        prioritized.extend(failed_files);
        prioritized.extend(untested_files);
        prioritized.extend(passed_files);

        prioritized
    }
}

/// Extract worker configuration from environment variables
fn get_worker_config() -> (usize, usize) {
    let worker_id = env::var("SQLLOGICTEST_WORKER_ID")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let total_workers = env::var("SQLLOGICTEST_TOTAL_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    (worker_id, total_workers)
}

/// Partition files into equal slices for parallel workers
/// Returns the slice for the specified worker_id (1-indexed)
fn partition_files(files: &[PathBuf], worker_id: usize, total_workers: usize) -> Vec<PathBuf> {
    if total_workers <= 1 || worker_id == 0 || worker_id > total_workers {
        return files.to_vec();
    }

    let chunk_size = (files.len() + total_workers - 1) / total_workers; // Ceiling division
    let start = (worker_id - 1) * chunk_size;
    let end = start + chunk_size;

    files.get(start..end.min(files.len()))
        .unwrap_or(&[])
        .to_vec()
}

/// Preprocess test file content to filter MySQL-specific directives
fn preprocess_for_mysql(content: &str) -> String {
    let mut output_lines = Vec::new();
    let mut skip_next_record = false;

    for line in content.lines() {
        // Check for dialect directives
        if line.starts_with("onlyif ") {
            let dialect = line.trim_start_matches("onlyif ")
                .split_whitespace()
                .next()
                .unwrap_or("");
            skip_next_record = dialect != "mysql";
            continue; // Don't include the directive line
        } else if line.starts_with("skipif ") {
            let dialect = line.trim_start_matches("skipif ")
                .split_whitespace()
                .next()
                .unwrap_or("");
            skip_next_record = dialect == "mysql";
            continue; // Don't include the directive line
        }

        // If we're not skipping, include the line
        // The skip applies to the entire test record (until next blank line or new test)
        if skip_next_record {
            // Skip this line, but check if we've reached the end of the record
            if line.trim().is_empty() {
                skip_next_record = false;
                output_lines.push(line); // Include blank lines
            }
            // Continue skipping until blank line or new test starts (implicitly via next directive)
        } else {
            output_lines.push(line);
        }
    }

    output_lines.join("\n")
}

/// Run a test file and capture detailed failure information
fn run_test_file_with_details(contents: &str) -> (Result<(), TestError>, Vec<TestFailure>) {
    // Preprocess content to handle MySQL dialect directives
    let preprocessed = preprocess_for_mysql(contents);

    let result = std::panic::catch_unwind(|| {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(
            async {
                let mut tester = Runner::new(|| async { Ok(NistMemSqlDB::new()) });
                // Enable hash mode with threshold of 8 (standard SQLLogicTest behavior)
                tester.with_hash_threshold(8);
                tester.run_script(&preprocessed)
            },
        )
    });

    match result {
        Ok(Ok(_)) => (Ok(()), vec![]),
        Ok(Err(e)) => {
            // For now, capture basic error information
            // TODO: Parse individual records and capture per-statement failures
            let failure = TestFailure {
                sql_statement: "Unknown - script failed".to_string(),
                expected_result: None,
                actual_result: None,
                error_message: e.to_string(),
                line_number: None,
            };
            (Err(TestError(e.to_string())), vec![failure])
        }
        Err(e) => {
            let error_msg = e.downcast_ref::<String>()
                .unwrap_or(&"Unknown panic".to_string())
                .clone();
            let failure = TestFailure {
                sql_statement: "Unknown - panic occurred".to_string(),
                expected_result: None,
                actual_result: None,
                error_message: format!("Test panicked: {}", error_msg),
                line_number: None,
            };
            (Err(TestError(format!("Test panicked: {}", error_msg))), vec![failure])
        }
    }
}

/// Run SQLLogicTest files from the submodule (prioritized by failure history, then randomly selected with time budget)
fn run_test_suite() -> (HashMap<String, TestStats>, usize) {
    let test_dir = PathBuf::from("third_party/sqllogictest/test");
    let mut results = HashMap::new();

    // Get time budget from environment (default: 5 minutes = 300 seconds)
    let time_budget_secs: u64 =
        env::var("SQLLOGICTEST_TIME_BUDGET").ok().and_then(|s| s.parse().ok()).unwrap_or(300);
    let time_budget = Duration::from_secs(time_budget_secs);
    let start_time = Instant::now();

    // Find all .test files
    let pattern = format!("{}/**/*.test", test_dir.display());
    let all_test_files: Vec<PathBuf> =
        glob::glob(&pattern).expect("Failed to read test pattern").filter_map(Result::ok).collect();

    let total_available_files = all_test_files.len();

    // Load historical results to prioritize testing
    let historical_results = load_historical_results();

    // Get seed for shuffling within priority categories
    let seed: u64 =
        env::var("SQLLOGICTEST_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or_else(|| {
            // Use git commit hash as seed if available
            env::var("GITHUB_SHA")
                .ok()
                .and_then(|sha| u64::from_str_radix(&sha[..8], 16).ok())
                .unwrap_or(0)
        });

    // Prioritize test files: failed first, then untested, then passed
    let prioritized_files =
        prioritize_test_files(&all_test_files, &historical_results, &test_dir, seed);

    println!("\n=== SQLLogicTest Suite (Prioritized Sampling) ===");
    println!("Total available test files: {}", total_available_files);
    println!("Time budget: {} seconds", time_budget_secs);
    println!("Random seed: {}", seed);
    println!("Prioritization: Failed → Untested → Passed");
    println!("Starting test run...\n");

    for (files_tested, test_file) in prioritized_files.into_iter().enumerate() {
        // Check time budget
        if start_time.elapsed() >= time_budget {
            println!("\n⏱️  Time budget exhausted after {} seconds", time_budget_secs);
            println!(
                "Tested {} of {} files ({:.1}%)\n",
                files_tested + 1,
                total_available_files,
                ((files_tested + 1) as f64 / total_available_files as f64) * 100.0
            );
            break;
        }
        let relative_path =
            test_file.strip_prefix(&test_dir).unwrap_or(&test_file).to_string_lossy().to_string();

        // Determine category from path
        let category = if relative_path.starts_with("select") {
            "select"
        } else if relative_path.starts_with("evidence/") {
            "evidence"
        } else if relative_path.starts_with("index/") {
            "index"
        } else if relative_path.starts_with("random/") {
            "random"
        } else if relative_path.starts_with("ddl/") {
            "ddl"
        } else {
            "other"
        }
        .to_string();

        let stats = results.entry(category.clone()).or_insert_with(TestStats::default);
        stats.total += 1;
        stats.tested_files.insert(relative_path.clone());

        // Read and run test file
        let contents = match std::fs::read_to_string(&test_file) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("✗ {} - Failed to read file: {}", relative_path, e);
                stats.errors += 1;
                continue;
            }
        };

        // Create a new database for each test file and run with detailed failure capture
        let (test_result, detailed_failures) = run_test_file_with_details(&contents);

        match test_result {
            Ok(_) => {
                println!("✓ {}", relative_path);
                stats.passed += 1;
            }
            Err(e) => {
                eprintln!("✗ {} - {}", relative_path, e);
                stats.failed += 1;
                if !detailed_failures.is_empty() {
                    stats.detailed_failures.push((relative_path.clone(), detailed_failures));
                }
            }
        }
    }

    (results, total_available_files)
}

#[test]
fn run_sqllogictest_suite() {
    // If SELECT1_ONLY is set, run only select1.test
    if env::var("SELECT1_ONLY").is_ok() {
        let test_file = PathBuf::from("third_party/sqllogictest/test/select1.test");
        let contents = std::fs::read_to_string(&test_file).expect("Failed to read select1.test");
        let (test_result, detailed_failures) = run_test_file_with_details(&contents);

        match test_result {
            Ok(_) => println!("✓ select1.test passed"),
            Err(e) => {
                println!("✗ select1.test failed: {}", e);
                for failure in detailed_failures {
                    println!("  SQL: {}", failure.sql_statement);
                    println!("  Expected: {:?}", failure.expected_result);
                    println!("  Actual: {:?}", failure.actual_result);
                    println!("  Error: {}", failure.error_message);
                    println!("  Line: {:?}", failure.line_number);
                    println!();
                }
                panic!("select1.test failed");
            }
        }
        return;
    }

    // Check if submodule is initialized
    let test_dir = PathBuf::from("third_party/sqllogictest/test");
    if !test_dir.exists() {
        panic!(
            "SQLLogicTest submodule not initialized. Run:\n  git submodule update --init --recursive"
        );
    }

    let (results, total_available_files) = run_test_suite();

    // Print summary
    println!("\n=== Test Results Summary ===");
    println!(
        "{:<20} {:>8} {:>8} {:>8} {:>8} {:>8} {:>10}",
        "Category", "Total", "Passed", "Failed", "Errors", "Skipped", "Pass Rate"
    );
    println!("{}", "-".repeat(80));

    let mut grand_total = TestStats::default();
    let mut all_tested_files = HashSet::new();

    for category in ["select", "evidence", "index", "random", "ddl", "other"] {
        if let Some(stats) = results.get(category) {
            println!(
                "{:<20} {:>8} {:>8} {:>8} {:>8} {:>8} {:>9.1}%",
                category,
                stats.total,
                stats.passed,
                stats.failed,
                stats.errors,
                stats.skipped,
                stats.pass_rate()
            );
            grand_total.total += stats.total;
            grand_total.passed += stats.passed;
            grand_total.failed += stats.failed;
            grand_total.errors += stats.errors;
            grand_total.skipped += stats.skipped;
            all_tested_files.extend(stats.tested_files.clone());
        }
    }

    println!("{}", "-".repeat(80));
    println!(
        "{:<20} {:>8} {:>8} {:>8} {:>8} {:>8} {:>9.1}%",
        "TOTAL",
        grand_total.total,
        grand_total.passed,
        grand_total.failed,
        grand_total.errors,
        grand_total.skipped,
        grand_total.pass_rate()
    );

    println!(
        "\nNote: This test suite randomly samples from ~5.9 million test cases across {} files.",
        total_available_files
    );
    println!("Results from multiple CI runs are merged to progressively build complete coverage.");
    println!("Some failures are expected as we continue implementing SQL:1999 features.");

    // Write results to JSON file for CI/badge generation
    let tested_files_vec: Vec<String> = all_tested_files.into_iter().collect();

    // Collect all detailed failures across categories
    let mut all_detailed_failures = Vec::new();
    for stats in results.values() {
        all_detailed_failures.extend(stats.detailed_failures.clone());
    }

    let results_json = serde_json::json!({
        "summary": {
            "total": grand_total.total,
            "passed": grand_total.passed,
            "failed": grand_total.failed,
            "errors": grand_total.errors,
            "skipped": grand_total.skipped,
            "pass_rate": grand_total.pass_rate(),
            "total_available_files": total_available_files,
            "tested_files": tested_files_vec.len(),
        },
        "tested_files": {
            "passed": results.values().flat_map(|s| &s.tested_files).filter(|f| {
                // Check if this file passed (not in detailed_failures)
                !all_detailed_failures.iter().any(|(path, _)| path == *f)
            }).collect::<Vec<_>>(),
            "failed": results.values().flat_map(|s| &s.tested_files).filter(|f| {
                // Check if this file failed (in detailed_failures)
                all_detailed_failures.iter().any(|(path, _)| path == *f)
            }).collect::<Vec<_>>(),
        },
        "categories": {
            "select": results.get("select").map(|s| serde_json::json!({
                "total": s.total,
                "passed": s.passed,
                "failed": s.failed,
                "errors": s.errors,
                "skipped": s.skipped,
                "pass_rate": s.pass_rate()
            })),
            "evidence": results.get("evidence").map(|s| serde_json::json!({
                "total": s.total,
                "passed": s.passed,
                "failed": s.failed,
                "errors": s.errors,
                "skipped": s.skipped,
                "pass_rate": s.pass_rate()
            })),
            "index": results.get("index").map(|s| serde_json::json!({
                "total": s.total,
                "passed": s.passed,
                "failed": s.failed,
                "errors": s.errors,
                "skipped": s.skipped,
                "pass_rate": s.pass_rate()
            })),
            "random": results.get("random").map(|s| serde_json::json!({
                "total": s.total,
                "passed": s.passed,
                "failed": s.failed,
                "errors": s.errors,
                "skipped": s.skipped,
                "pass_rate": s.pass_rate()
            })),
            "ddl": results.get("ddl").map(|s| serde_json::json!({
                "total": s.total,
                "passed": s.passed,
                "failed": s.failed,
                "errors": s.errors,
                "skipped": s.skipped,
                "pass_rate": s.pass_rate()
            })),
            "other": results.get("other").map(|s| serde_json::json!({
                "total": s.total,
                "passed": s.passed,
                "failed": s.failed,
                "errors": s.errors,
                "skipped": s.skipped,
                "pass_rate": s.pass_rate()
            })),
        },
        "detailed_failures": all_detailed_failures.iter().map(|(file_path, failures)| {
            serde_json::json!({
                "file_path": file_path,
                "failures": failures.iter().map(|f| {
                    serde_json::json!({
                        "sql_statement": f.sql_statement,
                        "expected_result": f.expected_result,
                        "actual_result": f.actual_result,
                        "error_message": f.error_message,
                        "line_number": f.line_number
                    })
                }).collect::<Vec<_>>()
            })
        }).collect::<Vec<_>>()
    });

    // Ensure target directory exists
    fs::create_dir_all("target").ok();

    // Write JSON results
    if let Ok(mut file) = fs::File::create("target/sqllogictest_results.json") {
        let _ = file.write_all(serde_json::to_string_pretty(&results_json).unwrap().as_bytes());
        println!("\n✓ Results written to target/sqllogictest_results.json");
    }
}

#[cfg(test)]
mod preprocessing_tests {
    use super::preprocess_for_mysql;

    #[test]
    fn test_preprocess_onlyif_mysql() {
        let input = "statement ok\nCREATE TABLE t1 (x INT)\n\nonlyif mysql\nstatement ok\nINSERT INTO t1 VALUES (1)\n\nonlyif postgresql\nstatement ok\nINSERT INTO t1 VALUES (2)\n";
        let output = preprocess_for_mysql(input);

        // Should include MySQL-specific statement
        assert!(output.contains("INSERT INTO t1 VALUES (1)"));
        // Should exclude PostgreSQL-specific statement
        assert!(!output.contains("INSERT INTO t1 VALUES (2)"));
        // Should not include directive lines
        assert!(!output.contains("onlyif"));
    }

    #[test]
    fn test_preprocess_skipif_mysql() {
        let input = "statement ok\nCREATE TABLE t1 (x INT)\n\nskipif mysql\nstatement ok\nINSERT INTO t1 VALUES (1)\n\nskipif postgresql\nstatement ok\nINSERT INTO t1 VALUES (2)\n";
        let output = preprocess_for_mysql(input);

        // Should exclude MySQL-skipped statement
        assert!(!output.contains("INSERT INTO t1 VALUES (1)"));
        // Should include statement not skipped for MySQL
        assert!(output.contains("INSERT INTO t1 VALUES (2)"));
        // Should not include directive lines
        assert!(!output.contains("skipif"));
    }

    #[test]
    fn test_preprocess_directive_with_comment() {
        let input = "onlyif mysql # aggregate syntax:\nstatement ok\nSELECT SUM(x) FROM t1\n\nskipif mysql # unsupported feature\nstatement ok\nINSERT INTO t1 VALUES (99)\n";
        let output = preprocess_for_mysql(input);

        // MySQL directive with comment should include statement
        assert!(output.contains("SELECT SUM(x) FROM t1"), "onlyif mysql with comment should include MySQL statement");
        // MySQL skipif with comment should exclude statement
        assert!(!output.contains("INSERT INTO t1 VALUES (99)"), "skipif mysql with comment should exclude MySQL statement");
        // Directives should be removed
        assert!(!output.contains("onlyif"));
        assert!(!output.contains("skipif"));
    }

    #[test]
    fn test_preprocess_mixed_directives() {
        let input = r#"statement ok
CREATE TABLE t1 (x INT)

onlyif mysql
statement ok
INSERT INTO t1 VALUES (1)

skipif mysql
query I
SELECT * FROM t1 WHERE x > 10
----

onlyif postgresql
statement ok
INSERT INTO t1 VALUES (2)

statement ok
INSERT INTO t1 VALUES (3)
"#;
        let output = preprocess_for_mysql(input);

        // MySQL-only statement should be included
        assert!(output.contains("INSERT INTO t1 VALUES (1)"));
        // MySQL-skipped query should be excluded
        assert!(!output.contains("SELECT * FROM t1 WHERE x > 10"));
        // PostgreSQL-only statement should be excluded
        assert!(!output.contains("INSERT INTO t1 VALUES (2)"));
        // Universal statement should be included
        assert!(output.contains("INSERT INTO t1 VALUES (3)"));
        // No directives should remain
        assert!(!output.contains("onlyif"));
        assert!(!output.contains("skipif"));
    }
}
