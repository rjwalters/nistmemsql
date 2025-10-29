//! SQL:1999 Conformance Testing using sqltest suite
//!
//! This test module runs SQL:1999 conformance tests extracted from the
//! upstream-recommended sqltest suite by Elliot Chance.

use executor::SelectExecutor;
use parser::Parser;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use storage::Database;

/// Test manifest containing all SQL:1999 conformance tests
#[derive(Debug, Deserialize, Serialize)]
struct TestManifest {
    version: String,
    source: String,
    url: String,
    test_count: usize,
    tests: Vec<TestCase>,
}

/// Individual test case from the manifest
#[derive(Debug, Deserialize, Serialize, Clone)]
struct TestCase {
    id: String,
    feature: String,
    category: String,
    sql: String,
    expect_success: bool,
}

/// Test results summary
#[derive(Debug, Default)]
struct TestResults {
    passed: usize,
    failed: usize,
    errors: usize,
    total: usize,
}

impl TestResults {
    fn record_pass(&mut self) {
        self.passed += 1;
        self.total += 1;
    }

    fn record_fail(&mut self) {
        self.failed += 1;
        self.total += 1;
    }

    fn record_error(&mut self) {
        self.errors += 1;
        self.total += 1;
    }

    fn pass_rate(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            (self.passed as f64 / self.total as f64) * 100.0
        }
    }
}

/// SQL:1999 Conformance Test Runner
struct SqltestRunner {
    manifest: TestManifest,
}

impl SqltestRunner {
    /// Load test manifest from file
    fn load(manifest_path: &Path) -> Result<Self, String> {
        let content = fs::read_to_string(manifest_path)
            .map_err(|e| format!("Failed to read manifest: {}", e))?;
        
        let manifest: TestManifest = serde_json::from_str(&content)
            .map_err(|e| format!("Failed to parse manifest: {}", e))?;
        
        Ok(Self { manifest })
    }

    /// Run all conformance tests
    fn run_all(&self) -> TestResults {
        let mut results = TestResults::default();
        let mut db = Database::new();

        println!("\n🧪 Running {} SQL:1999 conformance tests...\n", self.manifest.test_count);

        for test_case in &self.manifest.tests {
            match self.run_test(&mut db, test_case) {
                Ok(true) => {
                    results.record_pass();
                    print!(".");
                }
                Ok(false) => {
                    results.record_fail();
                    print!("F");
                    eprintln!("\n❌ FAIL: {} - {}", test_case.id, test_case.sql);
                }
                Err(e) => {
                    results.record_error();
                    print!("E");
                    eprintln!("\n⚠️  ERROR: {} - {}\n   {}", test_case.id, test_case.sql, e);
                }
            }

            // Newline every 50 tests for readability
            if results.total % 50 == 0 {
                println!();
            }
        }

        println!("\n");
        results
    }

    /// Run a single test case
    fn run_test(&self, db: &mut Database, test_case: &TestCase) -> Result<bool, String> {
        // Parse the SQL
        let parse_result = Parser::parse_sql(&test_case.sql);

        if !test_case.expect_success {
            // Test expects failure - check that it fails
            return Ok(parse_result.is_err());
        }

        // Test expects success
        let stmt = parse_result.map_err(|e| format!("Parse error: {:?}", e))?;

        // Try to execute the statement
        match stmt {
            ast::Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(db);
                executor
                    .execute(&select_stmt)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            ast::Statement::CreateTable(create_stmt) => {
                executor::CreateTableExecutor::execute(&create_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            ast::Statement::Insert(insert_stmt) => {
                executor::InsertExecutor::execute(db, &insert_stmt)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            ast::Statement::Update(update_stmt) => {
                executor::UpdateExecutor::execute(&update_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            ast::Statement::Delete(delete_stmt) => {
                executor::DeleteExecutor::execute(&delete_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            ast::Statement::DropTable(drop_stmt) => {
                executor::DropTableExecutor::execute(&drop_stmt, db)
                    .map_err(|e| format!("Execution error: {:?}", e))?;
                Ok(true)
            }
            ast::Statement::BeginTransaction(_)
            | ast::Statement::Commit(_)
            | ast::Statement::Rollback(_) => {
                // Transactions are no-ops currently
                Ok(true)
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn run_sql1999_conformance_suite() {
    let manifest_path = Path::new("tests/sql1999/manifest.json");
    
    let runner = SqltestRunner::load(manifest_path)
        .expect("Failed to load conformance test manifest");
    
    let results = runner.run_all();
    
    // Print summary
    println!("{}", "=".repeat(60));
    println!("SQL:1999 Conformance Test Results");
    println!("{}", "=".repeat(60));
    println!("Total:   {}", results.total);
    println!("Passed:  {} ✅", results.passed);
    println!("Failed:  {} ❌", results.failed);
    println!("Errors:  {} ⚠️", results.errors);
    println!("Pass Rate: {:.1}%", results.pass_rate());
    println!("{}", "=".repeat(60));
    
    // Save results to JSON
    let results_json = serde_json::json!({
        "total": results.total,
        "passed": results.passed,
        "failed": results.failed,
        "errors": results.errors,
        "pass_rate": results.pass_rate(),
    });
    
    fs::write(
        "target/sqltest_results.json",
        serde_json::to_string_pretty(&results_json).unwrap()
    ).ok();
    
    // Assert we have some passing tests (not expecting 100% initially)
    assert!(results.passed > 0, "No tests passed! Pass rate: {:.1}%", results.pass_rate());
    
    // Optional: Assert minimum pass rate
    // assert!(results.pass_rate() >= 50.0, "Pass rate too low: {:.1}%", results.pass_rate());
}
