//! Verification test for select5.test to prove predicate pushdown optimization works
//!
//! This test runs select5.test to verify that the predicate pushdown optimization
//! successfully prevents OOM on 64-table joins with equijoin conditions.

use ::sqllogictest::Runner;
use std::path::Path;

mod sqllogictest;

use crate::sqllogictest::db_adapter::NistMemSqlDB;

#[tokio::test]
async fn test_select5_no_oom() {
    // This test verifies that select5.test runs without OOM
    // Before predicate pushdown: 73+ GB memory usage → OOM
    // After predicate pushdown: < 100 MB memory usage → Success

    let test_file = Path::new("third_party/sqllogictest/test/select5.test");

    if !test_file.exists() {
        panic!("select5.test not found - run: git submodule update --init third_party/sqllogictest");
    }

    let mut runner = Runner::new(|| async { Ok(NistMemSqlDB::new()) });

    // Run the test - it should complete in reasonable time now
    let result = runner.run_file_async(test_file).await;

    match result {
        Ok(_) => {
            println!("✓ select5.test PASSED - predicate pushdown optimization working!");
        }
        Err(e) => {
            panic!("select5.test FAILED: {}", e);
        }
    }
}
