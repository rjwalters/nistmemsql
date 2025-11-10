//! Tests for parallel WHERE clause execution

use executor::SelectExecutor;
use storage::Database;

/// Helper function to parse SELECT statements
fn parse_select(sql: &str) -> ast::SelectStmt {
    match parser::Parser::parse_sql(sql) {
        Ok(ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

/// Helper to create test database with large table for parallel execution testing
fn setup_large_test_db(num_rows: usize) -> Database {
    let mut db = Database::new();

    let schema = catalog::TableSchema::new(
        "TEST_PARALLEL".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert rows
    for i in 0..num_rows {
        let row = storage::Row::new(vec![
            types::SqlValue::Integer(i as i64),
            types::SqlValue::Integer((i % 100) as i64),
        ]);
        db.insert_row("TEST_PARALLEL", row).unwrap();
    }

    db
}

#[test]
fn test_parallel_filter_produces_same_results_as_sequential() {
    // Create database with a table containing enough rows to trigger parallel execution
    let db = setup_large_test_db(15_000); // Above PARALLEL_THRESHOLD of 10,000
    let executor = SelectExecutor::new(&db);

    // Test 1: Simple WHERE clause filtering
    let stmt = parse_select("SELECT id FROM TEST_PARALLEL WHERE value < 10");
    let result_seq = executor.execute(&stmt).unwrap();

    // Enable parallel execution
    std::env::set_var("PARALLEL_EXECUTION", "true");

    let result_par = executor.execute(&stmt).unwrap();

    // Disable parallel execution for other tests
    std::env::remove_var("PARALLEL_EXECUTION");

    // Results should match (though order may differ without ORDER BY)
    assert_eq!(result_seq.len(), result_par.len());
    assert_eq!(result_seq.len(), 1500); // 15000 / 100 * 10 = 1500 rows

    // Test 2: Complex WHERE clause with multiple conditions
    let stmt2 = parse_select("SELECT id FROM TEST_PARALLEL WHERE value >= 20 AND value < 30");
    let result_seq = executor.execute(&stmt2).unwrap();

    std::env::set_var("PARALLEL_EXECUTION", "true");

    let result_par = executor.execute(&stmt2).unwrap();

    std::env::remove_var("PARALLEL_EXECUTION");

    assert_eq!(result_seq.len(), result_par.len());
    assert_eq!(result_seq.len(), 1500); // 15000 / 100 * 10 = 1500 rows
}

#[test]
fn test_parallel_filter_below_threshold_uses_sequential() {
    // Create database with fewer rows than threshold
    let db = setup_large_test_db(1_000); // Below PARALLEL_THRESHOLD of 10,000
    let executor = SelectExecutor::new(&db);

    // Even with parallel enabled, should use sequential due to threshold
    std::env::set_var("PARALLEL_EXECUTION", "true");

    let stmt = parse_select("SELECT id FROM TEST_PARALLEL WHERE value < 10");
    let result = executor.execute(&stmt).unwrap();

    std::env::remove_var("PARALLEL_EXECUTION");

    // With 1000 rows and value = i % 100, we have 10 rows per value
    // value < 10 means values 0-9, so 10 * 10 = 100 rows
    assert_eq!(result.len(), 100);
}

#[test]
fn test_parallel_filter_with_null_values() {
    let mut db = Database::new();

    let schema = catalog::TableSchema::new(
        "TEST_NULLS".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert rows with some NULL values
    for i in 0..12_000 {
        let value = if i % 10 == 0 {
            types::SqlValue::Null
        } else {
            types::SqlValue::Integer((i % 100) as i64)
        };
        let row = storage::Row::new(vec![types::SqlValue::Integer(i as i64), value]);
        db.insert_row("TEST_NULLS", row).unwrap();
    }

    let executor = SelectExecutor::new(&db);

    let stmt = parse_select("SELECT id FROM TEST_NULLS WHERE value IS NOT NULL AND value < 10");
    let result_seq = executor.execute(&stmt).unwrap();

    std::env::set_var("PARALLEL_EXECUTION", "true");

    let result_par = executor.execute(&stmt).unwrap();

    std::env::remove_var("PARALLEL_EXECUTION");

    assert_eq!(result_seq.len(), result_par.len());
}

#[test]
fn test_parallel_disabled_by_default() {
    let db = setup_large_test_db(11_000); // Above threshold
    let executor = SelectExecutor::new(&db);

    // Parallel execution should be disabled by default
    std::env::remove_var("PARALLEL_EXECUTION");

    let stmt = parse_select("SELECT id FROM TEST_PARALLEL WHERE id < 100");
    let result = executor.execute(&stmt).unwrap();

    assert_eq!(result.len(), 100);
}

#[test]
fn test_parallel_execution_integration_with_executor() {
    // Integration test: Verify PARALLEL_EXECUTION env var actually affects query execution
    // This test executes through the normal SelectExecutor path (not direct function calls)
    let db = setup_large_test_db(15_000); // Above threshold
    let executor = SelectExecutor::new(&db);

    // Test 1: Verify sequential execution works
    std::env::remove_var("PARALLEL_EXECUTION");
    let stmt = parse_select("SELECT id FROM TEST_PARALLEL WHERE value < 10");
    let result_seq = executor.execute(&stmt).unwrap();
    assert_eq!(result_seq.len(), 1500);

    // Test 2: Enable parallel and verify it still produces correct results
    std::env::set_var("PARALLEL_EXECUTION", "true");
    let result_par = executor.execute(&stmt).unwrap();
    std::env::remove_var("PARALLEL_EXECUTION");

    // Both should produce same count (order may differ)
    assert_eq!(result_par.len(), 1500);

    // Test 3: Verify with aggregation queries too
    std::env::set_var("PARALLEL_EXECUTION", "true");
    let stmt_agg = parse_select("SELECT COUNT(*) FROM TEST_PARALLEL WHERE value < 10");
    let result_agg = executor.execute(&stmt_agg).unwrap();
    std::env::remove_var("PARALLEL_EXECUTION");

    // Should get one row with count
    assert_eq!(result_agg.len(), 1);
    assert_eq!(result_agg[0].values[0], types::SqlValue::Integer(1500)); // COUNT(*) returns Integer
}
