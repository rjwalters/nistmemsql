//! Tests for short-circuit evaluation of AND/OR operators
//!
//! Related to issue #1039: Optimize query planning for complex nested WHERE clauses

use catalog::{ColumnSchema, TableSchema};
use executor::SelectExecutor;
use parser::Parser;
use storage::{Database, Row};
use types::{DataType, SqlValue};

/// Execute a SELECT query end-to-end: parse SQL → execute → return results.
fn execute_select(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

fn create_test_db() -> Database {
    let schema = TableSchema::new(
        "TEST".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("VALUE".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();
    db.insert_row("TEST", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)])).unwrap();
    db.insert_row("TEST", Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(20)])).unwrap();
    db.insert_row("TEST", Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(30)])).unwrap();
    db
}

#[test]
fn test_and_short_circuit_false_first() {
    let db = create_test_db();

    // Test: FALSE AND <anything> should short-circuit (not evaluate right side)
    // The query should succeed even though (1/0) would cause division by zero
    // because FALSE AND (1/0 = 1) should short-circuit and never evaluate 1/0
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE (1 = 2) AND (1/0 = 1)");

    // Should succeed and return empty result set (no rows match FALSE)
    assert!(result.is_ok(), "AND should short-circuit on FALSE: {:?}", result.err());
    let result = result.unwrap();
    assert_eq!(result.len(), 0, "Should return no rows");
}

#[test]
fn test_and_short_circuit_true_continues() {
    let db = create_test_db();

    // Test: TRUE AND <condition> should evaluate both sides
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE (1 = 1) AND (VALUE > 15)")
        .expect("Query should succeed");

    // Should return rows where value > 15 (id 2 and 3)
    assert_eq!(result.len(), 2);
}

#[test]
fn test_or_short_circuit_true_first() {
    let db = create_test_db();

    // Test: TRUE OR <anything> should short-circuit (not evaluate right side)
    // The query should succeed even though (1/0) would cause division by zero
    // because TRUE OR (1/0 = 1) should short-circuit and never evaluate 1/0
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE (1 = 1) OR (1/0 = 1)");

    // Should succeed and return all rows (all match TRUE)
    assert!(result.is_ok(), "OR should short-circuit on TRUE: {:?}", result.err());
    let result = result.unwrap();
    assert_eq!(result.len(), 3, "Should return all rows");
}

#[test]
fn test_or_short_circuit_false_continues() {
    let db = create_test_db();

    // Test: FALSE OR <condition> should evaluate both sides
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE (1 = 2) OR (VALUE > 15)")
        .expect("Query should succeed");

    // Should return rows where value > 15 (id 2 and 3)
    assert_eq!(result.len(), 2);
}

#[test]
fn test_nested_short_circuit() {
    let db = create_test_db();

    // Test nested short-circuit: (FALSE AND (TRUE OR (1/0 = 1)))
    // Should short-circuit at first FALSE, never evaluating inner OR or 1/0
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE (1 = 2) AND ((1 = 1) OR (1/0 = 1))");

    assert!(result.is_ok(), "Should short-circuit nested expression: {:?}", result.err());
    let result = result.unwrap();
    assert_eq!(result.len(), 0);
}

#[test]
fn test_deeply_nested_and_short_circuit() {
    let db = create_test_db();

    // Test deeply nested AND operations with early FALSE
    // This simulates the pattern in slt_good_34.test
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE \
         (1 = 2) AND \
         (VALUE > 5 AND (VALUE < 100 AND (VALUE > 0 AND (VALUE < 1000))))",
    );

    assert!(result.is_ok(), "Should short-circuit deeply nested AND: {:?}", result.err());
    let result = result.unwrap();
    assert_eq!(result.len(), 0);
}

#[test]
fn test_complex_boolean_expression() {
    let schema = TableSchema::new(
        "TEST".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("A".to_string(), DataType::Integer, false),
            ColumnSchema::new("B".to_string(), DataType::Integer, false),
            ColumnSchema::new("C".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();
    db.insert_row(
        "TEST",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(5),
            SqlValue::Integer(10),
            SqlValue::Integer(15),
        ]),
    )
    .unwrap();
    db.insert_row(
        "TEST",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Integer(20),
            SqlValue::Integer(25),
            SqlValue::Integer(30),
        ]),
    )
    .unwrap();
    db.insert_row(
        "TEST",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Integer(35),
            SqlValue::Integer(40),
            SqlValue::Integer(45),
        ]),
    )
    .unwrap();

    // Complex expression similar to slt_good_34.test pattern
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE \
         (A > 0) AND \
         ((B > 20 OR C > 40) AND \
          (A < 100 OR (B < 50 AND C > 0)))",
    )
    .expect("Query should succeed");

    // Should return rows 2 and 3
    assert_eq!(result.len(), 2);
}

#[test]
fn test_performance_improvement_simulation() {
    let schema = TableSchema::new(
        "TEST".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("A".to_string(), DataType::Integer, false),
            ColumnSchema::new("B".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert 100 rows
    for i in 1..=100 {
        db.insert_row(
            "TEST",
            Row::new(vec![
                SqlValue::Integer(i),
                SqlValue::Integer(i * 2),
                SqlValue::Integer(i * 3),
            ]),
        )
        .unwrap();
    }

    // This query has a FALSE condition early, followed by many complex conditions
    // With short-circuit, only the first condition should be evaluated
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE \
         (1 = 2) AND \
         (A > 50 OR B < 100) AND \
         (A < 200 OR B > 50) AND \
         (A > 10 OR B < 300) AND \
         (A < 500 OR B > 20) AND \
         (A > 0 OR B < 600)",
    )
    .expect("Query should succeed");

    // Should return 0 rows quickly due to short-circuit
    assert_eq!(result.len(), 0);
}

#[test]
fn test_and_with_column_reference_short_circuit() {
    let db = create_test_db();

    // When id = 1 (VALUE = 10), the first condition (VALUE > 100) is FALSE
    // So the second condition should not be evaluated
    // This ensures short-circuit works with column references, not just literals
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE (VALUE > 100) AND (1/0 = 1)",
    );

    assert!(result.is_ok(), "Should short-circuit with column reference: {:?}", result.err());
    let result = result.unwrap();
    assert_eq!(result.len(), 0);
}

#[test]
fn test_or_with_column_reference_short_circuit() {
    let db = create_test_db();

    // When the first condition (VALUE > 0) is TRUE for all rows,
    // the second condition should not be evaluated
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE (VALUE > 0) OR (1/0 = 1)",
    );

    assert!(result.is_ok(), "Should short-circuit with column reference: {:?}", result.err());
    let result = result.unwrap();
    assert_eq!(result.len(), 3);
}
