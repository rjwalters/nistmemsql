// Test for issue 955: IS NULL operator not working on computed expressions in WHERE clause
// The problem: IS NULL should work on any expression, including arithmetic operations

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

#[test]
fn test_is_null_on_arithmetic_expression() {
    let schema = TableSchema::new(
        "TAB0".to_string(),
        vec![ColumnSchema::new("COL0".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(99)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(100)])).unwrap();

    // Test: IS NULL on arithmetic expression should return no rows
    // since 15 * -99 = -1485, which is not NULL
    let results = execute_select(&db, "SELECT col0 FROM tab0 WHERE 15 * - 99 IS NULL")
        .expect("Query should succeed");

    assert_eq!(results.len(), 0, "15 * -99 is not NULL, should return 0 rows");
}

#[test]
fn test_is_null_with_unary_and_binary_ops() {
    let schema = TableSchema::new(
        "TAB0".to_string(),
        vec![
            ColumnSchema::new("COL0".to_string(), DataType::Integer, false),
            ColumnSchema::new("COL1".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(20)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(30), SqlValue::Integer(30)])).unwrap();

    // Test: Complex expression with unary and binary operators
    // + col0 - col1 should be an integer, not NULL
    let results = execute_select(&db, "SELECT col0 FROM tab0 WHERE + col0 - col1 IS NULL")
        .expect("Query should succeed");

    assert_eq!(results.len(), 0, "Arithmetic expression is not NULL");
}

#[test]
fn test_is_null_on_column_with_null_value() {
    let schema = TableSchema::new(
        "TAB0".to_string(),
        vec![
            ColumnSchema::new("COL0".to_string(), DataType::Integer, false),
            ColumnSchema::new("COL1".to_string(), DataType::Integer, true),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(20)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(30), SqlValue::Null])).unwrap();

    // Test: IS NULL should find rows where column is actually NULL
    let results = execute_select(&db, "SELECT col0 FROM tab0 WHERE col1 IS NULL")
        .expect("Query should succeed");

    assert_eq!(results.len(), 1, "Should find one row with NULL");
}

#[test]
fn test_is_not_null_on_arithmetic() {
    let schema = TableSchema::new(
        "TAB0".to_string(),
        vec![ColumnSchema::new("COL0".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(2)])).unwrap();

    // Test: IS NOT NULL on arithmetic expression should return all rows
    let results = execute_select(&db, "SELECT col0 FROM tab0 WHERE (col0 * 5) IS NOT NULL")
        .expect("Query should succeed");

    assert_eq!(results.len(), 2, "All arithmetic results are NOT NULL");
}
