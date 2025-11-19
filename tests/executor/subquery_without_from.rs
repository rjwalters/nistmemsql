//! Tests for subqueries in SELECT list without FROM clause (Issue #1152)
//!
//! These tests verify that subqueries (IN, scalar, EXISTS, ALL/ANY) work correctly
//! when used in the SELECT list without a FROM clause.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Execute a SELECT query end-to-end: parse SQL → execute → return results.
fn execute_select(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

// ========================================================================
// IN Subquery Tests
// ========================================================================

#[test]
fn test_in_subquery_without_from_match() {
    // Test IN subquery where value is in the set
    let schema = TableSchema::new(
        "T1".to_string(),
        vec![ColumnSchema::new("COL1".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(2)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(3)])).unwrap();

    // Test: 1 is IN the subquery result
    let results = execute_select(&db, "SELECT 1 IN (SELECT col1 FROM t1)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Boolean(true));
}

#[test]
fn test_in_subquery_without_from_no_match() {
    // Test IN subquery where value is not in the set
    let schema = TableSchema::new(
        "T1".to_string(),
        vec![ColumnSchema::new("COL1".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("T1", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(2)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(3)])).unwrap();

    // Test: 5 is NOT IN the subquery result
    let results = execute_select(&db, "SELECT 5 IN (SELECT col1 FROM t1)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Boolean(false));
}

#[test]
fn test_not_in_subquery_without_from() {
    // Test NOT IN subquery
    let schema = TableSchema::new(
        "T1".to_string(),
        vec![ColumnSchema::new("COL1".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("T1", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(2)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(3)])).unwrap();

    // Test: 5 is NOT IN the subquery result
    let results = execute_select(&db, "SELECT 5 NOT IN (SELECT col1 FROM t1)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Boolean(true));

    // Test: 1 IS IN the subquery result, so NOT IN is false
    let results = execute_select(&db, "SELECT 1 NOT IN (SELECT col1 FROM t1)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Boolean(false));
}

// ========================================================================
// Scalar Subquery Tests
// ========================================================================

#[test]
fn test_scalar_subquery_without_from() {
    // Test scalar subquery returning a single value
    let schema = TableSchema::new(
        "T1".to_string(),
        vec![ColumnSchema::new("COL1".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("T1", Row::new(vec![SqlValue::Integer(42)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(10)])).unwrap();

    // Test: Get first value from subquery
    let results = execute_select(&db, "SELECT (SELECT col1 FROM t1 LIMIT 1)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(42));
}

#[test]
fn test_scalar_subquery_without_from_empty() {
    // Test scalar subquery that returns no rows (should return NULL)
    let schema = TableSchema::new(
        "T1".to_string(),
        vec![ColumnSchema::new("COL1".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Test: Empty subquery returns NULL
    let results = execute_select(&db, "SELECT (SELECT col1 FROM t1 WHERE col1 > 100)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null);
}

// ========================================================================
// EXISTS Subquery Tests
// ========================================================================

#[test]
fn test_exists_subquery_without_from_true() {
    // Test EXISTS where subquery returns rows
    let schema = TableSchema::new(
        "T1".to_string(),
        vec![ColumnSchema::new("COL1".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("T1", Row::new(vec![SqlValue::Integer(1)])).unwrap();

    // Test: EXISTS returns true when subquery has rows
    let results = execute_select(&db, "SELECT EXISTS (SELECT * FROM t1)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Boolean(true));
}

#[test]
fn test_exists_subquery_without_from_false() {
    // Test EXISTS where subquery returns no rows
    let schema = TableSchema::new(
        "T1".to_string(),
        vec![ColumnSchema::new("COL1".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Test: EXISTS returns false when subquery has no rows
    let results = execute_select(&db, "SELECT EXISTS (SELECT * FROM t1 WHERE col1 > 100)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Boolean(false));
}

#[test]
fn test_not_exists_subquery_without_from() {
    // Test NOT EXISTS
    let schema = TableSchema::new(
        "T1".to_string(),
        vec![ColumnSchema::new("COL1".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Test: NOT EXISTS returns true when subquery has no rows
    let results =
        execute_select(&db, "SELECT NOT EXISTS (SELECT * FROM t1 WHERE col1 > 100)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Boolean(true));
}

// ========================================================================
// ALL/ANY Quantified Comparison Tests
// ========================================================================

#[test]
fn test_all_quantified_comparison_without_from() {
    // Test ALL quantified comparison
    let schema = TableSchema::new(
        "T1".to_string(),
        vec![ColumnSchema::new("COL1".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("T1", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(2)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(3)])).unwrap();

    // Test: 5 > ALL values (true)
    let results = execute_select(&db, "SELECT 5 > ALL (SELECT col1 FROM t1)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Boolean(true));

    // Test: 2 > ALL values (false, 2 is not > 2 or 3)
    let results = execute_select(&db, "SELECT 2 > ALL (SELECT col1 FROM t1)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Boolean(false));
}

#[test]
fn test_any_quantified_comparison_without_from() {
    // Test ANY/SOME quantified comparison
    let schema = TableSchema::new(
        "T1".to_string(),
        vec![ColumnSchema::new("COL1".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("T1", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(2)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(3)])).unwrap();

    // Test: 2 = ANY value (true, 2 equals 2)
    let results = execute_select(&db, "SELECT 2 = ANY (SELECT col1 FROM t1)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Boolean(true));

    // Test: 5 = ANY value (false, 5 not in set)
    let results = execute_select(&db, "SELECT 5 = ANY (SELECT col1 FROM t1)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Boolean(false));
}

#[test]
fn test_some_quantified_comparison_without_from() {
    // Test SOME (synonym for ANY)
    let schema = TableSchema::new(
        "T1".to_string(),
        vec![ColumnSchema::new("COL1".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("T1", Row::new(vec![SqlValue::Integer(10)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(20)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(30)])).unwrap();

    // Test: 15 < SOME value (true, 15 < 20 and 15 < 30)
    let results = execute_select(&db, "SELECT 15 < SOME (SELECT col1 FROM t1)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Boolean(true));
}

// ========================================================================
// Complex Expression Tests
// ========================================================================

#[test]
fn test_multiple_subqueries_without_from() {
    // Test multiple subqueries in same SELECT list
    let schema = TableSchema::new(
        "T1".to_string(),
        vec![ColumnSchema::new("COL1".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("T1", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(2)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(3)])).unwrap();

    // Test: Multiple subqueries in SELECT list
    let results = execute_select(
        &db,
        "SELECT 1 IN (SELECT col1 FROM t1), EXISTS (SELECT * FROM t1), (SELECT col1 FROM t1 LIMIT 1)",
    )
    .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Boolean(true)); // 1 IN (...)
    assert_eq!(results[0].values[1], SqlValue::Boolean(true)); // EXISTS (...)
    assert_eq!(results[0].values[2], SqlValue::Integer(1)); // scalar subquery
}

#[test]
fn test_subquery_with_arithmetic_expression() {
    // Test subquery combined with arithmetic
    let schema = TableSchema::new(
        "T1".to_string(),
        vec![ColumnSchema::new("COL1".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("T1", Row::new(vec![SqlValue::Integer(5)])).unwrap();

    // Test: Arithmetic with scalar subquery
    let results = execute_select(&db, "SELECT (SELECT col1 FROM t1 LIMIT 1) * 2").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(10));
}
