//! Tests for SQL three-valued logic with NULL in AND/OR operators
//!
//! These tests verify proper handling of NULL values in boolean expressions
//! according to the SQL standard.
//!
//! Related to issue #1039: Ensure short-circuit evaluation maintains correct
//! SQL three-valued logic semantics.

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

fn create_test_db_with_nulls() -> Database {
    let schema = TableSchema::new(
        "TEST".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("BOOL_VAL".to_string(), DataType::Boolean, true), // nullable
            ColumnSchema::new("NUM_VAL".to_string(), DataType::Integer, true),  // nullable
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Row 1: ID=1, BOOL_VAL=TRUE, NUM_VAL=10
    db.insert_row(
        "TEST",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Boolean(true),
            SqlValue::Integer(10),
        ]),
    )
    .unwrap();

    // Row 2: ID=2, BOOL_VAL=FALSE, NUM_VAL=20
    db.insert_row(
        "TEST",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Boolean(false),
            SqlValue::Integer(20),
        ]),
    )
    .unwrap();

    // Row 3: ID=3, BOOL_VAL=NULL, NUM_VAL=30
    db.insert_row(
        "TEST",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Null,
            SqlValue::Integer(30),
        ]),
    )
    .unwrap();

    // Row 4: ID=4, BOOL_VAL=NULL, NUM_VAL=NULL
    db.insert_row(
        "TEST",
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Null,
            SqlValue::Null,
        ]),
    )
    .unwrap();

    db
}

// ============================================================================
// AND Operator Tests with NULL
// ============================================================================

#[test]
fn test_false_and_null() {
    let db = create_test_db_with_nulls();

    // FALSE AND NULL should return FALSE (not NULL!)
    // This tests the row where BOOL_VAL=FALSE (id=2)
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE (1 = 2) AND (BOOL_VAL IS NULL)",
    )
    .expect("Query should succeed");

    // Should return 0 rows because FALSE AND anything = FALSE
    assert_eq!(result.len(), 0, "FALSE AND NULL should filter out all rows");
}

#[test]
fn test_null_and_false() {
    let db = create_test_db_with_nulls();

    // NULL AND FALSE should return FALSE (not NULL!)
    // SQL Standard: NULL AND FALSE = FALSE
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE (BOOL_VAL IS NULL) AND (1 = 2)",
    )
    .expect("Query should succeed");

    // Should return 0 rows because NULL AND FALSE = FALSE
    // This is a CRITICAL test - current implementation may return rows with NULL
    assert_eq!(
        result.len(),
        0,
        "NULL AND FALSE should return FALSE and filter out all rows"
    );
}

#[test]
fn test_null_and_true() {
    let db = create_test_db_with_nulls();

    // NULL AND TRUE should return NULL
    // In WHERE clause, NULL is treated as FALSE, so no rows returned
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE (BOOL_VAL IS NULL) AND (1 = 1)",
    )
    .expect("Query should succeed");

    // Should return 0 rows because NULL AND TRUE = NULL, and NULL in WHERE = FALSE
    assert_eq!(
        result.len(),
        0,
        "NULL AND TRUE should return NULL, which filters rows in WHERE clause"
    );
}

#[test]
fn test_true_and_null() {
    let db = create_test_db_with_nulls();

    // TRUE AND NULL should return NULL
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE (1 = 1) AND (BOOL_VAL IS NULL)",
    )
    .expect("Query should succeed");

    // Should return 0 rows because TRUE AND NULL = NULL, treated as FALSE in WHERE
    assert_eq!(result.len(), 0, "TRUE AND NULL should return NULL");
}

#[test]
fn test_null_and_null() {
    let db = create_test_db_with_nulls();

    // NULL AND NULL should return NULL
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE (BOOL_VAL IS NULL) AND (NUM_VAL IS NULL)",
    )
    .expect("Query should succeed");

    // Should return 0 rows because NULL AND NULL = NULL
    assert_eq!(result.len(), 0, "NULL AND NULL should return NULL");
}

// ============================================================================
// OR Operator Tests with NULL
// ============================================================================

#[test]
fn test_true_or_null() {
    let db = create_test_db_with_nulls();

    // TRUE OR NULL should return TRUE (not NULL!)
    // This should return all rows
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE (1 = 1) OR (BOOL_VAL IS NULL)",
    )
    .expect("Query should succeed");

    // Should return all 4 rows because TRUE OR anything = TRUE
    assert_eq!(result.len(), 4, "TRUE OR NULL should return TRUE");
}

#[test]
fn test_null_or_true() {
    let db = create_test_db_with_nulls();

    // NULL OR TRUE should return TRUE (not NULL!)
    // SQL Standard: NULL OR TRUE = TRUE
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE (BOOL_VAL IS NULL) OR (1 = 1)",
    )
    .expect("Query should succeed");

    // Should return all 4 rows because NULL OR TRUE = TRUE
    // This is a CRITICAL test - current implementation may return only NULL rows
    assert_eq!(
        result.len(),
        4,
        "NULL OR TRUE should return TRUE and include all rows"
    );
}

#[test]
fn test_false_or_null() {
    let db = create_test_db_with_nulls();

    // FALSE OR NULL should return NULL
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE (1 = 2) OR (BOOL_VAL IS NULL)",
    )
    .expect("Query should succeed");

    // Should return 0 rows because FALSE OR NULL = NULL, treated as FALSE in WHERE
    assert_eq!(result.len(), 0, "FALSE OR NULL should return NULL");
}

#[test]
fn test_null_or_false() {
    let db = create_test_db_with_nulls();

    // NULL OR FALSE should return NULL
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE (BOOL_VAL IS NULL) OR (1 = 2)",
    )
    .expect("Query should succeed");

    // Should return 0 rows because NULL OR FALSE = NULL
    assert_eq!(result.len(), 0, "NULL OR FALSE should return NULL");
}

#[test]
fn test_null_or_null() {
    let db = create_test_db_with_nulls();

    // NULL OR NULL should return NULL
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE (BOOL_VAL IS NULL) OR (NUM_VAL IS NULL)",
    )
    .expect("Query should succeed");

    // Should return 0 rows because NULL OR NULL = NULL
    assert_eq!(result.len(), 0, "NULL OR NULL should return NULL");
}

// ============================================================================
// Complex NULL Logic Tests
// ============================================================================

#[test]
fn test_complex_null_and_expression() {
    let db = create_test_db_with_nulls();

    // (NULL AND FALSE) OR TRUE should return TRUE
    // Because: NULL AND FALSE = FALSE, FALSE OR TRUE = TRUE
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE ((BOOL_VAL IS NULL) AND (1 = 2)) OR (1 = 1)",
    )
    .expect("Query should succeed");

    // Should return all 4 rows
    assert_eq!(result.len(), 4, "(NULL AND FALSE) OR TRUE should return TRUE");
}

#[test]
fn test_complex_null_or_expression() {
    let db = create_test_db_with_nulls();

    // (NULL OR TRUE) AND FALSE should return FALSE
    // Because: NULL OR TRUE = TRUE, TRUE AND FALSE = FALSE
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE ((BOOL_VAL IS NULL) OR (1 = 1)) AND (1 = 2)",
    )
    .expect("Query should succeed");

    // Should return 0 rows
    assert_eq!(result.len(), 0, "(NULL OR TRUE) AND FALSE should return FALSE");
}

#[test]
fn test_null_with_column_values() {
    let db = create_test_db_with_nulls();

    // Test with actual NULL column values
    // Row 3: BOOL_VAL=NULL, NUM_VAL=30
    // (BOOL_VAL IS NULL) returns TRUE for row 3
    // (NUM_VAL > 20) returns TRUE for rows 3 and 4
    // NULL AND TRUE = NULL (treated as FALSE in WHERE)

    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE BOOL_VAL IS NULL AND NUM_VAL > 20",
    )
    .expect("Query should succeed");

    // Should return row 3 (BOOL_VAL=NULL, NUM_VAL=30)
    // But NOT row 4 (BOOL_VAL=NULL, NUM_VAL=NULL) because NULL > 20 is NULL
    assert_eq!(result.len(), 1, "Should return only row with BOOL_VAL=NULL and NUM_VAL > 20");

    // Verify it's row 3
    let row = &result[0];
    assert_eq!(row.get(0), Some(&SqlValue::Integer(3)));
}

#[test]
fn test_null_short_circuit_safety() {
    let db = create_test_db_with_nulls();

    // This test ensures that NULL on left side of OR doesn't prevent
    // evaluating a TRUE on the right side
    // NULL OR TRUE should return TRUE, not NULL
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE (NUM_VAL IS NULL) OR (ID > 0)",
    )
    .expect("Query should succeed");

    // Should return all 4 rows because NULL OR TRUE = TRUE
    assert_eq!(
        result.len(),
        4,
        "NULL OR TRUE should evaluate right side and return TRUE"
    );
}
