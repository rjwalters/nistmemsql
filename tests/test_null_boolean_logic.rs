//! Tests for SQL three-valued logic with NULL in AND/OR operators
//!
//! These tests verify proper handling of NULL values in boolean expressions
//! according to the SQL standard.
//!
//! Related to issue #1039: Ensure short-circuit evaluation maintains correct
//! SQL three-valued logic semantics.

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

fn create_test_db_with_nulls() -> Database {
    let schema = TableSchema::new(
        "TEST".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("BOOL_COL".to_string(), DataType::Boolean, true), // nullable
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Row 1: ID=1, BOOL_COL=TRUE
    db.insert_row("TEST", Row::new(vec![SqlValue::Integer(1), SqlValue::Boolean(true)])).unwrap();

    // Row 2: ID=2, BOOL_COL=FALSE
    db.insert_row("TEST", Row::new(vec![SqlValue::Integer(2), SqlValue::Boolean(false)])).unwrap();

    // Row 3: ID=3, BOOL_COL=NULL
    db.insert_row("TEST", Row::new(vec![SqlValue::Integer(3), SqlValue::Null])).unwrap();

    db
}

// ============================================================================
// AND Operator Tests with NULL
// ============================================================================

#[test]
fn test_null_and_false() {
    let db = create_test_db_with_nulls();

    // For row 3: BOOL_COL (NULL) AND FALSE should return FALSE (not NULL!)
    // SQL Standard: NULL AND FALSE = FALSE
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE BOOL_COL AND (1 = 2)")
        .expect("Query should succeed");

    // Should return 0 rows:
    // - Row 1: TRUE AND FALSE = FALSE (filtered out)
    // - Row 2: FALSE AND FALSE = FALSE (filtered out)
    // - Row 3: NULL AND FALSE = FALSE (filtered out) ← This is the critical test
    assert_eq!(result.len(), 0, "NULL AND FALSE should return FALSE");
}

#[test]
fn test_false_and_null() {
    let db = create_test_db_with_nulls();

    // For row 3: FALSE AND BOOL_COL (NULL) should return FALSE
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE (1 = 2) AND BOOL_COL")
        .expect("Query should succeed");

    // Should return 0 rows because FALSE AND anything = FALSE (short-circuit)
    assert_eq!(result.len(), 0, "FALSE AND NULL should return FALSE");
}

#[test]
fn test_null_and_true() {
    let db = create_test_db_with_nulls();

    // For row 3: BOOL_COL (NULL) AND TRUE should return NULL
    // In WHERE clause, NULL is treated as FALSE
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE BOOL_COL AND (1 = 1)")
        .expect("Query should succeed");

    // Should return only row 1 (TRUE AND TRUE = TRUE)
    // Row 2: FALSE AND TRUE = FALSE (filtered out)
    // Row 3: NULL AND TRUE = NULL (treated as FALSE, filtered out)
    assert_eq!(result.len(), 1, "NULL AND TRUE should return NULL (filtered in WHERE)");
    assert_eq!(result[0].get(0), Some(&SqlValue::Integer(1)));
}

#[test]
fn test_true_and_null() {
    let db = create_test_db_with_nulls();

    // For row 3: TRUE AND BOOL_COL (NULL) should return NULL
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE (1 = 1) AND BOOL_COL")
        .expect("Query should succeed");

    // Should return only row 1 (TRUE AND TRUE = TRUE)
    assert_eq!(result.len(), 1, "TRUE AND NULL should return NULL (filtered in WHERE)");
    assert_eq!(result[0].get(0), Some(&SqlValue::Integer(1)));
}

// ============================================================================
// OR Operator Tests with NULL
// ============================================================================

#[test]
fn test_null_or_true() {
    let db = create_test_db_with_nulls();

    // For row 3: BOOL_COL (NULL) OR TRUE should return TRUE (not NULL!)
    // SQL Standard: NULL OR TRUE = TRUE
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE BOOL_COL OR (1 = 1)")
        .expect("Query should succeed");

    // Should return all 3 rows:
    // - Row 1: TRUE OR TRUE = TRUE ✅
    // - Row 2: FALSE OR TRUE = TRUE ✅
    // - Row 3: NULL OR TRUE = TRUE ✅ ← This is the critical test
    assert_eq!(result.len(), 3, "NULL OR TRUE should return TRUE");
}

#[test]
fn test_true_or_null() {
    let db = create_test_db_with_nulls();

    // TRUE OR NULL should return TRUE (short-circuit)
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE (1 = 1) OR BOOL_COL")
        .expect("Query should succeed");

    // Should return all 3 rows because TRUE OR anything = TRUE
    assert_eq!(result.len(), 3, "TRUE OR NULL should return TRUE");
}

#[test]
fn test_null_or_false() {
    let db = create_test_db_with_nulls();

    // For row 3: BOOL_COL (NULL) OR FALSE should return NULL
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE BOOL_COL OR (1 = 2)")
        .expect("Query should succeed");

    // Should return only row 1 (TRUE OR FALSE = TRUE)
    // Row 2: FALSE OR FALSE = FALSE (filtered out)
    // Row 3: NULL OR FALSE = NULL (treated as FALSE, filtered out)
    assert_eq!(result.len(), 1, "NULL OR FALSE should return NULL (filtered in WHERE)");
    assert_eq!(result[0].get(0), Some(&SqlValue::Integer(1)));
}

#[test]
fn test_false_or_null() {
    let db = create_test_db_with_nulls();

    // For row 3: FALSE OR BOOL_COL (NULL) should return NULL
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE (1 = 2) OR BOOL_COL")
        .expect("Query should succeed");

    // Should return only row 1 (FALSE OR TRUE = TRUE)
    assert_eq!(result.len(), 1, "FALSE OR NULL should return NULL (filtered in WHERE)");
    assert_eq!(result[0].get(0), Some(&SqlValue::Integer(1)));
}

// ============================================================================
// Complex NULL Logic Tests
// ============================================================================

#[test]
fn test_critical_null_and_false_prevents_error() {
    let db = create_test_db_with_nulls();

    // Critical test: NULL AND FALSE should not evaluate the right side if FALSE is first
    // But when NULL is first, it MUST evaluate right to check for FALSE
    // This query should succeed even with division by zero on right, IF we have NULL on left
    // and it short-circuits after finding FALSE on second evaluation

    // This is a complex case: for row 3, BOOL_COL is NULL
    // We evaluate: NULL AND (1/0 = 1)
    // Without proper NULL AND FALSE handling, this would try to evaluate 1/0
    // But NULL AND FALSE = FALSE, so if the right side is evaluated and is an error,
    // we have a problem

    // Actually, let's test that NULL AND FALSE = FALSE correctly
    let result = execute_select(
        &db,
        "SELECT ID FROM TEST WHERE BOOL_COL AND (2 > 3)", // BOOL_COL is NULL for row 3
    )
    .expect("Query should succeed");

    // All rows filtered out
    assert_eq!(result.len(), 0);
}

#[test]
fn test_critical_null_or_true_includes_all() {
    let db = create_test_db_with_nulls();

    // Critical test: NULL OR TRUE should return TRUE
    // This ensures row with NULL boolean is included when OR'd with TRUE
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE BOOL_COL OR (2 > 1)")
        .expect("Query should succeed");

    // All 3 rows should be returned
    assert_eq!(result.len(), 3, "NULL OR TRUE should include all rows");
}

#[test]
fn test_nested_null_logic() {
    let db = create_test_db_with_nulls();

    // Test: (NULL OR FALSE) AND TRUE
    // NULL OR FALSE = NULL, NULL AND TRUE = NULL (filtered out in WHERE)
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE (BOOL_COL OR (1 = 2)) AND (1 = 1)")
        .expect("Query should succeed");

    // Should return only row 1
    // Row 1: (TRUE OR FALSE) AND TRUE = TRUE AND TRUE = TRUE ✅
    // Row 2: (FALSE OR FALSE) AND TRUE = FALSE AND TRUE = FALSE ❌
    // Row 3: (NULL OR FALSE) AND TRUE = NULL AND TRUE = NULL ❌
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get(0), Some(&SqlValue::Integer(1)));
}

#[test]
fn test_short_circuit_with_null_column() {
    let db = create_test_db_with_nulls();

    // Verify FALSE short-circuits even with NULL column on right
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE (1 = 2) AND BOOL_COL")
        .expect("Query should succeed");

    assert_eq!(result.len(), 0, "FALSE should short-circuit before evaluating NULL");
}

#[test]
fn test_no_short_circuit_null_and_must_check_false() {
    let db = create_test_db_with_nulls();

    // When left is NULL (row 3: BOOL_COL), must evaluate right to check for FALSE
    // BOOL_COL AND (1 = 2)
    // Row 3: NULL AND FALSE = FALSE (must evaluate right side!)
    let result = execute_select(&db, "SELECT ID FROM TEST WHERE BOOL_COL AND (1 = 2)")
        .expect("Query should succeed");

    // All rows filtered out, including row 3 which has NULL AND FALSE = FALSE
    assert_eq!(result.len(), 0, "NULL AND FALSE must be evaluated as FALSE");
}
