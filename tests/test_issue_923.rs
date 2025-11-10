//! Test for issue #923: Support unary operators in aggregate function arguments

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

#[test]
fn test_max_with_unary_plus() {
    let schema = TableSchema::new(
        "TAB0".to_string(),
        vec![ColumnSchema::new("COL0".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(5)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(3)])).unwrap();

    // Test MAX(+COL0) - unary plus
    let result = execute_select(&db, "SELECT MAX(+col0) FROM tab0");
    println!("MAX(+col0) result: {:?}", result);

    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], SqlValue::Integer(5));
        }
        Err(e) => {
            panic!("Expected success, got error: {}", e);
        }
    }
}

#[test]
fn test_max_with_unary_minus() {
    let schema = TableSchema::new(
        "TAB0".to_string(),
        vec![ColumnSchema::new("COL0".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(5)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(3)])).unwrap();

    // Test MAX(-COL0) - unary minus
    let result = execute_select(&db, "SELECT MAX(-col0) FROM tab0");
    println!("MAX(-col0) result: {:?}", result);

    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1);
            // MAX(-col0) where col0 = {1, 5, 3} → {-1, -5, -3} → max is -1
            assert_eq!(rows[0].values[0], SqlValue::Integer(-1));
        }
        Err(e) => {
            panic!("Expected success, got error: {}", e);
        }
    }
}

#[test]
fn test_count_with_not() {
    let schema = TableSchema::new(
        "TAB0".to_string(),
        vec![ColumnSchema::new("COL1".to_string(), DataType::Boolean, true)], // nullable
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row("TAB0", Row::new(vec![SqlValue::Boolean(true)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Boolean(false)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Boolean(true)])).unwrap();

    // Test COUNT(NOT col1)
    let result = execute_select(&db, "SELECT COUNT(NOT col1) FROM tab0");
    println!("COUNT(NOT col1) result: {:?}", result);

    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1);
            // COUNT counts non-NULL values
            // NOT true = false, NOT false = true, NOT true = false
            // All non-NULL, so COUNT = 3
            assert_eq!(rows[0].values[0], SqlValue::Integer(3));
        }
        Err(e) => {
            panic!("Expected success, got error: {}", e);
        }
    }
}

#[test]
fn test_sum_with_unary_minus() {
    let schema = TableSchema::new(
        "TAB0".to_string(),
        vec![ColumnSchema::new("COL0".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(5)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(3)])).unwrap();

    // Test SUM(-COL0) - unary minus
    let result = execute_select(&db, "SELECT SUM(-col0) FROM tab0");
    println!("SUM(-col0) result: {:?}", result);

    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1);
            // SUM(-col0) where col0 = {1, 5, 3} → -1 + -5 + -3 = -9
            // SUM returns Numeric type
            match &rows[0].values[0] {
                SqlValue::Numeric(n) => {
                    assert_eq!(n.to_string(), "-9");
                }
                other => panic!("Expected Numeric(-9), got {:?}", other),
            }
        }
        Err(e) => {
            panic!("Expected success, got error: {}", e);
        }
    }
}
