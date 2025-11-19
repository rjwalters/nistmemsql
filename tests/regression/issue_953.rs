//! Test for issue #953: Column resolution fails for table aliases in multi-table queries

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
fn test_comma_separated_tables_with_alias() {
    // Create two tables
    let schema1 = TableSchema::new(
        "TAB1".to_string(),
        vec![
            ColumnSchema::new("COL0".to_string(), DataType::Integer, false),
            ColumnSchema::new("COL1".to_string(), DataType::Integer, false),
            ColumnSchema::new("COL2".to_string(), DataType::Integer, false),
        ],
    );

    let schema2 = TableSchema::new(
        "TAB2".to_string(),
        vec![
            ColumnSchema::new("COL0".to_string(), DataType::Integer, false),
            ColumnSchema::new("COL1".to_string(), DataType::Integer, false),
            ColumnSchema::new("COL2".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema1).unwrap();
    db.create_table(schema2).unwrap();

    // Insert test data
    db.insert_row(
        "TAB1",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(3)]),
    )
    .unwrap();

    db.insert_row(
        "TAB2",
        Row::new(vec![SqlValue::Integer(4), SqlValue::Integer(5), SqlValue::Integer(6)]),
    )
    .unwrap();

    // Test case 1: SELECT cor0.col0 FROM tab1, tab2 AS cor0
    // This should select column 0 from the aliased tab2 (as cor0)
    let result = execute_select(&db, "SELECT cor0.col0 FROM tab1, tab2 AS cor0");
    println!("Test 1 result: {:?}", result);

    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1, "Expected 1 row (1x1 CROSS JOIN)");
            assert_eq!(rows[0].values[0], SqlValue::Integer(4), "Expected value 4 from tab2.col0");
        }
        Err(e) => {
            panic!("Test 1 failed: {}", e);
        }
    }
}

#[test]
fn test_multiple_comma_separated_tables_with_alias() {
    // Create three tables
    let schema1 = TableSchema::new(
        "TAB1".to_string(),
        vec![
            ColumnSchema::new("COL0".to_string(), DataType::Integer, false),
            ColumnSchema::new("COL1".to_string(), DataType::Integer, false),
        ],
    );

    let schema2 = TableSchema::new(
        "TAB2".to_string(),
        vec![
            ColumnSchema::new("COL0".to_string(), DataType::Integer, false),
            ColumnSchema::new("COL1".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema1).unwrap();
    db.create_table(schema2.clone()).unwrap();

    // Insert test data
    db.insert_row("TAB1", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(2)])).unwrap();

    db.insert_row("TAB2", Row::new(vec![SqlValue::Integer(4), SqlValue::Integer(5)])).unwrap();

    // Test case: SELECT + cor0.col0 FROM tab1, tab2, tab1 cor0
    // This creates a 1x1x1 cross join and selects from the aliased tab1 (as cor0)
    let result = execute_select(&db, "SELECT + cor0.col0 FROM tab1, tab2, tab1 cor0");
    println!("Test 2 result: {:?}", result);

    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1, "Expected 1 row (1x1x1 CROSS JOIN)");
            assert_eq!(
                rows[0].values[0],
                SqlValue::Integer(1),
                "Expected value 1 from tab1 (aliased as cor0).col0"
            );
        }
        Err(e) => {
            panic!("Test 2 failed: {}", e);
        }
    }
}

#[test]
fn test_comma_with_expressions_using_alias() {
    // Create two tables
    let schema0 = TableSchema::new(
        "TAB0".to_string(),
        vec![
            ColumnSchema::new("COL0".to_string(), DataType::Integer, false),
            ColumnSchema::new("COL1".to_string(), DataType::Integer, false),
        ],
    );

    let schema2 = TableSchema::new(
        "TAB2".to_string(),
        vec![
            ColumnSchema::new("COL0".to_string(), DataType::Integer, false),
            ColumnSchema::new("COL1".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema0).unwrap();
    db.create_table(schema2).unwrap();

    // Insert test data
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(20)])).unwrap();

    db.insert_row("TAB2", Row::new(vec![SqlValue::Integer(4), SqlValue::Integer(5)])).unwrap();

    // Test case: SELECT DISTINCT - cor0.col1 + - cor0.col0 AS col1 FROM tab0, tab2 AS cor0
    // This should compute -(5) + -(4) = -9
    let result = execute_select(
        &db,
        "SELECT DISTINCT - cor0.col1 + - cor0.col0 AS col1 FROM tab0, tab2 AS cor0",
    );
    println!("Test 3 result: {:?}", result);

    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1, "Expected 1 row");
            assert_eq!(
                rows[0].values[0],
                SqlValue::Integer(-9),
                "Expected value -9 from calculation"
            );
        }
        Err(e) => {
            panic!("Test 3 failed: {}", e);
        }
    }
}
