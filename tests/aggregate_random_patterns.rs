//! Integration test for random aggregate patterns from sqllogictest suite
//! These tests capture real failure patterns from random/aggregates tests

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

fn execute_query(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

fn execute_statement(db: &Database, sql: &str) -> Result<(), String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    db.execute_statement(&stmt).map_err(|e| format!("Execution error: {:?}", e))
}

#[test]
fn test_aggregate_with_unary_operators() {
    let db = Database::new_in_memory();

    // Setup from slt_good_0.test
    execute_statement(&db, "CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER)").unwrap();
    execute_statement(&db, "INSERT INTO tab1 VALUES(51,14,96)").unwrap();
    execute_statement(&db, "INSERT INTO tab1 VALUES(85,5,59)").unwrap();
    execute_statement(&db, "INSERT INTO tab1 VALUES(91,47,68)").unwrap();

    // Test COUNT with double unary + operators on argument
    // From label-11 in slt_good_0.test
    let result = execute_query(
        &db,
        "SELECT - COUNT( ALL + + col1 ) AS col2, COUNT( ALL - - col1 ) FROM tab1",
    )
    .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Integer(-3));
    assert_eq!(result[0].values[1], SqlValue::Integer(3));
}

#[test]
fn test_aggregate_with_arithmetic_in_select() {
    let db = Database::new_in_memory();

    // Setup
    execute_statement(&db, "CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER)").unwrap();
    execute_statement(&db, "INSERT INTO tab0 VALUES(97,1,99)").unwrap();
    execute_statement(&db, "INSERT INTO tab0 VALUES(15,81,47)").unwrap();
    execute_statement(&db, "INSERT INTO tab0 VALUES(87,21,10)").unwrap();

    // Test COUNT with arithmetic in SELECT
    // From label-13 in slt_good_0.test
    let result = execute_query(&db, "SELECT - COUNT( * ) * - 31 AS col2 FROM tab0").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Integer(93));
}

#[test]
fn test_aggregate_sum_with_cast() {
    let db = Database::new_in_memory();

    // Setup
    execute_statement(&db, "CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER)").unwrap();
    execute_statement(&db, "INSERT INTO tab1 VALUES(51,14,96)").unwrap();

    // Test SUM with CAST
    let result =
        execute_query(&db, "SELECT + SUM( CAST( NULL AS SIGNED ) ) AS col1 FROM tab1").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Null);
}

#[test]
fn test_aggregate_with_complex_where() {
    let db = Database::new_in_memory();

    // Setup
    execute_statement(&db, "CREATE TABLE tab0(col0 INTEGER, col1 INTEGER)").unwrap();
    execute_statement(&db, "INSERT INTO tab0 VALUES(1, 2)").unwrap();

    // Test SUM with WHERE that filters all rows
    let result =
        execute_query(&db, "SELECT - SUM( 1 ) FROM tab0 AS cor0 WHERE NOT NULL NOT IN ( - col1 )")
            .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Null);
}

#[test]
fn test_aggregate_distinct_with_arithmetic() {
    let db = Database::new_in_memory();

    // Setup from slt_good_0.test
    execute_statement(&db, "CREATE TABLE tab1(col0 INTEGER, col1 INTEGER, col2 INTEGER)").unwrap();
    execute_statement(&db, "INSERT INTO tab1 VALUES(51,14,96)").unwrap();
    execute_statement(&db, "INSERT INTO tab1 VALUES(85,5,59)").unwrap();
    execute_statement(&db, "INSERT INTO tab1 VALUES(91,47,68)").unwrap();

    // Test DISTINCT with negation and aggregate
    let result =
        execute_query(&db, "SELECT DISTINCT + - SUM( DISTINCT - col1 ) FROM tab1").unwrap();
    assert_eq!(result.len(), 1);
    // sum(distinct -14, -5, -47) = -66, then + - (-66) = 66
    assert_eq!(result[0].values[0], SqlValue::Integer(66));
}
