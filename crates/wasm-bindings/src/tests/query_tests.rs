//! Query execution tests for WASM API
//!
//! Tests SELECT queries, WHERE clauses, and data type conversion

use crate::*;
use super::helpers::execute_sql;

#[test]
fn test_query_select() {
    let mut db = storage::Database::new();

    // Setup: Create and populate table
    execute_sql(&mut db, "CREATE TABLE customers (id INTEGER, name VARCHAR(50))").unwrap();
    execute_sql(&mut db, "INSERT INTO customers VALUES (1, 'Alice')").unwrap();
    execute_sql(&mut db, "INSERT INTO customers VALUES (2, 'Bob')").unwrap();

    // Execute SELECT query
    let sql = "SELECT id, name FROM customers";
    let stmt = parser::Parser::parse_sql(sql).expect("Parse failed");
    match stmt {
        ast::Statement::Select(select_stmt) => {
            let select_executor = executor::SelectExecutor::new(&db);
            let result =
                select_executor.execute_with_columns(&select_stmt).expect("Query failed");

            // Column names are normalized to uppercase
            assert_eq!(result.columns, vec!["ID", "NAME"]);
            assert_eq!(result.rows.len(), 2);
        }
        _ => panic!("Expected Select statement"),
    }
}

#[test]
fn test_query_where_clause() {
    let mut db = storage::Database::new();

    // Setup
    execute_sql(&mut db, "CREATE TABLE orders (id INTEGER, amount INTEGER)").unwrap();
    execute_sql(&mut db, "INSERT INTO orders VALUES (1, 100)").unwrap();
    execute_sql(&mut db, "INSERT INTO orders VALUES (2, 200)").unwrap();
    execute_sql(&mut db, "INSERT INTO orders VALUES (3, 150)").unwrap();

    // Query with WHERE clause
    let sql = "SELECT id, amount FROM orders WHERE amount > 120";
    let stmt = parser::Parser::parse_sql(sql).expect("Parse failed");
    match stmt {
        ast::Statement::Select(select_stmt) => {
            let select_executor = executor::SelectExecutor::new(&db);
            let result = select_executor.execute(&select_stmt).expect("Query failed");

            // Should only return orders with amount > 120 (order 2 and 3)
            assert_eq!(result.len(), 2);
        }
        _ => panic!("Expected Select statement"),
    }
}

#[test]
fn test_query_value_conversion() {
    let mut db = storage::Database::new();

    // Setup with various data types
    execute_sql(
        &mut db,
        "CREATE TABLE types_test (i INTEGER, f DOUBLE, s VARCHAR(50), b BOOLEAN)",
    )
    .unwrap();
    execute_sql(&mut db, "INSERT INTO types_test VALUES (42, 3.14, 'hello', TRUE)").unwrap();

    let sql = "SELECT i, f, s, b FROM types_test";
    let stmt = parser::Parser::parse_sql(sql).expect("Parse failed");
    match stmt {
        ast::Statement::Select(select_stmt) => {
            let select_executor = executor::SelectExecutor::new(&db);
            let result = select_executor.execute(&select_stmt).expect("Query failed");

            assert_eq!(result.len(), 1);
            let row = &result[0];

            // Verify value types are converted correctly
            assert_eq!(row.values[0], types::SqlValue::Integer(42));
            assert!(matches!(row.values[2], types::SqlValue::Varchar(_)));
            assert_eq!(row.values[3], types::SqlValue::Boolean(true));
        }
        _ => panic!("Expected Select statement"),
    }
}
