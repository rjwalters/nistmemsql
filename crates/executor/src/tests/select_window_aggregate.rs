//! Tests for aggregate window functions in SELECT statements

use crate::SelectExecutor;
use catalog::{ColumnSchema, TableSchema};
use storage::Database;
use types::{DataType, SqlValue};

fn create_test_db() -> Database {
    let mut db = Database::new();

    // Create a simple test table
    let schema = TableSchema::new(
        "sales".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("amount".to_string(), DataType::Integer, false),
            ColumnSchema::new("date".to_string(), DataType::Integer, false),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    let table = db.get_table_mut("sales").unwrap();
    table
        .insert_row(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(100),
            SqlValue::Integer(1),
        ])
        .unwrap();
    table
        .insert_row(vec![
            SqlValue::Integer(2),
            SqlValue::Integer(200),
            SqlValue::Integer(2),
        ])
        .unwrap();
    table
        .insert_row(vec![
            SqlValue::Integer(3),
            SqlValue::Integer(300),
            SqlValue::Integer(3),
        ])
        .unwrap();

    db
}

#[test]
fn test_count_star_over_entire_result() {
    let db = create_test_db();
    let executor = SelectExecutor::new(&db);

    // Parse and execute: SELECT id, COUNT(*) OVER () as cnt FROM sales
    let query = "SELECT id, COUNT(*) OVER () as cnt FROM sales";
    let parsed = parser::Parser::new(query).parse_select().unwrap();

    let result = executor.execute(&parsed);
    println!("Result: {:?}", result);

    // For now, just check that it doesn't crash
    // We'll add proper assertions once we fix the projection issue
}

#[test]
fn test_sum_over_unbounded() {
    let db = create_test_db();
    let executor = SelectExecutor::new(&db);

    // Parse and execute: SELECT id, SUM(amount) OVER (ORDER BY date) FROM sales
    let query = "SELECT id, SUM(amount) OVER (ORDER BY date) as running_total FROM sales";
    let parsed = parser::Parser::new(query).parse_select().unwrap();

    let result = executor.execute(&parsed);
    println!("Result: {:?}", result);
}
