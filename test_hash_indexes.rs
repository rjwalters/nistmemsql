// Simple test to verify hash indexes work for constraint validation
use std::time::Instant;
use storage::{Database, Row};
use types::SqlValue;
use catalog::{ColumnSchema, DataType, TableSchema};

fn main() {
    println!("Testing hash indexes for constraint validation...");

    let mut db = Database::new();

    // Create a table with primary key
    let schema = TableSchema::with_primary_key(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar(100), false),
        ],
        vec!["id".to_string()],
    );

    db.create_table(schema).unwrap();

    // Insert many rows
    println!("Inserting 1000 rows...");
    let start = Instant::now();
    for i in 0..1000 {
        let row = Row::new(vec![
            SqlValue::Integer(i),
            SqlValue::Varchar(format!("User {}", i)),
        ]);
        db.insert_row("users", row).unwrap();
    }
    let insert_time = start.elapsed();
    println!("Insert time: {:?}", insert_time);

    // Try to insert a duplicate - should fail quickly with hash index
    println!("Testing duplicate primary key constraint...");
    let start = Instant::now();
    let duplicate_row = Row::new(vec![
        SqlValue::Integer(0),  // Duplicate ID
        SqlValue::Varchar("Duplicate User".to_string()),
    ]);
    let result = db.insert_row("users", duplicate_row);
    let constraint_time = start.elapsed();
    println!("Constraint check time: {:?}", constraint_time);
    assert!(result.is_err(), "Expected constraint violation");

    println!("Hash indexes working correctly!");
}
