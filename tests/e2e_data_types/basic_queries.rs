//! End-to-end tests for basic SQL query features.

use super::fixtures::*;
use storage::{Database, Row};
use types::{DataType, SqlValue};
use catalog::{ColumnSchema, TableSchema};

#[test]
fn test_e2e_distinct() {
    // Create table with duplicate ages
    let schema = TableSchema::new(
        "PEOPLE".to_string(),
        vec![
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("AGE".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert rows with duplicate ages
    db.insert_row(
        "PEOPLE",
        Row::new(vec![
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Integer(25),
        ]),
    )
    .unwrap();
    db.insert_row(
        "PEOPLE",
        Row::new(vec![
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Integer(30),
        ]),
    )
    .unwrap();
    db.insert_row(
        "PEOPLE",
        Row::new(vec![
            SqlValue::Varchar("Charlie".to_string()),
            SqlValue::Integer(25),
        ]),
    )
    .unwrap();

    let results = execute_select(&db, "SELECT DISTINCT age FROM people").unwrap();
    assert_eq!(results.len(), 2, "DISTINCT should return 2 unique ages");
    assert_eq!(results[0].values[0], SqlValue::Integer(25));
    assert_eq!(results[1].values[0], SqlValue::Integer(30));
}

#[test]
fn test_e2e_group_by_count() {
    let schema = TableSchema::new(
        "SALES".to_string(),
        vec![
            ColumnSchema::new(
                "PRODUCT".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("QUANTITY".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row(
        "SALES",
        Row::new(vec![
            SqlValue::Varchar("Apple".to_string()),
            SqlValue::Integer(10),
        ]),
    )
    .unwrap();
    db.insert_row(
        "SALES",
        Row::new(vec![
            SqlValue::Varchar("Banana".to_string()),
            SqlValue::Integer(5),
        ]),
    )
    .unwrap();
    db.insert_row(
        "SALES",
        Row::new(vec![
            SqlValue::Varchar("Apple".to_string()),
            SqlValue::Integer(15),
        ]),
    )
    .unwrap();

    let results =
        execute_select(&db, "SELECT product, COUNT(*) FROM sales GROUP BY product").unwrap();
    assert_eq!(results.len(), 2);

    // Find Apple row
    let apple_row = results
        .iter()
        .find(|r| r.values[0] == SqlValue::Varchar("Apple".to_string()));
    assert!(apple_row.is_some());
    assert_eq!(apple_row.unwrap().values[1], SqlValue::Integer(2));

    // Find Banana row
    let banana_row = results
        .iter()
        .find(|r| r.values[0] == SqlValue::Varchar("Banana".to_string()));
    assert!(banana_row.is_some());
    assert_eq!(banana_row.unwrap().values[1], SqlValue::Integer(1));
}

#[test]
fn test_e2e_limit_offset() {
    let schema = create_users_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_users(&mut db);

    // Test LIMIT
    let results = execute_select(&db, "SELECT name FROM users LIMIT 2").unwrap();
    assert_eq!(results.len(), 2);

    // Test LIMIT with OFFSET
    let results = execute_select(&db, "SELECT name FROM users LIMIT 2 OFFSET 2").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].values[0],
        SqlValue::Varchar("Charlie".to_string())
    );
    assert_eq!(
        results[1].values[0],
        SqlValue::Varchar("Diana".to_string())
    );
}
