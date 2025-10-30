//! Tests for DISTINCT in aggregate functions (E091-07)
//!
//! Tests COUNT, SUM, AVG, MIN, MAX with DISTINCT quantifier
//! as specified in SQL:1999 Section 6.16 (Set functions)

use super::super::*;

/// Helper to create a test database with duplicate values
pub(crate) fn create_test_db_with_duplicates() -> storage::Database {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "sales".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert data with duplicate amounts: 100, 100, 200, 100, 300, 200
    // Unique values: 100, 200, 300 (3 distinct values)
    db.insert_row(
        "sales",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(100)]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(100)]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(200)]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![types::SqlValue::Integer(4), types::SqlValue::Integer(100)]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![types::SqlValue::Integer(5), types::SqlValue::Integer(300)]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![types::SqlValue::Integer(6), types::SqlValue::Integer(200)]),
    )
    .unwrap();

    db
}

mod count_distinct;
mod sum_distinct;
mod avg_distinct;
mod min_max_distinct;
mod edge_cases;
