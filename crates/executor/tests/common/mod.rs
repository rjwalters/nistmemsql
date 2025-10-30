//! Common test utilities for executor tests

use executor::ExpressionEvaluator;

/// Creates a test evaluator with a simple schema for testing.
/// Returns an evaluator and a simple test row.
#[allow(dead_code)] // Test helper - available for all test modules
pub fn create_test_evaluator() -> (ExpressionEvaluator<'static>, storage::Row) {
    let schema = Box::leak(Box::new(catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    )));

    let evaluator = ExpressionEvaluator::new(schema);
    let row = storage::Row::new(vec![types::SqlValue::Integer(1)]);

    (evaluator, row)
}

/// Sets up the standard employees test table with sample data.
/// This table is used across multiple update test files.
#[allow(dead_code)] // Test helper - available for all test modules
pub fn setup_test_table(db: &mut storage::Database) {
    // Create table schema
    let schema = catalog::TableSchema::new(
        "employees".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
            catalog::ColumnSchema::new("salary".to_string(), types::DataType::Integer, true),
            catalog::ColumnSchema::new(
                "department".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "employees",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
            types::SqlValue::Integer(45000),
            types::SqlValue::Varchar("Engineering".to_string()),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Bob".to_string()),
            types::SqlValue::Integer(48000),
            types::SqlValue::Varchar("Engineering".to_string()),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Varchar("Charlie".to_string()),
            types::SqlValue::Integer(42000),
            types::SqlValue::Varchar("Sales".to_string()),
        ]),
    )
    .unwrap();
}
