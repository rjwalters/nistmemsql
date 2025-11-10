//! Common test utilities for executor tests

pub mod insert_constraint_fixtures;

use vibesql_executor::ExpressionEvaluator;

/// Creates a test evaluator with a simple schema for testing.
/// Returns an evaluator and a simple test row.
#[allow(dead_code)] // Test helper - available for all test modules
pub fn create_test_evaluator() -> (ExpressionEvaluator<'static>, vibesql_storage::Row) {
    let schema = Box::leak(Box::new(vibesql_catalog::TableSchema::new(
        "test".to_string(),
        vec![vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false)],
    )));

    let evaluator = ExpressionEvaluator::new(schema);
    let row = vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]);

    (evaluator, row)
}

/// Sets up the standard employees test table with sample data.
/// This table is used across multiple update test files.
#[allow(dead_code)] // Test helper - available for all test modules
pub fn setup_test_table(db: &mut vibesql_storage::Database) {
    // Create table schema
    let schema = vibesql_catalog::TableSchema::new(
        "employees".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
            vibesql_catalog::ColumnSchema::new("salary".to_string(), vibesql_types::DataType::Integer, true),
            vibesql_catalog::ColumnSchema::new(
                "department".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Varchar("Alice".to_string()),
            vibesql_types::SqlValue::Integer(45000),
            vibesql_types::SqlValue::Varchar("Engineering".to_string()),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Varchar("Bob".to_string()),
            vibesql_types::SqlValue::Integer(48000),
            vibesql_types::SqlValue::Varchar("Engineering".to_string()),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Varchar("Charlie".to_string()),
            vibesql_types::SqlValue::Integer(42000),
            vibesql_types::SqlValue::Varchar("Sales".to_string()),
        ]),
    )
    .unwrap();
}
