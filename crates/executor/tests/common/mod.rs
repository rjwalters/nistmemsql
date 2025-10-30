//! Common test utilities for executor tests

use catalog;
use executor::ExpressionEvaluator;
use storage;
use types;

/// Creates a test evaluator with a simple schema for testing.
/// Returns an evaluator and a simple test row.
pub fn create_test_evaluator() -> (ExpressionEvaluator<'static>, storage::Row) {
    let schema = Box::leak(Box::new(catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    )));

    let evaluator = ExpressionEvaluator::new(schema);
    let row = storage::Row::new(vec![types::SqlValue::Integer(1)]);

    (evaluator, row)
}
