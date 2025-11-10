//! Test helpers and utilities for iterator tests

use super::*;
use crate::evaluator::CombinedExpressionEvaluator;
use storage::Row;
use types::SqlValue;

/// Helper to create a simple schema for testing
pub(crate) fn test_schema() -> CombinedSchema {
    let table_schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    CombinedSchema::from_table("test".to_string(), table_schema)
}

/// Helper to create two-table schema for join tests
pub(crate) fn test_join_schemas() -> (CombinedSchema, CombinedSchema) {
    let left_schema = catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, false),
        ],
    );
    let right_schema = catalog::TableSchema::new(
        "t2".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("data".to_string(), types::DataType::Integer, false),
        ],
    );
    (
        CombinedSchema::from_table("t1".to_string(), left_schema),
        CombinedSchema::from_table("t2".to_string(), right_schema),
    )
}

mod basic;
mod join;
mod phase_c;
