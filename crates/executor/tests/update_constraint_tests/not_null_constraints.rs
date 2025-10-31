use executor::{ExecutorError, UpdateExecutor};
use storage::Database;
use types::SqlValue;

#[path = "../common/mod.rs"]
mod common;

use common::setup_test_table;
use super::constraint_test_utils::create_update_with_id_clause;

#[test]
fn test_update_not_null_constraint_violation() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Try to set name (NOT NULL column) to NULL
    let stmt = create_update_with_id_clause("employees", "name", SqlValue::Null, 1);

    let result = UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("NOT NULL"));
            assert!(msg.contains("name"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

#[test]
fn test_update_nullable_column_to_null() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Set salary (nullable column) to NULL - should succeed
    let stmt = create_update_with_id_clause("employees", "salary", SqlValue::Null, 1);

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify salary was set to NULL
    let table = db.get_table("employees").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(2).unwrap(), &SqlValue::Null);
}
