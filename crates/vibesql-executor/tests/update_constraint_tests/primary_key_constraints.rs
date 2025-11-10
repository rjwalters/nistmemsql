use vibesql_executor::{ExecutorError, UpdateExecutor};
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

use super::constraint_test_utils::{
    create_update_with_id_clause, create_users_table_with_primary_key, insert_user_row_simple,
};

#[test]
fn test_update_primary_key_duplicate() {
    let mut db = Database::new();
    create_users_table_with_primary_key(&mut db);

    // Insert two rows
    insert_user_row_simple(&mut db, 1, "Alice").unwrap();
    insert_user_row_simple(&mut db, 2, "Bob").unwrap();

    // Try to update Bob's id to 1 (duplicate)
    let stmt = create_update_with_id_clause("users", "id", SqlValue::Integer(1), 2);

    let result = UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("PRIMARY KEY"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

#[test]
fn test_update_primary_key_to_unique_value() {
    let mut db = Database::new();
    create_users_table_with_primary_key(&mut db);

    // Insert two rows
    insert_user_row_simple(&mut db, 1, "Alice").unwrap();
    insert_user_row_simple(&mut db, 2, "Bob").unwrap();

    // Update Bob's id to 3 (unique) - should succeed
    let stmt = create_update_with_id_clause("users", "id", SqlValue::Integer(3), 2);

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify the update
    let table = db.get_table("users").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[1].get(0).unwrap(), &SqlValue::Integer(3));
}
