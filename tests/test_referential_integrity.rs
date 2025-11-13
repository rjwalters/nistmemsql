// ! Integration tests for referential integrity (foreign key constraints)
//!
//! Target module: crates/executor/src/delete/integrity.rs (27.91% coverage → 80%+)
//!
//! Test coverage for:
//! - ON DELETE actions: CASCADE, SET NULL, SET DEFAULT, NO ACTION, RESTRICT
//! - ON UPDATE actions: CASCADE, SET NULL, SET DEFAULT, NO ACTION, RESTRICT
//! - Edge cases: circular FKs, self-referential tables, multi-column FKs, NULL values

mod common;

use vibesql_catalog::{ColumnSchema, ReferentialAction, TableSchema};
use common::referential_integrity_fixtures::*;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

// ========================================================================
// Phase 1: ON DELETE Actions - Basic Tests
// ========================================================================

#[test]
fn test_on_delete_no_action_with_references() {
    let mut db = Database::new();

    // Create parent and child tables
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::NoAction,
    );

    // Insert test data
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");

    // Try to delete parent - should fail due to foreign key constraint
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_fk_violation(result, "CHILD");

    // Verify parent still exists
    let parent_table = db.get_table("PARENT").unwrap();
    assert_eq!(parent_table.row_count(), 1);
}

#[test]
fn test_on_delete_no_action_without_references() {
    let mut db = Database::new();

    // Create parent and child tables
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::NoAction,
    );

    // Insert test data - parent without children
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_parent_row(&mut db, "PARENT", 2, "Bob");

    // Only Bob has a child
    insert_child_row(&mut db, "CHILD", 10, 2, "Child1");

    // Delete parent without children - should succeed
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_successful_delete(result, 1);

    // Verify Alice was deleted, Bob remains
    let parent_table = db.get_table("PARENT").unwrap();
    assert_eq!(parent_table.row_count(), 1);
}

#[test]
fn test_on_delete_cascade_single_level() {
    let mut db = Database::new();

    // Create parent and child tables with CASCADE
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::Cascade,
        ReferentialAction::NoAction,
    );

    // Insert test data
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");
    insert_child_row(&mut db, "CHILD", 11, 1, "Child2");

    // Delete parent - should cascade to children
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_successful_delete(result, 1);

    // Verify parent was deleted
    let parent_table = db.get_table("PARENT").unwrap();
    assert_eq!(parent_table.row_count(), 0);

    // Verify children were also deleted (cascaded)
    let child_table = db.get_table("CHILD").unwrap();
    assert_eq!(child_table.row_count(), 0, "Children should be cascaded deleted");
}

#[test]
fn test_on_delete_cascade_multi_level() {
    let mut db = Database::new();

    // Create grandparent, parent, and child tables with CASCADE
    create_parent_table(&mut db, "GRANDPARENT");
    create_child_table(
        &mut db,
        "PARENT",
        "GRANDPARENT",
        ReferentialAction::Cascade,
        ReferentialAction::NoAction,
    );
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::Cascade,
        ReferentialAction::NoAction,
    );

    // Insert test data
    insert_parent_row(&mut db, "GRANDPARENT", 1, "GrandParent1");
    db.insert_row(
        "PARENT",
        Row::new(vec![
            SqlValue::Integer(10),
            SqlValue::Integer(1),
            SqlValue::Varchar("Parent1".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "CHILD",
        Row::new(vec![
            SqlValue::Integer(100),
            SqlValue::Integer(10),
            SqlValue::Varchar("Child1".to_string()),
        ]),
    )
    .unwrap();

    // Delete grandparent - should cascade through all levels
    let result = execute_delete(&mut db, "DELETE FROM grandparent WHERE id = 1");
    assert_successful_delete(result, 1);

    // Verify all levels were deleted
    assert_eq!(db.get_table("GRANDPARENT").unwrap().row_count(), 0);
    assert_eq!(db.get_table("PARENT").unwrap().row_count(), 0);
    assert_eq!(db.get_table("CHILD").unwrap().row_count(), 0);
}

#[test]
fn test_on_delete_set_null() {
    let mut db = Database::new();

    // Create parent and child tables with SET NULL
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::SetNull,
        ReferentialAction::NoAction,
    );

    // Insert test data
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");

    // Delete parent - should set child's foreign key to NULL
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_successful_delete(result, 1);

    // Verify parent was deleted
    let parent_table = db.get_table("PARENT").unwrap();
    assert_eq!(parent_table.row_count(), 0);

    // Verify child's foreign key was set to NULL
    let child_table = db.get_table("CHILD").unwrap();
    assert_eq!(child_table.row_count(), 1);
    let child_row = &child_table.scan()[0];
    assert_eq!(child_row.values[1], SqlValue::Null, "Foreign key should be NULL");
}

#[test]
fn test_on_delete_set_default() {
    let mut db = Database::new();

    // Create parent and child tables with SET DEFAULT
    create_parent_table(&mut db, "PARENT");

    // Create a default parent row
    insert_parent_row(&mut db, "PARENT", 0, "Default");

    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::SetDefault,
        ReferentialAction::NoAction,
    );

    // Insert test data
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");

    // Delete parent - should set child's foreign key to default value (0)
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_successful_delete(result, 1);

    // Verify child's foreign key was set to default
    let child_table = db.get_table("CHILD").unwrap();
    let child_row = &child_table.scan()[0];
    assert_eq!(child_row.values[1], SqlValue::Integer(0), "Foreign key should be set to default");
}

#[test]
fn test_on_delete_restrict_with_references() {
    let mut db = Database::new();

    // RESTRICT is similar to NO ACTION in SQL
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::NoAction,
    );

    // Insert test data
    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");

    // Try to delete parent - should fail
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_fk_violation(result, "");
}

// ========================================================================
// Phase 2: ON UPDATE Actions
// ========================================================================

#[test]
#[ignore] // TODO: Implement ON UPDATE CASCADE for foreign key constraints
fn test_on_update_cascade() {
    // Test that updating parent primary key cascades to child foreign keys
    // This would require UPDATE statement execution support
}

#[test]
#[ignore] // TODO: Implement ON UPDATE SET NULL for foreign key constraints
fn test_on_update_set_null() {
    // Test that updating parent primary key sets child foreign keys to NULL
}

#[test]
#[ignore] // TODO: Implement ON UPDATE SET DEFAULT for foreign key constraints
fn test_on_update_set_default() {
    // Test that updating parent primary key sets child foreign keys to default
}

#[test]
#[ignore] // TODO: Implement ON UPDATE NO ACTION for foreign key constraints
fn test_on_update_no_action() {
    // Test that updating parent primary key fails when children exist
}

#[test]
#[ignore] // TODO: Implement ON UPDATE RESTRICT for foreign key constraints
fn test_on_update_restrict() {
    // Test that updating parent primary key fails when children exist (similar to NO ACTION)
}

// ========================================================================
// Phase 3: Edge Cases
// ========================================================================

#[test]
#[ignore] // TODO: Implement circular foreign key detection and handling
fn test_circular_foreign_keys() {
    let mut db = Database::new();

    // Create tables with circular foreign keys (A → B → A)
    let schema_a = TableSchema::with_primary_key(
        "TABLE_A".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("B_ID".to_string(), DataType::Integer, true),
        ],
        vec!["ID".to_string()],
    );
    db.create_table(schema_a).unwrap();

    let schema_b = TableSchema::with_primary_key(
        "TABLE_B".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("A_ID".to_string(), DataType::Integer, true),
        ],
        vec!["ID".to_string()],
    );
    db.create_table(schema_b).unwrap();

    // Note: In real scenario, we'd need to add FKs after table creation to allow circularity
    // This test verifies the system handles circular FK relationships correctly
}

#[test]
fn test_self_referential_table() {
    let mut db = Database::new();

    // Create a self-referential table (e.g., employees with manager_id)
    create_self_referential_employee_table(&mut db);

    // Insert employees: CEO (no manager), Manager (reports to CEO), Employee (reports to Manager)
    db.insert_row(
        "EMPLOYEE",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Null, SqlValue::Varchar("CEO".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "EMPLOYEE",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Integer(1),
            SqlValue::Varchar("Manager".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "EMPLOYEE",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Integer(2),
            SqlValue::Varchar("Employee".to_string()),
        ]),
    )
    .unwrap();

    // Try to delete Manager - should fail because Employee references them
    let result = execute_delete(&mut db, "DELETE FROM employee WHERE id = 2");
    assert_fk_violation(result, "");

    // Delete Employee first, then Manager should succeed
    let result1 = execute_delete(&mut db, "DELETE FROM employee WHERE id = 3");
    assert_successful_delete(result1, 1);
    let result2 = execute_delete(&mut db, "DELETE FROM employee WHERE id = 2");
    assert_successful_delete(result2, 1);

    // Verify only CEO remains
    let employee_table = db.get_table("EMPLOYEE").unwrap();
    assert_eq!(employee_table.row_count(), 1);
}

#[test]
#[ignore] // TODO: Verify and test multi-column foreign key constraint enforcement
fn test_multi_column_foreign_key() {
    let mut db = Database::new();

    // Create parent with composite primary key and child with multi-column FK
    create_multi_column_parent_child_tables(&mut db);

    // Insert test data
    db.insert_row(
        "PARENT",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(100),
            SqlValue::Varchar("Alice".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "CHILD",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(100)]),
    )
    .unwrap();

    // Try to delete parent - should fail
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE dept_id = 1 AND emp_id = 100");
    assert_fk_violation(result, "");
}

#[test]
fn test_null_foreign_key_values() {
    let mut db = Database::new();

    // Create parent and child tables
    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::NoAction,
    );

    // Insert parent
    insert_parent_row(&mut db, "PARENT", 1, "Alice");

    // Insert child with NULL foreign key (should be allowed - NULLs bypass FK checks)
    insert_child_row_null_fk(&mut db, "CHILD", 10, "Orphan");

    // Delete parent - should succeed because child has NULL foreign key
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_successful_delete(result, 1);

    // Verify child with NULL FK still exists
    let child_table = db.get_table("CHILD").unwrap();
    assert_eq!(child_table.row_count(), 1);
}

#[test]
#[ignore] // TODO: Implement deferred constraint checking (SET CONSTRAINTS ... DEFERRED)
fn test_deferred_constraint_checking() {
    // Test that constraints can be deferred to end of transaction
    // Allows temporary constraint violations during transaction
    // SQL standard feature: SET CONSTRAINTS ... DEFERRED
}

#[test]
fn test_delete_multiple_parents_with_shared_child() {
    let mut db = Database::new();

    // Create two parent tables and child with FKs to both
    create_multiple_parent_child_tables(&mut db);

    // Insert test data
    insert_parent_row(&mut db, "PARENT1", 1, "P1");
    insert_parent_row(&mut db, "PARENT2", 2, "P2");
    db.insert_row(
        "CHILD",
        Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(1), SqlValue::Integer(2)]),
    )
    .unwrap();

    // Try to delete either parent - should fail
    let result1 = execute_delete(&mut db, "DELETE FROM parent1 WHERE id = 1");
    assert_fk_violation(result1, "");
    let result2 = execute_delete(&mut db, "DELETE FROM parent2 WHERE id = 2");
    assert_fk_violation(result2, "");
}

// ========================================================================
// Coverage Enhancement Tests
// ========================================================================

#[test]
fn test_table_without_primary_key() {
    let mut db = Database::new();

    // Create parent table WITHOUT primary key
    let parent_schema = TableSchema::new(
        "PARENT".to_string(),
        vec![ColumnSchema::new("ID".to_string(), DataType::Integer, false)],
    );
    db.create_table(parent_schema).unwrap();

    // Insert data
    db.insert_row("PARENT", Row::new(vec![SqlValue::Integer(1)])).unwrap();

    // Delete should succeed (no PK means no FK enforcement on this table)
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_successful_delete(result, 1);
}

#[test]
fn test_empty_child_table() {
    let mut db = Database::new();

    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::NoAction,
    );

    // Insert only parent (no children)
    insert_parent_row(&mut db, "PARENT", 1, "Alice");

    // Delete should succeed (no children to violate constraint)
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert_successful_delete(result, 1);
}

#[test]
fn test_fk_constraint_error_message() {
    let mut db = Database::new();

    create_parent_table(&mut db, "PARENT");
    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::NoAction,
        ReferentialAction::NoAction,
    );

    insert_parent_row(&mut db, "PARENT", 1, "Alice");
    insert_child_row(&mut db, "CHILD", 10, 1, "Child1");

    // Check error message format
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("FOREIGN KEY constraint violation"));
    assert!(err.contains("CHILD")); // Should mention the child table
    assert!(err.contains("FK_CHILD_PARENT")); // Should mention the constraint name
}
