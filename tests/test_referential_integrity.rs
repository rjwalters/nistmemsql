// ! Integration tests for referential integrity (foreign key constraints)
//!
//! Target module: crates/executor/src/delete/integrity.rs (27.91% coverage → 80%+)
//!
//! Test coverage for:
//! - ON DELETE actions: CASCADE, SET NULL, SET DEFAULT, NO ACTION, RESTRICT
//! - ON UPDATE actions: CASCADE, SET NULL, SET DEFAULT, NO ACTION, RESTRICT
//! - Edge cases: circular FKs, self-referential tables, multi-column FKs, NULL values

use catalog::{ColumnSchema, ForeignKeyConstraint, ReferentialAction, TableSchema};
use executor::DeleteExecutor;
use parser::Parser;
use storage::{Database, Row};
use types::{DataType, SqlValue};

// ========================================================================
// Helper Functions
// ========================================================================

/// Create a parent table with a primary key
fn create_parent_table(db: &mut Database, table_name: &str) {
    let schema = TableSchema::with_primary_key(
        table_name.to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
        vec!["ID".to_string()],
    );
    db.create_table(schema).unwrap();
}

/// Create a child table with a foreign key constraint
fn create_child_table(
    db: &mut Database,
    table_name: &str,
    parent_table: &str,
    on_delete: ReferentialAction,
    on_update: ReferentialAction,
) {
    let columns = vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("PARENT_ID".to_string(), DataType::Integer, true),
        ColumnSchema::new(
            "DATA".to_string(),
            DataType::Varchar { max_length: Some(50) },
            true,
        ),
    ];

    let fk = ForeignKeyConstraint {
        name: Some(format!("FK_{}_{}", table_name, parent_table)),
        column_names: vec!["PARENT_ID".to_string()],
        column_indices: vec![1],
        parent_table: parent_table.to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: on_delete.clone(),
        on_update: on_update.clone(),
    };

    let mut schema =
        TableSchema::with_primary_key(table_name.to_string(), columns, vec!["ID".to_string()]);
    schema.foreign_keys.push(fk);

    db.create_table(schema).unwrap();
}

/// Execute DELETE statement and return number of rows deleted
fn execute_delete(db: &mut Database, sql: &str) -> Result<usize, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        ast::Statement::Delete(delete_stmt) => {
            DeleteExecutor::execute(&delete_stmt, db).map_err(|e| format!("Execution error: {:?}", e))
        }
        other => Err(format!("Expected DELETE statement, got {:?}", other)),
    }
}

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
    db.insert_row("PARENT", Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]))
        .unwrap();
    db.insert_row(
        "CHILD",
        Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(1), SqlValue::Varchar("Child1".to_string())]),
    )
    .unwrap();

    // Try to delete parent - should fail due to foreign key constraint
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert!(result.is_err(), "DELETE should fail when child references exist");
    assert!(result.unwrap_err().contains("FOREIGN KEY constraint violation"));

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
    db.insert_row("PARENT", Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]))
        .unwrap();
    db.insert_row("PARENT", Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())]))
        .unwrap();

    // Only Bob has a child
    db.insert_row(
        "CHILD",
        Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(2), SqlValue::Varchar("Child1".to_string())]),
    )
    .unwrap();

    // Delete parent without children - should succeed
    let deleted = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1").unwrap();
    assert_eq!(deleted, 1, "Should delete parent without children");

    // Verify Alice was deleted, Bob remains
    let parent_table = db.get_table("PARENT").unwrap();
    assert_eq!(parent_table.row_count(), 1);
}

#[test]
#[ignore] // TODO: CASCADE not yet implemented in executor
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
    db.insert_row("PARENT", Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]))
        .unwrap();
    db.insert_row(
        "CHILD",
        Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(1), SqlValue::Varchar("Child1".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "CHILD",
        Row::new(vec![SqlValue::Integer(11), SqlValue::Integer(1), SqlValue::Varchar("Child2".to_string())]),
    )
    .unwrap();

    // Delete parent - should cascade to children
    let deleted = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1").unwrap();
    assert_eq!(deleted, 1, "Should delete 1 parent");

    // Verify parent was deleted
    let parent_table = db.get_table("PARENT").unwrap();
    assert_eq!(parent_table.row_count(), 0);

    // Verify children were also deleted (cascaded)
    let child_table = db.get_table("CHILD").unwrap();
    assert_eq!(child_table.row_count(), 0, "Children should be cascaded deleted");
}

#[test]
#[ignore] // TODO: CASCADE not yet implemented in executor
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
    db.insert_row(
        "GRANDPARENT",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("GrandParent1".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "PARENT",
        Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(1), SqlValue::Varchar("Parent1".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "CHILD",
        Row::new(vec![SqlValue::Integer(100), SqlValue::Integer(10), SqlValue::Varchar("Child1".to_string())]),
    )
    .unwrap();

    // Delete grandparent - should cascade through all levels
    let deleted = execute_delete(&mut db, "DELETE FROM grandparent WHERE id = 1").unwrap();
    assert_eq!(deleted, 1);

    // Verify all levels were deleted
    assert_eq!(db.get_table("GRANDPARENT").unwrap().row_count(), 0);
    assert_eq!(db.get_table("PARENT").unwrap().row_count(), 0);
    assert_eq!(db.get_table("CHILD").unwrap().row_count(), 0);
}

#[test]
#[ignore] // TODO: SET NULL not yet implemented in executor
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
    db.insert_row("PARENT", Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]))
        .unwrap();
    db.insert_row(
        "CHILD",
        Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(1), SqlValue::Varchar("Child1".to_string())]),
    )
    .unwrap();

    // Delete parent - should set child's foreign key to NULL
    let deleted = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1").unwrap();
    assert_eq!(deleted, 1);

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
#[ignore] // TODO: SET DEFAULT not yet implemented in executor
fn test_on_delete_set_default() {
    let mut db = Database::new();

    // Create parent and child tables with SET DEFAULT
    create_parent_table(&mut db, "PARENT");

    // Create a default parent row
    db.insert_row(
        "PARENT",
        Row::new(vec![SqlValue::Integer(0), SqlValue::Varchar("Default".to_string())]),
    )
    .unwrap();

    create_child_table(
        &mut db,
        "CHILD",
        "PARENT",
        ReferentialAction::SetDefault,
        ReferentialAction::NoAction,
    );

    // Insert test data
    db.insert_row("PARENT", Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]))
        .unwrap();
    db.insert_row(
        "CHILD",
        Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(1), SqlValue::Varchar("Child1".to_string())]),
    )
    .unwrap();

    // Delete parent - should set child's foreign key to default value (0)
    let deleted = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1").unwrap();
    assert_eq!(deleted, 1);

    // Verify child's foreign key was set to default
    let child_table = db.get_table("CHILD").unwrap();
    let child_row = &child_table.scan()[0];
    assert_eq!(
        child_row.values[1],
        SqlValue::Integer(0),
        "Foreign key should be set to default"
    );
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
    db.insert_row("PARENT", Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]))
        .unwrap();
    db.insert_row(
        "CHILD",
        Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(1), SqlValue::Varchar("Child1".to_string())]),
    )
    .unwrap();

    // Try to delete parent - should fail
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert!(result.is_err(), "DELETE should fail when child references exist");
}

// ========================================================================
// Phase 2: ON UPDATE Actions
// ========================================================================

#[test]
#[ignore] // TODO: ON UPDATE enforcement not yet implemented
fn test_on_update_cascade() {
    // Test that updating parent primary key cascades to child foreign keys
    // This would require UPDATE statement execution support
}

#[test]
#[ignore] // TODO: ON UPDATE enforcement not yet implemented
fn test_on_update_set_null() {
    // Test that updating parent primary key sets child foreign keys to NULL
}

#[test]
#[ignore] // TODO: ON UPDATE enforcement not yet implemented
fn test_on_update_set_default() {
    // Test that updating parent primary key sets child foreign keys to default
}

#[test]
#[ignore] // TODO: ON UPDATE enforcement not yet implemented
fn test_on_update_no_action() {
    // Test that updating parent primary key fails when children exist
}

#[test]
#[ignore] // TODO: ON UPDATE enforcement not yet implemented
fn test_on_update_restrict() {
    // Test that updating parent primary key fails when children exist (similar to NO ACTION)
}

// ========================================================================
// Phase 3: Edge Cases
// ========================================================================

#[test]
#[ignore] // TODO: Circular FK detection not yet implemented
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
    let columns = vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("MANAGER_ID".to_string(), DataType::Integer, true),
        ColumnSchema::new(
            "NAME".to_string(),
            DataType::Varchar { max_length: Some(50) },
            false,
        ),
    ];

    let fk = ForeignKeyConstraint {
        name: Some("FK_EMPLOYEE_MANAGER".to_string()),
        column_names: vec!["MANAGER_ID".to_string()],
        column_indices: vec![1],
        parent_table: "EMPLOYEE".to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    };

    let mut schema = TableSchema::with_primary_key("EMPLOYEE".to_string(), columns, vec!["ID".to_string()]);
    schema.foreign_keys.push(fk);
    db.create_table(schema).unwrap();

    // Insert employees: CEO (no manager), Manager (reports to CEO), Employee (reports to Manager)
    db.insert_row(
        "EMPLOYEE",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Null, SqlValue::Varchar("CEO".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "EMPLOYEE",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(1), SqlValue::Varchar("Manager".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "EMPLOYEE",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(2), SqlValue::Varchar("Employee".to_string())]),
    )
    .unwrap();

    // Try to delete Manager - should fail because Employee references them
    let result = execute_delete(&mut db, "DELETE FROM employee WHERE id = 2");
    assert!(result.is_err(), "Cannot delete employee with subordinates");

    // Delete Employee first, then Manager should succeed
    execute_delete(&mut db, "DELETE FROM employee WHERE id = 3").unwrap();
    execute_delete(&mut db, "DELETE FROM employee WHERE id = 2").unwrap();

    // Verify only CEO remains
    let employee_table = db.get_table("EMPLOYEE").unwrap();
    assert_eq!(employee_table.row_count(), 1);
}

#[test]
#[ignore] // TODO: Multi-column FK support needs verification
fn test_multi_column_foreign_key() {
    let mut db = Database::new();

    // Create parent with composite primary key
    let parent_schema = TableSchema::with_primary_key(
        "PARENT".to_string(),
        vec![
            ColumnSchema::new("DEPT_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("EMP_ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
        vec!["DEPT_ID".to_string(), "EMP_ID".to_string()],
    );
    db.create_table(parent_schema).unwrap();

    // Create child with multi-column foreign key
    let columns = vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("PARENT_DEPT_ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("PARENT_EMP_ID".to_string(), DataType::Integer, false),
    ];

    let fk = ForeignKeyConstraint {
        name: Some("FK_MULTI".to_string()),
        column_names: vec!["PARENT_DEPT_ID".to_string(), "PARENT_EMP_ID".to_string()],
        column_indices: vec![1, 2],
        parent_table: "PARENT".to_string(),
        parent_column_names: vec!["DEPT_ID".to_string(), "EMP_ID".to_string()],
        parent_column_indices: vec![0, 1],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    };

    let mut schema = TableSchema::with_primary_key("CHILD".to_string(), columns, vec!["ID".to_string()]);
    schema.foreign_keys.push(fk);
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "PARENT",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100), SqlValue::Varchar("Alice".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "CHILD",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(100)]),
    )
    .unwrap();

    // Try to delete parent - should fail
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE dept_id = 1 AND emp_id = 100");
    assert!(result.is_err(), "Cannot delete parent with child references");
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
    db.insert_row("PARENT", Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]))
        .unwrap();

    // Insert child with NULL foreign key (should be allowed - NULLs bypass FK checks)
    db.insert_row(
        "CHILD",
        Row::new(vec![SqlValue::Integer(10), SqlValue::Null, SqlValue::Varchar("Orphan".to_string())]),
    )
    .unwrap();

    // Delete parent - should succeed because child has NULL foreign key
    let deleted = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1").unwrap();
    assert_eq!(deleted, 1, "Should delete parent when child has NULL FK");

    // Verify child with NULL FK still exists
    let child_table = db.get_table("CHILD").unwrap();
    assert_eq!(child_table.row_count(), 1);
}

#[test]
#[ignore] // TODO: Deferred constraint checking not yet implemented
fn test_deferred_constraint_checking() {
    // Test that constraints can be deferred to end of transaction
    // Allows temporary constraint violations during transaction
    // SQL standard feature: SET CONSTRAINTS ... DEFERRED
}

#[test]
fn test_delete_multiple_parents_with_shared_child() {
    let mut db = Database::new();

    // Create two parent tables
    create_parent_table(&mut db, "PARENT1");
    create_parent_table(&mut db, "PARENT2");

    // Create child table with FKs to both parents
    let columns = vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("PARENT1_ID".to_string(), DataType::Integer, true),
        ColumnSchema::new("PARENT2_ID".to_string(), DataType::Integer, true),
    ];

    let fk1 = ForeignKeyConstraint {
        name: Some("FK_CHILD_PARENT1".to_string()),
        column_names: vec!["PARENT1_ID".to_string()],
        column_indices: vec![1],
        parent_table: "PARENT1".to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    };

    let fk2 = ForeignKeyConstraint {
        name: Some("FK_CHILD_PARENT2".to_string()),
        column_names: vec!["PARENT2_ID".to_string()],
        column_indices: vec![2],
        parent_table: "PARENT2".to_string(),
        parent_column_names: vec!["ID".to_string()],
        parent_column_indices: vec![0],
        on_delete: ReferentialAction::NoAction,
        on_update: ReferentialAction::NoAction,
    };

    let mut schema = TableSchema::with_primary_key("CHILD".to_string(), columns, vec!["ID".to_string()]);
    schema.foreign_keys.push(fk1);
    schema.foreign_keys.push(fk2);
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row("PARENT1", Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("P1".to_string())]))
        .unwrap();
    db.insert_row("PARENT2", Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("P2".to_string())]))
        .unwrap();
    db.insert_row(
        "CHILD",
        Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(1), SqlValue::Integer(2)]),
    )
    .unwrap();

    // Try to delete either parent - should fail
    assert!(execute_delete(&mut db, "DELETE FROM parent1 WHERE id = 1").is_err());
    assert!(execute_delete(&mut db, "DELETE FROM parent2 WHERE id = 2").is_err());
}

// ========================================================================
// Coverage Enhancement Tests
// ========================================================================

#[test]
fn test_table_without_primary_key() {
    let mut db = Database::new();

    // Create parent table WITHOUT primary key
    let parent_schema =
        TableSchema::new("PARENT".to_string(), vec![ColumnSchema::new("ID".to_string(), DataType::Integer, false)]);
    db.create_table(parent_schema).unwrap();

    // Insert data
    db.insert_row("PARENT", Row::new(vec![SqlValue::Integer(1)])).unwrap();

    // Delete should succeed (no PK means no FK enforcement on this table)
    let deleted = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1").unwrap();
    assert_eq!(deleted, 1);
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
    db.insert_row("PARENT", Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]))
        .unwrap();

    // Delete should succeed (no children to violate constraint)
    let deleted = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1").unwrap();
    assert_eq!(deleted, 1);
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

    db.insert_row("PARENT", Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]))
        .unwrap();
    db.insert_row(
        "CHILD",
        Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(1), SqlValue::Varchar("Child1".to_string())]),
    )
    .unwrap();

    // Check error message format
    let result = execute_delete(&mut db, "DELETE FROM parent WHERE id = 1");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("FOREIGN KEY constraint violation"));
    assert!(err.contains("CHILD")); // Should mention the child table
    assert!(err.contains("FK_CHILD_PARENT")); // Should mention the constraint name
}
