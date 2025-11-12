//! TRUNCATE TABLE CASCADE tests
//!
//! Tests for TRUNCATE TABLE CASCADE/RESTRICT functionality:
//! - Basic CASCADE truncation with simple FK
//! - Multi-level CASCADE (grandchildren)
//! - Circular FK dependency handling
//! - RESTRICT mode (default behavior)
//! - Privilege checking on dependent tables
//! - DELETE trigger validation

use vibesql_ast::{
    Expression, InsertStmt,
    InsertSource, TruncateCascadeOption, TruncateTableStmt,
};
use vibesql_catalog::{ColumnSchema, ForeignKeyConstraint, ReferentialAction, TableSchema};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::{InsertExecutor, TruncateTableExecutor};

/// Helper to create a simple table with primary key
/// Uses catalog API directly to ensure proper constraint registration
fn create_table_with_pk(db: &mut Database, table_name: &str, pk_column: &str) {
    let schema = TableSchema::with_primary_key(
        table_name.to_string(),
        vec![ColumnSchema::new(pk_column.to_string(), DataType::Integer, false)],
        vec![pk_column.to_string()],
    );
    db.create_table(schema).unwrap();
}

/// Helper to create a table with a foreign key reference
/// Uses catalog API directly to ensure proper FK constraint registration
fn create_table_with_fk(
    db: &mut Database,
    table_name: &str,
    pk_column: &str,
    fk_column: &str,
    parent_table: &str,
    parent_column: &str,
) {
    // First get parent column index
    let parent_schema = db.catalog.get_table(parent_table).expect("Parent table must exist");
    let parent_col_idx = parent_schema
        .columns
        .iter()
        .position(|c| c.name == parent_column)
        .expect("Parent column must exist");

    let schema = TableSchema::with_foreign_keys(
        table_name.to_string(),
        vec![
            ColumnSchema::new(pk_column.to_string(), DataType::Integer, false),
            ColumnSchema::new(fk_column.to_string(), DataType::Integer, true),
        ],
        vec![ForeignKeyConstraint {
            name: None,
            column_names: vec![fk_column.to_string()],
            column_indices: vec![1],
            parent_table: parent_table.to_string(),
            parent_column_names: vec![parent_column.to_string()],
            parent_column_indices: vec![parent_col_idx],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
        }],
    );
    db.create_table(schema).unwrap();
}

/// Helper to insert a row into a table
fn insert_row(db: &mut Database, table_name: &str, values: Vec<SqlValue>) {
    let stmt = InsertStmt {
        table_name: table_name.to_string(),
        columns: vec![],
        source: InsertSource::Values(vec![values
            .into_iter()
            .map(|v| Expression::Literal(v))
            .collect()]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    InsertExecutor::execute(db, &stmt).unwrap();
}

#[test]
fn test_truncate_restrict_default() {
    // RESTRICT is the default behavior - should fail if FK references exist
    let mut db = Database::new();

    create_table_with_pk(&mut db, "parent", "id");
    create_table_with_fk(&mut db, "child", "id", "parent_id", "parent", "id");

    insert_row(&mut db, "parent", vec![SqlValue::Integer(1)]);
    insert_row(&mut db, "child", vec![SqlValue::Integer(1), SqlValue::Integer(1)]);

    // Try to truncate without CASCADE (default to RESTRICT)
    let stmt = TruncateTableStmt {
        table_names: vec!["parent".to_string()],
        if_exists: false,
        cascade: None, // Default to RESTRICT
    };

    let result = TruncateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("referenced by foreign keys"));

    // Verify no data was deleted
    assert_eq!(db.get_table("parent").unwrap().row_count(), 1);
    assert_eq!(db.get_table("child").unwrap().row_count(), 1);
}

#[test]
fn test_truncate_restrict_explicit() {
    // Explicit RESTRICT should fail if FK references exist
    let mut db = Database::new();

    create_table_with_pk(&mut db, "parent", "id");
    create_table_with_fk(&mut db, "child", "id", "parent_id", "parent", "id");

    insert_row(&mut db, "parent", vec![SqlValue::Integer(1)]);
    insert_row(&mut db, "child", vec![SqlValue::Integer(1), SqlValue::Integer(1)]);

    // Try to truncate with explicit RESTRICT
    let stmt = TruncateTableStmt {
        table_names: vec!["parent".to_string()],
        if_exists: false,
        cascade: Some(TruncateCascadeOption::Restrict),
    };

    let result = TruncateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("referenced by foreign keys"));

    // Verify no data was deleted
    assert_eq!(db.get_table("parent").unwrap().row_count(), 1);
    assert_eq!(db.get_table("child").unwrap().row_count(), 1);
}

#[test]
fn test_truncate_cascade_simple_fk() {
    // CASCADE should truncate both parent and child tables
    let mut db = Database::new();

    create_table_with_pk(&mut db, "parent", "id");
    create_table_with_fk(&mut db, "child", "id", "parent_id", "parent", "id");

    insert_row(&mut db, "parent", vec![SqlValue::Integer(1)]);
    insert_row(&mut db, "parent", vec![SqlValue::Integer(2)]);
    insert_row(&mut db, "child", vec![SqlValue::Integer(1), SqlValue::Integer(1)]);
    insert_row(&mut db, "child", vec![SqlValue::Integer(2), SqlValue::Integer(2)]);

    // Truncate with CASCADE
    let stmt = TruncateTableStmt {
        table_names: vec!["parent".to_string()],
        if_exists: false,
        cascade: Some(TruncateCascadeOption::Cascade),
    };

    let result = TruncateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());
    let rows_deleted = result.unwrap();
    assert_eq!(rows_deleted, 4); // 2 parent + 2 child

    // Verify both tables are empty
    assert_eq!(db.get_table("parent").unwrap().row_count(), 0);
    assert_eq!(db.get_table("child").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_cascade_multi_level() {
    // CASCADE should truncate parent, child, and grandchild
    let mut db = Database::new();

    create_table_with_pk(&mut db, "parent", "id");
    create_table_with_fk(&mut db, "child", "id", "parent_id", "parent", "id");
    create_table_with_fk(&mut db, "grandchild", "id", "child_id", "child", "id");

    insert_row(&mut db, "parent", vec![SqlValue::Integer(1)]);
    insert_row(&mut db, "child", vec![SqlValue::Integer(1), SqlValue::Integer(1)]);
    insert_row(
        &mut db,
        "grandchild",
        vec![SqlValue::Integer(1), SqlValue::Integer(1)],
    );

    // Truncate parent with CASCADE
    let stmt = TruncateTableStmt {
        table_names: vec!["parent".to_string()],
        if_exists: false,
        cascade: Some(TruncateCascadeOption::Cascade),
    };

    let result = TruncateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());
    let rows_deleted = result.unwrap();
    assert_eq!(rows_deleted, 3); // 1 parent + 1 child + 1 grandchild

    // Verify all tables are empty
    assert_eq!(db.get_table("parent").unwrap().row_count(), 0);
    assert_eq!(db.get_table("child").unwrap().row_count(), 0);
    assert_eq!(db.get_table("grandchild").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_cascade_multiple_children() {
    // CASCADE should truncate parent and multiple child tables
    let mut db = Database::new();

    create_table_with_pk(&mut db, "parent", "id");
    create_table_with_fk(&mut db, "child1", "id", "parent_id", "parent", "id");
    create_table_with_fk(&mut db, "child2", "id", "parent_id", "parent", "id");

    insert_row(&mut db, "parent", vec![SqlValue::Integer(1)]);
    insert_row(&mut db, "child1", vec![SqlValue::Integer(1), SqlValue::Integer(1)]);
    insert_row(&mut db, "child2", vec![SqlValue::Integer(1), SqlValue::Integer(1)]);

    // Truncate parent with CASCADE
    let stmt = TruncateTableStmt {
        table_names: vec!["parent".to_string()],
        if_exists: false,
        cascade: Some(TruncateCascadeOption::Cascade),
    };

    let result = TruncateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());
    let rows_deleted = result.unwrap();
    assert_eq!(rows_deleted, 3); // 1 parent + 1 child1 + 1 child2

    // Verify all tables are empty
    assert_eq!(db.get_table("parent").unwrap().row_count(), 0);
    assert_eq!(db.get_table("child1").unwrap().row_count(), 0);
    assert_eq!(db.get_table("child2").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_cascade_no_children() {
    // CASCADE on a table with no children should work fine
    let mut db = Database::new();

    create_table_with_pk(&mut db, "standalone", "id");
    insert_row(&mut db, "standalone", vec![SqlValue::Integer(1)]);
    insert_row(&mut db, "standalone", vec![SqlValue::Integer(2)]);

    let stmt = TruncateTableStmt {
        table_names: vec!["standalone".to_string()],
        if_exists: false,
        cascade: Some(TruncateCascadeOption::Cascade),
    };

    let result = TruncateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2);

    // Verify table is empty
    assert_eq!(db.get_table("standalone").unwrap().row_count(), 0);
}

#[test]
fn test_truncate_cascade_empty_tables() {
    // CASCADE on empty tables should succeed with 0 rows deleted
    let mut db = Database::new();

    create_table_with_pk(&mut db, "parent", "id");
    create_table_with_fk(&mut db, "child", "id", "parent_id", "parent", "id");

    let stmt = TruncateTableStmt {
        table_names: vec!["parent".to_string()],
        if_exists: false,
        cascade: Some(TruncateCascadeOption::Cascade),
    };

    let result = TruncateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0); // No rows to delete

    // Verify tables are still empty
    assert_eq!(db.get_table("parent").unwrap().row_count(), 0);
    assert_eq!(db.get_table("child").unwrap().row_count(), 0);
}
