//! Reusable fixtures and utilities for referential integrity testing
//!
//! This module provides common table schema creation, data insertion helpers,
//! and assertion utilities for testing foreign key constraints across the test suite.

use catalog::{ColumnSchema, ForeignKeyConstraint, ReferentialAction, TableSchema};
use parser::Parser;
use storage::Database;
use types::DataType;

// ========================================================================
// Table Creation Helpers
// ========================================================================

/// Create a parent table with a primary key
///
/// Creates a standard parent table with:
/// - ID: Integer, NOT NULL, PRIMARY KEY
/// - NAME: VARCHAR(50), nullable
pub fn create_parent_table(db: &mut Database, table_name: &str) {
    let schema = TableSchema::with_primary_key(
        table_name.to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("NAME".to_string(), DataType::Varchar { max_length: Some(50) }, true),
        ],
        vec!["ID".to_string()],
    );
    db.create_table(schema).unwrap();
}

/// Create a child table with a foreign key constraint
///
/// Creates a child table with:
/// - ID: Integer, NOT NULL, PRIMARY KEY
/// - PARENT_ID: Integer, nullable, FOREIGN KEY to parent table
/// - DATA: VARCHAR(50), nullable
///
/// The foreign key constraint is named FK_{table_name}_{parent_table}
pub fn create_child_table(
    db: &mut Database,
    table_name: &str,
    parent_table: &str,
    on_delete: ReferentialAction,
    on_update: ReferentialAction,
) {
    let columns = vec![
        ColumnSchema::new("ID".to_string(), DataType::Integer, false),
        ColumnSchema::new("PARENT_ID".to_string(), DataType::Integer, true),
        ColumnSchema::new("DATA".to_string(), DataType::Varchar { max_length: Some(50) }, true),
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

// ========================================================================
// SQL Execution Helpers
// ========================================================================

/// Execute DELETE statement and return number of rows deleted
///
/// Parses the SQL, executes it against the database, and returns the
/// number of rows deleted. Returns an error if parsing or execution fails.
pub fn execute_delete(db: &mut Database, sql: &str) -> Result<usize, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        ast::Statement::Delete(delete_stmt) => {
            use executor::DeleteExecutor;
            DeleteExecutor::execute(&delete_stmt, db)
                .map_err(|e| format!("Execution error: {:?}", e))
        }
        other => Err(format!("Expected DELETE statement, got {:?}", other)),
    }
}
