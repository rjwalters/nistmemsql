//! Tests for AUTO_INCREMENT functionality

use vibesql_ast::{
    ColumnConstraint, ColumnConstraintKind, ColumnDef, CreateTableStmt, InsertSource, InsertStmt,
};
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

use crate::{CreateTableExecutor, InsertExecutor};

#[test]
fn test_auto_increment_basic_inserts() {
    let mut db = Database::new();

    // Create table with AUTO_INCREMENT
    let stmt = CreateTableStmt {
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![
                    ColumnConstraint { name: None, kind: ColumnConstraintKind::AutoIncrement },
                    ColumnConstraint { name: None, kind: ColumnConstraintKind::PrimaryKey },
                ],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "username".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create table: {:?}", result.err());

    // Insert without specifying id - should auto-generate 1
    let insert1 = InsertStmt {
        table_name: "users".to_string(),
        columns: vec!["username".to_string()],
        source: InsertSource::Values(vec![vec![vibesql_ast::Expression::Literal(
            SqlValue::Varchar("alice".to_string()),
        )]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    let result = InsertExecutor::execute(&mut db, &insert1);
    assert!(result.is_ok(), "Failed to insert alice: {:?}", result.err());

    // Insert without specifying id - should auto-generate 2
    let insert2 = InsertStmt {
        table_name: "users".to_string(),
        columns: vec!["username".to_string()],
        source: InsertSource::Values(vec![vec![vibesql_ast::Expression::Literal(
            SqlValue::Varchar("bob".to_string()),
        )]]),
        conflict_clause: None,
        on_duplicate_key_update: None,
    };
    let result = InsertExecutor::execute(&mut db, &insert2);
    assert!(result.is_ok(), "Failed to insert bob: {:?}", result.err());

    // Query to verify - should have auto-generated ids 1 and 2
    let table = db.get_table("users").unwrap();
    let rows = table.scan();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values[0], SqlValue::Integer(1)); // First id should be 1
    assert_eq!(rows[0].values[1], SqlValue::Varchar("alice".to_string()));
    assert_eq!(rows[1].values[0], SqlValue::Integer(2)); // Second id should be 2
    assert_eq!(rows[1].values[1], SqlValue::Varchar("bob".to_string()));
}

#[test]
fn test_multiple_auto_increment_error() {
    let mut db = Database::new();

    // Should fail - multiple AUTO_INCREMENT columns not allowed
    let stmt = CreateTableStmt {
        table_name: "bad".to_string(),
        columns: vec![
            ColumnDef {
                name: "id1".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                constraints: vec![ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::AutoIncrement,
                }],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "id2".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                constraints: vec![ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::AutoIncrement,
                }],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
        table_options: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    let error = result.unwrap_err().to_string();
    assert!(error.contains("Only one AUTO_INCREMENT column allowed"));
}
