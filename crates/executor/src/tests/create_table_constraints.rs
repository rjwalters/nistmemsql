
use ast::{ColumnConstraint, ColumnDef, CreateTableStmt, Expression, TableConstraint};
use storage::Database;
use types::DataType;

use crate::create_table::CreateTableExecutor;
use crate::errors::ExecutorError;

#[test]
fn test_create_table_with_column_primary_key() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: true, // This should be overridden by the PK constraint
                constraints: vec![ColumnConstraint::PrimaryKey],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: true,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    let schema = db.catalog.get_table("users").unwrap();
    assert_eq!(schema.primary_key, Some(vec!["id".to_string()]));
    assert_eq!(schema.get_column("id").unwrap().nullable, false); // PKs are implicitly NOT NULL
}

#[test]
fn test_create_table_with_table_primary_key() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: true, // This should be overridden
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "tenant_id".to_string(),
                data_type: DataType::Integer,
                nullable: true, // This should be overridden
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![TableConstraint::PrimaryKey {
            columns: vec!["id".to_string(), "tenant_id".to_string()],
        }],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    let schema = db.catalog.get_table("users").unwrap();
    assert_eq!(
        schema.primary_key,
        Some(vec!["id".to_string(), "tenant_id".to_string()])
    );
    assert_eq!(schema.get_column("id").unwrap().nullable, false);
    assert_eq!(schema.get_column("tenant_id").unwrap().nullable, false);
}

#[test]
fn test_create_table_with_multiple_primary_keys_fails() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        table_name: "users".to_string(),
        columns: vec![ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![ColumnConstraint::PrimaryKey],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![TableConstraint::PrimaryKey {
            columns: vec!["id".to_string()],
        }],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(matches!(result, Err(ExecutorError::MultiplePrimaryKeys)));
}

#[test]
fn test_create_table_with_column_unique_constraint() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "email".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                nullable: false,
                constraints: vec![ColumnConstraint::Unique],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    let schema = db.catalog.get_table("users").unwrap();
    assert_eq!(schema.unique_constraints.len(), 1);
    assert_eq!(schema.unique_constraints[0], vec!["email".to_string()]);
}

#[test]
fn test_create_table_with_table_unique_constraint() {
    let mut db = Database::new();
    let stmt = CreateTableStmt {
        table_name: "users".to_string(),
        columns: vec![
            ColumnDef {
                name: "first_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
            ColumnDef {
                name: "last_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                nullable: false,
                constraints: vec![],
                default_value: None,
                comment: None,
            },
        ],
        table_constraints: vec![TableConstraint::Unique {
            columns: vec!["first_name".to_string(), "last_name".to_string()],
        }],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    let schema = db.catalog.get_table("users").unwrap();
    assert_eq!(schema.unique_constraints.len(), 1);
    assert_eq!(
        schema.unique_constraints[0],
        vec!["first_name".to_string(), "last_name".to_string()]
    );
}

#[test]
fn test_create_table_with_check_constraint() {
    let mut db = Database::new();
    let check_expr = Expression::BinaryOp {
        left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
        op: ast::BinaryOperator::GreaterThan,
        right: Box::new(Expression::Literal(types::SqlValue::Integer(0))),
    };

    let stmt = CreateTableStmt {
        table_name: "products".to_string(),
        columns: vec![ColumnDef {
            name: "price".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            constraints: vec![ColumnConstraint::Check(Box::new(check_expr.clone()))],
            default_value: None,
            comment: None,
        }],
        table_constraints: vec![],
    };

    let result = CreateTableExecutor::execute(&stmt, &mut db);
    assert!(result.is_ok());

    let schema = db.catalog.get_table("products").unwrap();
    assert_eq!(schema.check_constraints.len(), 1);
    assert_eq!(schema.check_constraints[0].1, check_expr);
}
