//! Basic DELETE operation tests

use vibesql_ast::{BinaryOperator, DeleteStmt, Expression, WhereClause};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::DeleteExecutor;

fn setup_test_table(db: &mut Database) {
    // Create table schema
    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("active".to_string(), DataType::Boolean, false),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Boolean(true),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Boolean(false),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Charlie".to_string()),
            SqlValue::Boolean(true),
        ]),
    )
    .unwrap();
}

#[test]
fn test_delete_all_rows() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // DELETE FROM users;
    let stmt = DeleteStmt { only: false, table_name: "users".to_string(), where_clause: None };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 3);

    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 0);
}

#[test]
fn test_delete_with_simple_where() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // DELETE FROM users WHERE id = 2;
    let stmt = DeleteStmt {
        only: false,
        table_name: "users".to_string(),
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        })),
    };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 1);

    // Verify Bob is deleted, Alice and Charlie remain
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 2);

    let remaining: Vec<i64> = table
        .scan()
        .iter()
        .map(|row| if let SqlValue::Integer(id) = row.get(0).unwrap() { *id } else { 0 })
        .collect();

    assert!(remaining.contains(&1)); // Alice
    assert!(remaining.contains(&3)); // Charlie
    assert!(!remaining.contains(&2)); // Bob deleted
}

#[test]
fn test_delete_with_boolean_where() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // DELETE FROM users WHERE active = TRUE;
    let stmt = DeleteStmt {
        only: false,
        table_name: "users".to_string(),
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "active".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        })),
    };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 2); // Alice and Charlie

    // Verify only Bob remains
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);

    let remaining_id =
        if let SqlValue::Integer(id) = table.scan()[0].get(0).unwrap() { *id } else { 0 };
    assert_eq!(remaining_id, 2); // Bob
}

#[test]
fn test_delete_multiple_rows() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // DELETE FROM users WHERE id > 1;
    let stmt = DeleteStmt {
        only: false,
        table_name: "users".to_string(),
        where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        })),
    };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 2); // Bob and Charlie

    // Verify only Alice remains
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);

    let remaining_name = if let SqlValue::Varchar(name) = table.scan()[0].get(1).unwrap() {
        name.clone()
    } else {
        String::new()
    };
    assert_eq!(remaining_name, "Alice");
}
