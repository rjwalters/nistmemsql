//! JOIN tests
//!
//! Tests for INNER JOIN operations.

use super::super::*;

#[test]
fn test_inner_join_two_tables() {
    let mut db = storage::Database::new();

    let users_schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: 100 },
                true,
            ),
        ],
    );
    db.create_table(users_schema).unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Bob".to_string()),
        ]),
    )
    .unwrap();

    let orders_schema = catalog::TableSchema::new(
        "orders".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("user_id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(orders_schema).unwrap();
    db.insert_row(
        "orders",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(50),
        ]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(75),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            right: Box::new(ast::FromClause::Table { name: "orders".to_string(), alias: None }),
            join_type: ast::JoinType::Inner,
            condition: Some(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("users".to_string()),
                    column: "id".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::ColumnRef {
                    table: Some("orders".to_string()),
                    column: "user_id".to_string(),
                }),
            }),
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values.len(), 5); // users (2 cols) + orders (3 cols)
}
