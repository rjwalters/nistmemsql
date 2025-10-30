//! HAVING clause tests with aggregates
//!
//! Tests for filtering aggregate results with HAVING.

use super::super::*;

#[test]
fn test_having_clause() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "sales".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("dept".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(50),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "dept".to_string() },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "SUM".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "amount".to_string(),
                    }],
                    character_unit: None,
                },
                alias: None,
            },
        ],
        from: Some(ast::FromClause::Table { name: "sales".to_string(), alias: None }),
        where_clause: None,
        group_by: Some(vec![ast::Expression::ColumnRef {
            table: None,
            column: "dept".to_string(),
        }]),
        having: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::Function {
                name: "SUM".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "amount".to_string(),
                }],
                character_unit: None,
            }),
            op: ast::BinaryOperator::GreaterThan,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(150))),
        }),
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
    assert_eq!(result[0].values[1], types::SqlValue::Integer(300));
}
