//! HAVING clause tests with aggregates
//!
//! Tests for filtering aggregate results with HAVING.

use super::super::*;

#[test]
fn test_having_clause() {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "sales".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("id".to_string(), vibesql_types::DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new("dept".to_string(), vibesql_types::DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new("amount".to_string(), vibesql_types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(1),
            vibesql_types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(3),
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(50),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = vibesql_ast::SelectStmt {
        into_table: None,
        into_variables: None,        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "dept".to_string() },
                alias: None,
            },
            vibesql_ast::SelectItem::Expression {
                expr: vibesql_ast::Expression::Function {
                    name: "SUM".to_string(),
                    args: vec![vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "amount".to_string(),
                    }],
                    character_unit: None,
                },
                alias: None,
            },
        ],
        from: Some(vibesql_ast::FromClause::Table { name: "sales".to_string(), alias: None }),
        where_clause: None,
        group_by: Some(vec![vibesql_ast::Expression::ColumnRef {
            table: None,
            column: "dept".to_string(),
        }]),
        having: Some(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::Function {
                name: "SUM".to_string(),
                args: vec![vibesql_ast::Expression::ColumnRef {
                    table: None,
                    column: "amount".to_string(),
                }],
                character_unit: None,
            }),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(150))),
        }),
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Numeric(300.0));
}
