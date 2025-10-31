//! Edge case tests for aggregate functions
//!
//! Tests for decimal precision, mixed numeric types, and CASE expressions with aggregates.

use super::super::*;

#[test]
fn test_avg_precision_decimal() {
    // Edge case: AVG should preserve DECIMAL precision, not truncate to INTEGER
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "prices".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "price".to_string(),
                types::DataType::Numeric { precision: 10, scale: 2 },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "prices",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Numeric("10.50".parse().unwrap()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "prices",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Numeric("20.75".parse().unwrap()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "prices",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Numeric("15.25".parse().unwrap()),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "AVG".to_string(),
                args: vec![ast::Expression::ColumnRef { table: None, column: "price".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "prices".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // AVG(10.50, 20.75, 15.25) = 46.50 / 3 = 15.50
    match &result[0].values[0] {
        types::SqlValue::Numeric(s) => {
            let value: f64 = s.parse().unwrap();
            assert!((value - 15.50).abs() < 0.01, "Expected 15.50, got {}", value);
        }
        other => panic!("Expected Numeric value, got {:?}", other),
    }
}

#[test]
fn test_sum_mixed_numeric_types() {
    // Edge case: SUM on NUMERIC values should work
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "mixed_amounts".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "amount".to_string(),
                types::DataType::Numeric { precision: 10, scale: 2 },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "mixed_amounts",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Numeric("100.50".parse().unwrap()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "mixed_amounts",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Numeric("200.25".parse().unwrap()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "mixed_amounts",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Numeric("150.00".parse().unwrap()),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "SUM".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "amount".to_string(),
                }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "mixed_amounts".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // SUM(100.50, 200.25, 150.00) = 450.75
    assert_eq!(result[0].values[0], types::SqlValue::Numeric("450.75".parse().unwrap()));
}

#[test]
fn test_aggregate_with_case_expression() {
    // Edge case: Aggregates with CASE expressions - common real-world pattern
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "transactions".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "type".to_string(),
                types::DataType::Varchar { max_length: Some(10) },
                false,
            ),
            catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "transactions",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("credit".to_string()),
            types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "transactions",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("debit".to_string()),
            types::SqlValue::Integer(50),
        ]),
    )
    .unwrap();
    db.insert_row(
        "transactions",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Varchar("credit".to_string()),
            types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    // SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END)
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "SUM".to_string(),
                args: vec![ast::Expression::Case {
                    operand: None,
                    when_clauses: vec![ast::CaseWhen {
                        conditions: vec![ast::Expression::BinaryOp {
                            left: Box::new(ast::Expression::ColumnRef {
                                table: None,
                                column: "type".to_string(),
                            }),
                            op: ast::BinaryOperator::Equal,
                            right: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                                "credit".to_string(),
                            ))),
                        }],
                        result: ast::Expression::ColumnRef {
                            table: None,
                            column: "amount".to_string(),
                        },
                    }],
                    else_result: Some(Box::new(ast::Expression::Literal(
                        types::SqlValue::Integer(0),
                    ))),
                }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "transactions".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // SUM of credits only: 100 + 200 = 300 (debit of 50 becomes 0)
    assert_eq!(result[0].values[0], types::SqlValue::Integer(300));
}
