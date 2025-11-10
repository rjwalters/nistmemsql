//! COUNT, SUM, and AVG aggregate function tests
//!
//! Tests for basic aggregate functions and their behavior with NULL values.

use super::super::*;

#[test]
fn test_count_star_no_group_by() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(25)]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(30)]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(35)]),
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
                name: "COUNT".to_string(),
                args: vec![ast::Expression::Wildcard],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Numeric(3.0));
}

#[test]
fn test_sum_no_group_by() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "sales".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(100)]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(200)]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(150)]),
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
        from: Some(ast::FromClause::Table { name: "sales".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Numeric(450.0));
}

#[test]
fn test_count_with_nulls() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "data".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "data",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(10)]),
    )
    .unwrap();
    db.insert_row(
        "data",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Null]),
    )
    .unwrap();
    db.insert_row(
        "data",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(20)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    // COUNT(*) counts all rows including NULL
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "COUNT".to_string(),
                args: vec![ast::Expression::Wildcard],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "data".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Numeric(3.0)); // Counts all 3 rows
}

#[test]
fn test_sum_with_nulls() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "amounts".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "amounts",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(100)]),
    )
    .unwrap();
    db.insert_row(
        "amounts",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Null]),
    )
    .unwrap();
    db.insert_row(
        "amounts",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(200)]),
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
                args: vec![ast::Expression::ColumnRef { table: None, column: "value".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "amounts".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // SUM ignores NULL values, so 100 + 200 = 300
    assert_eq!(result[0].values[0], types::SqlValue::Numeric(300.0));
}

#[test]
fn test_avg_function() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "scores".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("score".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "scores",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(80)]),
    )
    .unwrap();
    db.insert_row(
        "scores",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(90)]),
    )
    .unwrap();
    db.insert_row(
        "scores",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(70)]),
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
                args: vec![ast::Expression::ColumnRef { table: None, column: "score".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "scores".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // AVG(80, 90, 70) = 240 / 3 = 80
    assert_eq!(result[0].values[0], types::SqlValue::Numeric(80.0));
}

#[test]
fn test_avg_with_nulls() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "ratings".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("rating".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "ratings",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(5)]),
    )
    .unwrap();
    db.insert_row(
        "ratings",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Null]),
    )
    .unwrap();
    db.insert_row(
        "ratings",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(3)]),
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
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "rating".to_string(),
                }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "ratings".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // AVG ignores NULL, so (5 + 3) / 2 = 4
    assert_eq!(result[0].values[0], types::SqlValue::Numeric(4.0));
}

#[test]
fn test_count_column_all_nulls() {
    // Edge case: COUNT(column) with ALL NULL values should return 0, not row count
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "null_data".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "null_data",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Null]),
    )
    .unwrap();
    db.insert_row(
        "null_data",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Null]),
    )
    .unwrap();
    db.insert_row(
        "null_data",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Null]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // COUNT(column) should return 0 when all values are NULL
    let stmt_count_col = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "COUNT".to_string(),
                args: vec![ast::Expression::ColumnRef { table: None, column: "value".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "null_data".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt_count_col).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Numeric(0.0)); // COUNT(col) with all NULLs = 0

    // COUNT(*) should still return row count
    let stmt_count_star = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "COUNT".to_string(),
                args: vec![ast::Expression::Wildcard],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "null_data".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt_count_star).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Numeric(3.0)); // COUNT(*) counts rows
}

// ============================================================================
// Tests for COUNT(*) in CASE expressions (Issue #1150)
// ============================================================================

#[test]
fn test_count_star_in_simple_case_expression() {
    // Test: CASE COUNT(*) WHEN 3 THEN 'three' ELSE 'other' END
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "data".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("data", storage::Row::new(vec![types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("data", storage::Row::new(vec![types::SqlValue::Integer(2)]))
        .unwrap();
    db.insert_row("data", storage::Row::new(vec![types::SqlValue::Integer(3)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Case {
                operand: Some(Box::new(ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![ast::Expression::Wildcard],
                })),
                when_clauses: vec![ast::CaseWhen {
                    conditions: vec![ast::Expression::Literal(types::SqlValue::Integer(3))],
                    result: ast::Expression::Literal(types::SqlValue::Varchar("three".to_string())),
                }],
                else_result: Some(Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "other".to_string(),
                )))),
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "data".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("three".to_string()));
}

#[test]
fn test_count_star_in_searched_case_expression() {
    // Test: CASE WHEN COUNT(*) > 2 THEN 'many' ELSE 'few' END
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "items".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("items", storage::Row::new(vec![types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("items", storage::Row::new(vec![types::SqlValue::Integer(2)]))
        .unwrap();
    db.insert_row("items", storage::Row::new(vec![types::SqlValue::Integer(3)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Case {
                operand: None,
                when_clauses: vec![ast::CaseWhen {
                    conditions: vec![ast::Expression::BinaryOp {
                        left: Box::new(ast::Expression::AggregateFunction {
                            name: "COUNT".to_string(),
                            distinct: false,
                            args: vec![ast::Expression::Wildcard],
                        }),
                        op: ast::BinaryOperator::GreaterThan,
                        right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(2))),
                    }],
                    result: ast::Expression::Literal(types::SqlValue::Varchar("many".to_string())),
                }],
                else_result: Some(Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "few".to_string(),
                )))),
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "items".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("many".to_string()));
}

#[test]
fn test_count_star_in_arithmetic_expression() {
    // Test: COUNT(*) * 10
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "records".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("records", storage::Row::new(vec![types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("records", storage::Row::new(vec![types::SqlValue::Integer(2)]))
        .unwrap();
    db.insert_row("records", storage::Row::new(vec![types::SqlValue::Integer(3)]))
        .unwrap();
    db.insert_row("records", storage::Row::new(vec![types::SqlValue::Integer(4)]))
        .unwrap();
    db.insert_row("records", storage::Row::new(vec![types::SqlValue::Integer(5)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![ast::Expression::Wildcard],
                }),
                op: ast::BinaryOperator::Multiply,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "records".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Float(50.0)); // 5 * 10
}

#[test]
fn test_count_star_in_case_then_clause() {
    // Test: CASE WHEN 1=1 THEN COUNT(*) ELSE 0 END
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(2)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Case {
                operand: None,
                when_clauses: vec![ast::CaseWhen {
                    conditions: vec![ast::Expression::BinaryOp {
                        left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(1))),
                        op: ast::BinaryOperator::Equal,
                        right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(1))),
                    }],
                    result: ast::Expression::AggregateFunction {
                        name: "COUNT".to_string(),
                        distinct: false,
                        args: vec![ast::Expression::Wildcard],
                    },
                }],
                else_result: Some(Box::new(ast::Expression::Literal(types::SqlValue::Integer(0)))),
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Numeric(2.0));
}

#[test]
fn test_count_star_in_nested_case_expression() {
    // Test: CASE COUNT(*) WHEN 2 THEN CASE WHEN 1=1 THEN 'two' END ELSE 'other' END
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "nested".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("nested", storage::Row::new(vec![types::SqlValue::Integer(1)]))
        .unwrap();
    db.insert_row("nested", storage::Row::new(vec![types::SqlValue::Integer(2)]))
        .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Case {
                operand: Some(Box::new(ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![ast::Expression::Wildcard],
                })),
                when_clauses: vec![ast::CaseWhen {
                    conditions: vec![ast::Expression::Literal(types::SqlValue::Integer(2))],
                    result: ast::Expression::Case {
                        operand: None,
                        when_clauses: vec![ast::CaseWhen {
                            conditions: vec![ast::Expression::BinaryOp {
                                left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(1))),
                                op: ast::BinaryOperator::Equal,
                                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(
                                    1,
                                ))),
                            }],
                            result: ast::Expression::Literal(types::SqlValue::Varchar("two".to_string())),
                        }],
                        else_result: None,
                    },
                }],
                else_result: Some(Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "other".to_string(),
                )))),
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "nested".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("two".to_string()));
}
