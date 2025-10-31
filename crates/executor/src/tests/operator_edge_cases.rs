//! Operator edge case tests
//!
//! Comprehensive tests for operator edge cases including:
//! - Unary operators (Plus, Minus)
//! - NULL propagation in all operators
//! - Complex nested expressions
//! - Mixed type operations
//! - Operator precedence
//! - String concatenation edge cases

use super::super::*;

// =============================================================================
// Unary Operator Tests
// =============================================================================

#[test]
fn test_unary_plus_integer() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::UnaryOp {
                op: ast::UnaryOperator::Plus,
                expr: Box::new(ast::Expression::Literal(types::SqlValue::Integer(42))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(42));
}

#[test]
fn test_unary_plus_float() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::UnaryOp {
                op: ast::UnaryOperator::Plus,
                expr: Box::new(ast::Expression::Literal(types::SqlValue::Float(3.14))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Float(3.14));
}

#[test]
fn test_unary_minus_integer() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr: Box::new(ast::Expression::Literal(types::SqlValue::Integer(42))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(-42));
}

#[test]
fn test_unary_minus_negative() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr: Box::new(ast::Expression::Literal(types::SqlValue::Integer(-42))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(42));
}

#[test]
fn test_unary_minus_numeric_string() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr: Box::new(ast::Expression::Literal(types::SqlValue::Numeric(
                    "123.45".to_string(),
                ))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Numeric("-123.45".to_string()));
}

#[test]
fn test_unary_minus_negative_numeric() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr: Box::new(ast::Expression::Literal(types::SqlValue::Numeric(
                    "-123.45".to_string(),
                ))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Numeric("123.45".to_string()));
}

#[test]
fn test_unary_plus_null() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::UnaryOp {
                op: ast::UnaryOperator::Plus,
                expr: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Null);
}

#[test]
fn test_unary_minus_null() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Null);
}

#[test]
fn test_unary_plus_invalid_type() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::UnaryOp {
                op: ast::UnaryOperator::Plus,
                expr: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "hello".to_string(),
                ))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::TypeMismatch { .. }));
}

#[test]
fn test_unary_minus_invalid_type() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "hello".to_string(),
                ))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::TypeMismatch { .. }));
}

// =============================================================================
// Complex Nested Expression Tests
// =============================================================================

#[test]
fn test_nested_arithmetic() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT ((5 + 3) * 2) - (10 / 2)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::BinaryOp {
                        left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
                        op: ast::BinaryOperator::Plus,
                        right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(3))),
                    }),
                    op: ast::BinaryOperator::Multiply,
                    right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(2))),
                }),
                op: ast::BinaryOperator::Minus,
                right: Box::new(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
                    op: ast::BinaryOperator::Divide,
                    right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(2))),
                }),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(11)); // (8 * 2) - 5 = 11
}

#[test]
fn test_nested_comparisons() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(15)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE (val > 10) AND (val < 20)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "val".to_string(),
                }),
                op: ast::BinaryOperator::GreaterThan,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
            }),
            op: ast::BinaryOperator::And,
            right: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "val".to_string(),
                }),
                op: ast::BinaryOperator::LessThan,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(20))),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(15));
}

// =============================================================================
// NULL Propagation Tests
// =============================================================================

#[test]
fn test_null_plus_integer() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
                op: ast::BinaryOperator::Plus,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Null);
}

#[test]
fn test_integer_plus_null() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
                op: ast::BinaryOperator::Plus,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Null);
}

#[test]
fn test_null_multiply_integer() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
                op: ast::BinaryOperator::Multiply,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Null);
}

#[test]
fn test_null_comparison() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Null);
}

// =============================================================================
// String Concatenation Edge Cases
// =============================================================================

#[test]
fn test_string_concat_basic() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "Hello".to_string(),
                ))),
                op: ast::BinaryOperator::Concat,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    " World".to_string(),
                ))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("Hello World".to_string()));
}

#[test]
fn test_string_concat_empty() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "Hello".to_string(),
                ))),
                op: ast::BinaryOperator::Concat,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Varchar("".to_string()))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("Hello".to_string()));
}

#[test]
fn test_string_concat_null() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "Hello".to_string(),
                ))),
                op: ast::BinaryOperator::Concat,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Null);
}

#[test]
fn test_string_concat_multiple() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // "Hello" || " " || "Beautiful" || " " || "World"
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::BinaryOp {
                        left: Box::new(ast::Expression::BinaryOp {
                            left: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                                "Hello".to_string(),
                            ))),
                            op: ast::BinaryOperator::Concat,
                            right: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                                " ".to_string(),
                            ))),
                        }),
                        op: ast::BinaryOperator::Concat,
                        right: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                            "Beautiful".to_string(),
                        ))),
                    }),
                    op: ast::BinaryOperator::Concat,
                    right: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                        " ".to_string(),
                    ))),
                }),
                op: ast::BinaryOperator::Concat,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "World".to_string(),
                ))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("Hello Beautiful World".to_string()));
}
