//! Tests for SELECT without FROM clause
//!
//! Tests queries that evaluate expressions without any table context

use super::super::*;

#[test]
fn test_select_literal_integers() {
    let db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::Literal(types::SqlValue::Integer(1)),
                alias: Some("one".to_string()),
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::Literal(types::SqlValue::Integer(2)),
                alias: Some("two".to_string()),
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::Literal(types::SqlValue::Integer(3)),
                alias: Some("three".to_string()),
            },
        ],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1); // Single row
    assert_eq!(result[0].values.len(), 3);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
    assert_eq!(result[0].values[1], types::SqlValue::Integer(2));
    assert_eq!(result[0].values[2], types::SqlValue::Integer(3));
}

#[test]
fn test_select_literal_mixed_types() {
    let db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::Literal(types::SqlValue::Integer(42)),
                alias: Some("num".to_string()),
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string())),
                alias: Some("text".to_string()),
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::Literal(types::SqlValue::Boolean(true)),
                alias: Some("flag".to_string()),
            },
        ],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(42));
    assert_eq!(result[0].values[1], types::SqlValue::Varchar("hello".to_string()));
    assert_eq!(result[0].values[2], types::SqlValue::Boolean(true));
}

#[test]
fn test_select_arithmetic_expression() {
    let db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(1))),
                    op: ast::BinaryOperator::Plus,
                    right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(1))),
                },
                alias: Some("sum".to_string()),
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(2))),
                    op: ast::BinaryOperator::Multiply,
                    right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(3))),
                },
                alias: Some("product".to_string()),
            },
        ],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(2)); // 1 + 1
    assert_eq!(result[0].values[1], types::SqlValue::Integer(6)); // 2 * 3
}

#[test]
fn test_select_function_call() {
    let db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "UPPER".to_string(),
                args: vec![ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()))],
                character_unit: None,
            },
            alias: Some("upper".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("HELLO".to_string()));
}

#[test]
fn test_select_star_without_from_fails() {
    let db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("SELECT * requires FROM clause"));
        }
        _ => panic!("Expected UnsupportedFeature error"),
    }
}

#[test]
fn test_column_reference_without_from_fails() {
    let db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::ColumnRef { table: None, column: "some_column".to_string() },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("Column reference requires FROM clause"));
        }
        _ => panic!("Expected UnsupportedFeature error"),
    }
}

#[test]
fn test_is_null_with_column_reference_fails() {
    let db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::IsNull {
                expr: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                negated: false,
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("Column reference requires FROM clause"));
        }
        _ => panic!("Expected UnsupportedFeature error"),
    }
}

#[test]
fn test_between_with_column_reference_fails() {
    let db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Between {
                expr: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "price".to_string(),
                }),
                low: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
                high: Box::new(ast::Expression::Literal(types::SqlValue::Integer(20))),
                negated: false,
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("Column reference requires FROM clause"));
        }
        _ => panic!("Expected UnsupportedFeature error"),
    }
}

#[test]
fn test_cast_with_column_reference_fails() {
    let db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Cast {
                expr: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                data_type: types::DataType::Varchar { max_length: Some(255) },
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("Column reference requires FROM clause"));
        }
        _ => panic!("Expected UnsupportedFeature error"),
    }
}

#[test]
fn test_like_with_column_reference_fails() {
    let db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Like {
                expr: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "name".to_string(),
                }),
                pattern: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "A%".to_string(),
                ))),
                negated: false,
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("Column reference requires FROM clause"));
        }
        _ => panic!("Expected UnsupportedFeature error"),
    }
}

#[test]
fn test_in_list_with_column_reference_fails() {
    let db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::InList {
                expr: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                values: vec![
                    ast::Expression::Literal(types::SqlValue::Integer(1)),
                    ast::Expression::Literal(types::SqlValue::Integer(2)),
                    ast::Expression::Literal(types::SqlValue::Integer(3)),
                ],
                negated: false,
            },
            alias: None,
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("Column reference requires FROM clause"));
        }
        _ => panic!("Expected UnsupportedFeature error"),
    }
}
