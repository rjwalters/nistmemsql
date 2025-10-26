//! Expression evaluator tests
//!
//! Tests for evaluating SQL expressions including literals, column references,
//! and binary operations.

use super::super::*;

#[test]
fn test_eval_integer_literal() {
    let schema = catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = storage::Row::new(vec![]);

    let expr = ast::Expression::Literal(types::SqlValue::Integer(42));
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(42));
}

#[test]
fn test_eval_string_literal() {
    let schema = catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = storage::Row::new(vec![]);

    let expr = ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()));
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_eval_null_literal() {
    let schema = catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = storage::Row::new(vec![]);

    let expr = ast::Expression::Literal(types::SqlValue::Null);
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Null);
}

#[test]
fn test_eval_column_ref() {
    let schema = catalog::TableSchema::new(
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
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = storage::Row::new(vec![
        types::SqlValue::Integer(1),
        types::SqlValue::Varchar("Alice".to_string()),
    ]);

    let expr = ast::Expression::ColumnRef { table: None, column: "id".to_string() };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(1));

    let expr = ast::Expression::ColumnRef { table: None, column: "name".to_string() };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("Alice".to_string()));
}

#[test]
fn test_eval_column_not_found() {
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = storage::Row::new(vec![types::SqlValue::Integer(1)]);

    let expr = ast::Expression::ColumnRef { table: None, column: "missing".to_string() };
    let err = evaluator.eval(&expr, &row).unwrap_err();
    match err {
        ExecutorError::ColumnNotFound(name) => assert_eq!(name, "missing"),
        other => panic!("Expected ColumnNotFound, got {:?}", other),
    }
}

#[test]
fn test_eval_addition() {
    let schema = catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = storage::Row::new(vec![]);

    let expr = ast::Expression::BinaryOp {
        left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
        op: ast::BinaryOperator::Plus,
        right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(15));
}
