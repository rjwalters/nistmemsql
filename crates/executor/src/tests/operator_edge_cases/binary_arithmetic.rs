//! Binary arithmetic operator edge case tests
//!
//! Tests for complex nested arithmetic expressions,
//! operator precedence, and associativity.

use crate::*;

#[test]
fn test_nested_arithmetic() {
    let db = storage::Database::new();
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
    // Arithmetic operations now return Numeric (DECIMAL), but division returns Float
    // (8 * 2) = Numeric(16.0), 16.0 - 5 involves Float conversion
    assert!(matches!(result[0].values[0], types::SqlValue::Float(_))); // Result is Float
    if let types::SqlValue::Float(f) = result[0].values[0] {
        assert!((f - 11.0).abs() < 0.01); // (8 * 2) - 5 = 11
    }
}

#[test]
fn test_integer_division_basic() {
    let db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT 81 DIV 31
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(81))),
                op: ast::BinaryOperator::IntegerDivide,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(31))),
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
    // 81 / 31 = 2.6129..., truncated to 2
    assert_eq!(result[0].values[0], types::SqlValue::Integer(2));
}

#[test]
fn test_integer_division_with_floats() {
    use crate::evaluator::operators::OperatorRegistry;

    // 10.7 DIV 3.2 should return 3 (not 3.34)
    let result =
        OperatorRegistry::eval_binary_op(
            &types::SqlValue::Float(10.7),
            &ast::BinaryOperator::IntegerDivide,
            &types::SqlValue::Float(3.2),
        )
        .unwrap();
    assert_eq!(result, types::SqlValue::Integer(3));
}

#[test]
fn test_integer_division_negative_operands() {
    use crate::evaluator::operators::OperatorRegistry;

    // 96 DIV -2 should return -48
    let result =
        OperatorRegistry::eval_binary_op(
            &types::SqlValue::Integer(96),
            &ast::BinaryOperator::IntegerDivide,
            &types::SqlValue::Integer(-2),
        )
        .unwrap();
    assert_eq!(result, types::SqlValue::Integer(-48));

    // -96 DIV 2 should return -48
    let result =
        OperatorRegistry::eval_binary_op(
            &types::SqlValue::Integer(-96),
            &ast::BinaryOperator::IntegerDivide,
            &types::SqlValue::Integer(2),
        )
        .unwrap();
    assert_eq!(result, types::SqlValue::Integer(-48));

    // -96 DIV -2 should return 48
    let result =
        OperatorRegistry::eval_binary_op(
            &types::SqlValue::Integer(-96),
            &ast::BinaryOperator::IntegerDivide,
            &types::SqlValue::Integer(-2),
        )
        .unwrap();
    assert_eq!(result, types::SqlValue::Integer(48));
}

#[test]
fn test_integer_division_by_zero() {
    use crate::evaluator::operators::OperatorRegistry;
    use crate::errors::ExecutorError;

    // 5 DIV 0 should return DivisionByZero error
    let result = OperatorRegistry::eval_binary_op(
        &types::SqlValue::Integer(5),
        &ast::BinaryOperator::IntegerDivide,
        &types::SqlValue::Integer(0),
    );
    assert!(matches!(result, Err(ExecutorError::DivisionByZero)));
}

#[test]
fn test_integer_division_equal_operands() {
    use crate::evaluator::operators::OperatorRegistry;

    // 5 DIV 5 should return 1
    let result =
        OperatorRegistry::eval_binary_op(
            &types::SqlValue::Integer(5),
            &ast::BinaryOperator::IntegerDivide,
            &types::SqlValue::Integer(5),
        )
        .unwrap();
    assert_eq!(result, types::SqlValue::Integer(1));
}

#[test]
fn test_modulo_operator() {
    use crate::evaluator::operators::OperatorRegistry;

    // 10 % 3 should return 1
    let result = OperatorRegistry::eval_binary_op(
        &types::SqlValue::Integer(10),
        &ast::BinaryOperator::Modulo,
        &types::SqlValue::Integer(3),
    )
    .unwrap();
    assert_eq!(result, types::SqlValue::Integer(1));

    // 15 % 4 should return 3
    let result = OperatorRegistry::eval_binary_op(
        &types::SqlValue::Integer(15),
        &ast::BinaryOperator::Modulo,
        &types::SqlValue::Integer(4),
    )
    .unwrap();
    assert_eq!(result, types::SqlValue::Integer(3));
}
