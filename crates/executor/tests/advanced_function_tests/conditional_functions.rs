//! Tests for conditional functions (GREATEST, LEAST, IF)

use crate::common::create_test_evaluator;

#[test]
fn test_greatest_integers() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "GREATEST".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Integer(10)),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
            ast::Expression::Literal(types::SqlValue::Integer(7)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(10));
}

#[test]
fn test_greatest_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "GREATEST".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Integer(10)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(10)); // NULL is ignored
}

#[test]
fn test_least_integers() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "LEAST".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Integer(10)),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
            ast::Expression::Literal(types::SqlValue::Integer(7)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(3));
}

#[test]
fn test_least_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "LEAST".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(3)); // NULL is ignored
}

#[test]
fn test_if_true_condition() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "IF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Boolean(true)),
            ast::Expression::Literal(types::SqlValue::Varchar("yes".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("no".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("yes".to_string()));
}

#[test]
fn test_if_false_condition() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "IF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Boolean(false)),
            ast::Expression::Literal(types::SqlValue::Varchar("yes".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("no".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("no".to_string()));
}

#[test]
fn test_if_null_condition() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "IF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Null),
            ast::Expression::Literal(types::SqlValue::Varchar("yes".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("no".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Varchar("no".to_string())); // NULL treated as false
}
