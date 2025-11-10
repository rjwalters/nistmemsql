//! Tests for date arithmetic operations (DATEDIFF, DATE_ADD, DATE_SUB with DATE types)

use crate::common::create_test_evaluator;

// ==================== DATEDIFF TESTS ====================

#[test]
fn test_datediff_basic() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-10".parse().unwrap())),
            ast::Expression::Literal(types::SqlValue::Date("2024-01-05".parse().unwrap())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(5)); // 10 - 5 = 5 days
}

#[test]
fn test_datediff_negative() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-05".parse().unwrap())),
            ast::Expression::Literal(types::SqlValue::Date("2024-01-10".parse().unwrap())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(-5)); // 5 - 10 = -5 days
}

#[test]
fn test_datediff_same_date() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".parse().unwrap())),
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".parse().unwrap())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(0));
}

// ==================== DATE_ADD TESTS ====================

#[test]
fn test_date_add_days() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".parse().unwrap())),
            ast::Expression::Literal(types::SqlValue::Integer(7)),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-01-22".parse().unwrap()));
}

#[test]
fn test_date_add_months() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".parse().unwrap())),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
            ast::Expression::Literal(types::SqlValue::Varchar("MONTH".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-04-15".parse().unwrap()));
}

#[test]
fn test_date_add_years() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".parse().unwrap())),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Varchar("YEAR".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2026-01-15".parse().unwrap()));
}

#[test]
fn test_date_add_negative() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".parse().unwrap())),
            ast::Expression::Literal(types::SqlValue::Integer(-5)),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-01-10".parse().unwrap()));
}

#[test]
fn test_adddate_alias() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "ADDDATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".parse().unwrap())),
            ast::Expression::Literal(types::SqlValue::Integer(10)),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-01-25".parse().unwrap()));
}

// ==================== DATE_SUB TESTS ====================

#[test]
fn test_date_sub_days() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_SUB".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".parse().unwrap())),
            ast::Expression::Literal(types::SqlValue::Integer(7)),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-01-08".parse().unwrap()));
}

#[test]
fn test_date_sub_months() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_SUB".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-04-15".parse().unwrap())),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Varchar("MONTH".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-02-15".parse().unwrap()));
}

#[test]
fn test_subdate_alias() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "SUBDATE".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Date("2024-01-15".parse().unwrap())),
            ast::Expression::Literal(types::SqlValue::Integer(5)),
            ast::Expression::Literal(types::SqlValue::Varchar("DAY".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Date("2024-01-10".parse().unwrap()));
}
