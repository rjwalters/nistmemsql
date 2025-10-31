//! Tests for timestamp operations with time components

use crate::common::create_test_evaluator;

#[test]
fn test_datediff_with_timestamps() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Timestamp("2024-01-10 15:30:00".to_string())),
            ast::Expression::Literal(types::SqlValue::Timestamp("2024-01-05 08:00:00".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(5)); // Time component ignored
}

#[test]
fn test_date_add_timestamp_with_time() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Timestamp("2024-01-15 14:30:00".to_string())),
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Varchar("HOUR".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Timestamp("2024-01-15 16:30:00".to_string()));
}

#[test]
fn test_extract_hour_from_timestamp() {
    let (evaluator, row) = create_test_evaluator();

    let expr = ast::Expression::Function {
        name: "EXTRACT".to_string(),
        args: vec![
            ast::Expression::Literal(types::SqlValue::Varchar("HOUR".to_string())),
            ast::Expression::Literal(types::SqlValue::Timestamp("2024-03-15 14:30:45".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, types::SqlValue::Integer(14));
}
