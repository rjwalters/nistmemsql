//! Common fixtures and constants for advanced function tests

use crate::common::create_test_evaluator;

/// Common mathematical constants used in tests
pub const EULER_NUMBER: f64 = 2.718281828;
pub const PI_APPROX: f64 = 3.14159265;

/// Helper to create a function expression with given name and arguments
pub fn create_function_expr(name: &str, args: Vec<ast::Expression>) -> ast::Expression {
    ast::Expression::Function { name: name.to_string(), args, character_unit: None }
}

/// Helper to create a literal expression from a SQL value
pub fn create_literal(value: types::SqlValue) -> ast::Expression {
    ast::Expression::Literal(value)
}

/// Setup for tests that need a standard evaluator and row
pub fn setup_test() -> (executor::ExpressionEvaluator<'static>, storage::Row) {
    create_test_evaluator()
}
