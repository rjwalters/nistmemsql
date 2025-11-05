//! Trait-based operator evaluation system
//!
//! This module provides a pluggable, testable operator system that replaces
//! the monolithic match statement in the core evaluator. Each operator category
//! (arithmetic, comparison, logical, string) is implemented in its own module
//! with dedicated logic and tests.

mod arithmetic;
mod comparison;
mod logical;
mod string;

use crate::errors::ExecutorError;
use types::SqlValue;

use arithmetic::ArithmeticOps;
use comparison::ComparisonOps;
use logical::LogicalOps;
use string::StringOps;

/// Trait for binary operator evaluation
#[allow(dead_code)]
pub(crate) trait BinaryOperator {
    /// Evaluate the operator on two SQL values
    fn evaluate(&self, left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError>;
}

/// Central registry for all binary operators
///
/// This provides a unified interface for evaluating binary operations,
/// dispatching to the appropriate specialized operator implementation.
pub(crate) struct OperatorRegistry;

impl OperatorRegistry {
    /// Evaluate a binary operation using the appropriate operator implementation
    #[inline]
    pub fn eval_binary_op(
        left: &SqlValue,
        op: &ast::BinaryOperator,
        right: &SqlValue,
    ) -> Result<SqlValue, ExecutorError> {
        use ast::BinaryOperator::*;

        // Short-circuit NULL handling (SQL three-valued logic)
        // NULL compared/operated with anything yields NULL
        if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
            return Ok(SqlValue::Null);
        }

        match op {
            // Arithmetic operators
            Plus => ArithmeticOps::add(left, right),
            Minus => ArithmeticOps::subtract(left, right),
            Multiply => ArithmeticOps::multiply(left, right),
            Divide => ArithmeticOps::divide(left, right),
            IntegerDivide => ArithmeticOps::integer_divide(left, right),
            Modulo => ArithmeticOps::modulo(left, right),

            // Comparison operators
            Equal => ComparisonOps::equal(left, right),
            NotEqual => ComparisonOps::not_equal(left, right),
            LessThan => ComparisonOps::less_than(left, right),
            LessThanOrEqual => ComparisonOps::less_than_or_equal(left, right),
            GreaterThan => ComparisonOps::greater_than(left, right),
            GreaterThanOrEqual => ComparisonOps::greater_than_or_equal(left, right),

            // Logical operators
            And => LogicalOps::and(left, right),
            Or => LogicalOps::or(left, right),

            // String operators
            Concat => StringOps::concat(left, right),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_handling() {
        use ast::BinaryOperator::*;

        // NULL + anything = NULL
        assert!(matches!(
            OperatorRegistry::eval_binary_op(&SqlValue::Null, &Plus, &SqlValue::Integer(1))
                .unwrap(),
            SqlValue::Null
        ));

        // anything + NULL = NULL
        assert!(matches!(
            OperatorRegistry::eval_binary_op(&SqlValue::Integer(1), &Plus, &SqlValue::Null)
                .unwrap(),
            SqlValue::Null
        ));

        // NULL comparison NULL = NULL
        assert!(matches!(
            OperatorRegistry::eval_binary_op(&SqlValue::Null, &Equal, &SqlValue::Null).unwrap(),
            SqlValue::Null
        ));
    }
}
