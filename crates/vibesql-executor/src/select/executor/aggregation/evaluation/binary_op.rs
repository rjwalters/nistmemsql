//! Binary and unary operator evaluation in aggregate context

use super::super::super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator},
};

/// Evaluate binary operations in aggregate context
pub(super) fn evaluate_binary(
    executor: &SelectExecutor,
    left: &vibesql_ast::Expression,
    op: &vibesql_ast::BinaryOperator,
    right: &vibesql_ast::Expression,
    group_rows: &[vibesql_storage::Row],
    group_key: &[vibesql_types::SqlValue],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    let left_val = executor.evaluate_with_aggregates(left, group_rows, group_key, evaluator)?;
    let right_val = executor.evaluate_with_aggregates(right, group_rows, group_key, evaluator)?;

    // Reuse the binary op evaluation logic from ExpressionEvaluator
    let temp_schema = vibesql_catalog::TableSchema::new("temp".to_string(), vec![]);
    let temp_evaluator = ExpressionEvaluator::with_database(&temp_schema, executor.database);
    temp_evaluator.eval_binary_op(&left_val, op, &right_val)
}

/// Evaluate unary operations in aggregate context
///
/// This is a helper function for evaluating unary operators (+, -, NOT) on values
/// that may result from aggregate functions like COUNT(*).
pub(super) fn evaluate_unary(
    executor: &SelectExecutor,
    op: &vibesql_ast::UnaryOperator,
    inner_expr: &vibesql_ast::Expression,
    group_rows: &[vibesql_storage::Row],
    group_key: &[vibesql_types::SqlValue],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    let val = executor.evaluate_with_aggregates(inner_expr, group_rows, group_key, evaluator)?;
    // Use shared eval_unary_op implementation
    crate::evaluator::eval_unary_op(op, &val)
}
