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
    let temp_evaluator = ExpressionEvaluator::new(&temp_schema);
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
    // Evaluate unary operator on the result
    eval_unary_op(op, &val)
}

/// Evaluate a unary operation on a value
fn eval_unary_op(
    op: &vibesql_ast::UnaryOperator,
    val: &vibesql_types::SqlValue,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    use vibesql_ast::UnaryOperator::*;
    use vibesql_types::SqlValue;

    match (op, val) {
        // Unary plus - identity operation (return value unchanged)
        (Plus, SqlValue::Integer(n)) => Ok(SqlValue::Integer(*n)),
        (Plus, SqlValue::Smallint(n)) => Ok(SqlValue::Smallint(*n)),
        (Plus, SqlValue::Bigint(n)) => Ok(SqlValue::Bigint(*n)),
        (Plus, SqlValue::Float(n)) => Ok(SqlValue::Float(*n)),
        (Plus, SqlValue::Real(n)) => Ok(SqlValue::Real(*n)),
        (Plus, SqlValue::Double(n)) => Ok(SqlValue::Double(*n)),
        (Plus, SqlValue::Numeric(s)) => Ok(SqlValue::Numeric(*s)),

        // Unary minus - negation
        (Minus, SqlValue::Integer(n)) => Ok(SqlValue::Integer(-n)),
        (Minus, SqlValue::Smallint(n)) => Ok(SqlValue::Smallint(-n)),
        (Minus, SqlValue::Bigint(n)) => Ok(SqlValue::Bigint(-n)),
        (Minus, SqlValue::Float(n)) => Ok(SqlValue::Float(-n)),
        (Minus, SqlValue::Real(n)) => Ok(SqlValue::Real(-n)),
        (Minus, SqlValue::Double(n)) => Ok(SqlValue::Double(-n)),
        (Minus, SqlValue::Numeric(f)) => Ok(SqlValue::Numeric(-*f)),

        // NULL propagation - unary operations on NULL return NULL
        (Plus | Minus, SqlValue::Null) => Ok(SqlValue::Null),

        // Unary NOT - logical negation
        (Not, SqlValue::Boolean(b)) => Ok(SqlValue::Boolean(!b)),
        (Not, SqlValue::Null) => Ok(SqlValue::Null), // NULL propagation for NOT

        // Type errors
        (Plus, val) => Err(ExecutorError::TypeMismatch {
            left: val.clone(),
            op: "unary +".to_string(),
            right: SqlValue::Null,
        }),
        (Minus, val) => Err(ExecutorError::TypeMismatch {
            left: val.clone(),
            op: "unary -".to_string(),
            right: SqlValue::Null,
        }),
        (Not, val) => Err(ExecutorError::TypeMismatch {
            left: val.clone(),
            op: "NOT".to_string(),
            right: SqlValue::Null,
        }),

        // Other unary operators are handled elsewhere
        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Unary operator {:?} not supported in aggregate context",
            op
        ))),
    }
}
