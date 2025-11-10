//! Simple expression evaluation in aggregate context (literals, column refs, etc.)

use super::super::super::builder::SelectExecutor;
use crate::{errors::ExecutorError, evaluator::CombinedExpressionEvaluator};

/// Evaluate simple expressions that delegate to the evaluator
///
/// Handles: Literal, ColumnRef, InList, Between, Cast, Like, IsNull
///
/// Note: CASE is now handled separately in case.rs to support aggregates
pub(super) fn evaluate(
    _executor: &SelectExecutor,
    expr: &ast::Expression,
    group_rows: &[storage::Row],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<types::SqlValue, ExecutorError> {
    match expr {
        // Literals can be evaluated without row context
        ast::Expression::Literal(val) => Ok(val.clone()),

        // Other expressions: delegate to evaluator using first row from group as context
        ast::Expression::ColumnRef { .. }
        | ast::Expression::InList { .. }
        | ast::Expression::Between { .. }
        | ast::Expression::Cast { .. }
        | ast::Expression::Like { .. }
        | ast::Expression::IsNull { .. } => {
            // Use first row from group as context
            if let Some(first_row) = group_rows.first() {
                evaluator.eval(expr, first_row)
            } else {
                Ok(types::SqlValue::Null)
            }
        }

        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Unexpected expression in simple evaluator: {:?}",
            expr
        ))),
    }
}
