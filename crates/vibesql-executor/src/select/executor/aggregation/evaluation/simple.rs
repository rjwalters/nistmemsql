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
    expr: &vibesql_ast::Expression,
    group_rows: &[vibesql_storage::Row],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    match expr {
        // Literals can be evaluated without row context
        vibesql_ast::Expression::Literal(val) => Ok(val.clone()),

        // Other expressions: delegate to evaluator using first row from group as context
        vibesql_ast::Expression::ColumnRef { .. }
        | vibesql_ast::Expression::InList { .. }
        | vibesql_ast::Expression::Between { .. }
        | vibesql_ast::Expression::Cast { .. }
        | vibesql_ast::Expression::Like { .. }
        | vibesql_ast::Expression::IsNull { .. } => {
            // Use first row from group as context
            if let Some(first_row) = group_rows.first() {
                evaluator.eval(expr, first_row)
            } else {
                Ok(vibesql_types::SqlValue::Null)
            }
        }

        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Unexpected expression in simple evaluator: {:?}",
            expr
        ))),
    }
}
