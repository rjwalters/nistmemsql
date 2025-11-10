//! CASE expression evaluation in aggregate context

use super::super::super::builder::SelectExecutor;
use crate::{errors::ExecutorError, evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator}};

/// Evaluate CASE expression with potential aggregates in operand/conditions/results
///
/// Handles both:
/// - Simple CASE: CASE COUNT(*) WHEN 5 THEN 'five' END
/// - Searched CASE: CASE WHEN COUNT(*) > 5 THEN 'many' END
pub(super) fn evaluate(
    executor: &SelectExecutor,
    operand: &Option<Box<ast::Expression>>,
    when_clauses: &[ast::CaseWhen],
    else_result: &Option<Box<ast::Expression>>,
    group_rows: &[storage::Row],
    group_key: &[types::SqlValue],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<types::SqlValue, ExecutorError> {
    match operand {
        // Simple CASE: CASE operand WHEN value THEN result ...
        Some(operand_expr) => {
            // Evaluate operand (may contain aggregates like COUNT(*))
            let operand_value = executor.evaluate_with_aggregates(
                operand_expr,
                group_rows,
                group_key,
                evaluator,
            )?;

            for when_clause in when_clauses {
                // Check if ANY condition matches (OR logic)
                for condition_expr in &when_clause.conditions {
                    // Evaluate condition (may contain aggregates)
                    let when_value = executor.evaluate_with_aggregates(
                        condition_expr,
                        group_rows,
                        group_key,
                        evaluator,
                    )?;

                    // Use IS NOT DISTINCT FROM semantics (NULL = NULL is TRUE)
                    if ExpressionEvaluator::values_are_equal(&operand_value, &when_value) {
                        // Evaluate result (may contain aggregates)
                        return executor.evaluate_with_aggregates(
                            &when_clause.result,
                            group_rows,
                            group_key,
                            evaluator,
                        );
                    }
                }
            }

            // No match - evaluate ELSE clause if present
            if let Some(else_expr) = else_result {
                executor.evaluate_with_aggregates(else_expr, group_rows, group_key, evaluator)
            } else {
                Ok(types::SqlValue::Null)
            }
        }

        // Searched CASE: CASE WHEN condition THEN result ...
        None => {
            for when_clause in when_clauses {
                // Each when_clause can have multiple conditions (OR logic within a clause)
                for condition_expr in &when_clause.conditions {
                    // Evaluate condition (may contain aggregates)
                    let condition_value = executor.evaluate_with_aggregates(
                        condition_expr,
                        group_rows,
                        group_key,
                        evaluator,
                    )?;

                    // Check if condition is TRUE (not FALSE or NULL)
                    let is_true = match condition_value {
                        types::SqlValue::Boolean(true) => true,
                        types::SqlValue::Boolean(false) | types::SqlValue::Null => false,
                        // SQLLogicTest compatibility: treat integers as truthy/falsy
                        types::SqlValue::Integer(0) => false,
                        types::SqlValue::Integer(_) => true,
                        types::SqlValue::Smallint(0) => false,
                        types::SqlValue::Smallint(_) => true,
                        types::SqlValue::Bigint(0) => false,
                        types::SqlValue::Bigint(_) => true,
                        types::SqlValue::Float(f) if f == 0.0 => false,
                        types::SqlValue::Float(_) => true,
                        types::SqlValue::Real(f) if f == 0.0 => false,
                        types::SqlValue::Real(_) => true,
                        types::SqlValue::Double(f) if f == 0.0 => false,
                        types::SqlValue::Double(_) => true,
                        other => {
                            return Err(ExecutorError::UnsupportedExpression(format!(
                                "CASE condition must evaluate to boolean, got: {:?}",
                                other
                            )))
                        }
                    };

                    if is_true {
                        // Evaluate result (may contain aggregates)
                        return executor.evaluate_with_aggregates(
                            &when_clause.result,
                            group_rows,
                            group_key,
                            evaluator,
                        );
                    }
                }
            }

            // No match - evaluate ELSE clause if present
            if let Some(else_expr) = else_result {
                executor.evaluate_with_aggregates(else_expr, group_rows, group_key, evaluator)
            } else {
                Ok(types::SqlValue::Null)
            }
        }
    }
}
