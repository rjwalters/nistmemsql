//! Simple expression evaluation in aggregate context (literals, column refs, etc.)

use super::super::super::builder::SelectExecutor;
use crate::{errors::ExecutorError, evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator}};

/// Import pattern matching function for LIKE evaluation
use crate::evaluator::pattern;

/// Re-import like_match for convenience
use pattern::like_match;

/// Evaluate expressions that may contain nested aggregates
///
/// Handles: Cast, Between, InList, Like, IsNull
///
/// These expressions need recursive evaluation because their sub-expressions
/// might contain aggregate functions.
pub(super) fn evaluate(
    executor: &SelectExecutor,
    expr: &vibesql_ast::Expression,
    group_rows: &[vibesql_storage::Row],
    group_key: &[vibesql_types::SqlValue],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    match expr {
        // CAST needs special handling to support nested aggregates
        // Example: CAST(MIN(74) AS SIGNED) or CAST(-MIN(74) AS SIGNED)
        vibesql_ast::Expression::Cast { expr: inner_expr, data_type } => {
            // Recursively evaluate the inner expression with aggregate support
            let inner_value = executor.evaluate_with_aggregates(inner_expr, group_rows, group_key, evaluator)?;

            // Cast the result to the target type using the casting module
            crate::evaluator::casting::cast_value(&inner_value, data_type)
        }

        // BETWEEN: expr BETWEEN low AND high
        // All three sub-expressions may contain aggregates
        vibesql_ast::Expression::Between { expr: test_expr, low, high, negated, symmetric } => {
            let test_val = executor.evaluate_with_aggregates(test_expr, group_rows, group_key, evaluator)?;
            let mut low_val = executor.evaluate_with_aggregates(low, group_rows, group_key, evaluator)?;
            let mut high_val = executor.evaluate_with_aggregates(high, group_rows, group_key, evaluator)?;

            // For SYMMETRIC: swap bounds if low > high
            if *symmetric {
                let gt_result = ExpressionEvaluator::eval_binary_op_static(
                    &low_val,
                    &vibesql_ast::BinaryOperator::GreaterThan,
                    &high_val,
                    vibesql_types::SqlMode::Standard,
                )?;

                if let vibesql_types::SqlValue::Boolean(true) = gt_result {
                    std::mem::swap(&mut low_val, &mut high_val);
                }
            }

            // Check if test_val >= low
            let ge_low = ExpressionEvaluator::eval_binary_op_static(
                &test_val,
                &vibesql_ast::BinaryOperator::GreaterThanOrEqual,
                &low_val,
                vibesql_types::SqlMode::Standard,
            )?;

            // Check if test_val <= high
            let le_high = ExpressionEvaluator::eval_binary_op_static(
                &test_val,
                &vibesql_ast::BinaryOperator::LessThanOrEqual,
                &high_val,
                vibesql_types::SqlMode::Standard,
            )?;

            // Combine with AND/OR depending on negated
            if *negated {
                // NOT BETWEEN: test_val < low OR test_val > high
                let lt_low = ExpressionEvaluator::eval_binary_op_static(
                    &test_val,
                    &vibesql_ast::BinaryOperator::LessThan,
                    &low_val,
                    vibesql_types::SqlMode::Standard,
                )?;
                let gt_high = ExpressionEvaluator::eval_binary_op_static(
                    &test_val,
                    &vibesql_ast::BinaryOperator::GreaterThan,
                    &high_val,
                    vibesql_types::SqlMode::Standard,
                )?;
                ExpressionEvaluator::eval_binary_op_static(
                    &lt_low,
                    &vibesql_ast::BinaryOperator::Or,
                    &gt_high,
                    vibesql_types::SqlMode::Standard,
                )
            } else {
                // BETWEEN: test_val >= low AND test_val <= high
                ExpressionEvaluator::eval_binary_op_static(
                    &ge_low,
                    &vibesql_ast::BinaryOperator::And,
                    &le_high,
                    vibesql_types::SqlMode::Standard,
                )
            }
        }

        // IN list: expr IN (val1, val2, ...)
        vibesql_ast::Expression::InList { expr: test_expr, values, negated } => {
            let test_val = executor.evaluate_with_aggregates(test_expr, group_rows, group_key, evaluator)?;

            // Evaluate all values in the list
            let mut list_values = Vec::new();
            for value_expr in values {
                list_values.push(executor.evaluate_with_aggregates(value_expr, group_rows, group_key, evaluator)?);
            }

            // Check if test_val is in the list
            let mut found = false;
            for list_val in &list_values {
                let eq_result = ExpressionEvaluator::eval_binary_op_static(
                    &test_val,
                    &vibesql_ast::BinaryOperator::Equal,
                    list_val,
                    vibesql_types::SqlMode::Standard,
                )?;

                if let vibesql_types::SqlValue::Boolean(true) = eq_result {
                    found = true;
                    break;
                }
            }

            Ok(vibesql_types::SqlValue::Boolean(if *negated { !found } else { found }))
        }

        // LIKE: expr LIKE pattern
        vibesql_ast::Expression::Like { expr: test_expr, pattern, negated } => {
            let test_val = executor.evaluate_with_aggregates(test_expr, group_rows, group_key, evaluator)?;
            let pattern_val = executor.evaluate_with_aggregates(pattern, group_rows, group_key, evaluator)?;

            // Extract string values
            let text = match test_val {
                vibesql_types::SqlValue::Varchar(ref s) | vibesql_types::SqlValue::Character(ref s) => s.clone(),
                vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
                _ => {
                    return Err(ExecutorError::TypeMismatch {
                        left: test_val,
                        op: "LIKE".to_string(),
                        right: pattern_val,
                    })
                }
            };

            let pattern_str = match pattern_val {
                vibesql_types::SqlValue::Varchar(ref s) | vibesql_types::SqlValue::Character(ref s) => s.clone(),
                vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
                _ => {
                    return Err(ExecutorError::TypeMismatch {
                        left: test_val,
                        op: "LIKE".to_string(),
                        right: pattern_val,
                    })
                }
            };

            // Perform pattern matching
            let matches = like_match(&text, &pattern_str);

            // Apply negation if needed
            let result = if *negated { !matches } else { matches };

            Ok(vibesql_types::SqlValue::Boolean(result))
        }

        // IS NULL / IS NOT NULL
        vibesql_ast::Expression::IsNull { expr: test_expr, negated } => {
            let value = executor.evaluate_with_aggregates(test_expr, group_rows, group_key, evaluator)?;
            let is_null = matches!(value, vibesql_types::SqlValue::Null);
            let result = if *negated { !is_null } else { is_null };
            Ok(vibesql_types::SqlValue::Boolean(result))
        }

        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Unexpected expression in simple evaluator: {:?}",
            expr
        ))),
    }
}

/// Evaluate expressions that CANNOT contain nested aggregates
///
/// Handles: Literal, ColumnRef
///
/// These are truly simple expressions that can be evaluated directly.
pub(super) fn evaluate_no_aggregates(
    expr: &vibesql_ast::Expression,
    group_rows: &[vibesql_storage::Row],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    match expr {
        // Literals can be evaluated without row context
        vibesql_ast::Expression::Literal(val) => Ok(val.clone()),

        // Column references: use first row from group as context
        vibesql_ast::Expression::ColumnRef { .. } => {
            if let Some(first_row) = group_rows.first() {
                evaluator.eval(expr, first_row)
            } else {
                Ok(vibesql_types::SqlValue::Null)
            }
        }

        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Unexpected expression in simple evaluator (no aggregates): {:?}",
            expr
        ))),
    }
}
