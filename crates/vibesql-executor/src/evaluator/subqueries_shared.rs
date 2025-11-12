//! Shared subquery evaluation logic
//!
//! This module contains shared implementations of subquery evaluation logic
//! that can be used by both ExpressionEvaluator and CombinedExpressionEvaluator.
//! This avoids code duplication and ensures consistent behavior across evaluators.

use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Evaluate IN subquery predicate with shared logic
///
/// This implements the core IN predicate logic that both ExpressionEvaluator
/// and CombinedExpressionEvaluator use. The key difference is how they obtain
/// the CombinedSchema - ExpressionEvaluator converts from TableSchema, while
/// CombinedExpressionEvaluator already has one.
///
/// ## SQL Semantics
///
/// Per SQL:1999 Section 8.4 (IN predicate):
/// - `expr IN (subquery)` returns TRUE if expr matches any value in subquery
/// - Returns FALSE if no match and no NULLs encountered
/// - Returns NULL if no match but NULLs were encountered
/// - **SQLite extension**: `NULL IN (empty set)` returns FALSE, not NULL
///
/// ## Parameters
///
/// - `expr_val`: The evaluated left-hand expression value
/// - `rows`: The subquery result rows
/// - `negated`: True for NOT IN, false for IN
///
/// ## Returns
///
/// - `Boolean(true)` if match found (or no match for NOT IN)
/// - `Boolean(false)` if no match (or match found for NOT IN)
/// - `Null` if no match but NULLs encountered in subquery
pub fn eval_in_subquery_core(
    expr_val: vibesql_types::SqlValue,
    rows: &[vibesql_storage::Row],
    negated: bool,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    // Empty set optimization (SQLite behavior):
    // If the subquery returns no rows, return FALSE for IN, TRUE for NOT IN
    // This is true regardless of whether the left expression is NULL
    // Rationale: No value can match an empty set
    if rows.is_empty() {
        return Ok(vibesql_types::SqlValue::Boolean(negated));
    }

    // If left expression is NULL, result is NULL (per SQL three-valued logic)
    if matches!(expr_val, vibesql_types::SqlValue::Null) {
        return Ok(vibesql_types::SqlValue::Null);
    }

    let mut found_null = false;

    // Check each row from subquery
    for subquery_row in rows {
        let subquery_val =
            subquery_row.get(0).ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })?;

        // Track if we encounter NULL
        if matches!(subquery_val, vibesql_types::SqlValue::Null) {
            found_null = true;
            continue;
        }

        // Compare using equality
        let eq_result = super::core::ExpressionEvaluator::eval_binary_op_static(
            &expr_val,
            &vibesql_ast::BinaryOperator::Equal,
            subquery_val,
        )?;

        // If we found a match, return TRUE (or FALSE if negated)
        if matches!(eq_result, vibesql_types::SqlValue::Boolean(true)) {
            return Ok(vibesql_types::SqlValue::Boolean(!negated));
        }
    }

    // No match found
    // If we encountered NULL, return NULL (per SQL three-valued logic)
    // Otherwise return FALSE (or TRUE if negated)
    if found_null {
        Ok(vibesql_types::SqlValue::Null)
    } else {
        Ok(vibesql_types::SqlValue::Boolean(negated))
    }
}
