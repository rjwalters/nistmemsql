//! WHERE clause filtering logic

use crate::{
    errors::ExecutorError,
    evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator},
};
use rayon::prelude::*;
use std::sync::Arc;

/// Apply WHERE clause filter to rows (Combined evaluator version)
///
/// Same as apply_where_filter but specifically for CombinedExpressionEvaluator.
/// Used in non-aggregation queries.
///
/// Accepts SelectExecutor for timeout enforcement. Timeout is checked every 1000 rows.
pub(super) fn apply_where_filter_combined<'a>(
    rows: Vec<vibesql_storage::Row>,
    where_expr: Option<&vibesql_ast::Expression>,
    evaluator: &CombinedExpressionEvaluator,
    executor: &crate::SelectExecutor<'a>,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    if where_expr.is_none() {
        // No WHERE clause, return all rows
        return Ok(rows);
    }

    let where_expr = where_expr.unwrap();
    let mut filtered_rows = Vec::new();
    let mut rows_processed = 0;
    const CHECK_INTERVAL: usize = 1000;

    for row in rows {
        // Check timeout every 1000 rows
        rows_processed += 1;
        if rows_processed % CHECK_INTERVAL == 0 {
            executor.check_timeout()?;
        }

        // Clear CSE cache before evaluating each row to prevent column values
        // from being incorrectly cached across different rows
        evaluator.clear_cse_cache();

        let include_row = match evaluator.eval(where_expr, &row)? {
            vibesql_types::SqlValue::Boolean(true) => true,
            vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Null => false,
            // SQLLogicTest compatibility: treat integers as truthy/falsy (C-like behavior)
            vibesql_types::SqlValue::Integer(0) => false,
            vibesql_types::SqlValue::Integer(_) => true,
            vibesql_types::SqlValue::Smallint(0) => false,
            vibesql_types::SqlValue::Smallint(_) => true,
            vibesql_types::SqlValue::Bigint(0) => false,
            vibesql_types::SqlValue::Bigint(_) => true,
            vibesql_types::SqlValue::Float(f) if f == 0.0 => false,
            vibesql_types::SqlValue::Float(_) => true,
            vibesql_types::SqlValue::Real(f) if f == 0.0 => false,
            vibesql_types::SqlValue::Real(_) => true,
            vibesql_types::SqlValue::Double(f) if f == 0.0 => false,
            vibesql_types::SqlValue::Double(_) => true,
            other => {
                return Err(ExecutorError::InvalidWhereClause(format!(
                    "WHERE clause must evaluate to boolean, got: {:?}",
                    other
                )))
            }
        };

        if include_row {
            filtered_rows.push(row);
        }
    }

    Ok(filtered_rows)
}

/// Apply WHERE clause filter to rows (Basic evaluator version)
///
/// Same as apply_where_filter but specifically for ExpressionEvaluator.
/// Used in aggregation queries.
///
/// Accepts SelectExecutor for timeout enforcement. Timeout is checked every 1000 rows.
#[allow(dead_code)]
pub(super) fn apply_where_filter_basic<'a>(
    rows: Vec<vibesql_storage::Row>,
    where_expr: Option<&vibesql_ast::Expression>,
    evaluator: &ExpressionEvaluator,
    executor: &crate::SelectExecutor<'a>,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    if where_expr.is_none() {
        // No WHERE clause, return all rows
        return Ok(rows);
    }

    let where_expr = where_expr.unwrap();
    let mut filtered_rows = Vec::new();
    let mut rows_processed = 0;
    const CHECK_INTERVAL: usize = 1000;

    for row in rows {
        // Check timeout every 1000 rows
        rows_processed += 1;
        if rows_processed % CHECK_INTERVAL == 0 {
            executor.check_timeout()?;
        }

        // Clear CSE cache before evaluating each row to prevent column values
        // from being incorrectly cached across different rows
        evaluator.clear_cse_cache();

        let include_row = match evaluator.eval(where_expr, &row)? {
            vibesql_types::SqlValue::Boolean(true) => true,
            vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Null => false,
            // SQLLogicTest compatibility: treat integers as truthy/falsy (C-like behavior)
            vibesql_types::SqlValue::Integer(0) => false,
            vibesql_types::SqlValue::Integer(_) => true,
            vibesql_types::SqlValue::Smallint(0) => false,
            vibesql_types::SqlValue::Smallint(_) => true,
            vibesql_types::SqlValue::Bigint(0) => false,
            vibesql_types::SqlValue::Bigint(_) => true,
            vibesql_types::SqlValue::Float(f) if f == 0.0 => false,
            vibesql_types::SqlValue::Float(_) => true,
            vibesql_types::SqlValue::Real(f) if f == 0.0 => false,
            vibesql_types::SqlValue::Real(_) => true,
            vibesql_types::SqlValue::Double(f) if f == 0.0 => false,
            vibesql_types::SqlValue::Double(_) => true,
            other => {
                return Err(ExecutorError::InvalidWhereClause(format!(
                    "WHERE must evaluate to boolean, got: {:?}",
                    other
                )))
            }
        };

        if include_row {
            filtered_rows.push(row);
        }
    }

    Ok(filtered_rows)
}

/// Check if parallel execution is enabled via environment variable
/// Defaults to false, can be enabled by setting PARALLEL_EXECUTION=true
fn is_parallel_execution_enabled() -> bool {
    std::env::var("PARALLEL_EXECUTION")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(false) // Default: disabled
}

/// Minimum number of rows to consider parallel execution
/// Below this threshold, sequential execution is used to avoid overhead
const PARALLEL_THRESHOLD: usize = 10_000;

/// Parallel version of apply_where_filter_combined
/// Uses rayon to evaluate WHERE predicates across multiple threads
#[allow(dead_code)]
pub(super) fn apply_where_filter_combined_parallel<'a>(
    rows: Vec<vibesql_storage::Row>,
    where_expr: Option<&vibesql_ast::Expression>,
    evaluator: &CombinedExpressionEvaluator,
    _executor: &crate::SelectExecutor<'a>,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    if where_expr.is_none() {
        return Ok(rows);
    }

    // Don't use parallel execution if below threshold
    if rows.len() < PARALLEL_THRESHOLD {
        return apply_where_filter_combined(rows, where_expr, evaluator, _executor);
    }

    let where_expr = where_expr.unwrap();

    // Clone the expression for thread-safe sharing
    let where_expr_arc = Arc::new(where_expr.clone());

    // Extract evaluator components before parallel execution
    let (schema, database, outer_row, outer_schema, window_mapping, enable_cse) =
        evaluator.get_parallel_components();

    // Use rayon's parallel iterator for filtering
    let result: Result<Vec<_>, ExecutorError> = rows
        .into_par_iter()
        .map(|row| {
            // Create a thread-local evaluator with independent caches
            let thread_evaluator = CombinedExpressionEvaluator::from_parallel_components(
                schema,
                database,
                outer_row,
                outer_schema,
                window_mapping,
                enable_cse,
            );

            // Evaluate predicate for this row
            let include_row = match thread_evaluator.eval(&where_expr_arc, &row)? {
                vibesql_types::SqlValue::Boolean(true) => true,
                vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Null => false,
                // SQLLogicTest compatibility: treat integers as truthy/falsy
                vibesql_types::SqlValue::Integer(0) => false,
                vibesql_types::SqlValue::Integer(_) => true,
                vibesql_types::SqlValue::Smallint(0) => false,
                vibesql_types::SqlValue::Smallint(_) => true,
                vibesql_types::SqlValue::Bigint(0) => false,
                vibesql_types::SqlValue::Bigint(_) => true,
                vibesql_types::SqlValue::Float(f) if f == 0.0 => false,
                vibesql_types::SqlValue::Float(_) => true,
                vibesql_types::SqlValue::Real(f) if f == 0.0 => false,
                vibesql_types::SqlValue::Real(_) => true,
                vibesql_types::SqlValue::Double(f) if f == 0.0 => false,
                vibesql_types::SqlValue::Double(_) => true,
                other => {
                    return Err(ExecutorError::InvalidWhereClause(format!(
                        "WHERE clause must evaluate to boolean, got: {:?}",
                        other
                    )))
                }
            };

            if include_row {
                Ok(Some(row))
            } else {
                Ok(None)
            }
        })
        .collect();

    // Filter out None values and extract Ok rows
    result.map(|v| v.into_iter().flatten().collect())
}

/// Auto-selecting WHERE filter that chooses between sequential and parallel execution
/// Uses parallel execution if enabled and row count exceeds threshold
pub(super) fn apply_where_filter_combined_auto<'a>(
    rows: Vec<vibesql_storage::Row>,
    where_expr: Option<&vibesql_ast::Expression>,
    evaluator: &CombinedExpressionEvaluator,
    executor: &crate::SelectExecutor<'a>,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    if is_parallel_execution_enabled() && rows.len() >= PARALLEL_THRESHOLD {
        apply_where_filter_combined_parallel(rows, where_expr, evaluator, executor)
    } else {
        apply_where_filter_combined(rows, where_expr, evaluator, executor)
    }
}
