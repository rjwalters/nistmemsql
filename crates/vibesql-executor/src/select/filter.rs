//! WHERE clause filtering logic

use crate::{
    errors::ExecutorError,
    evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator},
};
use rayon::prelude::*;
use std::sync::Arc;

use super::parallel::ParallelConfig;

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
    // Use pooled buffer to reduce allocation overhead during filtering
    let mut filtered_rows = executor.query_buffer_pool().get_row_buffer(rows.len());
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
            vibesql_types::SqlValue::Float(0.0) => false,
            vibesql_types::SqlValue::Float(_) => true,
            vibesql_types::SqlValue::Real(0.0) => false,
            vibesql_types::SqlValue::Real(_) => true,
            vibesql_types::SqlValue::Double(0.0) => false,
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

    // Move data to final result and return pooled buffer
    // This allows buffer reuse while avoiding clone overhead
    let result = std::mem::take(&mut filtered_rows);
    executor.query_buffer_pool().return_row_buffer(filtered_rows);
    Ok(result)
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
    // Use pooled buffer to reduce allocation overhead
    let mut filtered_rows = executor.query_buffer_pool().get_row_buffer(rows.len());
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
            vibesql_types::SqlValue::Float(0.0) => false,
            vibesql_types::SqlValue::Float(_) => true,
            vibesql_types::SqlValue::Real(0.0) => false,
            vibesql_types::SqlValue::Real(_) => true,
            vibesql_types::SqlValue::Double(0.0) => false,
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

    // Move data to final result and return pooled buffer
    // This allows buffer reuse while avoiding clone overhead
    let result = std::mem::take(&mut filtered_rows);
    executor.query_buffer_pool().return_row_buffer(filtered_rows);
    Ok(result)
}


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

    // Check if we should parallelize based on hardware-aware heuristics
    let config = ParallelConfig::global();
    if !config.should_parallelize_scan(rows.len()) {
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
                vibesql_types::SqlValue::Float(0.0) => false,
                vibesql_types::SqlValue::Float(_) => true,
                vibesql_types::SqlValue::Real(0.0) => false,
                vibesql_types::SqlValue::Real(_) => true,
                vibesql_types::SqlValue::Double(0.0) => false,
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

/// Auto-selecting WHERE filter that uses hardware-aware heuristics
/// to choose between sequential and parallel execution.
///
/// The decision is based on:
/// - Number of CPU cores available
/// - Row count
/// - Operation type (scan/filter)
/// - User override via PARALLEL_THRESHOLD environment variable
pub(super) fn apply_where_filter_combined_auto<'a>(
    rows: Vec<vibesql_storage::Row>,
    where_expr: Option<&vibesql_ast::Expression>,
    evaluator: &CombinedExpressionEvaluator,
    executor: &crate::SelectExecutor<'a>,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    // The parallel version now contains the heuristics and will
    // automatically fall back to sequential if needed
    apply_where_filter_combined_parallel(rows, where_expr, evaluator, executor)
}
