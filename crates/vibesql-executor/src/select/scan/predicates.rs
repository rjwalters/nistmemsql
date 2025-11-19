//! Predicate pushdown and filtering logic
//!
//! Handles optimization of WHERE clause predicates by:
//! - Applying pre-decomposed table-local predicates early during table scans
//! - Reducing intermediate result sizes before joins
//!
//! **Phase 1 Optimization**: This module now uses `PredicatePlan` to avoid redundant
//! WHERE clause decomposition. The plan is computed once at query start and passed through.

use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator,
    optimizer::PredicatePlan, schema::CombinedSchema,
};

#[cfg(feature = "parallel")]
use crate::select::parallel::ParallelConfig;

#[cfg(feature = "parallel")]
use rayon::prelude::*;

#[cfg(feature = "parallel")]
use std::sync::Arc;

/// Apply table-local predicates from a pre-computed predicate plan
///
/// This function implements predicate pushdown by filtering rows early,
/// before they contribute to larger Cartesian products in JOINs.
///
/// Uses parallel filtering when beneficial based on row count and hardware.
///
/// **Phase 1**: Now accepts `PredicatePlan` instead of decomposing WHERE clause internally.
/// **Phase 4**: Uses cost-based predicate ordering via selectivity estimation.
pub(crate) fn apply_table_local_predicates(
    rows: Vec<vibesql_storage::Row>,
    schema: CombinedSchema,
    predicate_plan: &PredicatePlan,
    table_name: &str,
    database: &vibesql_storage::Database,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    // Get table statistics for selectivity-based ordering
    let table_stats = database
        .get_table(table_name)
        .and_then(|table| table.get_statistics());

    // Get predicates ordered by selectivity (most selective first)
    // Falls back to parse order if statistics unavailable
    let ordered_preds = predicate_plan.get_table_filters_ordered(table_name, table_stats);

    // If there are table-local predicates, apply them
    if !ordered_preds.is_empty() {
        // Combine ordered predicates with AND
        let combined_where = combine_predicates_with_and(ordered_preds);

        // Create evaluator for filtering
        let evaluator = CombinedExpressionEvaluator::with_database(&schema, database);

        // Check if we should use parallel filtering
        #[cfg(feature = "parallel")]
        {
            let config = ParallelConfig::global();
            if config.should_parallelize_scan(rows.len()) {
                return apply_predicates_parallel(rows, combined_where, evaluator);
            }
        }

        // Sequential path for small datasets
        let mut filtered_rows = Vec::new();
        for row in rows {
            evaluator.clear_cse_cache();

            let include_row = match evaluator.eval(&combined_where, &row)? {
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
        return Ok(filtered_rows);
    }

    // No table-local predicates - return rows as-is
    Ok(rows)
}

/// Apply predicates using parallel execution
#[cfg(feature = "parallel")]
fn apply_predicates_parallel(
    rows: Vec<vibesql_storage::Row>,
    combined_where: vibesql_ast::Expression,
    evaluator: CombinedExpressionEvaluator,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    // Clone expression for thread-safe sharing
    let where_expr_arc = Arc::new(combined_where);

    // Extract evaluator components for parallel execution
    let (schema, database, outer_row, outer_schema, window_mapping, enable_cse) =
        evaluator.get_parallel_components();

    // Use rayon's parallel iterator for filtering
    let result: Result<Vec<_>, ExecutorError> = rows
        .into_par_iter()
        .map(|row| {
            // Create thread-local evaluator with independent caches
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

/// Helper function to combine predicates with AND operator
pub(crate) fn combine_predicates_with_and(mut predicates: Vec<vibesql_ast::Expression>) -> vibesql_ast::Expression {
    if predicates.is_empty() {
        // This shouldn't happen, but default to TRUE
        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true))
    } else if predicates.len() == 1 {
        predicates.pop().unwrap()
    } else {
        let mut result = predicates.remove(0);
        for predicate in predicates {
            result = vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::And,
                left: Box::new(result),
                right: Box::new(predicate),
            };
        }
        result
    }
}
