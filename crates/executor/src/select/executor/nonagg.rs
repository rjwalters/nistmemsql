//! Non-aggregation execution methods for SelectExecutor

use super::builder::SelectExecutor;
use super::index_optimization::{try_index_based_where_filtering, try_index_based_ordering};
use crate::errors::ExecutorError;
use crate::evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator};
use crate::optimizer::{optimize_where_clause, decompose_where_clause};
use crate::select::filter::apply_where_filter_combined;
use crate::select::helpers::{apply_distinct, apply_limit_offset};
use crate::select::join::FromResult;
use crate::select::order::{apply_order_by, RowWithSortKeys};
use crate::select::projection::project_row_combined;
use crate::select::window::{
    collect_order_by_window_functions, evaluate_order_by_window_functions,
    evaluate_window_functions, expression_has_window_function, has_window_functions,
};

impl SelectExecutor<'_> {
    /// Execute SELECT without aggregation
    pub(super) fn execute_without_aggregation(
        &self,
        stmt: &ast::SelectStmt,
        from_result: FromResult,
    ) -> Result<Vec<storage::Row>, ExecutorError> {
        let FromResult { schema, rows } = from_result;

        // Track memory used by FROM clause results (JOINs, table scans, etc.)
        let from_memory_bytes = std::mem::size_of::<storage::Row>() * rows.len()
            + rows.iter().map(|r| std::mem::size_of_val(r.values.as_slice())).sum::<usize>();
        self.track_memory_allocation(from_memory_bytes)?;

        // Decompose WHERE clause for predicate pushdown optimization
        // This analyzes the WHERE clause to identify:
        // - Table-local predicates (can be pushed to table scans)
        // - Equijoin conditions (can be optimized in join operators)
        // - Complex predicates (deferred to post-join filtering)
        let _predicate_decomposition = if let Some(where_expr) = &stmt.where_clause {
            match decompose_where_clause(Some(where_expr), &schema) {
                Ok(decomp) => Some(decomp),
                Err(_) => {
                    // If decomposition fails, continue with standard WHERE processing
                    None
                }
            }
        } else {
            None
        };

        // Create evaluator with outer context if available (outer schema is already a
        // CombinedSchema)
        let evaluator =
            if let (Some(outer_row), Some(outer_schema)) = (self._outer_row, self._outer_schema) {
                CombinedExpressionEvaluator::with_database_and_outer_context(
                    &schema,
                    self.database,
                    outer_row,
                    outer_schema,
                )
            } else {
                CombinedExpressionEvaluator::with_database(&schema, self.database)
            };

        // Try index-based WHERE optimization first
        let mut filtered_rows = if let Some(index_filtered) =
            try_index_based_where_filtering(self.database, stmt.where_clause.as_ref(), &rows, &schema)?
        {
            index_filtered
        } else {
            // Fall back to full WHERE clause evaluation
            // Note: Table-local predicates have already been pushed down and applied during table
            // scan. However, we still apply the full WHERE clause here for correctness
            // with JOINs and complex predicates that can't be pushed down.
            // The table-local predicates being applied twice is safe (same result) but not optimal.
            // TODO: In Phase 3, extract and remove table-local predicates here to avoid double
            // filtering
            let where_optimization = optimize_where_clause(stmt.where_clause.as_ref(), &evaluator)?;

            match where_optimization {
                crate::optimizer::WhereOptimization::AlwaysTrue => {
                    // WHERE TRUE - no filtering needed
                    rows
                }
                crate::optimizer::WhereOptimization::AlwaysFalse => {
                    // WHERE FALSE - return empty result
                    Vec::new()
                }
                crate::optimizer::WhereOptimization::Optimized(ref expr) => {
                    // Apply optimized WHERE clause
                    apply_where_filter_combined(rows, Some(expr), &evaluator, self)?
                }
                crate::optimizer::WhereOptimization::Unchanged(where_expr) => {
                    // Apply original WHERE clause
                    apply_where_filter_combined(rows, where_expr.as_ref(), &evaluator, self)?
                }
            }
        };

        // Post-FROM predicate optimization: apply table-local predicates
        // This filters rows right after FROM execution but before other operations
        // Note: Ideally these would be applied during table scan for better performance,
        // but applying them here is simpler and still provides memory benefit for joins
        if let Some(pred_decomp) = &_predicate_decomposition {
            if !pred_decomp.table_local_predicates.is_empty() {
                // Apply each table's local predicates
                // TODO: Implement filtering using table_local_predicates
                // For now, this is a placeholder for the structure
            }
        }

        // Check if SELECT list has window functions
        let has_select_windows = has_window_functions(&stmt.select_list);

        // Check if ORDER BY has window functions
        let has_order_by_windows = stmt
            .order_by
            .as_ref()
            .map(|order_by| order_by.iter().any(|item| expression_has_window_function(&item.expr)))
            .unwrap_or(false);

        // If there are window functions, evaluate them first
        // Window functions operate on the filtered result set
        let mut window_mapping = if has_select_windows {
            let (rows_with_windows, mapping) =
                evaluate_window_functions(filtered_rows, &stmt.select_list, &evaluator)?;
            filtered_rows = rows_with_windows;
            Some(mapping)
        } else {
            None
        };

        // If ORDER BY has window functions, evaluate those too
        if has_order_by_windows {
            let order_by_window_functions =
                collect_order_by_window_functions(stmt.order_by.as_ref().unwrap());
            if !order_by_window_functions.is_empty() {
                let (rows_with_order_by_windows, order_by_mapping) =
                    evaluate_order_by_window_functions(
                        filtered_rows,
                        order_by_window_functions,
                        &evaluator,
                        window_mapping.as_ref(),
                    )?;
                filtered_rows = rows_with_order_by_windows;

                // Merge mappings
                if let Some(ref mut existing_mapping) = window_mapping {
                    existing_mapping.extend(order_by_mapping);
                } else {
                    window_mapping = Some(order_by_mapping);
                }
            }
        }

        // Convert to RowWithSortKeys format
        let mut result_rows: Vec<RowWithSortKeys> =
            filtered_rows.into_iter().map(|row| (row, None)).collect();

        // Apply ORDER BY sorting if present
        if let Some(order_by) = &stmt.order_by {
            // Try to use index for ordering first
            if let Some(ordered_rows) =
                try_index_based_ordering(self.database, &result_rows, order_by, &schema, &stmt.from)?
            {
                result_rows = ordered_rows;
            } else {
                // Fall back to sorting
                // Create evaluator with window mapping for ORDER BY (if window functions are
                // present)
                let order_by_evaluator = if let Some(ref mapping) = window_mapping {
                    CombinedExpressionEvaluator::with_database_and_windows(
                        &schema,
                        self.database,
                        mapping,
                    )
                } else {
                    CombinedExpressionEvaluator::with_database(&schema, self.database)
                };
                result_rows =
                    apply_order_by(result_rows, order_by, &order_by_evaluator, &stmt.select_list)?;
            }
        }

        // Project columns from the sorted rows
        let mut final_rows = Vec::new();
        for (row, _) in result_rows {
            // Check timeout during projection
            self.check_timeout()?;

            // Clear CSE cache before projecting each row to prevent column values
            // from being incorrectly cached across different rows
            evaluator.clear_cse_cache();

            let projected_row = project_row_combined(
                &row,
                &stmt.select_list,
                &evaluator,
                &schema,
                &window_mapping,
            )?;

            // Track memory for each projected row
            let row_memory = std::mem::size_of::<storage::Row>()
                + std::mem::size_of_val(projected_row.values.as_slice());
            self.track_memory_allocation(row_memory)?;

            final_rows.push(projected_row);
        }

        // Apply DISTINCT if specified
        let final_rows = if stmt.distinct { apply_distinct(final_rows) } else { final_rows };

        // Don't apply LIMIT/OFFSET if we have a set operation - it will be applied later
        if stmt.set_operation.is_some() {
            Ok(final_rows)
        } else {
            Ok(apply_limit_offset(final_rows, stmt.limit, stmt.offset))
        }
    }

    /// Execute SELECT without FROM clause
    ///
    /// Evaluates expressions in the SELECT list without any table context.
    /// Returns a single row with the evaluated expressions.
    pub(super) fn execute_select_without_from(
        &self,
        stmt: &ast::SelectStmt,
    ) -> Result<Vec<storage::Row>, ExecutorError> {
        // Create an empty schema (no table context)
        let empty_schema = catalog::TableSchema::new("".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&empty_schema);

        // Create an empty row (no data to reference)
        let empty_row = storage::Row::new(vec![]);

        // Evaluate each item in the SELECT list
        let mut values = Vec::new();
        for item in &stmt.select_list {
            match item {
                ast::SelectItem::Wildcard { .. } | ast::SelectItem::QualifiedWildcard { .. } => {
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * and qualified wildcards require FROM clause".to_string(),
                    ));
                }
                ast::SelectItem::Expression { expr, .. } => {
                    // Check if expression references a column
                    if self.expression_references_column(expr) {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Column reference requires FROM clause".to_string(),
                        ));
                    }

                    // Evaluate the expression
                    let value = evaluator.eval(expr, &empty_row)?;
                    values.push(value);
                }
            }
        }

        // Return a single row with the evaluated values
        Ok(vec![storage::Row::new(values)])
    }

}
