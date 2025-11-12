//! Non-aggregation execution methods for SelectExecutor

use super::{
    builder::SelectExecutor,
    index_optimization::{try_index_based_ordering, try_index_based_where_filtering, try_spatial_index_optimization},
};
use crate::{
    errors::ExecutorError,
    evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator},
    optimizer::optimize_where_clause,
    select::{
        filter::apply_where_filter_combined_auto,
        helpers::{apply_distinct, apply_limit_offset},
        iterator::{FilterIterator, RowIterator, TableScanIterator},
        join::FromResult,
        order::{apply_order_by, RowWithSortKeys},
        projection::{project_row_combined, SelectProjectionIterator},
        window::{
            collect_order_by_window_functions, evaluate_order_by_window_functions,
            evaluate_window_functions, expression_has_window_function, has_window_functions,
        },
    },
};

impl SelectExecutor<'_> {
    /// Determine if we can use iterator-based execution for this query
    ///
    /// Iterator execution is beneficial for queries that don't require full materialization.
    /// We must materialize for: ORDER BY, DISTINCT, and window functions.
    fn can_use_iterator_execution(stmt: &vibesql_ast::SelectStmt) -> bool {
        // Can't use iterators if we have ORDER BY (requires sorting all rows)
        if stmt.order_by.is_some() {
            return false;
        }

        // Can't use iterators if we have DISTINCT (requires deduplication of all rows)
        if stmt.distinct {
            return false;
        }

        // Can't use iterators if we have window functions (requires full window frames)
        if has_window_functions(&stmt.select_list) {
            return false;
        }

        // Can't use iterators if ORDER BY has window functions
        if let Some(order_by) = &stmt.order_by {
            if order_by.iter().any(|item| expression_has_window_function(&item.expr)) {
                return false;
            }
        }

        // All checks passed - we can use iterator execution!
        true
    }

    /// Execute SELECT using iterator-based execution (for simple queries)
    ///
    /// This method uses lazy iteration to avoid materializing intermediate results.
    /// The pipeline: scan → filter → skip → take → collect → project
    /// WHERE filtering, OFFSET, and LIMIT are fully lazy, providing memory efficiency
    /// and early termination. Projection happens after materialization due to its
    /// complexity (wildcard expansion, expression evaluation, etc.).
    fn execute_with_iterators(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        from_result: FromResult,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        let schema = from_result.schema.clone();
        let rows = from_result.into_rows();

        // Create evaluator for WHERE clause
        let evaluator = if let (Some(outer_row), Some(outer_schema)) = (self._outer_row, self._outer_schema) {
            CombinedExpressionEvaluator::with_database_and_outer_context(
                &schema,
                self.database,
                outer_row,
                outer_schema,
            )
        } else {
            CombinedExpressionEvaluator::with_database(&schema, self.database)
        };

        // Stage 1: Table scan
        let mut iterator: Box<dyn RowIterator> = Box::new(TableScanIterator::new(schema.clone(), rows));

        // Stage 2: WHERE filter (if present)
        if let Some(where_expr) = &stmt.where_clause {
            // Optimize WHERE clause
            let where_optimization = optimize_where_clause(Some(where_expr), &evaluator)?;

            match where_optimization {
                crate::optimizer::WhereOptimization::AlwaysFalse => {
                    // WHERE FALSE - return empty result immediately
                    return Ok(Vec::new());
                }
                crate::optimizer::WhereOptimization::AlwaysTrue => {
                    // WHERE TRUE - no filtering needed, keep current iterator
                }
                crate::optimizer::WhereOptimization::Optimized(expr) => {
                    // Apply optimized WHERE clause - use the evaluator that has outer context if present
                    iterator = Box::new(FilterIterator::new(iterator, expr, evaluator.clone_for_new_expression()));
                }
                crate::optimizer::WhereOptimization::Unchanged(Some(expr)) => {
                    // Apply original WHERE clause - use the evaluator that has outer context if present
                    iterator = Box::new(FilterIterator::new(iterator, expr.clone(), evaluator.clone_for_new_expression()));
                }
                crate::optimizer::WhereOptimization::Unchanged(None) => {
                    // No WHERE clause - keep current iterator
                }
            }
        }

        // Stage 3: OFFSET (skip rows lazily)
        let mut iterator: Box<dyn Iterator<Item = _>> = if let Some(offset) = stmt.offset {
            let offset_usize = offset.max(0);
            Box::new(iterator.skip(offset_usize))
        } else {
            iterator
        };

        // Stage 4: LIMIT (take only needed rows)
        if let Some(limit) = stmt.limit {
            iterator = Box::new(iterator.take(limit));
        }

        // Stage 5: Materialize filtered results
        let mut filtered_rows = Vec::new();
        for row_result in iterator {
            // Check timeout during iteration
            self.check_timeout()?;
            filtered_rows.push(row_result?);
        }

        // Stage 5.5: Apply implicit ordering for deterministic results
        // Queries without explicit ORDER BY get sorted by all columns in schema order
        // This ensures SQLLogicTest compatibility and deterministic behavior
        if stmt.order_by.is_none() && !filtered_rows.is_empty() {
            use crate::select::grouping::compare_sql_values;

            filtered_rows.sort_by(|row_a, row_b| {
                // Compare column by column until we find a difference
                for i in 0..row_a.values.len().min(row_b.values.len()) {
                    let cmp = compare_sql_values(&row_a.values[i], &row_b.values[i]);
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Stage 6: Project columns (handles wildcards, expressions, etc.)
        let mut final_rows = Vec::new();
        for row in filtered_rows {
            // Clear CSE cache before projecting each row
            evaluator.clear_cse_cache();

            let projected_row = project_row_combined(
                &row,
                &stmt.select_list,
                &evaluator,
                &schema,
                &None, // No window functions in iterator path
            )?;

            final_rows.push(projected_row);
        }

        Ok(final_rows)
    }

    /// Execute SELECT without aggregation
    pub(super) fn execute_without_aggregation(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        from_result: FromResult,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Phase D: Use iterator-based execution for simple queries
        // This provides memory efficiency and early termination for LIMIT queries
        if Self::can_use_iterator_execution(stmt) {
            return self.execute_with_iterators(stmt, from_result);
        }

        // Fall back to materialized execution for complex queries
        // (ORDER BY, DISTINCT, window functions require full materialization)
        let schema = from_result.schema.clone();
        let rows = from_result.into_rows();

        // Track memory used by FROM clause results (JOINs, table scans, etc.)
        let from_memory_bytes = std::mem::size_of::<vibesql_storage::Row>() * rows.len()
            + rows.iter().map(|r| std::mem::size_of_val(r.values.as_slice())).sum::<usize>();
        self.track_memory_allocation(from_memory_bytes)?;

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
        // 1. Try spatial index optimization (for ST_Contains, ST_Intersects, etc.)
        let mut filtered_rows = if let Some(spatial_filtered) = try_spatial_index_optimization(
            self.database,
            stmt.where_clause.as_ref(),
            &rows,
            &schema,
        )? {
            spatial_filtered
        } else if let Some(index_filtered) = try_index_based_where_filtering(
            self.database,
            stmt.where_clause.as_ref(),
            &rows,
            &schema,
        )? {
            // 2. Try B-tree index optimization (for =, <, >, BETWEEN, IN, etc.)
            index_filtered
        } else {
            // 3. Fall back to full WHERE clause evaluation
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
                    // Apply optimized WHERE clause (uses parallel if enabled)
                    apply_where_filter_combined_auto(rows, Some(expr), &evaluator, self)?
                }
                crate::optimizer::WhereOptimization::Unchanged(where_expr) => {
                    // Apply original WHERE clause (uses parallel if enabled)
                    apply_where_filter_combined_auto(rows, where_expr.as_ref(), &evaluator, self)?
                }
            }
        };

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
            if let Some(ordered_rows) = try_index_based_ordering(
                self.database,
                &result_rows,
                order_by,
                &schema,
                &stmt.from,
                &stmt.select_list,
            )? {
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
        } else if !result_rows.is_empty() {
            // No explicit ORDER BY - apply implicit ordering for deterministic results
            // This ensures SQLLogicTest compatibility and deterministic behavior
            use crate::select::grouping::compare_sql_values;

            result_rows.sort_by(|(row_a, _), (row_b, _)| {
                // Compare column by column until we find a difference
                for i in 0..row_a.values.len().min(row_b.values.len()) {
                    let cmp = compare_sql_values(&row_a.values[i], &row_b.values[i]);
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Choose projection strategy based on DISTINCT and set operations
        // - If DISTINCT is present, we must project all rows first, then deduplicate
        // - If no DISTINCT, we can apply LIMIT/OFFSET before projection for better performance
        let final_rows = if stmt.distinct || stmt.set_operation.is_some() {
            // Eager projection: project all rows, then apply DISTINCT and/or LIMIT/OFFSET
            let mut projected_rows = Vec::new();
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
                let row_memory = std::mem::size_of::<vibesql_storage::Row>()
                    + std::mem::size_of_val(projected_row.values.as_slice());
                self.track_memory_allocation(row_memory)?;

                projected_rows.push(projected_row);
            }

            // Apply DISTINCT if specified
            let projected_rows = if stmt.distinct {
                apply_distinct(projected_rows)
            } else {
                projected_rows
            };

            // Don't apply LIMIT/OFFSET if we have a set operation - it will be applied later
            if stmt.set_operation.is_some() {
                projected_rows
            } else {
                apply_limit_offset(projected_rows, stmt.limit, stmt.offset)
            }
        } else {
            // Lazy projection: apply LIMIT/OFFSET first, then project only needed rows
            // This is more efficient when LIMIT is small and projection is expensive

            // Extract rows from RowWithSortKeys (discard sort keys)
            let rows: Vec<vibesql_storage::Row> = result_rows.into_iter().map(|(row, _)| row).collect();

            // Apply LIMIT/OFFSET to reduce rows before projection
            let limited_rows = apply_limit_offset(rows, stmt.limit, stmt.offset);

            // Create iterator for lazy projection
            let projection_iter = SelectProjectionIterator::new(
                limited_rows.into_iter().map(Ok),
                stmt.select_list.clone(),
                evaluator,
                schema.clone(),
                window_mapping.clone(),
            );

            // Collect projected rows with memory tracking
            let mut final_rows = Vec::new();
            for projected_result in projection_iter {
                // Check timeout during projection
                self.check_timeout()?;

                let projected_row = projected_result?;

                // Track memory for each projected row
                let row_memory = std::mem::size_of::<vibesql_storage::Row>()
                    + std::mem::size_of_val(projected_row.values.as_slice());
                self.track_memory_allocation(row_memory)?;

                final_rows.push(projected_row);
            }

            final_rows
        };

        Ok(final_rows)
    }

    /// Execute SELECT without FROM clause
    ///
    /// Evaluates expressions in the SELECT list without any table context.
    /// Returns a single row with the evaluated expressions.
    pub(super) fn execute_select_without_from(
        &self,
        stmt: &vibesql_ast::SelectStmt,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Create an empty schema (no table context)
        let empty_schema = vibesql_catalog::TableSchema::new("".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::with_database(&empty_schema, self.database);

        // Create an empty row (no data to reference)
        let empty_row = vibesql_storage::Row::new(vec![]);

        // Evaluate each item in the SELECT list
        let mut values = Vec::new();
        for item in &stmt.select_list {
            match item {
                vibesql_ast::SelectItem::Wildcard { .. } | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * and qualified wildcards require FROM clause".to_string(),
                    ));
                }
                vibesql_ast::SelectItem::Expression { expr, .. } => {
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
        Ok(vec![vibesql_storage::Row::new(values)])
    }
}
