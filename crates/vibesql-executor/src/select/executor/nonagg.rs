//! Non-aggregation execution methods for SelectExecutor

use super::{
    builder::SelectExecutor,
};
#[cfg(feature = "spatial")]
use super::index_optimization::try_spatial_index_optimization;
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
        vectorized::{
            filter_record_batch_simd, record_batch_to_rows, rows_to_record_batch,
            VECTORIZE_THRESHOLD,
        },
        window::{
            collect_order_by_window_functions, evaluate_order_by_window_functions,
            evaluate_window_functions, expression_has_window_function, has_window_functions,
        },
    },
};

/// Validate IN subqueries in WHERE clause before row iteration
/// This ensures schema validation happens even when there are no rows to process
fn validate_where_clause_subqueries(
    expr: &vibesql_ast::Expression,
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    use vibesql_ast::Expression;

    match expr {
        Expression::In { subquery, .. } => {
            // Validate that the subquery returns exactly 1 column (scalar subquery requirement)
            let column_count = compute_select_list_column_count(subquery, database)?;
            if column_count != 1 {
                return Err(ExecutorError::SubqueryColumnCountMismatch {
                    expected: 1,
                    actual: column_count,
                });
            }
            Ok(())
        }
        // Recurse into binary operations
        Expression::BinaryOp { left, right, .. } => {
            validate_where_clause_subqueries(left, database)?;
            validate_where_clause_subqueries(right, database)
        }
        // Recurse into unary operations
        Expression::UnaryOp { expr, .. } => validate_where_clause_subqueries(expr, database),
        // Recurse into other composite expressions
        Expression::IsNull { expr, .. } => validate_where_clause_subqueries(expr, database),
        Expression::InList { expr, values, .. } => {
            validate_where_clause_subqueries(expr, database)?;
            for val in values {
                validate_where_clause_subqueries(val, database)?;
            }
            Ok(())
        }
        Expression::Between { expr, low, high, .. } => {
            validate_where_clause_subqueries(expr, database)?;
            validate_where_clause_subqueries(low, database)?;
            validate_where_clause_subqueries(high, database)
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                validate_where_clause_subqueries(op, database)?;
            }
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    validate_where_clause_subqueries(cond, database)?;
                }
                validate_where_clause_subqueries(&when_clause.result, database)?;
            }
            if let Some(else_res) = else_result {
                validate_where_clause_subqueries(else_res, database)?;
            }
            Ok(())
        }
        // For all other expressions, no validation needed
        _ => Ok(()),
    }
}

/// Compute the number of columns in a SELECT statement's result
/// Handles wildcards by expanding them using table schemas from the database
fn compute_select_list_column_count(
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
) -> Result<usize, ExecutorError> {
    let mut count = 0;

    for item in &stmt.select_list {
        match item {
            vibesql_ast::SelectItem::Wildcard { .. } => {
                // Expand * to count all columns from all tables in FROM clause
                if let Some(from) = &stmt.from {
                    count += count_columns_in_from_clause(from, database)?;
                } else {
                    // SELECT * without FROM is an error (should be caught earlier)
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * requires FROM clause".to_string(),
                    ));
                }
            }
            vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                // Expand table.* to count columns from that specific table
                let tbl = database
                    .get_table(qualifier)
                    .ok_or_else(|| ExecutorError::TableNotFound(qualifier.clone()))?;
                count += tbl.schema.columns.len();
            }
            vibesql_ast::SelectItem::Expression { .. } => {
                // Each expression contributes one column
                count += 1;
            }
        }
    }

    Ok(count)
}

/// Count total columns in a FROM clause (handles joins and multiple tables)
fn count_columns_in_from_clause(
    from: &vibesql_ast::FromClause,
    database: &vibesql_storage::Database,
) -> Result<usize, ExecutorError> {
    match from {
        vibesql_ast::FromClause::Table { name, .. } => {
            let table = database
                .get_table(name)
                .ok_or_else(|| ExecutorError::TableNotFound(name.clone()))?;
            Ok(table.schema.columns.len())
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            let left_count = count_columns_in_from_clause(left, database)?;
            let right_count = count_columns_in_from_clause(right, database)?;
            Ok(left_count + right_count)
        }
        vibesql_ast::FromClause::Subquery { .. } => {
            // For subqueries in FROM, we'd need to execute them to know column count
            // This is complex, so for now we'll return an error
            // In practice, this case is rare in IN subqueries
            Err(ExecutorError::UnsupportedFeature(
                "Subqueries in FROM clause within IN predicates are not yet supported for schema validation".to_string(),
            ))
        }
    }
}

/// Try to apply SIMD filtering to rows using Apache Arrow
///
/// Returns (rows, used_simd) where:
/// - rows: The filtered rows (or original rows if SIMD not applicable)
/// - used_simd: true if SIMD was used, false if fallback to row-based is needed
///
/// SIMD path is used when:
/// - Row count >= VECTORIZE_THRESHOLD (100 rows)
/// - WHERE clause is a simple predicate (not too complex)
/// - All column types are supported by Arrow (Int64, Float64, Utf8, Boolean)
fn try_simd_filter(
    rows: Vec<vibesql_storage::Row>,
    where_expr: &vibesql_ast::Expression,
    schema: &crate::schema::CombinedSchema,
) -> Result<(Vec<vibesql_storage::Row>, bool), ExecutorError> {
    // Only use SIMD for datasets >= threshold
    if rows.len() < VECTORIZE_THRESHOLD {
        return Ok((rows, false));
    }

    // Extract column names from combined schema (in order)
    let mut column_names = vec![String::new(); schema.total_columns];
    for (start_idx, table_schema) in schema.table_schemas.values() {
        for (col_idx, col) in table_schema.columns.iter().enumerate() {
            column_names[start_idx + col_idx] = col.name.clone();
        }
    }

    // Try to convert rows to RecordBatch
    // If this fails (e.g., unsupported types), fall back to row-based filtering
    let batch = match rows_to_record_batch(&rows, &column_names) {
        Ok(batch) => batch,
        Err(_) => {
            // Conversion failed (unsupported types, etc.) - fall back to row-based
            return Ok((rows, false));
        }
    };

    // Apply SIMD filter
    // If this fails (e.g., complex predicates), fall back to row-based filtering
    let filtered_batch = match filter_record_batch_simd(&batch, where_expr) {
        Ok(filtered) => filtered,
        Err(_) => {
            // SIMD filter failed (complex predicate, etc.) - fall back to row-based
            return Ok((rows, false));
        }
    };

    // Convert filtered RecordBatch back to rows
    let filtered_rows = record_batch_to_rows(&filtered_batch)?;

    Ok((filtered_rows, true))
}

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
        let sorted_by = from_result.sorted_by.clone();
        let rows = from_result.into_rows();

        // Create evaluator for WHERE clause
        // Priority: 1) outer context (for subqueries) 2) procedural context 3) just database
        let evaluator = if let (Some(outer_row), Some(outer_schema)) = (self._outer_row, self._outer_schema) {
            CombinedExpressionEvaluator::with_database_and_outer_context(
                &schema,
                self.database,
                outer_row,
                outer_schema,
            )
        } else if let Some(proc_ctx) = self.procedural_context {
            CombinedExpressionEvaluator::with_database_and_procedural_context(
                &schema,
                self.database,
                proc_ctx,
            )
        } else {
            CombinedExpressionEvaluator::with_database(&schema, self.database)
        };

        // Validate WHERE clause subqueries upfront (before row iteration)
        // This ensures schema validation happens even for empty result sets
        if let Some(where_expr) = &stmt.where_clause {
            validate_where_clause_subqueries(where_expr, self.database)?;
        }

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
        // Use pooled buffer to reduce allocation overhead
        let mut filtered_rows = self.database.query_buffer_pool().get_row_buffer(128);
        for row_result in iterator {
            // Check timeout during iteration
            self.check_timeout()?;
            filtered_rows.push(row_result?);
        }

        // Stage 5.5: Apply implicit ordering for deterministic results
        // Queries without explicit ORDER BY get sorted by all columns in schema order
        // This ensures SQLLogicTest compatibility and deterministic behavior
        // Skip sorting if data is already sorted from index scan
        let needs_implicit_sort = stmt.order_by.is_none() && sorted_by.is_none() && !filtered_rows.is_empty();

        if needs_implicit_sort {
            use crate::select::grouping::compare_sql_values;

            #[cfg(feature = "parallel")]
            {
                use crate::select::parallel::ParallelConfig;
                use rayon::prelude::*;

                // Use parallel sorting for larger datasets
                let should_parallel = ParallelConfig::global().should_parallelize_sort(filtered_rows.len());

                if should_parallel {
                    filtered_rows.par_sort_by(|row_a, row_b| {
                        // Compare column by column until we find a difference
                        for i in 0..row_a.values.len().min(row_b.values.len()) {
                            let cmp = compare_sql_values(&row_a.values[i], &row_b.values[i]);
                            if cmp != std::cmp::Ordering::Equal {
                                return cmp;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                } else {
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
            }

            #[cfg(not(feature = "parallel"))]
            {
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
        }

        // Stage 6: Project columns (handles wildcards, expressions, etc.)
        // Use pooled buffer to reduce allocation overhead
        let mut final_rows = self.database.query_buffer_pool().get_row_buffer(filtered_rows.len());
        for row in &filtered_rows {
            // Clear CSE cache before projecting each row
            evaluator.clear_cse_cache();

            let projected_row = project_row_combined(
                row,
                &stmt.select_list,
                &evaluator,
                &schema,
                &None, // No window functions in iterator path
                self.database.query_buffer_pool(),
            )?;

            final_rows.push(projected_row);
        }

        // Clear CSE cache at end of query to prevent cross-query pollution
        // Cache can persist within a single query for performance, but must be
        // cleared between different SQL statements to avoid stale values
        evaluator.clear_cse_cache();

        // Return intermediate buffer to pool, then return final result
        self.database.query_buffer_pool().return_row_buffer(filtered_rows);
        let result = std::mem::take(&mut final_rows);
        self.database.query_buffer_pool().return_row_buffer(final_rows);
        Ok(result)
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
        let sorted_by = from_result.sorted_by.clone();
        let where_filtered = from_result.where_filtered;
        let rows = from_result.into_rows();

        // Track memory used by FROM clause results (JOINs, table scans, etc.)
        let from_memory_bytes = std::mem::size_of::<vibesql_storage::Row>() * rows.len()
            + rows.iter().map(|r| std::mem::size_of_val(r.values.as_slice())).sum::<usize>();
        self.track_memory_allocation(from_memory_bytes)?;

        // Create evaluator with procedural context support
        // Priority: 1) outer context (for subqueries) 2) procedural context 3) just database
        let evaluator =
            if let (Some(outer_row), Some(outer_schema)) = (self._outer_row, self._outer_schema) {
                CombinedExpressionEvaluator::with_database_and_outer_context(
                    &schema,
                    self.database,
                    outer_row,
                    outer_schema,
                )
            } else if let Some(proc_ctx) = self.procedural_context {
                CombinedExpressionEvaluator::with_database_and_procedural_context(
                    &schema,
                    self.database,
                    proc_ctx,
                )
            } else {
                CombinedExpressionEvaluator::with_database(&schema, self.database)
            };

        // Skip WHERE filtering if already applied during scan (e.g., by index scan)
        // This prevents redundant evaluation that can cause incorrect results
        //
        // NOTE: Index optimization has been moved to the scan level (execute_index_scan).
        // The FROM clause now handles all index-based optimizations (B-tree, spatial, IN clauses)
        // before returning rows, avoiding the row-index mismatch problem when predicate pushdown
        // is enabled. This fixes issues #1807, #1895, #1896, and #1902.
        //
        // Previously, we tried to apply index optimization here on `rows` from FROM, but:
        // 1. If predicate pushdown filtered rows, indices no longer match original table
        // 2. This caused incorrect results and crashes
        // 3. execute_index_scan() already handles this correctly at scan level
        //
        // Now we only do spatial optimization here as a special case, since it may not
        // always be pushed to scan level. All other index optimizations happen in FROM.
        let mut filtered_rows = if where_filtered {
            rows
        } else {
            // Try spatial index optimization if feature is enabled
            #[cfg(feature = "spatial")]
            {
                if let Some(spatial_filtered) = try_spatial_index_optimization(
                    self.database,
                    stmt.where_clause.as_ref(),
                    &rows,
                    &schema,
                )? {
                    // Spatial index optimization (ST_Contains, ST_Intersects, etc.)
                    spatial_filtered
                } else {
                    // Fall back to full WHERE clause evaluation
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
                            // Try SIMD filtering first (for large datasets)
                            let (filtered_rows, used_simd) = try_simd_filter(rows, expr, &schema)?;
                            if used_simd {
                                // SIMD path succeeded!
                                filtered_rows
                            } else {
                                // Fall back to row-based filtering (small dataset or unsupported predicate)
                                apply_where_filter_combined_auto(filtered_rows, Some(expr), &evaluator, self)?
                            }
                        }
                        crate::optimizer::WhereOptimization::Unchanged(where_expr) => {
                            // Try SIMD filtering first (for large datasets)
                            if let Some(ref expr) = where_expr {
                                let (filtered_rows, used_simd) = try_simd_filter(rows, expr, &schema)?;
                                if used_simd {
                                    // SIMD path succeeded!
                                    filtered_rows
                                } else {
                                    // Fall back to row-based filtering
                                    apply_where_filter_combined_auto(filtered_rows, where_expr.as_ref(), &evaluator, self)?
                                }
                            } else {
                                // No WHERE clause
                                rows
                            }
                        }
                    }
                }
            }

            #[cfg(not(feature = "spatial"))]
            {
                // Fall back to full WHERE clause evaluation
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
                        // Try SIMD filtering first (for large datasets)
                        let (filtered_rows, used_simd) = try_simd_filter(rows, expr, &schema)?;
                        if used_simd {
                            // SIMD path succeeded!
                            filtered_rows
                        } else {
                            // Fall back to row-based filtering (small dataset or unsupported predicate)
                            apply_where_filter_combined_auto(filtered_rows, Some(expr), &evaluator, self)?
                        }
                    }
                    crate::optimizer::WhereOptimization::Unchanged(where_expr) => {
                        // Try SIMD filtering first (for large datasets)
                        if let Some(ref expr) = where_expr {
                            let (filtered_rows, used_simd) = try_simd_filter(rows, expr, &schema)?;
                            if used_simd {
                                // SIMD path succeeded!
                                filtered_rows
                            } else {
                                // Fall back to row-based filtering
                                apply_where_filter_combined_auto(filtered_rows, where_expr.as_ref(), &evaluator, self)?
                            }
                        } else {
                            // No WHERE clause
                            rows
                        }
                    }
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
            // Check if results are already sorted by the index scan
            let already_sorted = if let Some(ref sorted_cols) = sorted_by {
                // Results are pre-sorted if:
                // 1. ORDER BY has same number of columns as sorted_cols
                // 2. Each column and sort direction matches
                if sorted_cols.len() == order_by.len() {
                    sorted_cols.iter().zip(order_by.iter()).all(|((col_name, sort_order), order_item)| {
                        // Check if column names match and sort direction matches
                        match &order_item.expr {
                            vibesql_ast::Expression::ColumnRef { table: None, column } => {
                                column == col_name && &order_item.direction == sort_order
                            }
                            _ => false,
                        }
                    })
                } else {
                    false
                }
            } else {
                false
            };

            if !already_sorted {
                // Fall back to sorting
                // Create evaluator for ORDER BY with procedural context support
                // Priority: 1) window mapping 2) outer context 3) procedural context 4) database only
                let order_by_evaluator = if let Some(ref mapping) = window_mapping {
                    CombinedExpressionEvaluator::with_database_and_windows(
                        &schema,
                        self.database,
                        mapping,
                    )
                } else if let (Some(outer_row), Some(outer_schema)) = (self._outer_row, self._outer_schema) {
                    CombinedExpressionEvaluator::with_database_and_outer_context(
                        &schema,
                        self.database,
                        outer_row,
                        outer_schema,
                    )
                } else if let Some(proc_ctx) = self.procedural_context {
                    CombinedExpressionEvaluator::with_database_and_procedural_context(
                        &schema,
                        self.database,
                        proc_ctx,
                    )
                } else {
                    CombinedExpressionEvaluator::with_database(&schema, self.database)
                };
                result_rows =
                    apply_order_by(result_rows, order_by, &order_by_evaluator, &stmt.select_list)?;
            }
        } else if sorted_by.is_none() && !result_rows.is_empty() {
            // No explicit ORDER BY - apply implicit ordering for deterministic results
            // This ensures SQLLogicTest compatibility and deterministic behavior
            // Skip sorting if data is already sorted from index scan
            use crate::select::grouping::compare_sql_values;

            #[cfg(feature = "parallel")]
            {
                use crate::select::parallel::ParallelConfig;
                use rayon::prelude::*;

                // Use parallel sorting for larger datasets
                let should_parallel = ParallelConfig::global().should_parallelize_sort(result_rows.len());

                if should_parallel {
                    result_rows.par_sort_by(|(row_a, _), (row_b, _)| {
                        // Compare column by column until we find a difference
                        for i in 0..row_a.values.len().min(row_b.values.len()) {
                            let cmp = compare_sql_values(&row_a.values[i], &row_b.values[i]);
                            if cmp != std::cmp::Ordering::Equal {
                                return cmp;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                } else {
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
            }

            #[cfg(not(feature = "parallel"))]
            {
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
        }

        // Choose projection strategy based on DISTINCT and set operations
        // - If DISTINCT is present, we must project all rows first, then deduplicate
        // - If no DISTINCT, we can apply LIMIT/OFFSET before projection for better performance
        let final_rows = if stmt.distinct || stmt.set_operation.is_some() {
            // Eager projection: project all rows, then apply DISTINCT and/or LIMIT/OFFSET
            // Use pooled buffer to reduce allocation overhead
            let mut projected_rows = self.database.query_buffer_pool().get_row_buffer(result_rows.len());
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
                    self.database.query_buffer_pool(),
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
                *self.database.query_buffer_pool(),
            );

            // Collect projected rows with memory tracking
            // Use pooled buffer to reduce allocation overhead
            let mut final_rows = self.database.query_buffer_pool().get_row_buffer(128);
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

            // Return buffer to pool and move data to result
            let result = std::mem::take(&mut final_rows);
            self.database.query_buffer_pool().return_row_buffer(final_rows);
            result
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
