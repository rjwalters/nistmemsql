//! Non-aggregation execution methods for SelectExecutor

use super::builder::SelectExecutor;
use crate::errors::ExecutorError;
use crate::evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator};
use crate::optimizer::optimize_where_clause;
use crate::select::filter::apply_where_filter_combined;
use crate::select::helpers::{apply_distinct, apply_limit_offset};
use crate::select::join::FromResult;
use crate::select::order::{apply_order_by, RowWithSortKeys};
use crate::select::projection::project_row_combined;
use crate::schema::CombinedSchema;
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

        // Create evaluator with outer context if available (outer schema is already a CombinedSchema)
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

        // Optimize WHERE clause with constant folding and dead code elimination
        let where_optimization = optimize_where_clause(stmt.where_clause.as_ref(), &evaluator)?;

        // Apply WHERE clause filter (optimized)
        let mut filtered_rows = match where_optimization {
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
                apply_where_filter_combined(rows, Some(expr), &evaluator)?
            }
            crate::optimizer::WhereOptimization::Unchanged(where_expr) => {
                // Apply original WHERE clause
                apply_where_filter_combined(rows, where_expr.as_ref(), &evaluator)?
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
            if let Some(ordered_rows) = self.try_index_based_ordering(&result_rows, order_by, &schema, &stmt.from)? {
                result_rows = ordered_rows;
            } else {
                // Fall back to sorting
                // Create evaluator with window mapping for ORDER BY (if window functions are present)
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
            let projected_row = project_row_combined(
                &row,
                &stmt.select_list,
                &evaluator,
                &schema,
                &window_mapping,
            )?;
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

    /// Try to use an index for ORDER BY optimization
    /// Returns ordered rows if an index can be used, None otherwise
    fn try_index_based_ordering(
        &self,
        rows: &[RowWithSortKeys],
        order_by: &[ast::OrderByItem],
        schema: &CombinedSchema,
        _from_clause: &Option<ast::FromClause>,
    ) -> Result<Option<Vec<RowWithSortKeys>>, ExecutorError> {
        // For now, only support single-column ORDER BY
        if order_by.len() != 1 {
            return Ok(None);
        }

        let order_item = &order_by[0];

        // Check if ORDER BY is on a simple column reference
        let column_name = match &order_item.expr {
            ast::Expression::ColumnRef { table: None, column } => column,
            _ => return Ok(None), // Complex expressions can't use index
        };

        // For now, assume single table and try to find any table that has this column
        let mut found_table = None;
        for (table_name, (_start_idx, table_schema)) in &schema.table_schemas {
        if table_schema.get_column_index(column_name).is_some() {
                found_table = Some(table_name.clone());
                break;
        }
        }

        let table_name = match found_table {
            Some(name) => name,
            None => return Ok(None),
        };

    // Find an index on this table and column
    let index_name = self.find_index_for_ordering(&table_name, column_name, order_item.direction.clone())?;
    if index_name.is_none() {
    return Ok(None);
    }
        let index_name = index_name.unwrap();

        // Get the index data
        let index_data = match self.database.get_index_data(&index_name) {
        Some(data) => data,
            None => return Ok(None),
        };

        // For this proof of concept, only use index when we have all rows from the table
        // Check by getting the table and comparing row counts
        let table_row_count = match self.database.get_table(&table_name) {
        Some(table) => table.row_count(),
            None => return Ok(None),
        };

        if rows.len() != table_row_count {
            // WHERE filtering was applied, can't use index easily
            return Ok(None);
        }

        // All rows are included, we can use the index directly
        // The index contains sorted key-value pairs with row indices
        let mut ordered_rows = Vec::new();
        for (_key, indices) in &index_data.data {
            for &row_idx in indices {
                if row_idx < rows.len() {
                    ordered_rows.push(rows[row_idx].clone());
                }
        }
        }

        // Reverse if DESC
        if order_item.direction == ast::OrderDirection::Desc {
        ordered_rows.reverse();
        }

        Ok(Some(ordered_rows))
        }

    /// Find an index that can be used for ordering by the given column
    fn find_index_for_ordering(
        &self,
        table_name: &str,
        column_name: &str,
        direction: ast::OrderDirection,
    ) -> Result<Option<String>, ExecutorError> {
        // For now, look through all indexes (this is inefficient but works for the proof of concept)
        // In a real implementation, we'd have better index lookup
        let all_indexes = self.database.list_indexes();
        for index_name in all_indexes {
            if let Some(metadata) = self.database.get_index(&index_name) {
            if metadata.table_name == table_name
                && metadata.columns.len() == 1
                    && metadata.columns[0].column_name == column_name
                    && metadata.columns[0].direction == direction {
                    return Ok(Some(index_name));
                }
            }
        }
        Ok(None)
        }
}
