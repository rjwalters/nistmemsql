//! Non-aggregation execution methods for SelectExecutor

use super::builder::SelectExecutor;
use crate::errors::ExecutorError;
use crate::evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator};
use crate::select::filter::apply_where_filter_combined;
use crate::select::helpers::{apply_distinct, apply_limit_offset};
use crate::select::join::FromResult;
use crate::select::order::{apply_order_by, RowWithSortKeys};
use crate::select::projection::project_row_combined;
use crate::select::window::{evaluate_window_functions, has_window_functions};

impl<'a> SelectExecutor<'a> {
    /// Execute SELECT without aggregation
    pub(super) fn execute_without_aggregation(
        &self,
        stmt: &ast::SelectStmt,
        from_result: FromResult,
    ) -> Result<Vec<storage::Row>, ExecutorError> {
        let FromResult { schema, rows } = from_result;
        eprintln!("DEBUG NONAGG: schema keys={:?}, row count={}", schema.table_schemas.keys().collect::<Vec<_>>(), rows.len());
        let evaluator = CombinedExpressionEvaluator::with_database(&schema, self.database);

        // Apply WHERE clause filter
        eprintln!("DEBUG NONAGG: About to apply WHERE filter");
        let mut filtered_rows = apply_where_filter_combined(rows, stmt.where_clause.as_ref(), &evaluator)?;
        eprintln!("DEBUG NONAGG: WHERE filter complete, {} rows", filtered_rows.len());

        // Check if SELECT list has window functions
        let has_windows = has_window_functions(&stmt.select_list);

        // If there are window functions, evaluate them first
        // Window functions operate on the filtered result set
        let window_mapping = if has_windows {
            let (rows_with_windows, mapping) = evaluate_window_functions(filtered_rows, &stmt.select_list, &evaluator)?;
            filtered_rows = rows_with_windows;
            Some(mapping)
        } else {
            None
        };

        // Convert to RowWithSortKeys format
        let mut result_rows: Vec<RowWithSortKeys> = filtered_rows.into_iter().map(|row| (row, None)).collect();

        // Apply ORDER BY sorting if present
        if let Some(order_by) = &stmt.order_by {
            result_rows = apply_order_by(result_rows, order_by, &evaluator)?;
        }

        // Project columns from the sorted rows
        let mut final_rows = Vec::new();
        for (row, _) in result_rows {
            let projected_row = project_row_combined(&row, &stmt.select_list, &evaluator, &window_mapping)?;
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
                ast::SelectItem::Wildcard => {
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * requires FROM clause".to_string(),
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
