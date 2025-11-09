//! Aggregation execution methods for SelectExecutor

#[path = "detection.rs"]
mod detection;

#[path = "evaluation.rs"]
mod evaluation;

use super::builder::SelectExecutor;
use crate::errors::ExecutorError;
use crate::evaluator::CombinedExpressionEvaluator;
use crate::optimizer::optimize_where_clause;
use crate::select::cte::CteResult;
use crate::select::filter::apply_where_filter_combined;
use crate::select::grouping::group_rows;
use crate::select::helpers::{apply_distinct, apply_limit_offset};
use std::collections::HashMap;

impl SelectExecutor<'_> {
    /// Execute SELECT with aggregation/GROUP BY
    pub(in crate::select::executor) fn execute_with_aggregation(
        &self,
        stmt: &ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Vec<storage::Row>, ExecutorError> {
        // Fast path: Simple COUNT(*) without filtering
        // This optimization avoids materializing all rows when we just need the count
        if let Some(table_name) = self.is_simple_count_star(stmt) {
            // If table doesn't exist, fall through to normal path which will produce proper error
            if let Some(table) = self.database.get_table(&table_name) {
                let count = table.row_count();
                return Ok(vec![storage::Row::new(vec![types::SqlValue::Integer(count as i64)])]);
            }
        }

        // Execute FROM clause (handles JOINs, subqueries, CTEs)
        let from_result = match &stmt.from {
            Some(from_clause) => self.execute_from(from_clause, cte_results)?,
            None => {
                // SELECT without FROM - create empty table for aggregates
                // SQL standard behavior: aggregate functions operate on empty set
                // - COUNT(*) returns 0 (no rows to count)
                // - COUNT(expr) returns 0 (no rows to evaluate)
                // - Other aggregates return NULL (no rows to aggregate)
                use crate::schema::CombinedSchema;
                use crate::select::join::FromResult;

                let empty_schema = catalog::TableSchema::new("".to_string(), vec![]);
                let combined_schema = CombinedSchema::from_table("".to_string(), empty_schema);

                // Empty result set - no rows to aggregate over
                FromResult { schema: combined_schema, rows: vec![] }
            }
        };

        // Create evaluator with outer context if available (outer schema is already a CombinedSchema)
        let evaluator =
            if let (Some(outer_row), Some(outer_schema)) = (self._outer_row, self._outer_schema) {
                CombinedExpressionEvaluator::with_database_and_outer_context(
                    &from_result.schema,
                    self.database,
                    outer_row,
                    outer_schema,
                )
            } else {
                CombinedExpressionEvaluator::with_database(&from_result.schema, self.database)
            };

        // Optimize WHERE clause with constant folding and dead code elimination
        let where_optimization = optimize_where_clause(stmt.where_clause.as_ref(), &evaluator)?;

        // Apply WHERE clause to filter joined rows (optimized)
        let filtered_rows = match where_optimization {
            crate::optimizer::WhereOptimization::AlwaysTrue => {
                // WHERE TRUE - no filtering needed
                from_result.rows
            }
            crate::optimizer::WhereOptimization::AlwaysFalse => {
                // WHERE FALSE - return empty result
                Vec::new()
            }
            crate::optimizer::WhereOptimization::Optimized(ref expr) => {
                // Apply optimized WHERE clause
                apply_where_filter_combined(from_result.rows, Some(expr), &evaluator)?
            }
            crate::optimizer::WhereOptimization::Unchanged(where_expr) => {
                // Apply original WHERE clause
                apply_where_filter_combined(from_result.rows, where_expr.as_ref(), &evaluator)?
            }
        };

        // Group rows
        let groups = if let Some(group_by_exprs) = &stmt.group_by {
            group_rows(&filtered_rows, group_by_exprs, &evaluator)?
        } else {
            // No GROUP BY - treat all rows as one group
            vec![(Vec::new(), filtered_rows)]
        };

        // Compute aggregates for each group and apply HAVING
        let mut result_rows = Vec::new();
        for (group_key, group_rows) in groups {
            // Check timeout during aggregation
            self.check_timeout()?;

            // Compute aggregates for this group
            let mut aggregate_results = Vec::new();
            for item in &stmt.select_list {
                match item {
                    ast::SelectItem::Expression { expr, .. } => {
                        let value = self.evaluate_with_aggregates(
                            expr,
                            &group_rows,
                            &group_key,
                            &evaluator,
                        )?;
                        aggregate_results.push(value);
                    }
                    ast::SelectItem::Wildcard { .. }
                    | ast::SelectItem::QualifiedWildcard { .. } => {
                        return Err(ExecutorError::UnsupportedFeature(
                            "SELECT * and qualified wildcards not supported with aggregates"
                                .to_string(),
                        ))
                    }
                }
            }

            // Apply HAVING filter
            let include_group = if let Some(having_expr) = &stmt.having {
                let having_result = self.evaluate_with_aggregates(
                    having_expr,
                    &group_rows,
                    &group_key,
                    &evaluator,
                )?;
                match having_result {
                    types::SqlValue::Boolean(true) => true,
                    types::SqlValue::Boolean(false) | types::SqlValue::Null => false,
                    // SQLLogicTest compatibility: treat integers as truthy/falsy (C-like behavior)
                    types::SqlValue::Integer(0) => false,
                    types::SqlValue::Integer(_) => true,
                    types::SqlValue::Smallint(0) => false,
                    types::SqlValue::Smallint(_) => true,
                    types::SqlValue::Bigint(0) => false,
                    types::SqlValue::Bigint(_) => true,
                    types::SqlValue::Float(f) if f == 0.0 => false,
                    types::SqlValue::Float(_) => true,
                    types::SqlValue::Real(f) if f == 0.0 => false,
                    types::SqlValue::Real(_) => true,
                    types::SqlValue::Double(f) if f == 0.0 => false,
                    types::SqlValue::Double(_) => true,
                    other => {
                        return Err(ExecutorError::InvalidWhereClause(format!(
                            "HAVING must evaluate to boolean, got: {:?}",
                            other
                        )))
                    }
                }
            } else {
                true
            };

            if include_group {
                result_rows.push(storage::Row::new(aggregate_results));
            }
        }

        // Apply ORDER BY if present
        let result_rows = if let Some(order_by) = &stmt.order_by {
            self.apply_order_by_to_aggregates(result_rows, stmt, order_by)?
        } else {
            result_rows
        };

        // Apply DISTINCT if specified
        let result_rows = if stmt.distinct { apply_distinct(result_rows) } else { result_rows };

        // Don't apply LIMIT/OFFSET if we have a set operation - it will be applied later
        if stmt.set_operation.is_some() {
            Ok(result_rows)
        } else {
            Ok(apply_limit_offset(result_rows, stmt.limit, stmt.offset))
        }
    }
}
