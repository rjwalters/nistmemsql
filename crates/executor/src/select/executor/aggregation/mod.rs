//! Aggregation execution methods for SelectExecutor

#[path = "detection.rs"]
mod detection;

#[path = "evaluation.rs"]
mod evaluation;

use super::builder::SelectExecutor;
use crate::errors::ExecutorError;
use crate::evaluator::CombinedExpressionEvaluator;
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
        // Execute FROM clause (handles JOINs, subqueries, CTEs)
        let from_result = match &stmt.from {
            Some(from_clause) => self.execute_from(from_clause, cte_results)?,
            None => {
                return Err(ExecutorError::UnsupportedFeature(
                    "SELECT without FROM not yet implemented".to_string(),
                ))
            }
        };

        eprintln!(
            "DEBUG AGG: schema keys={:?}",
            from_result.schema.table_schemas.keys().collect::<Vec<_>>()
        );
        eprintln!(
            "DEBUG AGG: outer_row={}, outer_schema={:?}",
            self._outer_row.is_some(),
            self._outer_schema.map(|s| s.table_schemas.keys().collect::<Vec<_>>())
        );

        // Create evaluator with outer context if available (outer schema is already a CombinedSchema)
        let evaluator =
            if let (Some(outer_row), Some(outer_schema)) = (self._outer_row, self._outer_schema) {
                eprintln!(
                    "DEBUG AGG: Creating evaluator WITH outer context, outer tables={:?}",
                    outer_schema.table_schemas.keys().collect::<Vec<_>>()
                );
                CombinedExpressionEvaluator::with_database_and_outer_context(
                    &from_result.schema,
                    self.database,
                    outer_row,
                    outer_schema,
                )
            } else {
                eprintln!("DEBUG AGG: Creating evaluator WITHOUT outer context");
                CombinedExpressionEvaluator::with_database(&from_result.schema, self.database)
            };

        // Apply WHERE clause to filter joined rows
        let filtered_rows =
            apply_where_filter_combined(from_result.rows, stmt.where_clause.as_ref(), &evaluator)?;

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
                    ast::SelectItem::Wildcard | ast::SelectItem::QualifiedWildcard { .. } => {
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
