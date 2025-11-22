//! Expression evaluation with aggregates for SelectExecutor
//!
//! This module coordinates expression evaluation in the context of aggregation,
//! delegating to specialized handlers for different expression types.

mod aggregate_function;
mod binary_op;
mod case;
mod function;
mod simple;
mod subquery;

use super::super::builder::SelectExecutor;
use crate::{errors::ExecutorError, evaluator::CombinedExpressionEvaluator};

impl SelectExecutor<'_> {
    /// Evaluate an expression in the context of aggregation
    ///
    /// This is the main entry point for expression evaluation during aggregation.
    /// It delegates to specialized handlers based on expression type.
    #[allow(clippy::only_used_in_recursion)]
    pub(in crate::select::executor) fn evaluate_with_aggregates(
        &self,
        expr: &vibesql_ast::Expression,
        group_rows: &[vibesql_storage::Row],
        group_key: &[vibesql_types::SqlValue],
        evaluator: &CombinedExpressionEvaluator,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        match expr {
            // Aggregate functions (AggregateFunction variant only)
            vibesql_ast::Expression::AggregateFunction { .. } => {
                aggregate_function::evaluate(self, expr, group_rows, evaluator)
            }

            // Regular functions (e.g., NULLIF wrapping an aggregate) - may contain aggregates in
            // arguments
            vibesql_ast::Expression::Function { .. } => {
                function::evaluate(self, expr, group_rows, group_key, evaluator)
            }

            // Binary operation - recursively evaluate both sides
            vibesql_ast::Expression::BinaryOp { left, op, right } => {
                binary_op::evaluate_binary(self, left, op, right, group_rows, group_key, evaluator)
            }

            // Unary operations - recursively evaluate inner expression with aggregates
            vibesql_ast::Expression::UnaryOp { op, expr: inner_expr } => {
                binary_op::evaluate_unary(self, op, inner_expr, group_rows, group_key, evaluator)
            }

            // Scalar subquery - delegate to evaluator
            vibesql_ast::Expression::ScalarSubquery(_) | vibesql_ast::Expression::Exists { .. } => {
                subquery::evaluate_scalar(self, expr, group_rows, evaluator)
            }

            // IN with subquery - special handling for aggregate left-hand side
            vibesql_ast::Expression::In { .. } => {
                subquery::evaluate_in(self, expr, group_rows, group_key, evaluator)
            }

            // Quantified comparison - special handling for aggregate left-hand side
            vibesql_ast::Expression::QuantifiedComparison { .. } => {
                subquery::evaluate_quantified(self, expr, group_rows, group_key, evaluator)
            }

            // CASE expression - may contain aggregates in operand, conditions, or results
            vibesql_ast::Expression::Case { operand, when_clauses, else_result } => case::evaluate(
                self,
                operand,
                when_clauses,
                else_result,
                group_rows,
                group_key,
                evaluator,
            ),

            // Simple expressions that can potentially contain aggregates
            vibesql_ast::Expression::Cast { .. }
            | vibesql_ast::Expression::Between { .. }
            | vibesql_ast::Expression::InList { .. }
            | vibesql_ast::Expression::Like { .. }
            | vibesql_ast::Expression::IsNull { .. }
            | vibesql_ast::Expression::Position { .. }
            | vibesql_ast::Expression::Trim { .. }
            | vibesql_ast::Expression::Interval { .. } => {
                simple::evaluate(self, expr, group_rows, group_key, evaluator)
            }

            // Truly simple expressions: Literal, ColumnRef (cannot contain aggregates)
            vibesql_ast::Expression::Literal(_)
            | vibesql_ast::Expression::ColumnRef { .. }
            | vibesql_ast::Expression::Wildcard
            | vibesql_ast::Expression::CurrentDate
            | vibesql_ast::Expression::CurrentTime { .. }
            | vibesql_ast::Expression::CurrentTimestamp { .. }
            | vibesql_ast::Expression::Default
            | vibesql_ast::Expression::NextValue { .. }
            | vibesql_ast::Expression::DuplicateKeyValue { .. }
            | vibesql_ast::Expression::PseudoVariable { .. }
            | vibesql_ast::Expression::SessionVariable { .. } => {
                simple::evaluate_no_aggregates(expr, group_rows, evaluator)
            }

            // Window functions and match against require special handling
            vibesql_ast::Expression::WindowFunction { .. } => {
                Err(ExecutorError::UnsupportedExpression(
                    "Window functions not supported in aggregate context".to_string(),
                ))
            }
            vibesql_ast::Expression::MatchAgainst { .. } => {
                Err(ExecutorError::UnsupportedExpression(
                    "MATCH...AGAINST not supported in aggregate context".to_string(),
                ))
            }
        }
    }

    /// Apply ORDER BY to aggregated results
    ///
    /// This method temporarily appends GROUP BY column values to each row to enable
    /// ORDER BY expressions that reference GROUP BY columns not in the SELECT list.
    /// After sorting, these extra columns are stripped to match the expected output schema.
    pub(in crate::select::executor) fn apply_order_by_to_aggregates(
        &self,
        mut rows: Vec<vibesql_storage::Row>,
        group_keys: Vec<Vec<vibesql_types::SqlValue>>,
        stmt: &vibesql_ast::SelectStmt,
        order_by: &[vibesql_ast::OrderByItem],
        expanded_select_list: &[vibesql_ast::SelectItem],
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Store the number of SELECT list columns (to strip GROUP BY columns later)
        let num_select_columns = expanded_select_list.len();

        // If we have GROUP BY, temporarily append group key values to enable ORDER BY
        // to reference GROUP BY columns that aren't in the SELECT list
        if let Some(group_by_exprs) = &stmt.group_by {
            // Append group_key values to each row
            for (row, group_key) in rows.iter_mut().zip(group_keys.iter()) {
                row.values.extend(group_key.clone());
            }

            // Build schema with both SELECT list columns AND GROUP BY columns
            let mut result_columns = Vec::new();

            // Add SELECT list columns
            for (idx, item) in expanded_select_list.iter().enumerate() {
                match item {
                    vibesql_ast::SelectItem::Expression { expr, alias } => {
                        let column_name = if let Some(alias) = alias {
                            alias.clone()
                        } else {
                            // Try to extract column name from expression
                            match expr {
                                vibesql_ast::Expression::ColumnRef { column, .. } => column.clone(),
                                vibesql_ast::Expression::AggregateFunction { name, .. } => {
                                    name.to_lowercase()
                                }
                                _ => format!("col{}", idx + 1),
                            }
                        };
                        result_columns.push(vibesql_catalog::ColumnSchema::new(
                            column_name,
                            vibesql_types::DataType::Varchar { max_length: Some(255) }, // Placeholder type
                            true,
                        ));
                    }
                    vibesql_ast::SelectItem::Wildcard { .. }
                    | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                        // This should not happen after expansion, but keep for safety
                        return Err(ExecutorError::UnsupportedFeature(
                            "SELECT * and qualified wildcards not supported with aggregates"
                                .to_string(),
                        ));
                    }
                }
            }

            // Add GROUP BY columns to schema (so ORDER BY can reference them)
            for (idx, group_by_expr) in group_by_exprs.iter().enumerate() {
                let column_name = match group_by_expr {
                    vibesql_ast::Expression::ColumnRef { column, .. } => column.clone(),
                    _ => format!("__groupby_{}", idx),
                };
                result_columns.push(vibesql_catalog::ColumnSchema::new(
                    column_name,
                    vibesql_types::DataType::Varchar { max_length: Some(255) }, // Placeholder type
                    true,
                ));
            }

            let result_table_schema =
                vibesql_catalog::TableSchema::new("result".to_string(), result_columns);

            // Create a CombinedSchema for the result set (with GROUP BY columns included)
            let mut table_schemas = std::collections::HashMap::new();
            table_schemas.insert("result".to_string(), (0, result_table_schema.clone()));
            let result_schema = crate::schema::CombinedSchema {
                table_schemas,
                total_columns: result_table_schema.columns.len(),
            };

            let result_evaluator =
                CombinedExpressionEvaluator::with_database(&result_schema, self.database);

            // Evaluate ORDER BY expressions and attach sort keys to rows
            let mut rows_with_keys: Vec<(
                vibesql_storage::Row,
                Vec<(vibesql_types::SqlValue, vibesql_ast::OrderDirection)>,
            )> = Vec::new();
            for row in rows {
                // Clear CSE cache before evaluating each row to prevent column values
                // from being incorrectly cached across different rows
                result_evaluator.clear_cse_cache();

                let mut sort_keys = Vec::new();
                for order_item in order_by {
                    let key_value = result_evaluator.eval(&order_item.expr, &row)?;
                    sort_keys.push((key_value, order_item.direction.clone()));
                }
                rows_with_keys.push((row, sort_keys));
            }

            // Sort using the sort keys
            rows_with_keys.sort_by(|(_, keys_a), (_, keys_b)| {
                use crate::select::grouping::compare_sql_values;

                for ((val_a, dir), (val_b, _)) in keys_a.iter().zip(keys_b.iter()) {
                    let cmp = match dir {
                        vibesql_ast::OrderDirection::Asc => compare_sql_values(val_a, val_b),
                        vibesql_ast::OrderDirection::Desc => {
                            compare_sql_values(val_a, val_b).reverse()
                        }
                    };

                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });

            // Strip GROUP BY columns from rows (keep only SELECT list columns)
            let final_rows: Vec<vibesql_storage::Row> = rows_with_keys
                .into_iter()
                .map(|(mut row, _)| {
                    row.values.truncate(num_select_columns);
                    row
                })
                .collect();

            Ok(final_rows)
        } else {
            // No GROUP BY - use original logic
            let mut result_columns = Vec::new();
            for (idx, item) in expanded_select_list.iter().enumerate() {
                match item {
                    vibesql_ast::SelectItem::Expression { expr, alias } => {
                        let column_name = if let Some(alias) = alias {
                            alias.clone()
                        } else {
                            // Try to extract column name from expression
                            match expr {
                                vibesql_ast::Expression::ColumnRef { column, .. } => column.clone(),
                                vibesql_ast::Expression::AggregateFunction { name, .. } => {
                                    name.to_lowercase()
                                }
                                _ => format!("col{}", idx + 1),
                            }
                        };
                        result_columns.push(vibesql_catalog::ColumnSchema::new(
                            column_name,
                            vibesql_types::DataType::Varchar { max_length: Some(255) }, // Placeholder type
                            true,
                        ));
                    }
                    vibesql_ast::SelectItem::Wildcard { .. }
                    | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                        // This should not happen after expansion, but keep for safety
                        return Err(ExecutorError::UnsupportedFeature(
                            "SELECT * and qualified wildcards not supported with aggregates"
                                .to_string(),
                        ));
                    }
                }
            }

            let result_table_schema =
                vibesql_catalog::TableSchema::new("result".to_string(), result_columns);

            // Create a CombinedSchema for the result set
            let mut table_schemas = std::collections::HashMap::new();
            table_schemas.insert("result".to_string(), (0, result_table_schema.clone()));
            let result_schema = crate::schema::CombinedSchema {
                table_schemas,
                total_columns: result_table_schema.columns.len(),
            };

            let result_evaluator =
                CombinedExpressionEvaluator::with_database(&result_schema, self.database);

            // Evaluate ORDER BY expressions and attach sort keys to rows
            let mut rows_with_keys: Vec<(
                vibesql_storage::Row,
                Vec<(vibesql_types::SqlValue, vibesql_ast::OrderDirection)>,
            )> = Vec::new();
            for row in rows {
                // Clear CSE cache before evaluating each row to prevent column values
                // from being incorrectly cached across different rows
                result_evaluator.clear_cse_cache();

                let mut sort_keys = Vec::new();
                for order_item in order_by {
                    let key_value = result_evaluator.eval(&order_item.expr, &row)?;
                    sort_keys.push((key_value, order_item.direction.clone()));
                }
                rows_with_keys.push((row, sort_keys));
            }

            // Sort using the sort keys
            rows_with_keys.sort_by(|(_, keys_a), (_, keys_b)| {
                use crate::select::grouping::compare_sql_values;

                for ((val_a, dir), (val_b, _)) in keys_a.iter().zip(keys_b.iter()) {
                    let cmp = match dir {
                        vibesql_ast::OrderDirection::Asc => compare_sql_values(val_a, val_b),
                        vibesql_ast::OrderDirection::Desc => {
                            compare_sql_values(val_a, val_b).reverse()
                        }
                    };

                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });

            // Extract rows without sort keys
            Ok(rows_with_keys.into_iter().map(|(row, _)| row).collect())
        }
    }
}
