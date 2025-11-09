//! Expression evaluation with aggregates for SelectExecutor
//!
//! This module coordinates expression evaluation in the context of aggregation,
//! delegating to specialized handlers for different expression types.

mod aggregate_function;
mod binary_op;
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
        expr: &ast::Expression,
        group_rows: &[storage::Row],
        group_key: &[types::SqlValue],
        evaluator: &CombinedExpressionEvaluator,
    ) -> Result<types::SqlValue, ExecutorError> {
        match expr {
            // Aggregate functions (AggregateFunction variant only)
            ast::Expression::AggregateFunction { .. } => {
                aggregate_function::evaluate(self, expr, group_rows, evaluator)
            }

            // Regular functions (e.g., NULLIF wrapping an aggregate) - may contain aggregates in
            // arguments
            ast::Expression::Function { .. } => {
                function::evaluate(self, expr, group_rows, group_key, evaluator)
            }

            // Binary operation - recursively evaluate both sides
            ast::Expression::BinaryOp { left, op, right } => {
                binary_op::evaluate_binary(self, left, op, right, group_rows, group_key, evaluator)
            }

            // Unary operations - recursively evaluate inner expression with aggregates
            ast::Expression::UnaryOp { op, expr: inner_expr } => {
                binary_op::evaluate_unary(self, op, inner_expr, group_rows, group_key, evaluator)
            }

            // Scalar subquery - delegate to evaluator
            ast::Expression::ScalarSubquery(_) | ast::Expression::Exists { .. } => {
                subquery::evaluate_scalar(self, expr, group_rows, evaluator)
            }

            // IN with subquery - special handling for aggregate left-hand side
            ast::Expression::In { .. } => {
                subquery::evaluate_in(self, expr, group_rows, group_key, evaluator)
            }

            // Quantified comparison - special handling for aggregate left-hand side
            ast::Expression::QuantifiedComparison { .. } => {
                subquery::evaluate_quantified(self, expr, group_rows, group_key, evaluator)
            }

            // Simple expressions: Literal, ColumnRef, InList, Between, Cast, Like, IsNull, Case
            ast::Expression::Literal(_)
            | ast::Expression::ColumnRef { .. }
            | ast::Expression::InList { .. }
            | ast::Expression::Between { .. }
            | ast::Expression::Cast { .. }
            | ast::Expression::Like { .. }
            | ast::Expression::IsNull { .. }
            | ast::Expression::Case { .. } => simple::evaluate(self, expr, group_rows, evaluator),

            _ => Err(ExecutorError::UnsupportedExpression(format!(
                "Unsupported expression in aggregate context: {:?}",
                expr
            ))),
        }
    }

    /// Apply ORDER BY to aggregated results
    pub(in crate::select::executor) fn apply_order_by_to_aggregates(
        &self,
        rows: Vec<storage::Row>,
        stmt: &ast::SelectStmt,
        order_by: &[ast::OrderByItem],
    ) -> Result<Vec<storage::Row>, ExecutorError> {
        // Build a schema from the SELECT list to enable ORDER BY column resolution
        let mut result_columns = Vec::new();
        for (idx, item) in stmt.select_list.iter().enumerate() {
            match item {
                ast::SelectItem::Expression { expr, alias } => {
                    let column_name = if let Some(alias) = alias {
                        alias.clone()
                    } else {
                        // Try to extract column name from expression
                        match expr {
                            ast::Expression::ColumnRef { column, .. } => column.clone(),
                            ast::Expression::AggregateFunction { name, .. } => name.to_lowercase(),
                            _ => format!("col{}", idx + 1),
                        }
                    };
                    result_columns.push(catalog::ColumnSchema::new(
                        column_name,
                        types::DataType::Varchar { max_length: Some(255) }, // Placeholder type
                        true,
                    ));
                }
                ast::SelectItem::Wildcard { .. } | ast::SelectItem::QualifiedWildcard { .. } => {
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * and qualified wildcards not supported with aggregates"
                            .to_string(),
                    ));
                }
            }
        }

        let result_table_schema = catalog::TableSchema::new("result".to_string(), result_columns);

        // Create a CombinedSchema for the result set
        let mut table_schemas = std::collections::HashMap::new();
        table_schemas.insert("result".to_string(), (0, result_table_schema.clone()));
        let result_schema = crate::schema::CombinedSchema {
            table_schemas,
            total_columns: result_table_schema.columns.len(),
        };

        let result_evaluator = CombinedExpressionEvaluator::new(&result_schema);

        // Evaluate ORDER BY expressions and attach sort keys to rows
        let mut rows_with_keys: Vec<(storage::Row, Vec<(types::SqlValue, ast::OrderDirection)>)> =
            Vec::new();
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
                    ast::OrderDirection::Asc => compare_sql_values(val_a, val_b),
                    ast::OrderDirection::Desc => compare_sql_values(val_a, val_b).reverse(),
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
