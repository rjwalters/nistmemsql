//! Aggregation execution methods for SelectExecutor

use super::builder::SelectExecutor;
use crate::errors::ExecutorError;
use crate::evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator};
use crate::select::cte::CteResult;
use crate::select::filter::apply_where_filter_combined;
use crate::select::grouping::{group_rows, AggregateAccumulator};
use crate::select::helpers::{apply_distinct, apply_limit_offset};
use std::collections::HashMap;

impl<'a> SelectExecutor<'a> {
    /// Check if SELECT list contains aggregate functions
    pub(super) fn has_aggregates(&self, select_list: &[ast::SelectItem]) -> bool {
        select_list.iter().any(|item| match item {
            ast::SelectItem::Expression { expr, .. } => self.expression_has_aggregate(expr),
            _ => false,
        })
    }

    /// Check if an expression contains aggregate functions
    #[allow(clippy::only_used_in_recursion)]
    pub(super) fn expression_has_aggregate(&self, expr: &ast::Expression) -> bool {
        match expr {
            // New AggregateFunction variant
            ast::Expression::AggregateFunction { .. } => true,
            // Old Function variant (backwards compatibility)
            ast::Expression::Function { name, .. } => {
                matches!(name.to_uppercase().as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
            }
            ast::Expression::BinaryOp { left, right, .. } => {
                self.expression_has_aggregate(left) || self.expression_has_aggregate(right)
            }
            _ => false,
        }
    }

    /// Execute SELECT with aggregation/GROUP BY
    pub(super) fn execute_with_aggregation(
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

        let evaluator = CombinedExpressionEvaluator::with_database(&from_result.schema, self.database);

        // Apply WHERE clause to filter joined rows
        let filtered_rows = apply_where_filter_combined(from_result.rows, stmt.where_clause.as_ref(), &evaluator)?;

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
                    ast::SelectItem::Wildcard => {
                        return Err(ExecutorError::UnsupportedFeature(
                            "SELECT * not supported with aggregates".to_string(),
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

    /// Evaluate an expression in the context of aggregation
    #[allow(clippy::only_used_in_recursion)]
    pub(super) fn evaluate_with_aggregates(
        &self,
        expr: &ast::Expression,
        group_rows: &[storage::Row],
        _group_key: &[types::SqlValue],
        evaluator: &CombinedExpressionEvaluator,
    ) -> Result<types::SqlValue, ExecutorError> {
        match expr {
            // New AggregateFunction variant
            ast::Expression::AggregateFunction { name, distinct, args } => {
                let mut acc = AggregateAccumulator::new(name, *distinct)?;

                // Special handling for COUNT(*)
                if name.to_uppercase() == "COUNT" && args.len() == 1 {
                    let is_count_star = matches!(args[0], ast::Expression::Wildcard)
                        || matches!(
                            &args[0],
                            ast::Expression::ColumnRef { table: None, column } if column == "*"
                        );

                    if is_count_star {
                        // COUNT(*) - count all rows (DISTINCT not allowed with *)
                        if *distinct {
                            return Err(ExecutorError::UnsupportedExpression(
                                "COUNT(DISTINCT *) is not valid SQL".to_string()
                            ));
                        }
                        for _ in group_rows {
                            acc.accumulate(&types::SqlValue::Integer(1));
                        }
                        return Ok(acc.finalize());
                    }
                }

                // Regular aggregate - evaluate argument for each row
                if args.len() != 1 {
                    return Err(ExecutorError::UnsupportedExpression(format!(
                        "Aggregate functions expect 1 argument, got {}",
                        args.len()
                    )));
                }

                for row in group_rows {
                    let value = evaluator.eval(&args[0], row)?;
                    acc.accumulate(&value);
                }

                Ok(acc.finalize())
            }

            // Old Function variant (backwards compatibility)
            ast::Expression::Function { name, args } => {
                let mut acc = AggregateAccumulator::new(name, false)?;

                // Special handling for COUNT(*)
                if name.to_uppercase() == "COUNT" && args.len() == 1 {
                    // Parser represents COUNT(*) as either Wildcard or ColumnRef { column: "*" }
                    let is_count_star = matches!(args[0], ast::Expression::Wildcard)
                        || matches!(
                            &args[0],
                            ast::Expression::ColumnRef { table: None, column } if column == "*"
                        );

                    if is_count_star {
                        // COUNT(*) - count all rows
                        for _ in group_rows {
                            acc.accumulate(&types::SqlValue::Integer(1));
                        }
                        return Ok(acc.finalize());
                    }
                }

                // Regular aggregate - evaluate argument for each row
                if args.len() != 1 {
                    return Err(ExecutorError::UnsupportedExpression(format!(
                        "Aggregate functions expect 1 argument, got {}",
                        args.len()
                    )));
                }

                for row in group_rows {
                    let value = evaluator.eval(&args[0], row)?;
                    acc.accumulate(&value);
                }

                Ok(acc.finalize())
            }

            // Binary operation - recursively evaluate both sides
            ast::Expression::BinaryOp { left, op, right } => {
                let left_val =
                    self.evaluate_with_aggregates(left, group_rows, _group_key, evaluator)?;
                let right_val =
                    self.evaluate_with_aggregates(right, group_rows, _group_key, evaluator)?;

                // Reuse the binary op evaluation logic from ExpressionEvaluator
                // Create a temporary evaluator for this
                let temp_schema = catalog::TableSchema::new("temp".to_string(), vec![]);
                let temp_evaluator = ExpressionEvaluator::new(&temp_schema);
                temp_evaluator.eval_binary_op(&left_val, op, &right_val)
            }

            // Scalar subquery - delegate to evaluator
            ast::Expression::ScalarSubquery(_) | ast::Expression::Exists { .. } => {
                // Use first row from group as context for subquery evaluation
                if let Some(first_row) = group_rows.first() {
                    evaluator.eval(expr, first_row)
                } else {
                    Ok(types::SqlValue::Null)
                }
            }

            // IN with subquery - special handling for aggregate left-hand side
            ast::Expression::In { expr: left_expr, subquery, negated } => {
                // Evaluate left-hand expression (which may be an aggregate)
                let left_val = self.evaluate_with_aggregates(left_expr, group_rows, _group_key, evaluator)?;

                // Execute subquery to get values to compare against
                let database = self.database;
                let select_executor = crate::select::SelectExecutor::new(database);
                let rows = select_executor.execute(subquery)?;

                // Check subquery column count
                if subquery.select_list.len() != 1 {
                    return Err(ExecutorError::SubqueryColumnCountMismatch {
                        expected: 1,
                        actual: subquery.select_list.len(),
                    });
                }

                // If left value is NULL, result is NULL
                if matches!(left_val, types::SqlValue::Null) {
                    return Ok(types::SqlValue::Null);
                }

                let mut found_null = false;

                // Check each row from subquery
                for subquery_row in &rows {
                    let subquery_val = subquery_row
                        .get(0)
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })?;

                    // Track if we encounter NULL
                    if matches!(subquery_val, types::SqlValue::Null) {
                        found_null = true;
                        continue;
                    }

                    // Compare using equality
                    if left_val == *subquery_val {
                        return Ok(types::SqlValue::Boolean(!negated));
                    }
                }

                // No match found
                if found_null {
                    Ok(types::SqlValue::Null)
                } else {
                    Ok(types::SqlValue::Boolean(*negated))
                }
            }

            // Quantified comparison - special handling for aggregate left-hand side
            ast::Expression::QuantifiedComparison { expr: left_expr, op, quantifier, subquery } => {
                // Evaluate left-hand expression (which may be an aggregate)
                let left_val = self.evaluate_with_aggregates(left_expr, group_rows, _group_key, evaluator)?;

                // Execute subquery
                let database = self.database;
                let select_executor = crate::select::SelectExecutor::new(database);
                let rows = select_executor.execute(subquery)?;

                // Empty subquery special cases
                if rows.is_empty() {
                    return Ok(types::SqlValue::Boolean(matches!(quantifier, ast::Quantifier::All)));
                }

                // If left value is NULL, return NULL
                if matches!(left_val, types::SqlValue::Null) {
                    return Ok(types::SqlValue::Null);
                }

                let mut has_null = false;

                match quantifier {
                    ast::Quantifier::All => {
                        for subquery_row in &rows {
                            if subquery_row.values.len() != 1 {
                                return Err(ExecutorError::SubqueryColumnCountMismatch {
                                    expected: 1,
                                    actual: subquery_row.values.len(),
                                });
                            }

                            let right_val = &subquery_row.values[0];

                            if matches!(right_val, types::SqlValue::Null) {
                                has_null = true;
                                continue;
                            }

                            // Create temp evaluator for comparison
                            let temp_schema = catalog::TableSchema::new("temp".to_string(), vec![]);
                            let temp_evaluator = ExpressionEvaluator::new(&temp_schema);
                            let cmp_result = temp_evaluator.eval_binary_op(&left_val, op, right_val)?;

                            match cmp_result {
                                types::SqlValue::Boolean(false) => return Ok(types::SqlValue::Boolean(false)),
                                types::SqlValue::Null => has_null = true,
                                _ => {}
                            }
                        }

                        if has_null {
                            Ok(types::SqlValue::Null)
                        } else {
                            Ok(types::SqlValue::Boolean(true))
                        }
                    }

                    ast::Quantifier::Any | ast::Quantifier::Some => {
                        for subquery_row in &rows {
                            if subquery_row.values.len() != 1 {
                                return Err(ExecutorError::SubqueryColumnCountMismatch {
                                    expected: 1,
                                    actual: subquery_row.values.len(),
                                });
                            }

                            let right_val = &subquery_row.values[0];

                            if matches!(right_val, types::SqlValue::Null) {
                                has_null = true;
                                continue;
                            }

                            // Create temp evaluator for comparison
                            let temp_schema = catalog::TableSchema::new("temp".to_string(), vec![]);
                            let temp_evaluator = ExpressionEvaluator::new(&temp_schema);
                            let cmp_result = temp_evaluator.eval_binary_op(&left_val, op, right_val)?;

                            match cmp_result {
                                types::SqlValue::Boolean(true) => return Ok(types::SqlValue::Boolean(true)),
                                types::SqlValue::Null => has_null = true,
                                _ => {}
                            }
                        }

                        if has_null {
                            Ok(types::SqlValue::Null)
                        } else {
                            Ok(types::SqlValue::Boolean(false))
                        }
                    }
                }
            }

            // Other expressions that might contain subqueries or be useful in HAVING:
            // Delegate to evaluator using first row from group as context
            ast::Expression::ColumnRef { .. }
            | ast::Expression::Literal(_)
            | ast::Expression::InList { .. }
            | ast::Expression::Between { .. }
            | ast::Expression::Cast { .. }
            | ast::Expression::Like { .. }
            | ast::Expression::IsNull { .. }
            | ast::Expression::UnaryOp { .. }
            | ast::Expression::Case { .. } => {
                // Use first row from group as context
                if let Some(first_row) = group_rows.first() {
                    evaluator.eval(expr, first_row)
                } else {
                    Ok(types::SqlValue::Null)
                }
            }

            _ => Err(ExecutorError::UnsupportedExpression(format!(
                "Unsupported expression in aggregate context: {:?}",
                expr
            ))),
        }
    }

    /// Apply ORDER BY to aggregated results
    fn apply_order_by_to_aggregates(
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
                        types::DataType::Varchar { max_length: 255 }, // Placeholder type
                        true,
                    ));
                }
                ast::SelectItem::Wildcard => {
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * not supported with aggregates".to_string(),
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
        let mut rows_with_keys: Vec<(storage::Row, Vec<(types::SqlValue, ast::OrderDirection)>)> = Vec::new();
        for row in rows {
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
