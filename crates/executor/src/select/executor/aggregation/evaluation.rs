//! Expression evaluation with aggregates for SelectExecutor

use super::super::builder::SelectExecutor;
use crate::errors::ExecutorError;
use crate::evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator};
use crate::select::grouping::AggregateAccumulator;

impl SelectExecutor<'_> {
    /// Evaluate an expression in the context of aggregation
    #[allow(clippy::only_used_in_recursion)]
    pub(in crate::select::executor) fn evaluate_with_aggregates(
        &self,
        expr: &ast::Expression,
        group_rows: &[storage::Row],
        _group_key: &[types::SqlValue],
        evaluator: &CombinedExpressionEvaluator,
    ) -> Result<types::SqlValue, ExecutorError> {
        match expr {
            // Aggregate functions (both new and old variants)
            ast::Expression::AggregateFunction { .. } | ast::Expression::Function { .. } => {
                self.evaluate_aggregate_function(expr, group_rows, evaluator)
            }

            // Binary operation - recursively evaluate both sides
            ast::Expression::BinaryOp { left, op, right } => self
                .evaluate_binary_op_with_aggregates(
                    left, op, right, group_rows, _group_key, evaluator,
                ),

            // Scalar subquery - delegate to evaluator
            ast::Expression::ScalarSubquery(_) | ast::Expression::Exists { .. } => {
                self.evaluate_scalar_subquery_with_aggregates(expr, group_rows, evaluator)
            }

            // IN with subquery - special handling for aggregate left-hand side
            ast::Expression::In { expr: left_expr, subquery, negated } => self
                .evaluate_in_predicate_with_aggregates(
                    left_expr, subquery, *negated, group_rows, _group_key, evaluator,
                ),

            // Quantified comparison - special handling for aggregate left-hand side
            ast::Expression::QuantifiedComparison { expr: left_expr, op, quantifier, subquery } => {
                self.evaluate_quantified_comparison_with_aggregates(
                    left_expr, op, quantifier, subquery, group_rows, _group_key, evaluator,
                )
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

    /// Evaluate aggregate function expressions (COUNT, SUM, AVG, MIN, MAX)
    /// Handles both AggregateFunction and Function variants (for backwards compatibility)
    pub(in crate::select::executor) fn evaluate_aggregate_function(
        &self,
        expr: &ast::Expression,
        group_rows: &[storage::Row],
        evaluator: &CombinedExpressionEvaluator,
    ) -> Result<types::SqlValue, ExecutorError> {
        // Extract name, distinct, and args from either variant
        let (name, distinct, args) = match expr {
            ast::Expression::AggregateFunction { name, distinct, args } => (name, *distinct, args),
            ast::Expression::Function { name, args, character_unit: _ } => (name, false, args),
            _ => unreachable!("evaluate_aggregate_function called with non-aggregate expression"),
        };

        let mut acc = AggregateAccumulator::new(name, distinct)?;

        // Special handling for COUNT(*)
        if name.to_uppercase() == "COUNT" && args.len() == 1 {
            let is_count_star = matches!(args[0], ast::Expression::Wildcard)
                || matches!(
                    &args[0],
                    ast::Expression::ColumnRef { table: None, column } if column == "*"
                );

            if is_count_star {
                // COUNT(*) - count all rows (DISTINCT not allowed with *)
                if distinct {
                    return Err(ExecutorError::UnsupportedExpression(
                        "COUNT(DISTINCT *) is not valid SQL".to_string(),
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

    /// Evaluate binary operations in aggregate context
    pub(in crate::select::executor) fn evaluate_binary_op_with_aggregates(
        &self,
        left: &ast::Expression,
        op: &ast::BinaryOperator,
        right: &ast::Expression,
        group_rows: &[storage::Row],
        group_key: &[types::SqlValue],
        evaluator: &CombinedExpressionEvaluator,
    ) -> Result<types::SqlValue, ExecutorError> {
        let left_val = self.evaluate_with_aggregates(left, group_rows, group_key, evaluator)?;
        let right_val = self.evaluate_with_aggregates(right, group_rows, group_key, evaluator)?;

        // Reuse the binary op evaluation logic from ExpressionEvaluator
        let temp_schema = catalog::TableSchema::new("temp".to_string(), vec![]);
        let temp_evaluator = ExpressionEvaluator::new(&temp_schema);
        temp_evaluator.eval_binary_op(&left_val, op, &right_val)
    }

    /// Evaluate scalar subqueries and EXISTS expressions in aggregate context
    pub(in crate::select::executor) fn evaluate_scalar_subquery_with_aggregates(
        &self,
        expr: &ast::Expression,
        group_rows: &[storage::Row],
        evaluator: &CombinedExpressionEvaluator,
    ) -> Result<types::SqlValue, ExecutorError> {
        // Use first row from group as context for subquery evaluation
        if let Some(first_row) = group_rows.first() {
            evaluator.eval(expr, first_row)
        } else {
            Ok(types::SqlValue::Null)
        }
    }

    /// Evaluate IN predicate with subquery in aggregate context
    pub(in crate::select::executor) fn evaluate_in_predicate_with_aggregates(
        &self,
        left_expr: &ast::Expression,
        subquery: &ast::SelectStmt,
        negated: bool,
        group_rows: &[storage::Row],
        group_key: &[types::SqlValue],
        evaluator: &CombinedExpressionEvaluator,
    ) -> Result<types::SqlValue, ExecutorError> {
        // Evaluate left-hand expression (which may be an aggregate)
        let left_val =
            self.evaluate_with_aggregates(left_expr, group_rows, group_key, evaluator)?;

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
            let subquery_val =
                subquery_row.get(0).ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })?;

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
            Ok(types::SqlValue::Boolean(negated))
        }
    }

    /// Evaluate quantified comparison (ALL/ANY/SOME) with subquery in aggregate context
    pub(in crate::select::executor) fn evaluate_quantified_comparison_with_aggregates(
        &self,
        left_expr: &ast::Expression,
        op: &ast::BinaryOperator,
        quantifier: &ast::Quantifier,
        subquery: &ast::SelectStmt,
        group_rows: &[storage::Row],
        group_key: &[types::SqlValue],
        evaluator: &CombinedExpressionEvaluator,
    ) -> Result<types::SqlValue, ExecutorError> {
        // Evaluate left-hand expression (which may be an aggregate)
        let left_val =
            self.evaluate_with_aggregates(left_expr, group_rows, group_key, evaluator)?;

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
                        types::SqlValue::Boolean(false) => {
                            return Ok(types::SqlValue::Boolean(false))
                        }
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
                        types::SqlValue::Boolean(true) => {
                            return Ok(types::SqlValue::Boolean(true))
                        }
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
        let mut rows_with_keys: Vec<(storage::Row, Vec<(types::SqlValue, ast::OrderDirection)>)> =
            Vec::new();
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
