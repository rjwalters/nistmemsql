use std::cmp::Ordering;
use std::collections::HashSet;

use crate::errors::ExecutorError;
use crate::evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator};
use crate::schema::CombinedSchema;

mod grouping;
mod join;
mod projection;

use grouping::{compare_sql_values, group_rows, AggregateAccumulator};
use join::{nested_loop_join, FromResult};
use projection::project_row_combined;

/// Row with optional sort keys for ORDER BY
type RowWithSortKeys = (storage::Row, Option<Vec<(types::SqlValue, ast::OrderDirection)>>);

/// Executes SELECT queries
pub struct SelectExecutor<'a> {
    database: &'a storage::Database,
}

impl<'a> SelectExecutor<'a> {
    /// Create a new SELECT executor
    pub fn new(database: &'a storage::Database) -> Self {
        SelectExecutor { database }
    }

    /// Execute a SELECT statement
    pub fn execute(&self, stmt: &ast::SelectStmt) -> Result<Vec<storage::Row>, ExecutorError> {
        let has_aggregates = self.has_aggregates(&stmt.select_list) || stmt.having.is_some();
        let has_group_by = stmt.group_by.is_some();

        if has_aggregates || has_group_by {
            self.execute_with_aggregation(stmt)
        } else {
            let from_clause = stmt.from.as_ref().ok_or_else(|| {
                ExecutorError::UnsupportedFeature(
                    "SELECT without FROM not yet implemented".to_string(),
                )
            })?;
            let from_result = self.execute_from(from_clause)?;
            self.execute_without_aggregation(stmt, from_result)
        }
    }

    /// Check if SELECT list contains aggregate functions
    fn has_aggregates(&self, select_list: &[ast::SelectItem]) -> bool {
        select_list.iter().any(|item| match item {
            ast::SelectItem::Expression { expr, .. } => self.expression_has_aggregate(expr),
            _ => false,
        })
    }

    /// Check if an expression contains aggregate functions
    #[allow(clippy::only_used_in_recursion)]
    fn expression_has_aggregate(&self, expr: &ast::Expression) -> bool {
        match expr {
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
    fn execute_with_aggregation(
        &self,
        stmt: &ast::SelectStmt,
    ) -> Result<Vec<storage::Row>, ExecutorError> {
        // Get table
        let table_name = match &stmt.from {
            Some(ast::FromClause::Table { name, alias: _ }) => name,
            Some(_) => {
                return Err(ExecutorError::UnsupportedFeature(
                    "Aggregates only supported on single tables".to_string(),
                ))
            }
            None => {
                return Err(ExecutorError::UnsupportedFeature(
                    "SELECT without FROM not yet implemented".to_string(),
                ))
            }
        };

        let table = self
            .database
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

        let evaluator = ExpressionEvaluator::with_database(&table.schema, self.database);

        // Scan and filter rows
        let mut filtered_rows = Vec::new();
        for row in table.scan() {
            let include_row = if let Some(where_expr) = &stmt.where_clause {
                match evaluator.eval(where_expr, row)? {
                    types::SqlValue::Boolean(true) => true,
                    types::SqlValue::Boolean(false) | types::SqlValue::Null => false,
                    other => {
                        return Err(ExecutorError::InvalidWhereClause(format!(
                            "WHERE must evaluate to boolean, got: {:?}",
                            other
                        )))
                    }
                }
            } else {
                true
            };

            if include_row {
                filtered_rows.push(row.clone());
            }
        }

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
        if stmt.order_by.is_some() {
            return Err(ExecutorError::UnsupportedFeature(
                "ORDER BY with aggregates not yet implemented".to_string(),
            ));
        }

        // Apply DISTINCT if specified
        let result_rows = if stmt.distinct {
            apply_distinct(result_rows)
        } else {
            result_rows
        };

        Ok(apply_limit_offset(result_rows, stmt.limit, stmt.offset))
    }

    /// Execute SELECT without aggregation
    fn execute_without_aggregation(
        &self,
        stmt: &ast::SelectStmt,
        from_result: FromResult,
    ) -> Result<Vec<storage::Row>, ExecutorError> {
        let FromResult { schema, rows } = from_result;
        let evaluator = CombinedExpressionEvaluator::with_database(&schema, self.database);

        // Scan all rows and filter with WHERE clause
        let mut result_rows: Vec<RowWithSortKeys> = Vec::new();

        for row in rows.into_iter() {
            // Apply WHERE filter
            let include_row = if let Some(where_expr) = &stmt.where_clause {
                match evaluator.eval(where_expr, &row)? {
                    types::SqlValue::Boolean(true) => true,
                    types::SqlValue::Boolean(false) | types::SqlValue::Null => false,
                    other => {
                        return Err(ExecutorError::InvalidWhereClause(format!(
                            "WHERE clause must evaluate to boolean, got: {:?}",
                            other
                        )))
                    }
                }
            } else {
                true // No WHERE clause, include all rows
            };

            if include_row {
                result_rows.push((row, None));
            }
        }

        // Apply ORDER BY if present
        if let Some(order_by) = &stmt.order_by {
            // Evaluate ORDER BY expressions for each row
            for (row, sort_keys) in &mut result_rows {
                let mut keys = Vec::new();
                for order_item in order_by {
                    let key_value = evaluator.eval(&order_item.expr, row)?;
                    keys.push((key_value, order_item.direction.clone()));
                }
                *sort_keys = Some(keys);
            }

            // Sort by the evaluated keys
            result_rows.sort_by(|(_, keys_a), (_, keys_b)| {
                let keys_a = keys_a.as_ref().unwrap();
                let keys_b = keys_b.as_ref().unwrap();

                for ((val_a, dir), (val_b, _)) in keys_a.iter().zip(keys_b.iter()) {
                    let cmp = match dir {
                        ast::OrderDirection::Asc => compare_sql_values(val_a, val_b),
                        ast::OrderDirection::Desc => compare_sql_values(val_a, val_b).reverse(),
                    };

                    if cmp != Ordering::Equal {
                        return cmp;
                    }
                }
                Ordering::Equal
            });
        }

        // Project columns from the sorted rows
        let mut final_rows = Vec::new();
        for (row, _) in result_rows {
            let projected_row = project_row_combined(&row, &stmt.select_list, &evaluator)?;
            final_rows.push(projected_row);
        }

        // Apply DISTINCT if specified
        let final_rows = if stmt.distinct {
            apply_distinct(final_rows)
        } else {
            final_rows
        };

        Ok(apply_limit_offset(final_rows, stmt.limit, stmt.offset))
    }

    /// Execute a FROM clause (table or join) and return combined schema and rows
    fn execute_from(&self, from: &ast::FromClause) -> Result<FromResult, ExecutorError> {
        match from {
            ast::FromClause::Table { name, alias: _ } => {
                // Simple table scan
                let table = self
                    .database
                    .get_table(name)
                    .ok_or_else(|| ExecutorError::TableNotFound(name.clone()))?;

                let schema = CombinedSchema::from_table(name.clone(), table.schema.clone());
                let rows = table.scan().to_vec();

                Ok(FromResult { schema, rows })
            }
            ast::FromClause::Join { left, right, join_type, condition } => {
                // Execute left and right sides
                let left_result = self.execute_from(left)?;
                let right_result = self.execute_from(right)?;

                // Perform nested loop join
                nested_loop_join(left_result, right_result, join_type, condition, self.database)
            }
            ast::FromClause::Subquery { query, alias } => {
                // Execute subquery to get rows
                let rows = self.execute(query)?;

                // Derive schema from SELECT list
                let mut column_names = Vec::new();
                let mut column_types = Vec::new();

                for (i, item) in query.select_list.iter().enumerate() {
                    match item {
                        ast::SelectItem::Wildcard => {
                            return Err(ExecutorError::UnsupportedFeature(
                                "SELECT * not yet supported in derived tables".to_string(),
                            ));
                        }
                        ast::SelectItem::Expression { expr: _, alias: col_alias } => {
                            // Use alias if provided, otherwise generate column name
                            let col_name = if let Some(a) = col_alias {
                                a.clone()
                            } else {
                                format!("column{}", i + 1)
                            };
                            column_names.push(col_name);

                            // Infer type from first row if available
                            let col_type = if let Some(first_row) = rows.first() {
                                if i < first_row.values.len() {
                                    first_row.values[i].get_type()
                                } else {
                                    types::DataType::Null
                                }
                            } else {
                                types::DataType::Null
                            };
                            column_types.push(col_type);
                        }
                    }
                }

                // Create schema with table alias
                let schema = CombinedSchema::from_derived_table(
                    alias.clone(),
                    column_names,
                    column_types,
                );

                Ok(FromResult { schema, rows })
            }
        }
    }

    /// Evaluate an expression in the context of aggregation
    #[allow(clippy::only_used_in_recursion)]
    fn evaluate_with_aggregates(
        &self,
        expr: &ast::Expression,
        group_rows: &[storage::Row],
        _group_key: &[types::SqlValue],
        evaluator: &ExpressionEvaluator,
    ) -> Result<types::SqlValue, ExecutorError> {
        match expr {
            // Aggregate function
            ast::Expression::Function { name, args } => {
                let mut acc = AggregateAccumulator::new(name)?;

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

            // Column reference - use value from group key
            ast::Expression::ColumnRef { .. } => {
                // For GROUP BY columns, use the first row's value (they're all the same)
                if let Some(first_row) = group_rows.first() {
                    evaluator.eval(expr, first_row)
                } else {
                    Ok(types::SqlValue::Null)
                }
            }

            // Binary operation
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

            // Literal
            ast::Expression::Literal(val) => Ok(val.clone()),

            _ => Err(ExecutorError::UnsupportedExpression(format!(
                "Unsupported expression in aggregate context: {:?}",
                expr
            ))),
        }
    }
}

/// Apply DISTINCT to remove duplicate rows
///
/// Uses a HashSet to track unique rows. This requires SqlValue to implement
/// Hash and Eq, which we've implemented with SQL semantics:
/// - NULL == NULL for grouping
/// - NaN == NaN for grouping
fn apply_distinct(rows: Vec<storage::Row>) -> Vec<storage::Row> {
    let mut seen = HashSet::new();
    let mut result = Vec::new();

    for row in rows {
        // Try to insert the row's values into the set
        // If insertion succeeds (wasn't already present), keep the row
        if seen.insert(row.values.clone()) {
            result.push(row);
        }
    }

    result
}

fn apply_limit_offset(
    rows: Vec<storage::Row>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Vec<storage::Row> {
    let start = offset.unwrap_or(0);
    if start >= rows.len() {
        return Vec::new();
    }

    let max_take = rows.len() - start;
    let take = limit.unwrap_or(max_take).min(max_take);

    rows.into_iter().skip(start).take(take).collect()
}
