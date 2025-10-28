use std::collections::HashMap;

use crate::errors::ExecutorError;
use crate::evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator};

mod cte;
mod filter;
mod grouping;
mod helpers;
mod join;
mod order;
mod projection;
mod scan;
mod set_operations;
mod window;

use cte::{execute_ctes, CteResult};
use filter::{apply_where_filter_basic, apply_where_filter_combined};
use grouping::{group_rows, AggregateAccumulator};
use helpers::{apply_distinct, apply_limit_offset};
use join::FromResult;
use order::{apply_order_by, RowWithSortKeys};
use projection::project_row_combined;
use scan::execute_from_clause;
use set_operations::apply_set_operation;
use window::{evaluate_window_functions, has_window_functions};

/// Result of a SELECT query including column metadata
pub struct SelectResult {
    /// Column names derived from the SELECT list
    pub columns: Vec<String>,
    /// Result rows
    pub rows: Vec<storage::Row>,
}

/// Executes SELECT queries
pub struct SelectExecutor<'a> {
    database: &'a storage::Database,
    outer_row: Option<&'a storage::Row>,
    outer_schema: Option<&'a catalog::TableSchema>,
}

impl<'a> SelectExecutor<'a> {
    /// Create a new SELECT executor
    pub fn new(database: &'a storage::Database) -> Self {
        SelectExecutor {
            database,
            outer_row: None,
            outer_schema: None,
        }
    }

    /// Create a new SELECT executor with outer context for correlated subqueries
    pub fn new_with_outer_context(
        database: &'a storage::Database,
        outer_row: &'a storage::Row,
        outer_schema: &'a catalog::TableSchema,
    ) -> Self {
        SelectExecutor {
            database,
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
        }
    }

    /// Execute a SELECT statement
    pub fn execute(&self, stmt: &ast::SelectStmt) -> Result<Vec<storage::Row>, ExecutorError> {
        // Execute CTEs if present
        let cte_results = if let Some(with_clause) = &stmt.with_clause {
            execute_ctes(with_clause, |query, cte_ctx| {
                self.execute_with_ctes(query, cte_ctx)
            })?
        } else {
            HashMap::new()
        };

        // Execute the main query with CTE context
        self.execute_with_ctes(stmt, &cte_results)
    }

    /// Execute a SELECT statement and return both columns and rows
    pub fn execute_with_columns(&self, stmt: &ast::SelectStmt) -> Result<SelectResult, ExecutorError> {
        // First, get the FROM result to access the schema
        let from_result = if let Some(from_clause) = &stmt.from {
            let cte_results = if let Some(with_clause) = &stmt.with_clause {
                execute_ctes(with_clause, |query, cte_ctx| {
                    self.execute_with_ctes(query, cte_ctx)
                })?
            } else {
                HashMap::new()
            };
            Some(self.execute_from(from_clause, &cte_results)?)
        } else {
            None
        };

        // Derive column names from the SELECT list
        let columns = self.derive_column_names(&stmt.select_list, from_result.as_ref())?;

        // Execute the query to get rows
        let rows = self.execute(stmt)?;

        Ok(SelectResult { columns, rows })
    }

    /// Derive column names from SELECT list
    fn derive_column_names(
        &self,
        select_list: &[ast::SelectItem],
        from_result: Option<&FromResult>,
    ) -> Result<Vec<String>, ExecutorError> {
        let mut column_names = Vec::new();

        for item in select_list {
            match item {
                ast::SelectItem::Wildcard => {
                    // SELECT * - expand to all column names from schema
                    if let Some(from_res) = from_result {
                        // Get all column names in order from the combined schema
                        let mut table_columns: Vec<(usize, String)> = Vec::new();

                        for (_table_name, (start_index, schema)) in &from_res.schema.table_schemas {
                            for (col_idx, col_schema) in schema.columns.iter().enumerate() {
                                table_columns.push((start_index + col_idx, col_schema.name.clone()));
                            }
                        }

                        // Sort by index to maintain column order
                        table_columns.sort_by_key(|(idx, _)| *idx);

                        for (_, name) in table_columns {
                            column_names.push(name);
                        }
                    } else {
                        return Err(ExecutorError::UnsupportedFeature(
                            "SELECT * without FROM not supported".to_string(),
                        ));
                    }
                }
                ast::SelectItem::Expression { expr, alias } => {
                    // If there's an alias, use it
                    if let Some(alias_name) = alias {
                        column_names.push(alias_name.clone());
                    } else {
                        // Derive name from the expression
                        column_names.push(self.derive_expression_name(expr));
                    }
                }
            }
        }

        Ok(column_names)
    }

    /// Derive a column name from an expression
    fn derive_expression_name(&self, expr: &ast::Expression) -> String {
        match expr {
            ast::Expression::ColumnRef { table: _, column } => column.clone(),
            ast::Expression::Function { name, args } => {
                // For functions, use name(args) format
                let args_str = if args.is_empty() {
                    "*".to_string()
                } else {
                    args.iter()
                        .map(|arg| self.derive_expression_name(arg))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                format!("{}({})", name, args_str)
            }
            ast::Expression::BinaryOp { left, op, right } => {
                // For binary operations, create descriptive name
                format!(
                    "({} {} {})",
                    self.derive_expression_name(left),
                    match op {
                        ast::BinaryOperator::Plus => "+",
                        ast::BinaryOperator::Minus => "-",
                        ast::BinaryOperator::Multiply => "*",
                        ast::BinaryOperator::Divide => "/",
                        ast::BinaryOperator::Equal => "=",
                        ast::BinaryOperator::NotEqual => "!=",
                        ast::BinaryOperator::LessThan => "<",
                        ast::BinaryOperator::LessThanOrEqual => "<=",
                        ast::BinaryOperator::GreaterThan => ">",
                        ast::BinaryOperator::GreaterThanOrEqual => ">=",
                        ast::BinaryOperator::And => "AND",
                        ast::BinaryOperator::Or => "OR",
                        ast::BinaryOperator::Concat => "||",
                        _ => "?",
                    },
                    self.derive_expression_name(right)
                )
            }
            ast::Expression::Literal(val) => {
                // For literals, use the value representation
                format!("{:?}", val)
            }
            _ => "?column?".to_string(), // Default for other expression types
        }
    }

    /// Execute SELECT statement with CTE context
    fn execute_with_ctes(
        &self,
        stmt: &ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Vec<storage::Row>, ExecutorError> {
        // Execute the left-hand side query
        let has_aggregates = self.has_aggregates(&stmt.select_list) || stmt.having.is_some();
        let has_group_by = stmt.group_by.is_some();

        let mut results = if has_aggregates || has_group_by {
            self.execute_with_aggregation(stmt, cte_results)?
        } else if let Some(from_clause) = &stmt.from {
            let from_result = self.execute_from(from_clause, cte_results)?;
            self.execute_without_aggregation(stmt, from_result)?
        } else {
            // SELECT without FROM - evaluate expressions as a single row
            self.execute_select_without_from(stmt)?
        };

        // Handle set operations (UNION, INTERSECT, EXCEPT)
        if let Some(set_op) = &stmt.set_operation {
            // Execute the right-hand side query
            let right_results = self.execute(&set_op.right)?;

            // Apply the set operation
            results = apply_set_operation(results, right_results, set_op)?;

            // Apply LIMIT/OFFSET to the final combined result
            results = apply_limit_offset(results, stmt.limit, stmt.offset);
        }

        Ok(results)
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
        cte_results: &HashMap<String, CteResult>,
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

        // Check if table is a CTE first, then check database
        let (schema, rows) = if let Some((cte_schema, cte_rows)) = cte_results.get(table_name) {
            // Use CTE result
            (cte_schema.clone(), cte_rows.clone())
        } else {
            // Use database table
            let table = self
                .database
                .get_table(table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;
            (table.schema.clone(), table.scan().to_vec())
        };

        let evaluator = match (self.outer_row, self.outer_schema) {
            (Some(outer_row), Some(outer_schema)) => {
                ExpressionEvaluator::with_database_and_outer_context(
                    &schema,
                    self.database,
                    outer_row,
                    outer_schema,
                )
            }
            _ => ExpressionEvaluator::with_database(&schema, self.database),
        };

        // Scan and filter rows with WHERE clause
        let filtered_rows = apply_where_filter_basic(rows, stmt.where_clause.as_ref(), &evaluator)?;

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
        let result_rows = if stmt.distinct { apply_distinct(result_rows) } else { result_rows };

        // Don't apply LIMIT/OFFSET if we have a set operation - it will be applied later
        if stmt.set_operation.is_some() {
            Ok(result_rows)
        } else {
            Ok(apply_limit_offset(result_rows, stmt.limit, stmt.offset))
        }
    }

    /// Execute SELECT without aggregation
    fn execute_without_aggregation(
        &self,
        stmt: &ast::SelectStmt,
        from_result: FromResult,
    ) -> Result<Vec<storage::Row>, ExecutorError> {
        let FromResult { schema, rows } = from_result;
        let evaluator = CombinedExpressionEvaluator::with_database(&schema, self.database);

        // Apply WHERE clause filter
        let mut filtered_rows = apply_where_filter_combined(rows, stmt.where_clause.as_ref(), &evaluator)?;

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

    /// Execute a FROM clause (table or join) and return combined schema and rows
    fn execute_from(
        &self,
        from: &ast::FromClause,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<FromResult, ExecutorError> {
        execute_from_clause(from, cte_results, self.database, |query| self.execute(query))
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

    /// Execute SELECT without FROM clause
    ///
    /// Evaluates expressions in the SELECT list without any table context.
    /// Returns a single row with the evaluated expressions.
    fn execute_select_without_from(
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

    /// Check if an expression references a column (which requires FROM clause)
    fn expression_references_column(&self, expr: &ast::Expression) -> bool {
        match expr {
            ast::Expression::ColumnRef { .. } => true,

            ast::Expression::BinaryOp { left, right, .. } => {
                self.expression_references_column(left) || self.expression_references_column(right)
            }

            ast::Expression::UnaryOp { expr, .. } => {
                self.expression_references_column(expr)
            }

            ast::Expression::Function { args, .. } => {
                args.iter().any(|arg| self.expression_references_column(arg))
            }

            ast::Expression::IsNull { expr, .. } => {
                self.expression_references_column(expr)
            }

            ast::Expression::InList { expr, values, .. } => {
                self.expression_references_column(expr)
                    || values.iter().any(|v| self.expression_references_column(v))
            }

            ast::Expression::Between { expr, low, high, .. } => {
                self.expression_references_column(expr)
                    || self.expression_references_column(low)
                    || self.expression_references_column(high)
            }

            ast::Expression::Cast { expr, .. } => {
                self.expression_references_column(expr)
            }

            ast::Expression::Position { substring, string } => {
                self.expression_references_column(substring)
                    || self.expression_references_column(string)
            }

            ast::Expression::Like { expr, pattern, .. } => {
                self.expression_references_column(expr)
                    || self.expression_references_column(pattern)
            }

            ast::Expression::In { expr, .. } => {
                // Note: subquery could reference outer columns but that's a different case
                self.expression_references_column(expr)
            }

            ast::Expression::QuantifiedComparison { expr, .. } => {
                self.expression_references_column(expr)
            }

            ast::Expression::Case { operand, when_clauses, else_result } => {
                operand.as_ref().map_or(false, |e| self.expression_references_column(e))
                    || when_clauses.iter().any(|(cond, res)| {
                        self.expression_references_column(cond) || self.expression_references_column(res)
                    })
                    || else_result.as_ref().map_or(false, |e| self.expression_references_column(e))
            }

            ast::Expression::WindowFunction { function, over } => {
                // Check window function arguments
                let args_reference_column = match function {
                    ast::WindowFunctionSpec::Aggregate { args, .. }
                    | ast::WindowFunctionSpec::Ranking { args, .. }
                    | ast::WindowFunctionSpec::Value { args, .. } => {
                        args.iter().any(|arg| self.expression_references_column(arg))
                    }
                };

                // Check PARTITION BY and ORDER BY clauses
                let partition_references = over.partition_by.as_ref().map_or(false, |exprs| {
                    exprs.iter().any(|e| self.expression_references_column(e))
                });

                let order_references = over.order_by.as_ref().map_or(false, |items| {
                    items.iter().any(|item| self.expression_references_column(&item.expr))
                });

                args_reference_column || partition_references || order_references
            }

            // These don't contain column references:
            ast::Expression::Literal(_) => false,
            ast::Expression::Wildcard => false,
            ast::Expression::ScalarSubquery(_) => false, // Subquery has its own scope
            ast::Expression::Exists { .. } => false,     // Subquery has its own scope
        }
    }
}
