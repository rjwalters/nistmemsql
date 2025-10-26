use std::cmp::Ordering;

use crate::errors::ExecutorError;
use crate::evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator};
use crate::schema::CombinedSchema;

/// Result of executing a FROM clause
struct FromResult {
    schema: CombinedSchema,
    rows: Vec<storage::Row>,
}

/// Accumulator for aggregate functions
#[derive(Debug, Clone)]
enum AggregateAccumulator {
    Count(i64),
    Sum(i64),
    Avg { sum: i64, count: i64 },
    Min(Option<types::SqlValue>),
    Max(Option<types::SqlValue>),
}

impl AggregateAccumulator {
    fn new(function_name: &str) -> Result<Self, ExecutorError> {
        match function_name.to_uppercase().as_str() {
            "COUNT" => Ok(AggregateAccumulator::Count(0)),
            "SUM" => Ok(AggregateAccumulator::Sum(0)),
            "AVG" => Ok(AggregateAccumulator::Avg { sum: 0, count: 0 }),
            "MIN" => Ok(AggregateAccumulator::Min(None)),
            "MAX" => Ok(AggregateAccumulator::Max(None)),
            _ => Err(ExecutorError::UnsupportedExpression(format!(
                "Unknown aggregate function: {}",
                function_name
            ))),
        }
    }

    fn accumulate(&mut self, value: &types::SqlValue) {
        match (self, value) {
            // COUNT - counts non-NULL values
            (AggregateAccumulator::Count(_), types::SqlValue::Null) => {}
            (AggregateAccumulator::Count(ref mut count), _) => {
                *count += 1;
            }

            // SUM - sums integer values, ignores NULLs
            (AggregateAccumulator::Sum(ref mut sum), types::SqlValue::Integer(val)) => {
                *sum += val;
            }
            (AggregateAccumulator::Sum(_), types::SqlValue::Null) => {}

            // AVG - computes average of integer values, ignores NULLs
            (
                AggregateAccumulator::Avg { ref mut sum, ref mut count },
                types::SqlValue::Integer(val),
            ) => {
                *sum += val;
                *count += 1;
            }
            (AggregateAccumulator::Avg { .. }, types::SqlValue::Null) => {}

            // MIN - finds minimum value, ignores NULLs
            (AggregateAccumulator::Min(ref mut current_min), val @ types::SqlValue::Integer(_))
            | (AggregateAccumulator::Min(ref mut current_min), val @ types::SqlValue::Varchar(_))
            | (AggregateAccumulator::Min(ref mut current_min), val @ types::SqlValue::Boolean(_)) => {
                if let Some(ref current) = current_min {
                    if compare_sql_values(val, current) == Ordering::Less {
                        *current_min = Some(val.clone());
                    }
                } else {
                    *current_min = Some(val.clone());
                }
            }
            (AggregateAccumulator::Min(_), types::SqlValue::Null) => {}

            // MAX - finds maximum value, ignores NULLs
            (AggregateAccumulator::Max(ref mut current_max), val @ types::SqlValue::Integer(_))
            | (AggregateAccumulator::Max(ref mut current_max), val @ types::SqlValue::Varchar(_))
            | (AggregateAccumulator::Max(ref mut current_max), val @ types::SqlValue::Boolean(_)) => {
                if let Some(ref current) = current_max {
                    if compare_sql_values(val, current) == Ordering::Greater {
                        *current_max = Some(val.clone());
                    }
                } else {
                    *current_max = Some(val.clone());
                }
            }
            (AggregateAccumulator::Max(_), types::SqlValue::Null) => {}

            _ => {
                // Type mismatch or unsupported type - ignore for now
            }
        }
    }

    fn finalize(&self) -> types::SqlValue {
        match self {
            AggregateAccumulator::Count(count) => types::SqlValue::Integer(*count),
            AggregateAccumulator::Sum(sum) => types::SqlValue::Integer(*sum),
            AggregateAccumulator::Avg { sum, count } => {
                if *count == 0 {
                    types::SqlValue::Null
                } else {
                    types::SqlValue::Integer(sum / count)
                }
            }
            AggregateAccumulator::Min(val) => val.clone().unwrap_or(types::SqlValue::Null),
            AggregateAccumulator::Max(val) => val.clone().unwrap_or(types::SqlValue::Null),
        }
    }
}

/// Compare two SqlValues for ordering purposes
fn compare_sql_values(a: &types::SqlValue, b: &types::SqlValue) -> Ordering {
    use types::SqlValue::*;

    match (a, b) {
        // Integer comparison
        (Integer(x), Integer(y)) => x.cmp(y),

        // String comparison
        (Varchar(x), Varchar(y)) => x.cmp(y),

        // Boolean comparison
        (Boolean(x), Boolean(y)) => x.cmp(y),

        // NULL handling - NULL is considered "less than" any non-NULL value
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,

        // Type mismatch - shouldn't happen with proper type checking
        // Return Equal to maintain stability in sort
        _ => Ordering::Equal,
    }
}

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

        let evaluator = ExpressionEvaluator::new(&table.schema);

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
            self.group_rows(&filtered_rows, group_by_exprs, &evaluator)?
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

        Ok(apply_limit_offset(result_rows, stmt.limit, stmt.offset))
    }

    /// Execute SELECT without aggregation
    fn execute_without_aggregation(
        &self,
        stmt: &ast::SelectStmt,
        from_result: FromResult,
    ) -> Result<Vec<storage::Row>, ExecutorError> {
        let FromResult { schema, rows } = from_result;
        let evaluator = CombinedExpressionEvaluator::new(&schema);

        // Scan all rows and filter with WHERE clause
        let mut result_rows: Vec<(
            storage::Row,
            Option<Vec<(types::SqlValue, ast::OrderDirection)>>,
        )> = Vec::new();

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
            let projected_row = self.project_row_combined(&row, &stmt.select_list, &evaluator)?;
            final_rows.push(projected_row);
        }

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
                self.nested_loop_join(left_result, right_result, join_type, condition)
            }
        }
    }

    /// Perform nested loop join between two FROM results
    fn nested_loop_join(
        &self,
        left: FromResult,
        right: FromResult,
        join_type: &ast::JoinType,
        condition: &Option<ast::Expression>,
    ) -> Result<FromResult, ExecutorError> {
        match join_type {
            ast::JoinType::Inner => self.nested_loop_inner_join(left, right, condition),
            ast::JoinType::LeftOuter => self.nested_loop_left_outer_join(left, right, condition),
            _ => Err(ExecutorError::UnsupportedFeature(format!(
                "JOIN type {:?} not yet implemented",
                join_type
            ))),
        }
    }

    /// Group rows by GROUP BY expressions
    fn group_rows(
        &self,
        rows: &[storage::Row],
        group_by_exprs: &[ast::Expression],
        evaluator: &ExpressionEvaluator,
    ) -> Result<Vec<(Vec<types::SqlValue>, Vec<storage::Row>)>, ExecutorError> {
        let mut groups: Vec<(Vec<types::SqlValue>, Vec<storage::Row>)> = Vec::new();

        for row in rows {
            // Evaluate GROUP BY expressions to get the group key
            let mut key = Vec::new();
            for expr in group_by_exprs {
                let value = evaluator.eval(expr, row)?;
                key.push(value);
            }

            // Find existing group or create new one
            let mut found = false;
            for (existing_key, group_rows) in &mut groups {
                if existing_key == &key {
                    group_rows.push(row.clone());
                    found = true;
                    break;
                }
            }

            if !found {
                groups.push((key, vec![row.clone()]));
            }
        }

        Ok(groups)
    }

    /// Evaluate an expression in the context of aggregation
    fn evaluate_with_aggregates(
        &self,
        expr: &ast::Expression,
        group_rows: &[storage::Row],
        group_key: &[types::SqlValue],
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
                    self.evaluate_with_aggregates(left, group_rows, group_key, evaluator)?;
                let right_val =
                    self.evaluate_with_aggregates(right, group_rows, group_key, evaluator)?;

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

    /// Nested loop INNER JOIN implementation
    fn nested_loop_inner_join(
        &self,
        left: FromResult,
        right: FromResult,
        condition: &Option<ast::Expression>,
    ) -> Result<FromResult, ExecutorError> {
        // Extract right table name (assume single table for now)
        let right_table_name = right
            .schema
            .table_schemas
            .keys()
            .next()
            .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
            .clone();

        let right_schema = right
            .schema
            .table_schemas
            .get(&right_table_name)
            .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
            .1
            .clone();

        // Combine schemas
        let combined_schema = CombinedSchema::combine(left.schema, right_table_name, right_schema);
        let evaluator = CombinedExpressionEvaluator::new(&combined_schema);

        // Nested loop join algorithm
        let mut result_rows = Vec::new();
        for left_row in &left.rows {
            for right_row in &right.rows {
                // Concatenate rows
                let mut combined_values = left_row.values.clone();
                combined_values.extend(right_row.values.clone());
                let combined_row = storage::Row::new(combined_values);

                // Evaluate join condition
                let matches = if let Some(cond) = condition {
                    match evaluator.eval(cond, &combined_row)? {
                        types::SqlValue::Boolean(true) => true,
                        types::SqlValue::Boolean(false) => false,
                        types::SqlValue::Null => false,
                        other => {
                            return Err(ExecutorError::InvalidWhereClause(format!(
                                "JOIN condition must evaluate to boolean, got: {:?}",
                                other
                            )))
                        }
                    }
                } else {
                    true // No condition = CROSS JOIN
                };

                if matches {
                    result_rows.push(combined_row);
                }
            }
        }

        Ok(FromResult { schema: combined_schema, rows: result_rows })
    }

    /// Nested loop LEFT OUTER JOIN implementation
    fn nested_loop_left_outer_join(
        &self,
        left: FromResult,
        right: FromResult,
        condition: &Option<ast::Expression>,
    ) -> Result<FromResult, ExecutorError> {
        // Extract right table name and schema
        let right_table_name = right
            .schema
            .table_schemas
            .keys()
            .next()
            .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
            .clone();

        let right_schema = right
            .schema
            .table_schemas
            .get(&right_table_name)
            .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
            .1
            .clone();

        let right_column_count = right_schema.columns.len();

        // Combine schemas
        let combined_schema = CombinedSchema::combine(left.schema, right_table_name, right_schema);
        let evaluator = CombinedExpressionEvaluator::new(&combined_schema);

        // Nested loop LEFT OUTER JOIN algorithm
        let mut result_rows = Vec::new();
        for left_row in &left.rows {
            let mut matched = false;

            for right_row in &right.rows {
                // Concatenate rows
                let mut combined_values = left_row.values.clone();
                combined_values.extend(right_row.values.clone());
                let combined_row = storage::Row::new(combined_values);

                // Evaluate join condition
                let matches = if let Some(cond) = condition {
                    match evaluator.eval(cond, &combined_row)? {
                        types::SqlValue::Boolean(true) => true,
                        types::SqlValue::Boolean(false) => false,
                        types::SqlValue::Null => false,
                        other => {
                            return Err(ExecutorError::InvalidWhereClause(format!(
                                "JOIN condition must evaluate to boolean, got: {:?}",
                                other
                            )))
                        }
                    }
                } else {
                    true // No condition = CROSS JOIN
                };

                if matches {
                    result_rows.push(combined_row);
                    matched = true;
                }
            }

            // If no match found, add left row with NULLs for right columns
            if !matched {
                let mut combined_values = left_row.values.clone();
                // Add NULL values for all right table columns
                combined_values.extend(vec![types::SqlValue::Null; right_column_count]);
                result_rows.push(storage::Row::new(combined_values));
            }
        }

        Ok(FromResult { schema: combined_schema, rows: result_rows })
    }

    /// Project columns from a row based on SELECT list (combined schema version)
    fn project_row_combined(
        &self,
        row: &storage::Row,
        columns: &[ast::SelectItem],
        evaluator: &CombinedExpressionEvaluator,
    ) -> Result<storage::Row, ExecutorError> {
        let mut values = Vec::new();

        for item in columns {
            match item {
                ast::SelectItem::Wildcard => {
                    // SELECT * - include all columns
                    values.extend(row.values.iter().cloned());
                }
                ast::SelectItem::Expression { expr, alias: _ } => {
                    // Evaluate the expression
                    let value = evaluator.eval(expr, row)?;
                    values.push(value);
                }
            }
        }

        Ok(storage::Row::new(values))
    }
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
