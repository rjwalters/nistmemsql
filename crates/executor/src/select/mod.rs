use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

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

/// CTE result: (schema, rows)
type CteResult = (catalog::TableSchema, Vec<storage::Row>);

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
            self.execute_ctes(with_clause)?
        } else {
            HashMap::new()
        };

        // Execute the main query with CTE context
        self.execute_with_ctes(stmt, &cte_results)
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
        } else {
            let from_clause = stmt.from.as_ref().ok_or_else(|| {
                ExecutorError::UnsupportedFeature(
                    "SELECT without FROM not yet implemented".to_string(),
                )
            })?;
            let from_result = self.execute_from(from_clause, cte_results)?;
            self.execute_without_aggregation(stmt, from_result)?
        };

        // Handle set operations (UNION, INTERSECT, EXCEPT)
        if let Some(set_op) = &stmt.set_operation {
            // Execute the right-hand side query
            let right_results = self.execute(&set_op.right)?;

            // Apply the set operation
            results = self.apply_set_operation(results, right_results, set_op)?;

            // Apply LIMIT/OFFSET to the final combined result
            results = apply_limit_offset(results, stmt.limit, stmt.offset);
        }

        Ok(results)
    }

    /// Execute all CTEs and return their results
    fn execute_ctes(
        &self,
        ctes: &[ast::CommonTableExpr],
    ) -> Result<HashMap<String, CteResult>, ExecutorError> {
        let mut cte_results = HashMap::new();

        // Execute each CTE in order
        // CTEs can reference previously defined CTEs
        for cte in ctes {
            // Execute the CTE query with accumulated CTE results so far
            // This allows later CTEs to reference earlier ones
            let rows = self.execute_with_ctes(&cte.query, &cte_results)?;

            //  Determine the schema for this CTE
            let schema = self.derive_cte_schema(cte, &rows)?;

            // Store the CTE result
            cte_results.insert(cte.name.clone(), (schema, rows));
        }

        Ok(cte_results)
    }

    /// Derive the schema for a CTE from its query and results
    fn derive_cte_schema(
        &self,
        cte: &ast::CommonTableExpr,
        rows: &[storage::Row],
    ) -> Result<catalog::TableSchema, ExecutorError> {
        // If column names are explicitly specified, use those
        if let Some(column_names) = &cte.columns {
            // Get data types from first row (if available)
            if let Some(first_row) = rows.first() {
                if first_row.values.len() != column_names.len() {
                    return Err(ExecutorError::UnsupportedFeature(
                        format!(
                            "CTE column count mismatch: specified {} columns but query returned {}",
                            column_names.len(),
                            first_row.values.len()
                        ),
                    ));
                }

                let columns = column_names
                    .iter()
                    .zip(&first_row.values)
                    .map(|(name, value)| {
                        let data_type = Self::infer_type_from_value(value);
                        catalog::ColumnSchema::new(name.clone(), data_type, true) // nullable for simplicity
                    })
                    .collect();

                Ok(catalog::TableSchema::new(cte.name.clone(), columns))
            } else {
                // Empty result set - create schema with VARCHAR columns
                let columns = column_names
                    .iter()
                    .map(|name| {
                        catalog::ColumnSchema::new(
                            name.clone(),
                            types::DataType::Varchar { max_length: 255 },
                            true,
                        )
                    })
                    .collect();

                Ok(catalog::TableSchema::new(cte.name.clone(), columns))
            }
        } else {
            // No explicit column names - infer from query SELECT list
            if let Some(first_row) = rows.first() {
                let columns = cte
                    .query
                    .select_list
                    .iter()
                    .enumerate()
                    .map(|(i, item)| {
                        let data_type = Self::infer_type_from_value(&first_row.values[i]);

                        // Extract column name from SELECT item
                        let col_name = match item {
                            ast::SelectItem::Wildcard => format!("col{}", i),
                            ast::SelectItem::Expression { expr, alias } => {
                                if let Some(a) = alias {
                                    a.clone()
                                } else {
                                    // Try to extract name from expression
                                    match expr {
                                        ast::Expression::ColumnRef { table: _, column } => column.clone(),
                                        _ => format!("col{}", i),
                                    }
                                }
                            }
                        };

                        catalog::ColumnSchema::new(col_name, data_type, true) // nullable
                    })
                    .collect();

                Ok(catalog::TableSchema::new(cte.name.clone(), columns))
            } else {
                // Empty CTE with no column specification
                Err(ExecutorError::UnsupportedFeature(
                    "Cannot infer schema for empty CTE without column list".to_string(),
                ))
            }
        }
    }

    /// Infer data type from a SQL value
    fn infer_type_from_value(value: &types::SqlValue) -> types::DataType {
        match value {
            types::SqlValue::Null => types::DataType::Varchar { max_length: 255 }, // default
            types::SqlValue::Integer(_) => types::DataType::Integer,
            types::SqlValue::Varchar(_) => types::DataType::Varchar { max_length: 255 },
            types::SqlValue::Character(_) => types::DataType::Character { length: 1 },
            types::SqlValue::Boolean(_) => types::DataType::Boolean,
            types::SqlValue::Float(_) => types::DataType::Float,
            types::SqlValue::Double(_) => types::DataType::DoublePrecision,
            types::SqlValue::Numeric(_) => types::DataType::Numeric {
                precision: 10,
                scale: 2,
            },
            types::SqlValue::Real(_) => types::DataType::Real,
            types::SqlValue::Smallint(_) => types::DataType::Smallint,
            types::SqlValue::Bigint(_) => types::DataType::Bigint,
            types::SqlValue::Date(_) => types::DataType::Varchar { max_length: 255 }, // TODO: proper date type
            types::SqlValue::Time(_) => types::DataType::Varchar { max_length: 255 }, // TODO: proper time type
            types::SqlValue::Timestamp(_) => types::DataType::Varchar { max_length: 255 }, // TODO: proper timestamp type
            types::SqlValue::Interval(_) => types::DataType::Varchar { max_length: 255 }, // TODO: proper interval type
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

        // Scan and filter rows
        let mut filtered_rows = Vec::new();
        for row in &rows {
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
        match from {
            ast::FromClause::Table { name, alias } => {
                // Check if table is a CTE first, then check database
                if let Some((cte_schema, cte_rows)) = cte_results.get(name) {
                    // Use CTE result
                    let table_name = alias.as_ref().unwrap_or(name);
                    let schema = CombinedSchema::from_table(table_name.clone(), cte_schema.clone());
                    let rows = cte_rows.clone();
                    Ok(FromResult { schema, rows })
                } else {
                    // Use database table
                    let table = self
                        .database
                        .get_table(name)
                        .ok_or_else(|| ExecutorError::TableNotFound(name.clone()))?;

                    let table_name = alias.as_ref().unwrap_or(name);
                    let schema = CombinedSchema::from_table(table_name.clone(), table.schema.clone());
                    let rows = table.scan().to_vec();

                    Ok(FromResult { schema, rows })
                }
            }
            ast::FromClause::Join { left, right, join_type, condition } => {
                // Execute left and right sides
                let left_result = self.execute_from(left, cte_results)?;
                let right_result = self.execute_from(right, cte_results)?;

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
                let schema =
                    CombinedSchema::from_derived_table(alias.clone(), column_names, column_types);

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

    /// Apply a set operation (UNION, INTERSECT, EXCEPT) to two result sets
    fn apply_set_operation(
        &self,
        left: Vec<storage::Row>,
        right: Vec<storage::Row>,
        set_op: &ast::SetOperation,
    ) -> Result<Vec<storage::Row>, ExecutorError> {
        // Validate that both result sets have the same number of columns
        if !left.is_empty() && !right.is_empty() {
            let left_cols = left[0].values.len();
            let right_cols = right[0].values.len();
            if left_cols != right_cols {
                return Err(ExecutorError::SubqueryColumnCountMismatch {
                    expected: left_cols,
                    actual: right_cols,
                });
            }
        }

        match set_op.op {
            ast::SetOperator::Union => {
                if set_op.all {
                    // UNION ALL: combine all rows from both queries
                    let mut result = left;
                    result.extend(right);
                    Ok(result)
                } else {
                    // UNION (DISTINCT): combine and remove duplicates
                    let mut result = left;
                    result.extend(right);
                    Ok(apply_distinct(result))
                }
            }

            ast::SetOperator::Intersect => {
                if set_op.all {
                    // INTERSECT ALL: return rows that appear in both (with multiplicity)
                    // Count occurrences in right side
                    let mut right_counts = std::collections::HashMap::new();
                    for row in &right {
                        *right_counts.entry(row.values.clone()).or_insert(0) += 1;
                    }

                    // For each left row, if it appears in right, include it and decrement count
                    let mut result = Vec::new();
                    for row in left {
                        if let Some(count) = right_counts.get_mut(&row.values) {
                            if *count > 0 {
                                result.push(row);
                                *count -= 1;
                            }
                        }
                    }
                    Ok(result)
                } else {
                    // INTERSECT (DISTINCT): return unique rows that appear in both
                    let right_set: std::collections::HashSet<_> =
                        right.iter().map(|row| row.values.clone()).collect();

                    let mut result = Vec::new();
                    let mut seen = std::collections::HashSet::new();
                    for row in left {
                        if right_set.contains(&row.values) && seen.insert(row.values.clone()) {
                            result.push(row);
                        }
                    }
                    Ok(result)
                }
            }

            ast::SetOperator::Except => {
                if set_op.all {
                    // EXCEPT ALL: return rows from left that don't appear in right (with multiplicity)
                    // Count occurrences in right side
                    let mut right_counts = std::collections::HashMap::new();
                    for row in &right {
                        *right_counts.entry(row.values.clone()).or_insert(0) += 1;
                    }

                    // For each left row, if it doesn't appear in right (or count exhausted), include it
                    let mut result = Vec::new();
                    for row in left {
                        match right_counts.get_mut(&row.values) {
                            None => {
                                // Row not in right side, include it
                                result.push(row);
                            }
                            Some(count) if *count == 0 => {
                                // All instances from right side already used, include it
                                result.push(row);
                            }
                            Some(count) => {
                                // Row exists in right side, decrement count (exclude this instance)
                                *count -= 1;
                            }
                        }
                    }
                    Ok(result)
                } else {
                    // EXCEPT (DISTINCT): return unique rows from left that don't appear in right
                    let right_set: std::collections::HashSet<_> =
                        right.iter().map(|row| row.values.clone()).collect();

                    let mut result = Vec::new();
                    let mut seen = std::collections::HashSet::new();
                    for row in left {
                        if !right_set.contains(&row.values) && seen.insert(row.values.clone()) {
                            result.push(row);
                        }
                    }
                    Ok(result)
                }
            }
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
