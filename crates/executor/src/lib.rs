//! Executor - SQL Query Execution Engine
//!
//! This crate provides query execution functionality for SQL statements.

// ============================================================================
// Expression Evaluator
// ============================================================================

/// Evaluates expressions in the context of a row
pub struct ExpressionEvaluator<'a> {
    schema: &'a catalog::TableSchema,
}

impl<'a> ExpressionEvaluator<'a> {
    /// Create a new expression evaluator for a given schema
    pub fn new(schema: &'a catalog::TableSchema) -> Self {
        ExpressionEvaluator { schema }
    }

    /// Evaluate an expression in the context of a row
    pub fn eval(
        &self,
        expr: &ast::Expression,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        match expr {
            // Literals - just return the value
            ast::Expression::Literal(val) => Ok(val.clone()),

            // Column reference - look up column index and get value from row
            ast::Expression::ColumnRef { table: _, column } => {
                let col_index = self
                    .schema
                    .get_column_index(column)
                    .ok_or_else(|| ExecutorError::ColumnNotFound(column.clone()))?;
                row.get(col_index)
                    .cloned()
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index })
            }

            // Binary operations
            ast::Expression::BinaryOp { left, op, right } => {
                let left_val = self.eval(left, row)?;
                let right_val = self.eval(right, row)?;
                self.eval_binary_op(&left_val, op, &right_val)
            }

            // TODO: Implement other expression types
            _ => Err(ExecutorError::UnsupportedExpression(format!("{:?}", expr))),
        }
    }

    /// Evaluate a binary operation
    pub fn eval_binary_op(
        &self,
        left: &types::SqlValue,
        op: &ast::BinaryOperator,
        right: &types::SqlValue,
    ) -> Result<types::SqlValue, ExecutorError> {
        use ast::BinaryOperator::*;
        use types::SqlValue::*;

        match (left, op, right) {
            // Integer arithmetic
            (Integer(a), Plus, Integer(b)) => Ok(Integer(a + b)),
            (Integer(a), Minus, Integer(b)) => Ok(Integer(a - b)),
            (Integer(a), Multiply, Integer(b)) => Ok(Integer(a * b)),
            (Integer(a), Divide, Integer(b)) => {
                if *b == 0 {
                    Err(ExecutorError::DivisionByZero)
                } else {
                    Ok(Integer(a / b))
                }
            }

            // Integer comparisons
            (Integer(a), Equal, Integer(b)) => Ok(Boolean(a == b)),
            (Integer(a), NotEqual, Integer(b)) => Ok(Boolean(a != b)),
            (Integer(a), LessThan, Integer(b)) => Ok(Boolean(a < b)),
            (Integer(a), LessThanOrEqual, Integer(b)) => Ok(Boolean(a <= b)),
            (Integer(a), GreaterThan, Integer(b)) => Ok(Boolean(a > b)),
            (Integer(a), GreaterThanOrEqual, Integer(b)) => Ok(Boolean(a >= b)),

            // String comparisons
            (Varchar(a), Equal, Varchar(b)) => Ok(Boolean(a == b)),
            (Varchar(a), NotEqual, Varchar(b)) => Ok(Boolean(a != b)),

            // Boolean logic
            (Boolean(a), And, Boolean(b)) => Ok(Boolean(*a && *b)),
            (Boolean(a), Or, Boolean(b)) => Ok(Boolean(*a || *b)),

            // NULL comparisons - NULL compared to anything is NULL (three-valued logic)
            (Null, _, _) | (_, _, Null) => Ok(Null),

            // Type mismatch
            _ => Err(ExecutorError::TypeMismatch {
                left: left.clone(),
                op: format!("{:?}", op),
                right: right.clone(),
            }),
        }
    }
}

// ============================================================================
// Aggregate Functions
// ============================================================================

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
            (AggregateAccumulator::Count(_), types::SqlValue::Null) => {
                // Don't count NULLs
            }
            (AggregateAccumulator::Count(ref mut count), _) => {
                *count += 1;
            }

            // SUM - sums integer values, ignores NULLs
            (AggregateAccumulator::Sum(ref mut sum), types::SqlValue::Integer(val)) => {
                *sum += val;
            }
            (AggregateAccumulator::Sum(_), types::SqlValue::Null) => {
                // Ignore NULLs
            }

            // AVG - computes average of integer values, ignores NULLs
            (
                AggregateAccumulator::Avg { ref mut sum, ref mut count },
                types::SqlValue::Integer(val),
            ) => {
                *sum += val;
                *count += 1;
            }
            (AggregateAccumulator::Avg { .. }, types::SqlValue::Null) => {
                // Ignore NULLs
            }

            // MIN - finds minimum value, ignores NULLs
            (AggregateAccumulator::Min(ref mut current_min), val @ types::SqlValue::Integer(_))
            | (AggregateAccumulator::Min(ref mut current_min), val @ types::SqlValue::Varchar(_))
            | (AggregateAccumulator::Min(ref mut current_min), val @ types::SqlValue::Boolean(_)) => {
                if let Some(ref current) = current_min {
                    if compare_sql_values(val, current) == std::cmp::Ordering::Less {
                        *current_min = Some(val.clone());
                    }
                } else {
                    *current_min = Some(val.clone());
                }
            }
            (AggregateAccumulator::Min(_), types::SqlValue::Null) => {
                // Ignore NULLs
            }

            // MAX - finds maximum value, ignores NULLs
            (AggregateAccumulator::Max(ref mut current_max), val @ types::SqlValue::Integer(_))
            | (AggregateAccumulator::Max(ref mut current_max), val @ types::SqlValue::Varchar(_))
            | (AggregateAccumulator::Max(ref mut current_max), val @ types::SqlValue::Boolean(_)) => {
                if let Some(ref current) = current_max {
                    if compare_sql_values(val, current) == std::cmp::Ordering::Greater {
                        *current_max = Some(val.clone());
                    }
                } else {
                    *current_max = Some(val.clone());
                }
            }
            (AggregateAccumulator::Max(_), types::SqlValue::Null) => {
                // Ignore NULLs
            }

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

// ============================================================================
// SELECT Executor
// ============================================================================

/// Compare two SqlValues for ordering purposes
fn compare_sql_values(a: &types::SqlValue, b: &types::SqlValue) -> std::cmp::Ordering {
    use types::SqlValue::*;

    match (a, b) {
        // Integer comparison
        (Integer(x), Integer(y)) => x.cmp(y),

        // String comparison
        (Varchar(x), Varchar(y)) => x.cmp(y),

        // Boolean comparison
        (Boolean(x), Boolean(y)) => x.cmp(y),

        // NULL handling - NULL is considered "less than" any non-NULL value
        (Null, Null) => std::cmp::Ordering::Equal,
        (Null, _) => std::cmp::Ordering::Less,
        (_, Null) => std::cmp::Ordering::Greater,

        // Type mismatch - shouldn't happen with proper type checking
        // Return Equal to maintain stability in sort
        _ => std::cmp::Ordering::Equal,
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
        // Check if this query has aggregates or GROUP BY
        let has_aggregates = self.has_aggregates(&stmt.select_list) || stmt.having.is_some();
        let has_group_by = stmt.group_by.is_some();

        if has_aggregates || has_group_by {
            self.execute_with_aggregation(stmt)
        } else {
            self.execute_without_aggregation(stmt)
        }
    }

    /// Check if SELECT list contains aggregate functions
    fn has_aggregates(&self, select_list: &[ast::SelectItem]) -> bool {
        for item in select_list {
            match item {
                ast::SelectItem::Expression { expr, .. } => {
                    if self.expression_has_aggregate(expr) {
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
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
            _ => {
                return Err(ExecutorError::UnsupportedFeature(
                    "Aggregates only supported on single tables".to_string(),
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
            vec![(vec![], filtered_rows)]
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
        if let Some(_order_by) = &stmt.order_by {
            return Err(ExecutorError::UnsupportedFeature(
                "ORDER BY with aggregates not yet implemented".to_string(),
            ));
        }

        Ok(result_rows)
    }

    /// Execute SELECT without aggregation
    fn execute_without_aggregation(
        &self,
        stmt: &ast::SelectStmt,
    ) -> Result<Vec<storage::Row>, ExecutorError> {
        // For now, only support simple single-table queries
        let table_name = match &stmt.from {
            Some(ast::FromClause::Table { name, alias: _ }) => name,
            Some(ast::FromClause::Join { .. }) => {
                return Err(ExecutorError::UnsupportedFeature(
                    "JOIN not yet implemented".to_string(),
                ))
            }
            None => {
                return Err(ExecutorError::UnsupportedFeature(
                    "SELECT without FROM not yet implemented".to_string(),
                ))
            }
        };

        // Get the table
        let table = self
            .database
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

        let evaluator = ExpressionEvaluator::new(&table.schema);

        // Scan all rows and filter with WHERE clause
        let mut result_rows = Vec::new();
        for row in table.scan() {
            // Apply WHERE filter
            let include_row = if let Some(where_expr) = &stmt.where_clause {
                match evaluator.eval(where_expr, row)? {
                    types::SqlValue::Boolean(true) => true,
                    types::SqlValue::Boolean(false) => false,
                    types::SqlValue::Null => false, // NULL in WHERE is treated as false
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
                // Store original row for ORDER BY evaluation
                result_rows.push((row.clone(), None));
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
                    let cmp = compare_sql_values(val_a, val_b);
                    let cmp = match dir {
                        ast::OrderDirection::Asc => cmp,
                        ast::OrderDirection::Desc => cmp.reverse(),
                    };

                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Project columns from the sorted rows
        let mut final_rows = Vec::new();
        for (row, _) in result_rows {
            let projected_row = self.project_row(&row, &stmt.select_list, &evaluator)?;
            final_rows.push(projected_row);
        }

        Ok(final_rows)
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
                    if matches!(args[0], ast::Expression::Wildcard) {
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

    /// Project columns from a row based on SELECT list
    fn project_row(
        &self,
        row: &storage::Row,
        columns: &[ast::SelectItem],
        evaluator: &ExpressionEvaluator,
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

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutorError {
    TableNotFound(String),
    ColumnNotFound(String),
    ColumnIndexOutOfBounds { index: usize },
    TypeMismatch { left: types::SqlValue, op: String, right: types::SqlValue },
    DivisionByZero,
    InvalidWhereClause(String),
    UnsupportedExpression(String),
    UnsupportedFeature(String),
}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutorError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
            ExecutorError::ColumnNotFound(name) => write!(f, "Column '{}' not found", name),
            ExecutorError::ColumnIndexOutOfBounds { index } => {
                write!(f, "Column index {} out of bounds", index)
            }
            ExecutorError::TypeMismatch { left, op, right } => {
                write!(f, "Type mismatch: {:?} {} {:?}", left, op, right)
            }
            ExecutorError::DivisionByZero => write!(f, "Division by zero"),
            ExecutorError::InvalidWhereClause(msg) => write!(f, "Invalid WHERE clause: {}", msg),
            ExecutorError::UnsupportedExpression(msg) => {
                write!(f, "Unsupported expression: {}", msg)
            }
            ExecutorError::UnsupportedFeature(msg) => write!(f, "Unsupported feature: {}", msg),
        }
    }
}

impl std::error::Error for ExecutorError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Expression Evaluator Tests
    // ========================================================================

    #[test]
    fn test_eval_integer_literal() {
        let schema = catalog::TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = storage::Row::new(vec![]);

        let expr = ast::Expression::Literal(types::SqlValue::Integer(42));
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, types::SqlValue::Integer(42));
    }

    #[test]
    fn test_eval_string_literal() {
        let schema = catalog::TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = storage::Row::new(vec![]);

        let expr = ast::Expression::Literal(types::SqlValue::Varchar("hello".to_string()));
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, types::SqlValue::Varchar("hello".to_string()));
    }

    #[test]
    fn test_eval_null_literal() {
        let schema = catalog::TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = storage::Row::new(vec![]);

        let expr = ast::Expression::Literal(types::SqlValue::Null);
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, types::SqlValue::Null);
    }

    #[test]
    fn test_eval_column_ref() {
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 100 },
                    true,
                ),
            ],
        );
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
        ]);

        // Test id column
        let expr = ast::Expression::ColumnRef { table: None, column: "id".to_string() };
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, types::SqlValue::Integer(1));

        // Test name column
        let expr = ast::Expression::ColumnRef { table: None, column: "name".to_string() };
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, types::SqlValue::Varchar("Alice".to_string()));
    }

    #[test]
    fn test_eval_column_not_found() {
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = storage::Row::new(vec![types::SqlValue::Integer(1)]);

        let expr = ast::Expression::ColumnRef { table: None, column: "missing".to_string() };
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_err());
        match result.unwrap_err() {
            ExecutorError::ColumnNotFound(name) => assert_eq!(name, "missing"),
            _ => panic!("Expected ColumnNotFound error"),
        }
    }

    #[test]
    fn test_eval_addition() {
        let schema = catalog::TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = storage::Row::new(vec![]);

        let expr = ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
            op: ast::BinaryOperator::Plus,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
        };
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, types::SqlValue::Integer(15));
    }

    #[test]
    fn test_eval_comparison() {
        let schema = catalog::TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = storage::Row::new(vec![]);

        let expr = ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
            op: ast::BinaryOperator::GreaterThan,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
        };
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, types::SqlValue::Boolean(true));
    }

    #[test]
    fn test_eval_division_by_zero() {
        let schema = catalog::TableSchema::new("test".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&schema);
        let row = storage::Row::new(vec![]);

        let expr = ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
            op: ast::BinaryOperator::Divide,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(0))),
        };
        let result = evaluator.eval(&expr, &row);
        assert!(result.is_err());
        match result.unwrap_err() {
            ExecutorError::DivisionByZero => {}
            _ => panic!("Expected DivisionByZero error"),
        }
    }

    // ========================================================================
    // SELECT Executor Tests
    // ========================================================================

    #[test]
    fn test_select_star() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 100 },
                    true,
                ),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Varchar("Alice".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(2),
                types::SqlValue::Varchar("Bob".to_string()),
            ]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
        assert_eq!(result[0].values[1], types::SqlValue::Varchar("Alice".to_string()));
        assert_eq!(result[1].values[0], types::SqlValue::Integer(2));
        assert_eq!(result[1].values[1], types::SqlValue::Varchar("Bob".to_string()));
    }

    #[test]
    fn test_select_with_where() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(25)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(17)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(30)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: Some(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "age".to_string(),
                }),
                op: ast::BinaryOperator::GreaterThanOrEqual,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(18))),
            }),
            group_by: None,
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 2); // Only users with age >= 18
        assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
        assert_eq!(result[1].values[0], types::SqlValue::Integer(3));
    }

    #[test]
    fn test_select_specific_columns() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 100 },
                    true,
                ),
                catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Varchar("Alice".to_string()),
                types::SqlValue::Integer(25),
            ]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![
                ast::SelectItem::Expression {
                    expr: ast::Expression::ColumnRef { table: None, column: "name".to_string() },
                    alias: None,
                },
                ast::SelectItem::Expression {
                    expr: ast::Expression::ColumnRef { table: None, column: "age".to_string() },
                    alias: None,
                },
            ],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values.len(), 2); // Only name and age
        assert_eq!(result[0].values[0], types::SqlValue::Varchar("Alice".to_string()));
        assert_eq!(result[0].values[1], types::SqlValue::Integer(25));
    }

    // ========================================================================
    // ORDER BY Tests
    // ========================================================================

    #[test]
    fn test_order_by_single_column_asc() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(30)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(20)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(25)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: Some(vec![ast::OrderByItem {
                expr: ast::Expression::ColumnRef { table: None, column: "age".to_string() },
                direction: ast::OrderDirection::Asc,
            }]),
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 3);
        // Results should be sorted by age ascending: 20, 25, 30
        assert_eq!(result[0].values[1], types::SqlValue::Integer(20));
        assert_eq!(result[1].values[1], types::SqlValue::Integer(25));
        assert_eq!(result[2].values[1], types::SqlValue::Integer(30));
    }

    #[test]
    fn test_order_by_single_column_desc() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(30)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(20)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(25)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: Some(vec![ast::OrderByItem {
                expr: ast::Expression::ColumnRef { table: None, column: "age".to_string() },
                direction: ast::OrderDirection::Desc,
            }]),
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 3);
        // Results should be sorted by age descending: 30, 25, 20
        assert_eq!(result[0].values[1], types::SqlValue::Integer(30));
        assert_eq!(result[1].values[1], types::SqlValue::Integer(25));
        assert_eq!(result[2].values[1], types::SqlValue::Integer(20));
    }

    #[test]
    fn test_order_by_string_column() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 100 },
                    true,
                ),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Varchar("Charlie".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(2),
                types::SqlValue::Varchar("Alice".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(3),
                types::SqlValue::Varchar("Bob".to_string()),
            ]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: Some(vec![ast::OrderByItem {
                expr: ast::Expression::ColumnRef { table: None, column: "name".to_string() },
                direction: ast::OrderDirection::Asc,
            }]),
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 3);
        // Results should be sorted alphabetically: Alice, Bob, Charlie
        assert_eq!(result[0].values[1], types::SqlValue::Varchar("Alice".to_string()));
        assert_eq!(result[1].values[1], types::SqlValue::Varchar("Bob".to_string()));
        assert_eq!(result[2].values[1], types::SqlValue::Varchar("Charlie".to_string()));
    }

    #[test]
    fn test_order_by_multiple_columns() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("dept".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        // Department 1: ages 30, 25
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(30),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(25),
            ]),
        )
        .unwrap();
        // Department 2: ages 20, 35
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(3),
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(20),
            ]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(4),
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(35),
            ]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: Some(vec![
                ast::OrderByItem {
                    expr: ast::Expression::ColumnRef { table: None, column: "dept".to_string() },
                    direction: ast::OrderDirection::Asc,
                },
                ast::OrderByItem {
                    expr: ast::Expression::ColumnRef { table: None, column: "age".to_string() },
                    direction: ast::OrderDirection::Desc,
                },
            ]),
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 4);
        // Results should be sorted by dept ASC, then age DESC within each dept
        // Dept 1: 30, 25
        // Dept 2: 35, 20
        assert_eq!(result[0].values[1], types::SqlValue::Integer(1));
        assert_eq!(result[0].values[2], types::SqlValue::Integer(30));
        assert_eq!(result[1].values[1], types::SqlValue::Integer(1));
        assert_eq!(result[1].values[2], types::SqlValue::Integer(25));
        assert_eq!(result[2].values[1], types::SqlValue::Integer(2));
        assert_eq!(result[2].values[2], types::SqlValue::Integer(35));
        assert_eq!(result[3].values[1], types::SqlValue::Integer(2));
        assert_eq!(result[3].values[2], types::SqlValue::Integer(20));
    }

    #[test]
    fn test_order_by_with_where() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(30)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(15)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(25)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(4), types::SqlValue::Integer(20)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: Some(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "age".to_string(),
                }),
                op: ast::BinaryOperator::GreaterThanOrEqual,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(18))),
            }),
            group_by: None,
            having: None,
            order_by: Some(vec![ast::OrderByItem {
                expr: ast::Expression::ColumnRef { table: None, column: "age".to_string() },
                direction: ast::OrderDirection::Asc,
            }]),
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 3); // Only age >= 18
                                     // Results should be sorted by age ascending: 20, 25, 30 (15 is filtered out)
        assert_eq!(result[0].values[1], types::SqlValue::Integer(20));
        assert_eq!(result[1].values[1], types::SqlValue::Integer(25));
        assert_eq!(result[2].values[1], types::SqlValue::Integer(30));
    }

    // ========================================================================
    // Aggregate Function Tests
    // ========================================================================

    #[test]
    fn test_count_star_no_group_by() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(25)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(30)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(35)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        // SELECT COUNT(*) FROM users
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "COUNT".to_string(),
                    args: vec![ast::Expression::Wildcard],
                },
                alias: None,
            }],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1); // One row with the count
        assert_eq!(result[0].values[0], types::SqlValue::Integer(3));
    }

    #[test]
    fn test_count_column_no_group_by() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, true),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(25)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Null]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(35)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        // SELECT COUNT(age) FROM users - should count non-NULL values only
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "COUNT".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "age".to_string(),
                    }],
                },
                alias: None,
            }],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(2)); // NULL not counted
    }

    #[test]
    fn test_sum_no_group_by() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "sales".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "sales",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(100)]),
        )
        .unwrap();
        db.insert_row(
            "sales",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(200)]),
        )
        .unwrap();
        db.insert_row(
            "sales",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(150)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        // SELECT SUM(amount) FROM sales
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "SUM".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "amount".to_string(),
                    }],
                },
                alias: None,
            }],
            from: Some(ast::FromClause::Table { name: "sales".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(450));
    }

    #[test]
    fn test_avg_no_group_by() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "scores".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("score".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "scores",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(80)]),
        )
        .unwrap();
        db.insert_row(
            "scores",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(90)]),
        )
        .unwrap();
        db.insert_row(
            "scores",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(70)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        // SELECT AVG(score) FROM scores
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "AVG".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "score".to_string(),
                    }],
                },
                alias: None,
            }],
            from: Some(ast::FromClause::Table { name: "scores".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(80)); // (80+90+70)/3 = 80
    }

    #[test]
    fn test_min_max_no_group_by() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "values".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "values",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(50)]),
        )
        .unwrap();
        db.insert_row(
            "values",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(30)]),
        )
        .unwrap();
        db.insert_row(
            "values",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(80)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);

        // SELECT MIN(val) FROM values
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "MIN".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "val".to_string(),
                    }],
                },
                alias: None,
            }],
            from: Some(ast::FromClause::Table { name: "values".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(30));

        // SELECT MAX(val) FROM values
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "MAX".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "val".to_string(),
                    }],
                },
                alias: None,
            }],
            from: Some(ast::FromClause::Table { name: "values".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(80));
    }

    // ========================================================================
    // GROUP BY Tests
    // ========================================================================

    #[test]
    fn test_group_by_with_count() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "sales".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("dept".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        // Dept 1: 2 sales
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(100),
            ]),
        )
        .unwrap();
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(200),
            ]),
        )
        .unwrap();
        // Dept 2: 1 sale
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(3),
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(150),
            ]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        // SELECT dept, COUNT(*) FROM sales GROUP BY dept
        let stmt = ast::SelectStmt {
            select_list: vec![
                ast::SelectItem::Expression {
                    expr: ast::Expression::ColumnRef { table: None, column: "dept".to_string() },
                    alias: None,
                },
                ast::SelectItem::Expression {
                    expr: ast::Expression::Function {
                        name: "COUNT".to_string(),
                        args: vec![ast::Expression::Wildcard],
                    },
                    alias: None,
                },
            ],
            from: Some(ast::FromClause::Table { name: "sales".to_string(), alias: None }),
            where_clause: None,
            group_by: Some(vec![ast::Expression::ColumnRef {
                table: None,
                column: "dept".to_string(),
            }]),
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 2); // Two groups
                                     // Dept 1 has 2 sales, Dept 2 has 1 sale
                                     // Results may be in any order, so check both possibilities
        if result[0].values[0] == types::SqlValue::Integer(1) {
            assert_eq!(result[0].values[1], types::SqlValue::Integer(2));
            assert_eq!(result[1].values[0], types::SqlValue::Integer(2));
            assert_eq!(result[1].values[1], types::SqlValue::Integer(1));
        } else {
            assert_eq!(result[0].values[0], types::SqlValue::Integer(2));
            assert_eq!(result[0].values[1], types::SqlValue::Integer(1));
            assert_eq!(result[1].values[0], types::SqlValue::Integer(1));
            assert_eq!(result[1].values[1], types::SqlValue::Integer(2));
        }
    }

    #[test]
    fn test_group_by_with_sum() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "sales".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("dept".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        // Dept 1: total 300
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(100),
            ]),
        )
        .unwrap();
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(200),
            ]),
        )
        .unwrap();
        // Dept 2: total 150
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(3),
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(150),
            ]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        // SELECT dept, SUM(amount) FROM sales GROUP BY dept
        let stmt = ast::SelectStmt {
            select_list: vec![
                ast::SelectItem::Expression {
                    expr: ast::Expression::ColumnRef { table: None, column: "dept".to_string() },
                    alias: None,
                },
                ast::SelectItem::Expression {
                    expr: ast::Expression::Function {
                        name: "SUM".to_string(),
                        args: vec![ast::Expression::ColumnRef {
                            table: None,
                            column: "amount".to_string(),
                        }],
                    },
                    alias: None,
                },
            ],
            from: Some(ast::FromClause::Table { name: "sales".to_string(), alias: None }),
            where_clause: None,
            group_by: Some(vec![ast::Expression::ColumnRef {
                table: None,
                column: "dept".to_string(),
            }]),
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 2);
        if result[0].values[0] == types::SqlValue::Integer(1) {
            assert_eq!(result[0].values[1], types::SqlValue::Integer(300));
            assert_eq!(result[1].values[0], types::SqlValue::Integer(2));
            assert_eq!(result[1].values[1], types::SqlValue::Integer(150));
        } else {
            assert_eq!(result[0].values[0], types::SqlValue::Integer(2));
            assert_eq!(result[0].values[1], types::SqlValue::Integer(150));
            assert_eq!(result[1].values[0], types::SqlValue::Integer(1));
            assert_eq!(result[1].values[1], types::SqlValue::Integer(300));
        }
    }

    #[test]
    fn test_group_by_with_multiple_aggregates() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "sales".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("dept".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        // Dept 1: 2 sales, total 300, avg 150
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(100),
            ]),
        )
        .unwrap();
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(200),
            ]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        // SELECT dept, COUNT(*), SUM(amount), AVG(amount) FROM sales GROUP BY dept
        let stmt = ast::SelectStmt {
            select_list: vec![
                ast::SelectItem::Expression {
                    expr: ast::Expression::ColumnRef { table: None, column: "dept".to_string() },
                    alias: None,
                },
                ast::SelectItem::Expression {
                    expr: ast::Expression::Function {
                        name: "COUNT".to_string(),
                        args: vec![ast::Expression::Wildcard],
                    },
                    alias: None,
                },
                ast::SelectItem::Expression {
                    expr: ast::Expression::Function {
                        name: "SUM".to_string(),
                        args: vec![ast::Expression::ColumnRef {
                            table: None,
                            column: "amount".to_string(),
                        }],
                    },
                    alias: None,
                },
                ast::SelectItem::Expression {
                    expr: ast::Expression::Function {
                        name: "AVG".to_string(),
                        args: vec![ast::Expression::ColumnRef {
                            table: None,
                            column: "amount".to_string(),
                        }],
                    },
                    alias: None,
                },
            ],
            from: Some(ast::FromClause::Table { name: "sales".to_string(), alias: None }),
            where_clause: None,
            group_by: Some(vec![ast::Expression::ColumnRef {
                table: None,
                column: "dept".to_string(),
            }]),
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1); // One group
        assert_eq!(result[0].values[0], types::SqlValue::Integer(1)); // dept
        assert_eq!(result[0].values[1], types::SqlValue::Integer(2)); // count
        assert_eq!(result[0].values[2], types::SqlValue::Integer(300)); // sum
        assert_eq!(result[0].values[3], types::SqlValue::Integer(150)); // avg
    }

    #[test]
    fn test_having_clause() {
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "sales".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("dept".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        // Dept 1: total 300
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(100),
            ]),
        )
        .unwrap();
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(200),
            ]),
        )
        .unwrap();
        // Dept 2: total 50
        db.insert_row(
            "sales",
            storage::Row::new(vec![
                types::SqlValue::Integer(3),
                types::SqlValue::Integer(2),
                types::SqlValue::Integer(50),
            ]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        // SELECT dept, SUM(amount) FROM sales GROUP BY dept HAVING SUM(amount) > 100
        let stmt = ast::SelectStmt {
            select_list: vec![
                ast::SelectItem::Expression {
                    expr: ast::Expression::ColumnRef { table: None, column: "dept".to_string() },
                    alias: None,
                },
                ast::SelectItem::Expression {
                    expr: ast::Expression::Function {
                        name: "SUM".to_string(),
                        args: vec![ast::Expression::ColumnRef {
                            table: None,
                            column: "amount".to_string(),
                        }],
                    },
                    alias: None,
                },
            ],
            from: Some(ast::FromClause::Table { name: "sales".to_string(), alias: None }),
            where_clause: None,
            group_by: Some(vec![ast::Expression::ColumnRef {
                table: None,
                column: "dept".to_string(),
            }]),
            having: Some(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::Function {
                    name: "SUM".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "amount".to_string(),
                    }],
                }),
                op: ast::BinaryOperator::GreaterThan,
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(100))),
            }),
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1); // Only dept 1 (total 300 > 100)
        assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
        assert_eq!(result[0].values[1], types::SqlValue::Integer(300));
    }
}
