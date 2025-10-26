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

            // Wildcard - only valid in aggregate functions, shouldn't be evaluated directly
            ast::Expression::Wildcard => Err(ExecutorError::UnsupportedExpression(
                "Wildcard (*) can only be used with aggregate functions like COUNT(*)".to_string(),
            )),

            // TODO: Implement other expression types
            _ => Err(ExecutorError::UnsupportedExpression(format!("{:?}", expr))),
        }
    }

    /// Evaluate a binary operation
    fn eval_binary_op(
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
// Aggregate Function Support
// ============================================================================

/// Checks if an expression contains an aggregate function
fn contains_aggregate(expr: &ast::Expression) -> bool {
    match expr {
        ast::Expression::Function { name, .. } => {
            matches!(
                name.to_uppercase().as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
            )
        }
        ast::Expression::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        ast::Expression::UnaryOp { expr, .. } => contains_aggregate(expr),
        _ => false,
    }
}

/// Compute an aggregate function over a set of rows
fn compute_aggregate(
    func_name: &str,
    args: &[ast::Expression],
    rows: &[storage::Row],
    schema: &catalog::TableSchema,
) -> Result<types::SqlValue, ExecutorError> {
    let evaluator = ExpressionEvaluator::new(schema);

    match func_name.to_uppercase().as_str() {
        "COUNT" => {
            // COUNT(*) counts all rows
            // COUNT(expr) counts non-NULL values
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "COUNT expects 1 argument, got {}",
                    args.len()
                )));
            }

            // Check if it's COUNT(*) - parser represents this as ColumnRef { column: "*" }
            let is_count_star = match &args[0] {
                ast::Expression::Wildcard => true,
                ast::Expression::ColumnRef { table: None, column } if column == "*" => true,
                _ => false,
            };

            if is_count_star {
                // COUNT(*) - count all rows
                Ok(types::SqlValue::Integer(rows.len() as i64))
            } else {
                // COUNT(column) - count non-NULL values
                let mut count = 0;
                for row in rows {
                    let val = evaluator.eval(&args[0], row)?;
                    if !matches!(val, types::SqlValue::Null) {
                        count += 1;
                    }
                }
                Ok(types::SqlValue::Integer(count))
            }
        }
        "SUM" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "SUM expects 1 argument, got {}",
                    args.len()
                )));
            }
            // SUM ignores NULL values
            let mut sum = 0i64;
            for row in rows {
                let val = evaluator.eval(&args[0], row)?;
                match val {
                    types::SqlValue::Integer(n) => sum += n,
                    types::SqlValue::Null => {} // Ignore NULLs
                    _ => {
                        return Err(ExecutorError::UnsupportedFeature(
                            "SUM only supports integer values".to_string(),
                        ))
                    }
                }
            }
            Ok(types::SqlValue::Integer(sum))
        }
        "AVG" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "AVG expects 1 argument, got {}",
                    args.len()
                )));
            }
            // AVG ignores NULL values and computes sum/count
            let mut sum = 0i64;
            let mut count = 0i64;
            for row in rows {
                let val = evaluator.eval(&args[0], row)?;
                match val {
                    types::SqlValue::Integer(n) => {
                        sum += n;
                        count += 1;
                    }
                    types::SqlValue::Null => {} // Ignore NULLs
                    _ => {
                        return Err(ExecutorError::UnsupportedFeature(
                            "AVG only supports integer values".to_string(),
                        ))
                    }
                }
            }
            if count == 0 {
                Ok(types::SqlValue::Null) // AVG of empty set is NULL
            } else {
                Ok(types::SqlValue::Integer(sum / count))
            }
        }
        "MIN" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "MIN expects 1 argument, got {}",
                    args.len()
                )));
            }
            // MIN ignores NULL values
            let mut min_val: Option<types::SqlValue> = None;
            for row in rows {
                let val = evaluator.eval(&args[0], row)?;
                if matches!(val, types::SqlValue::Null) {
                    continue; // Ignore NULLs
                }
                match &min_val {
                    None => min_val = Some(val),
                    Some(current_min) => {
                        if compare_sql_values(&val, current_min) == std::cmp::Ordering::Less {
                            min_val = Some(val);
                        }
                    }
                }
            }
            Ok(min_val.unwrap_or(types::SqlValue::Null))
        }
        "MAX" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "MAX expects 1 argument, got {}",
                    args.len()
                )));
            }
            // MAX ignores NULL values
            let mut max_val: Option<types::SqlValue> = None;
            for row in rows {
                let val = evaluator.eval(&args[0], row)?;
                if matches!(val, types::SqlValue::Null) {
                    continue; // Ignore NULLs
                }
                match &max_val {
                    None => max_val = Some(val),
                    Some(current_max) => {
                        if compare_sql_values(&val, current_max) == std::cmp::Ordering::Greater {
                            max_val = Some(val);
                        }
                    }
                }
            }
            Ok(max_val.unwrap_or(types::SqlValue::Null))
        }
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "Aggregate function {} not yet implemented",
            func_name
        ))),
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

        // Check if query contains aggregates
        let has_aggregates = stmt.select_list.iter().any(|item| match item {
            ast::SelectItem::Expression { expr, .. } => contains_aggregate(expr),
            ast::SelectItem::Wildcard => false,
        });

        // Scan all rows and filter with WHERE clause
        let mut filtered_rows = Vec::new();
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
                filtered_rows.push(row.clone());
            }
        }

        // If query has aggregates, handle aggregate execution
        if has_aggregates {
            // For now, only support aggregates without GROUP BY
            if stmt.group_by.is_some() {
                return Err(ExecutorError::UnsupportedFeature(
                    "GROUP BY with aggregates not yet implemented".to_string(),
                ));
            }

            // Compute aggregates over all filtered rows
            let mut agg_values = Vec::new();
            for item in &stmt.select_list {
                match item {
                    ast::SelectItem::Expression { expr, .. } => {
                        let value = self.eval_aggregate_expr(expr, &filtered_rows, &table.schema)?;
                        agg_values.push(value);
                    }
                    ast::SelectItem::Wildcard => {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Wildcard not supported with aggregates".to_string(),
                        ))
                    }
                }
            }

            // Return single row with aggregate results
            return Ok(vec![storage::Row::new(agg_values)]);
        }

        // Non-aggregate query: use existing logic
        let mut result_rows = Vec::new();
        for row in filtered_rows {
            // Store original row for ORDER BY evaluation
            result_rows.push((row, None));
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

    /// Evaluate an expression that may contain aggregates
    fn eval_aggregate_expr(
        &self,
        expr: &ast::Expression,
        rows: &[storage::Row],
        schema: &catalog::TableSchema,
    ) -> Result<types::SqlValue, ExecutorError> {
        match expr {
            ast::Expression::Function { name, args } => {
                // Compute aggregate function
                compute_aggregate(name, args, rows, schema)
            }
            _ => {
                // For non-aggregate expressions, evaluate on first row (or return error if no rows)
                // This handles cases like: SELECT COUNT(*), 42 FROM users
                if rows.is_empty() {
                    // With no rows, literals should still work
                    if let ast::Expression::Literal(val) = expr {
                        return Ok(val.clone());
                    }
                    return Err(ExecutorError::UnsupportedFeature(
                        "Non-aggregate expression with no rows".to_string(),
                    ));
                }
                let evaluator = ExpressionEvaluator::new(schema);
                evaluator.eval(expr, &rows[0])
            }
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
        // Test: SELECT COUNT(*) FROM users
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
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(22)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
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
        assert_eq!(result.len(), 1); // One row for the aggregate
        assert_eq!(result[0].values.len(), 1); // One column
        assert_eq!(result[0].values[0], types::SqlValue::Integer(3)); // COUNT(*) = 3
    }

    #[test]
    fn test_count_column_with_nulls() {
        // Test: SELECT COUNT(age) FROM users WHERE age can have NULLs
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, true), // nullable
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
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Null]), // NULL age
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(30)]),
        )
        .unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![types::SqlValue::Integer(4), types::SqlValue::Null]), // NULL age
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
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
        assert_eq!(result[0].values.len(), 1);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(2)); // COUNT(age) = 2 (excludes NULLs)
    }

    #[test]
    fn test_sum_aggregate() {
        // Test: SELECT SUM(amount) FROM transactions
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "transactions".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "amount".to_string(),
                    types::DataType::Integer,
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "transactions",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(100)]),
        )
        .unwrap();
        db.insert_row(
            "transactions",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(250)]),
        )
        .unwrap();
        db.insert_row(
            "transactions",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(75)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
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
            from: Some(ast::FromClause::Table {
                name: "transactions".to_string(),
                alias: None,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values.len(), 1);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(425)); // 100 + 250 + 75 = 425
    }

    #[test]
    fn test_avg_aggregate() {
        // Test: SELECT AVG(score) FROM tests
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "tests".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("score".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "tests",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(80)]),
        )
        .unwrap();
        db.insert_row(
            "tests",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(90)]),
        )
        .unwrap();
        db.insert_row(
            "tests",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(70)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
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
            from: Some(ast::FromClause::Table { name: "tests".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values.len(), 1);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(80)); // (80 + 90 + 70) / 3 = 80
    }

    #[test]
    fn test_min_aggregate() {
        // Test: SELECT MIN(price) FROM products
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "products".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("price".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "products",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(150)]),
        )
        .unwrap();
        db.insert_row(
            "products",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(75)]),
        )
        .unwrap();
        db.insert_row(
            "products",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(200)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "MIN".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "price".to_string(),
                    }],
                },
                alias: None,
            }],
            from: Some(ast::FromClause::Table { name: "products".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values.len(), 1);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(75)); // MIN = 75
    }

    #[test]
    fn test_max_aggregate() {
        // Test: SELECT MAX(temperature) FROM readings
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "readings".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "temperature".to_string(),
                    types::DataType::Integer,
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();
        db.insert_row(
            "readings",
            storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(22)]),
        )
        .unwrap();
        db.insert_row(
            "readings",
            storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(28)]),
        )
        .unwrap();
        db.insert_row(
            "readings",
            storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(19)]),
        )
        .unwrap();

        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "MAX".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "temperature".to_string(),
                    }],
                },
                alias: None,
            }],
            from: Some(ast::FromClause::Table {
                name: "readings".to_string(),
                alias: None,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values.len(), 1);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(28)); // MAX = 28
    }

    #[test]
    fn test_multiple_aggregates() {
        // Test: SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM sales
        let mut db = storage::Database::new();
        let schema = catalog::TableSchema::new(
            "sales".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "amount".to_string(),
                    types::DataType::Integer,
                    false,
                ),
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
        let stmt = ast::SelectStmt {
            select_list: vec![
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
                ast::SelectItem::Expression {
                    expr: ast::Expression::Function {
                        name: "MIN".to_string(),
                        args: vec![ast::Expression::ColumnRef {
                            table: None,
                            column: "amount".to_string(),
                        }],
                    },
                    alias: None,
                },
                ast::SelectItem::Expression {
                    expr: ast::Expression::Function {
                        name: "MAX".to_string(),
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
            group_by: None,
            having: None,
            order_by: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1); // One row for aggregates
        assert_eq!(result[0].values.len(), 5); // Five aggregate columns
        assert_eq!(result[0].values[0], types::SqlValue::Integer(3)); // COUNT(*) = 3
        assert_eq!(result[0].values[1], types::SqlValue::Integer(450)); // SUM = 450
        assert_eq!(result[0].values[2], types::SqlValue::Integer(150)); // AVG = 150
        assert_eq!(result[0].values[3], types::SqlValue::Integer(100)); // MIN = 100
        assert_eq!(result[0].values[4], types::SqlValue::Integer(200)); // MAX = 200
    }
}
