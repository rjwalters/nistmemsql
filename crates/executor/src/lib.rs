//! Executor - SQL Query Execution Engine
//!
//! This crate provides query execution functionality for SQL statements.

use std::collections::HashMap;

// ============================================================================
// Combined Schema - for JOIN operations
// ============================================================================

/// Represents the combined schema from multiple tables (for JOINs)
#[derive(Debug, Clone)]
struct CombinedSchema {
    /// Map from table name to (start_index, TableSchema)
    /// start_index is where this table's columns begin in the combined row
    table_schemas: HashMap<String, (usize, catalog::TableSchema)>,
    /// Total number of columns across all tables
    total_columns: usize,
}

impl CombinedSchema {
    /// Create a new combined schema from a single table
    fn from_table(table_name: String, schema: catalog::TableSchema) -> Self {
        let total_columns = schema.columns.len();
        let mut table_schemas = HashMap::new();
        table_schemas.insert(table_name, (0, schema));
        CombinedSchema { table_schemas, total_columns }
    }

    /// Combine two schemas (for JOIN operations)
    fn combine(left: CombinedSchema, right_table: String, right_schema: catalog::TableSchema) -> Self {
        let mut table_schemas = left.table_schemas;
        let left_total = left.total_columns;
        let right_columns = right_schema.columns.len();
        table_schemas.insert(right_table, (left_total, right_schema));
        CombinedSchema {
            table_schemas,
            total_columns: left_total + right_columns,
        }
    }

    /// Look up a column by name (optionally qualified with table name)
    fn get_column_index(&self, table: Option<&str>, column: &str) -> Option<usize> {
        if let Some(table_name) = table {
            // Qualified column reference (table.column)
            if let Some((start_index, schema)) = self.table_schemas.get(table_name) {
                schema.get_column_index(column).map(|idx| start_index + idx)
            } else {
                None
            }
        } else {
            // Unqualified column reference - search all tables
            for (start_index, schema) in self.table_schemas.values() {
                if let Some(idx) = schema.get_column_index(column) {
                    return Some(start_index + idx);
                }
            }
            None
        }
    }
}

// ============================================================================
// FROM Clause Result
// ============================================================================

/// Result of executing a FROM clause
struct FromResult {
    schema: CombinedSchema,
    rows: Vec<storage::Row>,
}

// ============================================================================
// Expression Evaluator
// ============================================================================

/// Evaluates expressions in the context of a row
pub struct ExpressionEvaluator<'a> {
    schema: &'a catalog::TableSchema,
}

/// Evaluates expressions with combined schema (for JOINs)
struct CombinedExpressionEvaluator<'a> {
    schema: &'a CombinedSchema,
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
    fn eval_binary_op(
        &self,
        left: &types::SqlValue,
        op: &ast::BinaryOperator,
        right: &types::SqlValue,
    ) -> Result<types::SqlValue, ExecutorError> {
        Self::eval_binary_op_static(left, op, right)
    }

    /// Static version of eval_binary_op for shared logic
    fn eval_binary_op_static(
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
// Combined Expression Evaluator - for JOINs
// ============================================================================

impl<'a> CombinedExpressionEvaluator<'a> {
    /// Create a new combined expression evaluator
    fn new(schema: &'a CombinedSchema) -> Self {
        CombinedExpressionEvaluator { schema }
    }

    /// Evaluate an expression in the context of a combined row
    fn eval(
        &self,
        expr: &ast::Expression,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        match expr {
            // Literals - just return the value
            ast::Expression::Literal(val) => Ok(val.clone()),

            // Column reference - look up column index (with optional table qualifier)
            ast::Expression::ColumnRef { table, column } => {
                let col_index = self
                    .schema
                    .get_column_index(table.as_deref(), column)
                    .ok_or_else(|| ExecutorError::ColumnNotFound(column.clone()))?;
                row.get(col_index)
                    .cloned()
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index })
            }

            // Binary operations
            ast::Expression::BinaryOp { left, op, right } => {
                let left_val = self.eval(left, row)?;
                let right_val = self.eval(right, row)?;
                ExpressionEvaluator::eval_binary_op_static(&left_val, op, &right_val)
            }

            // TODO: Implement other expression types
            _ => Err(ExecutorError::UnsupportedExpression(format!("{:?}", expr))),
        }
    }
}

// ============================================================================
// SELECT Executor
// ============================================================================

/// Compare two SqlValues for ordering purposes
#[allow(dead_code)] // Will be used when ORDER BY is re-added
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
        // Execute the FROM clause to get rows
        let from_result = match &stmt.from {
            Some(from_clause) => self.execute_from(from_clause)?,
            None => {
                return Err(ExecutorError::UnsupportedFeature(
                    "SELECT without FROM not yet implemented".to_string(),
                ))
            }
        };

        let evaluator = CombinedExpressionEvaluator::new(&from_result.schema);

        // Scan all rows and filter with WHERE clause
        let mut result_rows = Vec::new();
        for row in &from_result.rows {
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
                // Project columns
                let projected_row =
                    self.project_row_combined(row, &stmt.select_list, &evaluator)?;
                result_rows.push(projected_row);
            }
        }

        // TODO: Re-add ORDER BY support (removed during JOIN merge, needs to be adapted for CombinedExpressionEvaluator)

        // Apply LIMIT and OFFSET
        let start = stmt.offset.unwrap_or(0);
        let end = if let Some(limit) = stmt.limit {
            start + limit
        } else {
            result_rows.len()
        };

        // Slice the results based on OFFSET and LIMIT
        let final_rows = if start >= result_rows.len() {
            // Offset beyond result set
            vec![]
        } else {
            result_rows[start..end.min(result_rows.len())].to_vec()
        };

        Ok(final_rows)
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
            ast::FromClause::Join {
                left,
                right,
                join_type,
                condition,
            } => {
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

        Ok(FromResult {
            schema: combined_schema,
            rows: result_rows,
        })
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
        let combined_schema =
            CombinedSchema::combine(left.schema, right_table_name, right_schema);
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

        Ok(FromResult {
            schema: combined_schema,
            rows: result_rows,
        })
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
            limit: None,
            offset: None,
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
            limit: None,
            offset: None,
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
            limit: None,
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values.len(), 2); // Only name and age
        assert_eq!(result[0].values[0], types::SqlValue::Varchar("Alice".to_string()));
        assert_eq!(result[0].values[1], types::SqlValue::Integer(25));
    }

    // ========================================================================
    // JOIN Execution Tests
    // ========================================================================

    #[test]
    fn test_inner_join_two_tables() {
        // Setup database with two tables: users and orders
        let mut db = storage::Database::new();

        // Create users table
        let users_schema = catalog::TableSchema::new(
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
        db.create_table(users_schema).unwrap();
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

        // Create orders table
        let orders_schema = catalog::TableSchema::new(
            "orders".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("user_id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "product".to_string(),
                    types::DataType::Varchar { max_length: 100 },
                    true,
                ),
            ],
        );
        db.create_table(orders_schema).unwrap();
        db.insert_row(
            "orders",
            storage::Row::new(vec![
                types::SqlValue::Integer(100),
                types::SqlValue::Integer(1),
                types::SqlValue::Varchar("Widget".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "orders",
            storage::Row::new(vec![
                types::SqlValue::Integer(101),
                types::SqlValue::Integer(1),
                types::SqlValue::Varchar("Gadget".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "orders",
            storage::Row::new(vec![
                types::SqlValue::Integer(102),
                types::SqlValue::Integer(2),
                types::SqlValue::Varchar("Doohickey".to_string()),
            ]),
        )
        .unwrap();

        // Execute: SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id
        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Join {
                left: Box::new(ast::FromClause::Table {
                    name: "users".to_string(),
                    alias: None,
                }),
                right: Box::new(ast::FromClause::Table {
                    name: "orders".to_string(),
                    alias: None,
                }),
                join_type: ast::JoinType::Inner,
                condition: Some(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::ColumnRef {
                        table: Some("users".to_string()),
                        column: "id".to_string(),
                    }),
                    op: ast::BinaryOperator::Equal,
                    right: Box::new(ast::Expression::ColumnRef {
                        table: Some("orders".to_string()),
                        column: "user_id".to_string(),
                    }),
                }),
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();

        // Should have 3 rows (Alice has 2 orders, Bob has 1 order)
        assert_eq!(result.len(), 3);

        // Each row should have 5 columns (users.id, users.name, orders.id, orders.user_id, orders.product)
        assert_eq!(result[0].values.len(), 5);

        // First row: Alice + Widget
        assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
        assert_eq!(result[0].values[1], types::SqlValue::Varchar("Alice".to_string()));
        assert_eq!(result[0].values[2], types::SqlValue::Integer(100));
        assert_eq!(result[0].values[3], types::SqlValue::Integer(1));
        assert_eq!(result[0].values[4], types::SqlValue::Varchar("Widget".to_string()));

        // Second row: Alice + Gadget
        assert_eq!(result[1].values[0], types::SqlValue::Integer(1));
        assert_eq!(result[1].values[1], types::SqlValue::Varchar("Alice".to_string()));
        assert_eq!(result[1].values[2], types::SqlValue::Integer(101));
        assert_eq!(result[1].values[3], types::SqlValue::Integer(1));
        assert_eq!(result[1].values[4], types::SqlValue::Varchar("Gadget".to_string()));

        // Third row: Bob + Doohickey
        assert_eq!(result[2].values[0], types::SqlValue::Integer(2));
        assert_eq!(result[2].values[1], types::SqlValue::Varchar("Bob".to_string()));
        assert_eq!(result[2].values[2], types::SqlValue::Integer(102));
        assert_eq!(result[2].values[3], types::SqlValue::Integer(2));
        assert_eq!(result[2].values[4], types::SqlValue::Varchar("Doohickey".to_string()));
    }

    #[test]
    fn test_inner_join_with_no_matches() {
        // Setup database where join produces no results
        let mut db = storage::Database::new();

        // Create users table
        let users_schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        db.create_table(users_schema).unwrap();
        db.insert_row("users", storage::Row::new(vec![types::SqlValue::Integer(1)]))
            .unwrap();

        // Create orders table (no matching user_id)
        let orders_schema = catalog::TableSchema::new(
            "orders".to_string(),
            vec![catalog::ColumnSchema::new(
                "user_id".to_string(),
                types::DataType::Integer,
                false,
            )],
        );
        db.create_table(orders_schema).unwrap();
        db.insert_row("orders", storage::Row::new(vec![types::SqlValue::Integer(999)]))
            .unwrap();

        // Execute: SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id
        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Join {
                left: Box::new(ast::FromClause::Table {
                    name: "users".to_string(),
                    alias: None,
                }),
                right: Box::new(ast::FromClause::Table {
                    name: "orders".to_string(),
                    alias: None,
                }),
                join_type: ast::JoinType::Inner,
                condition: Some(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::ColumnRef {
                        table: Some("users".to_string()),
                        column: "id".to_string(),
                    }),
                    op: ast::BinaryOperator::Equal,
                    right: Box::new(ast::Expression::ColumnRef {
                        table: Some("orders".to_string()),
                        column: "user_id".to_string(),
                    }),
                }),
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();

        // Should have 0 rows (no matching records)
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_left_outer_join() {
        // Setup database with users and orders where some users have no orders
        let mut db = storage::Database::new();

        // Create users table
        let users_schema = catalog::TableSchema::new(
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
        db.create_table(users_schema).unwrap();
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
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(3),
                types::SqlValue::Varchar("Charlie".to_string()),
            ]),
        )
        .unwrap();

        // Create orders table (only Alice and Bob have orders)
        let orders_schema = catalog::TableSchema::new(
            "orders".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("user_id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "product".to_string(),
                    types::DataType::Varchar { max_length: 100 },
                    true,
                ),
            ],
        );
        db.create_table(orders_schema).unwrap();
        db.insert_row(
            "orders",
            storage::Row::new(vec![
                types::SqlValue::Integer(100),
                types::SqlValue::Integer(1),
                types::SqlValue::Varchar("Widget".to_string()),
            ]),
        )
        .unwrap();
        db.insert_row(
            "orders",
            storage::Row::new(vec![
                types::SqlValue::Integer(101),
                types::SqlValue::Integer(2),
                types::SqlValue::Varchar("Gadget".to_string()),
            ]),
        )
        .unwrap();

        // Execute: SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id
        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Join {
                left: Box::new(ast::FromClause::Table {
                    name: "users".to_string(),
                    alias: None,
                }),
                right: Box::new(ast::FromClause::Table {
                    name: "orders".to_string(),
                    alias: None,
                }),
                join_type: ast::JoinType::LeftOuter,
                condition: Some(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::ColumnRef {
                        table: Some("users".to_string()),
                        column: "id".to_string(),
                    }),
                    op: ast::BinaryOperator::Equal,
                    right: Box::new(ast::Expression::ColumnRef {
                        table: Some("orders".to_string()),
                        column: "user_id".to_string(),
                    }),
                }),
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();

        // Should have 3 rows (Alice + order, Bob + order, Charlie + NULLs)
        assert_eq!(result.len(), 3);

        // First row: Alice + Widget
        assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
        assert_eq!(result[0].values[1], types::SqlValue::Varchar("Alice".to_string()));
        assert_eq!(result[0].values[2], types::SqlValue::Integer(100));
        assert_eq!(result[0].values[3], types::SqlValue::Integer(1));
        assert_eq!(result[0].values[4], types::SqlValue::Varchar("Widget".to_string()));

        // Second row: Bob + Gadget
        assert_eq!(result[1].values[0], types::SqlValue::Integer(2));
        assert_eq!(result[1].values[1], types::SqlValue::Varchar("Bob".to_string()));
        assert_eq!(result[1].values[2], types::SqlValue::Integer(101));
        assert_eq!(result[1].values[3], types::SqlValue::Integer(2));
        assert_eq!(result[1].values[4], types::SqlValue::Varchar("Gadget".to_string()));

        // Third row: Charlie + NULLs (no matching order)
        assert_eq!(result[2].values[0], types::SqlValue::Integer(3));
        assert_eq!(result[2].values[1], types::SqlValue::Varchar("Charlie".to_string()));
        assert_eq!(result[2].values[2], types::SqlValue::Null);
        assert_eq!(result[2].values[3], types::SqlValue::Null);
        assert_eq!(result[2].values[4], types::SqlValue::Null);
    }

    #[test]
    fn test_three_table_join() {
        // Setup database with users, orders, and products
        let mut db = storage::Database::new();

        // Create users table
        let users_schema = catalog::TableSchema::new(
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
        db.create_table(users_schema).unwrap();
        db.insert_row(
            "users",
            storage::Row::new(vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Varchar("Alice".to_string()),
            ]),
        )
        .unwrap();

        // Create orders table
        let orders_schema = catalog::TableSchema::new(
            "orders".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("user_id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "product_id".to_string(),
                    types::DataType::Integer,
                    false,
                ),
            ],
        );
        db.create_table(orders_schema).unwrap();
        db.insert_row(
            "orders",
            storage::Row::new(vec![
                types::SqlValue::Integer(100),
                types::SqlValue::Integer(1),
                types::SqlValue::Integer(500),
            ]),
        )
        .unwrap();

        // Create products table
        let products_schema = catalog::TableSchema::new(
            "products".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: 100 },
                    true,
                ),
            ],
        );
        db.create_table(products_schema).unwrap();
        db.insert_row(
            "products",
            storage::Row::new(vec![
                types::SqlValue::Integer(500),
                types::SqlValue::Varchar("Widget".to_string()),
            ]),
        )
        .unwrap();

        // Execute: SELECT * FROM users
        //   INNER JOIN orders ON users.id = orders.user_id
        //   INNER JOIN products ON orders.product_id = products.id
        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Join {
                left: Box::new(ast::FromClause::Join {
                    left: Box::new(ast::FromClause::Table {
                        name: "users".to_string(),
                        alias: None,
                    }),
                    right: Box::new(ast::FromClause::Table {
                        name: "orders".to_string(),
                        alias: None,
                    }),
                    join_type: ast::JoinType::Inner,
                    condition: Some(ast::Expression::BinaryOp {
                        left: Box::new(ast::Expression::ColumnRef {
                            table: Some("users".to_string()),
                            column: "id".to_string(),
                        }),
                        op: ast::BinaryOperator::Equal,
                        right: Box::new(ast::Expression::ColumnRef {
                            table: Some("orders".to_string()),
                            column: "user_id".to_string(),
                        }),
                    }),
                }),
                right: Box::new(ast::FromClause::Table {
                    name: "products".to_string(),
                    alias: None,
                }),
                join_type: ast::JoinType::Inner,
                condition: Some(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::ColumnRef {
                        table: Some("orders".to_string()),
                        column: "product_id".to_string(),
                    }),
                    op: ast::BinaryOperator::Equal,
                    right: Box::new(ast::Expression::ColumnRef {
                        table: Some("products".to_string()),
                        column: "id".to_string(),
                    }),
                }),
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();

        // Should have 1 row (Alice + order 100 + Widget)
        assert_eq!(result.len(), 1);

        // Row should have 7 columns (users.id, users.name, orders.id, orders.user_id, orders.product_id, products.id, products.name)
        assert_eq!(result[0].values.len(), 7);

        // Verify: users.id=1, users.name="Alice", orders.id=100, orders.user_id=1, orders.product_id=500, products.id=500, products.name="Widget"
        assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
        assert_eq!(result[0].values[1], types::SqlValue::Varchar("Alice".to_string()));
        assert_eq!(result[0].values[2], types::SqlValue::Integer(100));
        assert_eq!(result[0].values[3], types::SqlValue::Integer(1));
        assert_eq!(result[0].values[4], types::SqlValue::Integer(500));
        assert_eq!(result[0].values[5], types::SqlValue::Integer(500));
        assert_eq!(result[0].values[6], types::SqlValue::Varchar("Widget".to_string()));
    }

    // ========================================================================
    // LIMIT and OFFSET Tests
    // ========================================================================

    #[test]
    fn test_limit_basic() {
        // Setup database with 4 users
        let mut db = storage::Database::new();
        let users_schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        db.create_table(users_schema).unwrap();
        for i in 1..=4 {
            db.insert_row("users", storage::Row::new(vec![types::SqlValue::Integer(i)]))
                .unwrap();
        }

        // Execute: SELECT * FROM users LIMIT 2
        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: Some(2),
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
        assert_eq!(result[1].values[0], types::SqlValue::Integer(2));
    }

    #[test]
    fn test_offset_basic() {
        // Setup database with 4 users
        let mut db = storage::Database::new();
        let users_schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        db.create_table(users_schema).unwrap();
        for i in 1..=4 {
            db.insert_row("users", storage::Row::new(vec![types::SqlValue::Integer(i)]))
                .unwrap();
        }

        // Execute: SELECT * FROM users OFFSET 2
        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: Some(2),
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(3));
        assert_eq!(result[1].values[0], types::SqlValue::Integer(4));
    }

    #[test]
    fn test_limit_and_offset() {
        // Setup database with 10 users
        let mut db = storage::Database::new();
        let users_schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        db.create_table(users_schema).unwrap();
        for i in 1..=10 {
            db.insert_row("users", storage::Row::new(vec![types::SqlValue::Integer(i)]))
                .unwrap();
        }

        // Execute: SELECT * FROM users LIMIT 3 OFFSET 2
        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: Some(3),
            offset: Some(2),
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].values[0], types::SqlValue::Integer(3));
        assert_eq!(result[1].values[0], types::SqlValue::Integer(4));
        assert_eq!(result[2].values[0], types::SqlValue::Integer(5));
    }

    #[test]
    fn test_offset_beyond_result_set() {
        // Setup database with 3 users
        let mut db = storage::Database::new();
        let users_schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        db.create_table(users_schema).unwrap();
        for i in 1..=3 {
            db.insert_row("users", storage::Row::new(vec![types::SqlValue::Integer(i)]))
                .unwrap();
        }

        // Execute: SELECT * FROM users OFFSET 10 (beyond result set)
        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: Some(10),
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_limit_greater_than_result_set() {
        // Setup database with 3 users
        let mut db = storage::Database::new();
        let users_schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        db.create_table(users_schema).unwrap();
        for i in 1..=3 {
            db.insert_row("users", storage::Row::new(vec![types::SqlValue::Integer(i)]))
                .unwrap();
        }

        // Execute: SELECT * FROM users LIMIT 100 (greater than result set)
        let executor = SelectExecutor::new(&db);
        let stmt = ast::SelectStmt {
            select_list: vec![ast::SelectItem::Wildcard],
            from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: Some(100),
            offset: None,
        };

        let result = executor.execute(&stmt).unwrap();
        assert_eq!(result.len(), 3); // Should return all 3 users
    }
}
