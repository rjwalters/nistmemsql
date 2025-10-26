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
// SELECT Executor
// ============================================================================

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
                // Project columns
                let projected_row = self.project_row(row, &stmt.select_list, &evaluator)?;
                result_rows.push(projected_row);
            }
        }

        Ok(result_rows)
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
}
