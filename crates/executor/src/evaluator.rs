use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;

/// Evaluates expressions in the context of a row
pub struct ExpressionEvaluator<'a> {
    schema: &'a catalog::TableSchema,
    outer_row: Option<&'a storage::Row>,
    outer_schema: Option<&'a catalog::TableSchema>,
}

/// Evaluates expressions with combined schema (for JOINs)
pub(crate) struct CombinedExpressionEvaluator<'a> {
    schema: &'a CombinedSchema,
}

impl<'a> ExpressionEvaluator<'a> {
    /// Create a new expression evaluator for a given schema
    pub fn new(schema: &'a catalog::TableSchema) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: None,
            outer_schema: None,
        }
    }

    /// Create a new expression evaluator with outer query context for correlated subqueries
    pub fn with_outer_context(
        schema: &'a catalog::TableSchema,
        outer_row: &'a storage::Row,
        outer_schema: &'a catalog::TableSchema,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
        }
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
                // Try to resolve in inner schema first
                if let Some(col_index) = self.schema.get_column_index(column) {
                    return row.get(col_index)
                        .cloned()
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                }

                // If not found in inner schema and outer context exists, try outer schema
                if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
                    if let Some(col_index) = outer_schema.get_column_index(column) {
                        return outer_row.get(col_index)
                            .cloned()
                            .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                    }
                }

                // Column not found in either schema
                Err(ExecutorError::ColumnNotFound(column.clone()))
            }

            // Binary operations
            ast::Expression::BinaryOp { left, op, right } => {
                let left_val = self.eval(left, row)?;
                let right_val = self.eval(right, row)?;
                self.eval_binary_op(&left_val, op, &right_val)
            }

            // CASE expression
            ast::Expression::Case {
                operand,
                when_clauses,
                else_result,
            } => self.eval_case(operand, when_clauses, else_result, row),

            // IN operator with subquery
            ast::Expression::In { expr, subquery: _, negated: _ } => {
                // TODO: Full implementation requires database access to execute subquery
                // This requires refactoring ExpressionEvaluator to have database reference
                // For now, evaluate the left expression to ensure it's valid
                let _left_val = self.eval(expr, row)?;
                Err(ExecutorError::UnsupportedFeature(
                    "IN with subquery requires database access - implementation pending".to_string()
                ))
            }

            // TODO: Implement other expression types
            _ => Err(ExecutorError::UnsupportedExpression(format!("{:?}", expr))),
        }
    }

    /// Evaluate a binary operation
    pub(crate) fn eval_binary_op(
        &self,
        left: &types::SqlValue,
        op: &ast::BinaryOperator,
        right: &types::SqlValue,
    ) -> Result<types::SqlValue, ExecutorError> {
        Self::eval_binary_op_static(left, op, right)
    }

    /// Static version of eval_binary_op for shared logic
    pub(crate) fn eval_binary_op_static(
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

            // Boolean comparisons
            (Boolean(a), Equal, Boolean(b)) => Ok(Boolean(a == b)),
            (Boolean(a), NotEqual, Boolean(b)) => Ok(Boolean(a != b)),

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

    /// Evaluate CASE expression
    fn eval_case(
        &self,
        operand: &Option<Box<ast::Expression>>,
        when_clauses: &[(ast::Expression, ast::Expression)],
        else_result: &Option<Box<ast::Expression>>,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        match operand {
            // Simple CASE: CASE operand WHEN value THEN result ...
            Some(operand_expr) => {
                let operand_value = self.eval(operand_expr, row)?;

                // Iterate through WHEN clauses
                for (when_value_expr, then_result_expr) in when_clauses {
                    let when_value = self.eval(when_value_expr, row)?;

                    // Compare operand to when_value using SQL equality semantics
                    // IMPORTANT: Use IS NOT DISTINCT FROM for NULL-safe comparison
                    if Self::values_are_equal(&operand_value, &when_value) {
                        return self.eval(then_result_expr, row);
                    }
                }
            }

            // Searched CASE: CASE WHEN condition THEN result ...
            None => {
                // Iterate through WHEN clauses
                for (when_condition_expr, then_result_expr) in when_clauses {
                    let condition_result = self.eval(when_condition_expr, row)?;

                    // Check if condition is TRUE (not just truthy)
                    if matches!(condition_result, types::SqlValue::Boolean(true)) {
                        return self.eval(then_result_expr, row);
                    }
                    // Note: NULL and FALSE both skip this branch
                }
            }
        }

        // No WHEN matched, evaluate ELSE or return NULL
        match else_result {
            Some(else_expr) => self.eval(else_expr, row),
            None => Ok(types::SqlValue::Null),
        }
    }

    /// Compare two SQL values for equality (NULL-safe for simple CASE)
    /// Uses IS NOT DISTINCT FROM semantics where NULL = NULL is TRUE
    pub(crate) fn values_are_equal(left: &types::SqlValue, right: &types::SqlValue) -> bool {
        use types::SqlValue::*;

        // SQL:1999 semantics for CASE equality:
        // - NULL = NULL is TRUE (different from WHERE clause behavior!)
        // - This is "IS NOT DISTINCT FROM" semantics
        match (left, right) {
            (Null, Null) => true,
            (Null, _) | (_, Null) => false,
            (Integer(a), Integer(b)) => a == b,
            (Varchar(a), Varchar(b)) => a == b,
            (Boolean(a), Boolean(b)) => a == b,
            _ => false, // Type mismatch = not equal
        }
    }
}

impl<'a> CombinedExpressionEvaluator<'a> {
    /// Create a new combined expression evaluator
    pub(crate) fn new(schema: &'a CombinedSchema) -> Self {
        CombinedExpressionEvaluator { schema }
    }

    /// Evaluate an expression in the context of a combined row
    pub(crate) fn eval(
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

            // CASE expression
            ast::Expression::Case {
                operand,
                when_clauses,
                else_result,
            } => self.eval_case(operand, when_clauses, else_result, row),

            // IN operator with subquery
            ast::Expression::In { expr, subquery: _, negated: _ } => {
                // TODO: Full implementation requires database access to execute subquery
                // This requires refactoring CombinedExpressionEvaluator to have database reference
                // For now, evaluate the left expression to ensure it's valid
                let _left_val = self.eval(expr, row)?;
                Err(ExecutorError::UnsupportedFeature(
                    "IN with subquery requires database access - implementation pending".to_string()
                ))
            }

            // TODO: Implement other expression types
            _ => Err(ExecutorError::UnsupportedExpression(format!("{:?}", expr))),
        }
    }

    /// Evaluate CASE expression
    fn eval_case(
        &self,
        operand: &Option<Box<ast::Expression>>,
        when_clauses: &[(ast::Expression, ast::Expression)],
        else_result: &Option<Box<ast::Expression>>,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        match operand {
            // Simple CASE: CASE operand WHEN value THEN result ...
            Some(operand_expr) => {
                let operand_value = self.eval(operand_expr, row)?;

                // Iterate through WHEN clauses
                for (when_value_expr, then_result_expr) in when_clauses {
                    let when_value = self.eval(when_value_expr, row)?;

                    // Compare operand to when_value using SQL equality semantics
                    if ExpressionEvaluator::values_are_equal(&operand_value, &when_value) {
                        return self.eval(then_result_expr, row);
                    }
                }
            }

            // Searched CASE: CASE WHEN condition THEN result ...
            None => {
                // Iterate through WHEN clauses
                for (when_condition_expr, then_result_expr) in when_clauses {
                    let condition_result = self.eval(when_condition_expr, row)?;

                    // Check if condition is TRUE (not just truthy)
                    if matches!(condition_result, types::SqlValue::Boolean(true)) {
                        return self.eval(then_result_expr, row);
                    }
                }
            }
        }

        // No WHEN matched, evaluate ELSE or return NULL
        match else_result {
            Some(else_expr) => self.eval(else_expr, row),
            None => Ok(types::SqlValue::Null),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::{ColumnSchema, TableSchema};
    use types::{DataType, SqlValue};

    #[test]
    fn test_evaluator_with_outer_context_resolves_inner_column() {
        // Create inner schema with "inner_col"
        let inner_schema = TableSchema::new(
            "inner".to_string(),
            vec![ColumnSchema::new(
                "inner_col".to_string(),
                DataType::Integer,
                false,
            )],
        );

        // Create outer schema with "outer_col"
        let outer_schema = TableSchema::new(
            "outer".to_string(),
            vec![ColumnSchema::new(
                "outer_col".to_string(),
                DataType::Integer,
                false,
            )],
        );

        let outer_row = storage::Row::new(vec![SqlValue::Integer(100)]);
        let inner_row = storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Should resolve inner_col from inner row
        let expr = ast::Expression::ColumnRef {
            table: None,
            column: "inner_col".to_string(),
        };

        let result = evaluator.eval(&expr, &inner_row).unwrap();
        assert_eq!(result, SqlValue::Integer(42));
    }

    #[test]
    fn test_evaluator_with_outer_context_resolves_outer_column() {
        // Create inner schema with "inner_col"
        let inner_schema = TableSchema::new(
            "inner".to_string(),
            vec![ColumnSchema::new(
                "inner_col".to_string(),
                DataType::Integer,
                false,
            )],
        );

        // Create outer schema with "outer_col"
        let outer_schema = TableSchema::new(
            "outer".to_string(),
            vec![ColumnSchema::new(
                "outer_col".to_string(),
                DataType::Integer,
                false,
            )],
        );

        let outer_row = storage::Row::new(vec![SqlValue::Integer(100)]);
        let inner_row = storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Should resolve outer_col from outer row (not in inner schema)
        let expr = ast::Expression::ColumnRef {
            table: None,
            column: "outer_col".to_string(),
        };

        let result = evaluator.eval(&expr, &inner_row).unwrap();
        assert_eq!(result, SqlValue::Integer(100));
    }

    #[test]
    fn test_evaluator_with_outer_context_inner_shadows_outer() {
        // Both schemas have "col" - inner should win
        let inner_schema = TableSchema::new(
            "inner".to_string(),
            vec![ColumnSchema::new("col".to_string(), DataType::Integer, false)],
        );

        let outer_schema = TableSchema::new(
            "outer".to_string(),
            vec![ColumnSchema::new("col".to_string(), DataType::Integer, false)],
        );

        let outer_row = storage::Row::new(vec![SqlValue::Integer(999)]);
        let inner_row = storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        let expr = ast::Expression::ColumnRef {
            table: None,
            column: "col".to_string(),
        };

        let result = evaluator.eval(&expr, &inner_row).unwrap();
        // Should get inner value (42), not outer (999)
        assert_eq!(result, SqlValue::Integer(42));
    }

    #[test]
    fn test_evaluator_with_outer_context_column_not_found() {
        let inner_schema = TableSchema::new(
            "inner".to_string(),
            vec![ColumnSchema::new(
                "inner_col".to_string(),
                DataType::Integer,
                false,
            )],
        );

        let outer_schema = TableSchema::new(
            "outer".to_string(),
            vec![ColumnSchema::new(
                "outer_col".to_string(),
                DataType::Integer,
                false,
            )],
        );

        let outer_row = storage::Row::new(vec![SqlValue::Integer(100)]);
        let inner_row = storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Try to resolve non-existent column
        let expr = ast::Expression::ColumnRef {
            table: None,
            column: "nonexistent".to_string(),
        };

        let result = evaluator.eval(&expr, &inner_row);
        assert!(matches!(result, Err(ExecutorError::ColumnNotFound(_))));
    }

    #[test]
    fn test_evaluator_without_outer_context() {
        // Normal evaluator without outer context
        let schema = TableSchema::new(
            "table".to_string(),
            vec![ColumnSchema::new("col".to_string(), DataType::Integer, false)],
        );

        let evaluator = ExpressionEvaluator::new(&schema);
        let row = storage::Row::new(vec![SqlValue::Integer(42)]);

        let expr = ast::Expression::ColumnRef {
            table: None,
            column: "col".to_string(),
        };

        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Integer(42));
    }
}
