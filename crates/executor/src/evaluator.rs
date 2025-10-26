use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;

/// Evaluates expressions in the context of a row
pub struct ExpressionEvaluator<'a> {
    schema: &'a catalog::TableSchema,
}

/// Evaluates expressions with combined schema (for JOINs)
pub(crate) struct CombinedExpressionEvaluator<'a> {
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

            // TODO: Implement other expression types
            _ => Err(ExecutorError::UnsupportedExpression(format!("{:?}", expr))),
        }
    }
}
