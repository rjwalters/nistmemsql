//! Main evaluation entry point and basic expression types

use crate::errors::ExecutorError;
use super::super::core::ExpressionEvaluator;
use types::SqlValue;

impl<'a> ExpressionEvaluator<'a> {
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
                self.eval_column_ref(column, row)
            }

            // Binary operations
            ast::Expression::BinaryOp { left, op, right } => {
                let left_val = self.eval(left, row)?;
                let right_val = self.eval(right, row)?;
                self.eval_binary_op(&left_val, op, &right_val)
            }

            // CASE expression
            ast::Expression::Case { operand, when_clauses, else_result } => {
                self.eval_case(operand, when_clauses, else_result, row)
            }

            // IN operator with subquery
            ast::Expression::In { expr, subquery, negated } => {
                self.eval_in_subquery(expr, subquery, *negated, row)
            }

            // Scalar subquery
            ast::Expression::ScalarSubquery(subquery) => {
                self.eval_scalar_subquery(subquery, row)
            }

            // BETWEEN predicate
            ast::Expression::Between { expr, low, high, negated } => {
                self.eval_between(expr, low, high, *negated, row)
            }

            // CAST expression
            ast::Expression::Cast { expr, data_type } => {
                self.eval_cast(expr, data_type, row)
            }

            // POSITION expression
            ast::Expression::Position { substring, string } => {
                self.eval_position(substring, string, row)
            }

            // LIKE pattern matching
            ast::Expression::Like { expr, pattern, negated } => {
                self.eval_like(expr, pattern, *negated, row)
            }

            // IN list (value list)
            ast::Expression::InList { expr, values, negated } => {
                self.eval_in_list(expr, values, *negated, row)
            }

            // EXISTS predicate
            ast::Expression::Exists { subquery, negated } => {
                self.eval_exists(subquery, *negated, row)
            }

            // Quantified comparison (ALL/ANY/SOME)
            ast::Expression::QuantifiedComparison { expr, op, quantifier, subquery } => {
                self.eval_quantified(expr, op, quantifier, subquery, row)
            }

            // Function call
            ast::Expression::Function { name, args } => {
                self.eval_function(name, args, row)
            }

            // Unsupported expressions
            ast::Expression::Wildcard => Err(ExecutorError::UnsupportedExpression(
                "Wildcard (*) not supported in expressions".to_string(),
            )),

            // Unary operations
            ast::Expression::UnaryOp { op, expr } => {
                let val = self.eval(expr, row)?;
                self.eval_unary_op(op, &val)
            }

            ast::Expression::IsNull { expr, negated } => {
                let value = self.eval(expr, row)?;
                let is_null = matches!(value, SqlValue::Null);
                let result = if *negated { !is_null } else { is_null };
                Ok(SqlValue::Boolean(result))
            }

            ast::Expression::WindowFunction { .. } => Err(ExecutorError::UnsupportedExpression(
                "Window functions should be evaluated separately".to_string(),
            )),
        }
    }

    /// Evaluate column reference
    fn eval_column_ref(
        &self,
        column: &str,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        // Try to resolve in inner schema first
        if let Some(col_index) = self.schema.get_column_index(column) {
            return row
                .get(col_index)
                .cloned()
                .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
        }

        // If not found in inner schema and outer context exists, try outer schema
        if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
            if let Some(col_index) = outer_schema.get_column_index(column) {
                return outer_row
                    .get(col_index)
                    .cloned()
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
            }
        }

        // Column not found in either schema
        Err(ExecutorError::ColumnNotFound(column.to_string()))
    }

    /// Evaluate a unary operation
    fn eval_unary_op(
        &self,
        op: &ast::UnaryOperator,
        val: &types::SqlValue,
    ) -> Result<types::SqlValue, ExecutorError> {
        use ast::UnaryOperator::*;
        use types::SqlValue;

        match (op, val) {
            // Unary plus - identity operation (return value unchanged)
            (Plus, SqlValue::Integer(n)) => Ok(SqlValue::Integer(*n)),
            (Plus, SqlValue::Smallint(n)) => Ok(SqlValue::Smallint(*n)),
            (Plus, SqlValue::Bigint(n)) => Ok(SqlValue::Bigint(*n)),
            (Plus, SqlValue::Float(n)) => Ok(SqlValue::Float(*n)),
            (Plus, SqlValue::Real(n)) => Ok(SqlValue::Real(*n)),
            (Plus, SqlValue::Double(n)) => Ok(SqlValue::Double(*n)),
            (Plus, SqlValue::Numeric(s)) => Ok(SqlValue::Numeric(s.clone())),

            // Unary minus - negation
            (Minus, SqlValue::Integer(n)) => Ok(SqlValue::Integer(-n)),
            (Minus, SqlValue::Smallint(n)) => Ok(SqlValue::Smallint(-n)),
            (Minus, SqlValue::Bigint(n)) => Ok(SqlValue::Bigint(-n)),
            (Minus, SqlValue::Float(n)) => Ok(SqlValue::Float(-n)),
            (Minus, SqlValue::Real(n)) => Ok(SqlValue::Real(-n)),
            (Minus, SqlValue::Double(n)) => Ok(SqlValue::Double(-n)),
            (Minus, SqlValue::Numeric(s)) => {
                // Negate numeric string: if starts with -, remove it; otherwise add -
                let negated = if s.starts_with('-') {
                    s[1..].to_string()
                } else {
                    format!("-{}", s)
                };
                Ok(SqlValue::Numeric(negated))
            }

            // NULL propagation - unary operations on NULL return NULL
            (Plus | Minus, SqlValue::Null) => Ok(SqlValue::Null),

            // Type errors
            (Plus, val) => Err(ExecutorError::TypeMismatch {
                left: val.clone(),
                op: "unary +".to_string(),
                right: SqlValue::Null,
            }),
            (Minus, val) => Err(ExecutorError::TypeMismatch {
                left: val.clone(),
                op: "unary -".to_string(),
                right: SqlValue::Null,
            }),

            // Other unary operators (NOT, IS NULL, etc.) are handled elsewhere
            _ => Err(ExecutorError::UnsupportedExpression(
                format!("Unary operator {:?} not supported in this context", op),
            )),
        }
    }
}
