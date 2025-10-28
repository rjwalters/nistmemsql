//! Main evaluation entry point and basic expression types

use crate::errors::ExecutorError;
use super::super::core::ExpressionEvaluator;

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

            ast::Expression::UnaryOp { .. } => Err(ExecutorError::UnsupportedExpression(
                "Unary operators not yet implemented".to_string(),
            )),

            ast::Expression::IsNull { .. } => Err(ExecutorError::UnsupportedExpression(
                "IS NULL should be handled as BinaryOp".to_string(),
            )),

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
}
