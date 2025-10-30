//! Main evaluation entry point and basic expression types

use super::super::core::ExpressionEvaluator;
use crate::errors::ExecutorError;
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

            // DEFAULT keyword - not allowed in SELECT/WHERE expressions
            ast::Expression::Default => Err(ExecutorError::UnsupportedExpression(
                "DEFAULT keyword is only valid in INSERT VALUES and UPDATE SET clauses".to_string(),
            )),

            // Column reference - look up column index and get value from row
            ast::Expression::ColumnRef { table: _, column } => self.eval_column_ref(column, row),

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
            ast::Expression::ScalarSubquery(subquery) => self.eval_scalar_subquery(subquery, row),

            // BETWEEN predicate
            ast::Expression::Between { expr, low, high, negated } => {
                self.eval_between(expr, low, high, *negated, row)
            }

            // CAST expression
            ast::Expression::Cast { expr, data_type } => self.eval_cast(expr, data_type, row),

            // POSITION expression
            ast::Expression::Position { substring, string, character_unit: _ } => {
                self.eval_position(substring, string, row)
            }

            // TRIM expression
            ast::Expression::Trim { position, removal_char, string } => {
                self.eval_trim(position, removal_char, string, row)
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
            ast::Expression::Function { name, args, character_unit: _ } => self.eval_function(name, args, row),

            // Current date/time functions
            ast::Expression::CurrentDate => {
                super::super::functions::eval_scalar_function("CURRENT_DATE", &[])
            }
            ast::Expression::CurrentTime { precision: _ } => {
                // For now, ignore precision and call existing function
                // Phase 2 will implement precision-aware formatting
                super::super::functions::eval_scalar_function("CURRENT_TIME", &[])
            }
            ast::Expression::CurrentTimestamp { precision: _ } => {
                // For now, ignore precision and call existing function
                // Phase 2 will implement precision-aware formatting
                super::super::functions::eval_scalar_function("CURRENT_TIMESTAMP", &[])
            }

            // Unsupported expressions
            ast::Expression::Wildcard => Err(ExecutorError::UnsupportedExpression(
                "Wildcard (*) not supported in expressions".to_string(),
            )),

            // Unary operations
            ast::Expression::UnaryOp { op, expr } => {
                let val = self.eval(expr, row)?;
                super::operators::eval_unary_op(op, &val)
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

            ast::Expression::AggregateFunction { .. } => Err(ExecutorError::UnsupportedExpression(
                "Aggregate functions should be evaluated in aggregation context".to_string(),
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
