///! Main evaluation entry point for combined expressions

use crate::errors::ExecutorError;
use super::super::core::{CombinedExpressionEvaluator, ExpressionEvaluator};

impl<'a> CombinedExpressionEvaluator<'a> {
    /// Evaluate an expression in the context of a combined row
    /// This is the main entry point for expression evaluation
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
            ast::Expression::Case { operand, when_clauses, else_result } => {
                self.eval_case(operand, when_clauses, else_result, row)
            }

            // IN operator with subquery
            ast::Expression::In { expr, subquery, negated } => {
                self.eval_in_subquery(expr, subquery, *negated, row)
            }

            // Scalar subquery - must return exactly one row and one column
            ast::Expression::ScalarSubquery(subquery) => {
                self.eval_scalar_subquery(subquery, row)
            }

            // BETWEEN predicate: expr BETWEEN low AND high
            ast::Expression::Between { expr, low, high, negated } => {
                self.eval_between(expr, low, high, *negated, row)
            }

            // CAST expression: CAST(expr AS data_type)
            ast::Expression::Cast { expr, data_type } => {
                self.eval_cast(expr, data_type, row)
            }

            // LIKE pattern matching: expr LIKE pattern
            ast::Expression::Like { expr, pattern, negated } => {
                self.eval_like(expr, pattern, *negated, row)
            }

            // IN operator with value list: expr IN (val1, val2, ...)
            ast::Expression::InList { expr, values, negated } => {
                self.eval_in_list(expr, values, *negated, row)
            }

            // EXISTS predicate: EXISTS (SELECT ...)
            ast::Expression::Exists { subquery, negated } => {
                self.eval_exists(subquery, *negated, row)
            }

            // Quantified comparison: expr op ALL/ANY/SOME (SELECT ...)
            ast::Expression::QuantifiedComparison { expr, op, quantifier, subquery } => {
                self.eval_quantified(expr, op, quantifier, subquery, row)
            }

            // IS NULL / IS NOT NULL
            ast::Expression::IsNull { expr, negated } => {
                self.eval_is_null(expr, *negated, row)
            }

            // Function expressions - handle scalar functions (not aggregates)
            ast::Expression::Function { name, args } => {
                self.eval_function(name, args, row)
            }

            // Unary operations (delegate to shared function)
            ast::Expression::UnaryOp { op, expr } => {
                self.eval_unary(op, expr, row)
            }

            // Unsupported expressions
            _ => Err(ExecutorError::UnsupportedExpression(format!("{:?}", expr))),
        }
    }
}
