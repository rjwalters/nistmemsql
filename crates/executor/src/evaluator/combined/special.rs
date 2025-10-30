use super::super::casting::cast_value;
use super::super::core::{CombinedExpressionEvaluator, ExpressionEvaluator};
use super::super::functions::eval_scalar_function;
///! Special expression forms (CASE, CAST, Function calls)
use crate::errors::ExecutorError;

impl<'a> CombinedExpressionEvaluator<'a> {
    /// Evaluate CASE expression
    pub(super) fn eval_case(
        &self,
        operand: &Option<Box<ast::Expression>>,
        when_clauses: &[ast::CaseWhen],
        else_result: &Option<Box<ast::Expression>>,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        match operand {
            // Simple CASE: CASE operand WHEN value THEN result ...
            Some(operand_expr) => {
                let operand_value = self.eval(operand_expr, row)?;

                // Iterate through WHEN clauses
                for when_clause in when_clauses {
                    // Check if ANY condition matches (OR logic)
                    for condition_expr in &when_clause.conditions {
                        let when_value = self.eval(condition_expr, row)?;

                        // Compare operand to when_value using SQL equality semantics
                        if ExpressionEvaluator::values_are_equal(&operand_value, &when_value) {
                            return self.eval(&when_clause.result, row);
                        }
                    }
                }
            }

            // Searched CASE: CASE WHEN condition THEN result ...
            None => {
                // Iterate through WHEN clauses
                for when_clause in when_clauses {
                    // Check if ANY condition is TRUE (OR logic)
                    for condition_expr in &when_clause.conditions {
                        let condition_result = self.eval(condition_expr, row)?;

                        // Check if condition is TRUE (not just truthy)
                        if matches!(condition_result, types::SqlValue::Boolean(true)) {
                            return self.eval(&when_clause.result, row);
                        }
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

    /// Evaluate CAST expression: CAST(expr AS data_type)
    /// Explicit type conversion
    pub(super) fn eval_cast(
        &self,
        expr: &ast::Expression,
        data_type: &types::DataType,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let value = self.eval(expr, row)?;
        cast_value(&value, data_type)
    }

    /// Evaluate function call
    /// Note: Aggregates (COUNT, SUM, etc.) are handled in SelectExecutor
    pub(super) fn eval_function(
        &self,
        name: &str,
        args: &[ast::Expression],
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        // Evaluate all arguments
        let mut arg_values = Vec::new();
        for arg in args {
            arg_values.push(self.eval(arg, row)?);
        }

        // Call shared scalar function evaluator
        eval_scalar_function(name, &arg_values)
    }

    /// Evaluate unary operation
    pub(super) fn eval_unary(
        &self,
        op: &ast::UnaryOperator,
        expr: &ast::Expression,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let val = self.eval(expr, row)?;
        super::super::expressions::operators::eval_unary_op(op, &val)
    }
}
