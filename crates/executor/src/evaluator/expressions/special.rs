//! Special expression forms (CASE, Function calls)

use super::super::core::ExpressionEvaluator;
use super::super::functions::eval_scalar_function;
use crate::errors::ExecutorError;

impl<'a> ExpressionEvaluator<'a> {
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

                for when_clause in when_clauses {
                    // Check if ANY condition matches (OR logic)
                    for condition_expr in &when_clause.conditions {
                        let when_value = self.eval(condition_expr, row)?;

                        if super::super::core::ExpressionEvaluator::values_are_equal(
                            &operand_value,
                            &when_value,
                        ) {
                            return self.eval(&when_clause.result, row);
                        }
                    }
                }
            }

            // Searched CASE: CASE WHEN condition THEN result ...
            None => {
                for when_clause in when_clauses {
                    // Check if ANY condition is TRUE (OR logic)
                    for condition_expr in &when_clause.conditions {
                        let condition_result = self.eval(condition_expr, row)?;

                        if matches!(condition_result, types::SqlValue::Boolean(true)) {
                            return self.eval(&when_clause.result, row);
                        }
                    }
                }
            }
        }

        match else_result {
            Some(else_expr) => self.eval(else_expr, row),
            None => Ok(types::SqlValue::Null),
        }
    }

    /// Evaluate function call
    pub(super) fn eval_function(
        &self,
        name: &str,
        args: &[ast::Expression],
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let mut arg_values = Vec::new();
        for arg in args {
            arg_values.push(self.eval(arg, row)?);
        }

        eval_scalar_function(name, &arg_values)
    }
}
