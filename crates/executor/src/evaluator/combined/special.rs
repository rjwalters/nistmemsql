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

    /// Evaluate TRIM expression
    pub(super) fn eval_trim(
        &self,
        position: &Option<ast::TrimPosition>,
        removal_char: &types::SqlValue,
        string_value: &types::SqlValue,
    ) -> Result<types::SqlValue, ExecutorError> {
        use ast::TrimPosition;

        // Handle NULL input
        if matches!(string_value, types::SqlValue::Null) {
            return Ok(types::SqlValue::Null);
        }

        // Extract string and character to remove
        let s = match string_value {
            types::SqlValue::Varchar(s) => s,
            types::SqlValue::Character(s) => s,
            _ => {
                return Err(ExecutorError::UnsupportedFeature(
                    "TRIM requires string argument".to_string(),
                ))
            }
        };

        let trim_char = match removal_char {
            types::SqlValue::Varchar(c) if c.len() == 1 => c.chars().next().unwrap(),
            types::SqlValue::Character(c) if c.len() == 1 => c.chars().next().unwrap(),
            types::SqlValue::Varchar(c) if c == " " => ' ',
            _ => {
                return Err(ExecutorError::UnsupportedFeature(
                    "TRIM removal character must be single character".to_string(),
                ))
            }
        };

        // Apply trim based on position
        let result = match position.as_ref().unwrap_or(&TrimPosition::Both) {
            TrimPosition::Both => s.trim_matches(trim_char).to_string(),
            TrimPosition::Leading => s.trim_start_matches(trim_char).to_string(),
            TrimPosition::Trailing => s.trim_end_matches(trim_char).to_string(),
        };

        Ok(types::SqlValue::Varchar(result))
    }
}
