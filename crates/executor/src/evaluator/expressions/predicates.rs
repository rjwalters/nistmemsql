//! Predicate evaluation methods (BETWEEN, LIKE, IN, POSITION)

use crate::errors::ExecutorError;
use super::super::core::ExpressionEvaluator;
use super::super::casting::cast_value;
use super::super::pattern::like_match;

impl<'a> ExpressionEvaluator<'a> {
    /// Evaluate BETWEEN predicate
    pub(super) fn eval_between(
        &self,
        expr: &ast::Expression,
        low: &ast::Expression,
        high: &ast::Expression,
        negated: bool,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let expr_val = self.eval(expr, row)?;
        let low_val = self.eval(low, row)?;
        let high_val = self.eval(high, row)?;

        let ge_low = self.eval_binary_op(
            &expr_val,
            &ast::BinaryOperator::GreaterThanOrEqual,
            &low_val,
        )?;

        let le_high = self.eval_binary_op(
            &expr_val,
            &ast::BinaryOperator::LessThanOrEqual,
            &high_val,
        )?;

        if negated {
            let lt_low = self.eval_binary_op(
                &expr_val,
                &ast::BinaryOperator::LessThan,
                &low_val,
            )?;
            let gt_high = self.eval_binary_op(
                &expr_val,
                &ast::BinaryOperator::GreaterThan,
                &high_val,
            )?;
            self.eval_binary_op(&lt_low, &ast::BinaryOperator::Or, &gt_high)
        } else {
            self.eval_binary_op(&ge_low, &ast::BinaryOperator::And, &le_high)
        }
    }

    /// Evaluate CAST expression
    pub(super) fn eval_cast(
        &self,
        expr: &ast::Expression,
        data_type: &types::DataType,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let value = self.eval(expr, row)?;
        cast_value(&value, data_type)
    }

    /// Evaluate POSITION expression
    pub(super) fn eval_position(
        &self,
        substring: &ast::Expression,
        string: &ast::Expression,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let substring_val = self.eval(substring, row)?;
        let string_val = self.eval(string, row)?;

        match (&substring_val, &string_val) {
            (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => {
                Ok(types::SqlValue::Null)
            }
            (
                types::SqlValue::Varchar(needle) | types::SqlValue::Character(needle),
                types::SqlValue::Varchar(haystack) | types::SqlValue::Character(haystack),
            ) => {
                match haystack.find(needle.as_str()) {
                    Some(pos) => Ok(types::SqlValue::Integer((pos + 1) as i64)),
                    None => Ok(types::SqlValue::Integer(0)),
                }
            }
            _ => Err(ExecutorError::TypeMismatch {
                left: substring_val.clone(),
                op: "POSITION".to_string(),
                right: string_val.clone(),
            }),
        }
    }

    /// Evaluate LIKE predicate
    pub(super) fn eval_like(
        &self,
        expr: &ast::Expression,
        pattern: &ast::Expression,
        negated: bool,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let expr_val = self.eval(expr, row)?;
        let pattern_val = self.eval(pattern, row)?;

        let text = match expr_val {
            types::SqlValue::Varchar(ref s) | types::SqlValue::Character(ref s) => s.clone(),
            types::SqlValue::Null => return Ok(types::SqlValue::Null),
            _ => {
                return Err(ExecutorError::TypeMismatch {
                    left: expr_val,
                    op: "LIKE".to_string(),
                    right: pattern_val,
                })
            }
        };

        let pattern_str = match pattern_val {
            types::SqlValue::Varchar(ref s) | types::SqlValue::Character(ref s) => s.clone(),
            types::SqlValue::Null => return Ok(types::SqlValue::Null),
            _ => {
                return Err(ExecutorError::TypeMismatch {
                    left: expr_val,
                    op: "LIKE".to_string(),
                    right: pattern_val,
                })
            }
        };

        let matches = like_match(&text, &pattern_str);
        let result = if negated { !matches } else { matches };

        Ok(types::SqlValue::Boolean(result))
    }

    /// Evaluate IN list predicate
    pub(super) fn eval_in_list(
        &self,
        expr: &ast::Expression,
        values: &[ast::Expression],
        negated: bool,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let expr_val = self.eval(expr, row)?;

        if matches!(expr_val, types::SqlValue::Null) {
            return Ok(types::SqlValue::Null);
        }

        let mut found_null = false;

        for value_expr in values {
            let value = self.eval(value_expr, row)?;

            if matches!(value, types::SqlValue::Null) {
                found_null = true;
                continue;
            }

            let eq_result = self.eval_binary_op(&expr_val, &ast::BinaryOperator::Equal, &value)?;

            if matches!(eq_result, types::SqlValue::Boolean(true)) {
                return Ok(types::SqlValue::Boolean(!negated));
            }
        }

        if found_null {
            Ok(types::SqlValue::Null)
        } else {
            Ok(types::SqlValue::Boolean(negated))
        }
    }
}
