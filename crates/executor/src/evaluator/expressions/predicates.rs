//! Predicate evaluation methods (BETWEEN, LIKE, IN, POSITION)

use super::super::casting::cast_value;
use super::super::core::ExpressionEvaluator;
use super::super::pattern::like_match;
use crate::errors::ExecutorError;

impl ExpressionEvaluator<'_> {
    /// Evaluate BETWEEN predicate
    /// If symmetric: swaps low and high if low > high before evaluation
    pub(super) fn eval_between(
        &self,
        expr: &ast::Expression,
        low: &ast::Expression,
        high: &ast::Expression,
        negated: bool,
        symmetric: bool,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let expr_val = self.eval(expr, row)?;
        let mut low_val = self.eval(low, row)?;
        let mut high_val = self.eval(high, row)?;

        // For SYMMETRIC: swap bounds if low > high
        if symmetric {
            let gt_result =
                self.eval_binary_op(&low_val, &ast::BinaryOperator::GreaterThan, &high_val)?;

            if let types::SqlValue::Boolean(true) = gt_result {
                std::mem::swap(&mut low_val, &mut high_val);
            }
        }

        let ge_low =
            self.eval_binary_op(&expr_val, &ast::BinaryOperator::GreaterThanOrEqual, &low_val)?;

        let le_high =
            self.eval_binary_op(&expr_val, &ast::BinaryOperator::LessThanOrEqual, &high_val)?;

        if negated {
            let lt_low =
                self.eval_binary_op(&expr_val, &ast::BinaryOperator::LessThan, &low_val)?;
            let gt_high =
                self.eval_binary_op(&expr_val, &ast::BinaryOperator::GreaterThan, &high_val)?;
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
            (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => Ok(types::SqlValue::Null),
            (
                types::SqlValue::Varchar(needle) | types::SqlValue::Character(needle),
                types::SqlValue::Varchar(haystack) | types::SqlValue::Character(haystack),
            ) => match haystack.find(needle.as_str()) {
                Some(pos) => Ok(types::SqlValue::Integer((pos + 1) as i64)),
                None => Ok(types::SqlValue::Integer(0)),
            },
            _ => Err(ExecutorError::TypeMismatch {
                left: substring_val.clone(),
                op: "POSITION".to_string(),
                right: string_val.clone(),
            }),
        }
    }

    /// Evaluate TRIM expression
    /// TRIM([position] [removal_char FROM] string)
    pub(super) fn eval_trim(
        &self,
        position: &Option<ast::TrimPosition>,
        removal_char: &Option<Box<ast::Expression>>,
        string: &ast::Expression,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let string_val = self.eval(string, row)?;

        // Handle NULL string
        if matches!(string_val, types::SqlValue::Null) {
            return Ok(types::SqlValue::Null);
        }

        // Extract the string value
        let s = match &string_val {
            types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => s.as_str(),
            _ => {
                return Err(ExecutorError::TypeMismatch {
                    left: string_val.clone(),
                    op: "TRIM".to_string(),
                    right: types::SqlValue::Null,
                })
            }
        };

        // Determine the character(s) to remove
        // Need to extract owned String to avoid lifetime issues
        let char_to_remove: String = if let Some(removal_expr) = removal_char {
            let removal_val = self.eval(removal_expr, row)?;

            // Handle NULL removal character
            if matches!(removal_val, types::SqlValue::Null) {
                return Ok(types::SqlValue::Null);
            }

            match removal_val {
                types::SqlValue::Varchar(c) | types::SqlValue::Character(c) => c,
                _ => {
                    return Err(ExecutorError::TypeMismatch {
                        left: removal_val.clone(),
                        op: "TRIM".to_string(),
                        right: string_val.clone(),
                    })
                }
            }
        } else {
            " ".to_string() // Default to space
        };

        let char_to_remove_str = char_to_remove.as_str();

        // Apply trimming based on position (default is Both)
        let result = match position.as_ref().unwrap_or(&ast::TrimPosition::Both) {
            ast::TrimPosition::Both => {
                // Trim from both sides
                let mut result = s;
                while result.starts_with(char_to_remove_str) && !result.is_empty() {
                    result = &result[char_to_remove_str.len()..];
                }
                while result.ends_with(char_to_remove_str) && !result.is_empty() {
                    result = &result[..result.len() - char_to_remove_str.len()];
                }
                result.to_string()
            }
            ast::TrimPosition::Leading => {
                // Trim from start only
                let mut result = s;
                while result.starts_with(char_to_remove_str) && !result.is_empty() {
                    result = &result[char_to_remove_str.len()..];
                }
                result.to_string()
            }
            ast::TrimPosition::Trailing => {
                // Trim from end only
                let mut result = s;
                while result.ends_with(char_to_remove_str) && !result.is_empty() {
                    result = &result[..result.len() - char_to_remove_str.len()];
                }
                result.to_string()
            }
        };

        Ok(types::SqlValue::Varchar(result))
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
