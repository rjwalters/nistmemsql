///! Predicate evaluation for combined expressions (BETWEEN, LIKE, IN)

use crate::errors::ExecutorError;
use super::super::core::{CombinedExpressionEvaluator, ExpressionEvaluator};
use super::super::pattern::like_match;

impl<'a> CombinedExpressionEvaluator<'a> {
    /// Evaluate BETWEEN predicate: expr BETWEEN low AND high
    /// Equivalent to: expr >= low AND expr <= high
    /// If negated: expr < low OR expr > high
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

        // Check if expr >= low
        let ge_low = ExpressionEvaluator::eval_binary_op_static(
            &expr_val,
            &ast::BinaryOperator::GreaterThanOrEqual,
            &low_val,
        )?;

        // Check if expr <= high
        let le_high = ExpressionEvaluator::eval_binary_op_static(
            &expr_val,
            &ast::BinaryOperator::LessThanOrEqual,
            &high_val,
        )?;

        // Combine with AND/OR depending on negated
        if negated {
            // NOT BETWEEN: expr < low OR expr > high
            let lt_low = ExpressionEvaluator::eval_binary_op_static(
                &expr_val,
                &ast::BinaryOperator::LessThan,
                &low_val,
            )?;
            let gt_high = ExpressionEvaluator::eval_binary_op_static(
                &expr_val,
                &ast::BinaryOperator::GreaterThan,
                &high_val,
            )?;
            ExpressionEvaluator::eval_binary_op_static(&lt_low, &ast::BinaryOperator::Or, &gt_high)
        } else {
            // BETWEEN: expr >= low AND expr <= high
            ExpressionEvaluator::eval_binary_op_static(&ge_low, &ast::BinaryOperator::And, &le_high)
        }
    }

    /// Evaluate LIKE pattern matching: expr LIKE pattern
    /// Supports wildcards: % (any chars), _ (single char)
    pub(super) fn eval_like(
        &self,
        expr: &ast::Expression,
        pattern: &ast::Expression,
        negated: bool,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let expr_val = self.eval(expr, row)?;
        let pattern_val = self.eval(pattern, row)?;

        // Extract string values
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

        // Perform pattern matching
        let matches = like_match(&text, &pattern_str);

        // Apply negation if needed
        let result = if negated { !matches } else { matches };

        Ok(types::SqlValue::Boolean(result))
    }

    /// Evaluate TRIM expression: TRIM([position] [removal_char FROM] string)
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

    /// Evaluate IN operator with value list: expr IN (val1, val2, ...)
    /// SQL:1999 Section 8.4: IN predicate
    /// Returns TRUE if expr equals any value in the list
    /// Returns FALSE if no match and no NULLs
    /// Returns NULL if no match and list contains NULL
    pub(super) fn eval_in_list(
        &self,
        expr: &ast::Expression,
        values: &[ast::Expression],
        negated: bool,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let expr_val = self.eval(expr, row)?;

        // If left expression is NULL, result is NULL
        if matches!(expr_val, types::SqlValue::Null) {
            return Ok(types::SqlValue::Null);
        }

        let mut found_null = false;

        // Check each value in the list
        for value_expr in values {
            let value = self.eval(value_expr, row)?;

            // Track if we encounter NULL
            if matches!(value, types::SqlValue::Null) {
                found_null = true;
                continue;
            }

            // Compare using equality
            let eq_result = ExpressionEvaluator::eval_binary_op_static(&expr_val, &ast::BinaryOperator::Equal, &value)?;

            // If we found a match, return TRUE (or FALSE if negated)
            if matches!(eq_result, types::SqlValue::Boolean(true)) {
                return Ok(types::SqlValue::Boolean(!negated));
            }
        }

        // No match found
        // If we encountered NULL, return NULL (per SQL three-valued logic)
        // Otherwise return FALSE (or TRUE if negated)
        if found_null {
            Ok(types::SqlValue::Null)
        } else {
            Ok(types::SqlValue::Boolean(negated))
        }
    }

    /// Evaluate IS NULL / IS NOT NULL
    pub(super) fn eval_is_null(
        &self,
        expr: &ast::Expression,
        negated: bool,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let value = self.eval(expr, row)?;
        let is_null = matches!(value, types::SqlValue::Null);
        let result = if negated { !is_null } else { is_null };
        Ok(types::SqlValue::Boolean(result))
    }
}
