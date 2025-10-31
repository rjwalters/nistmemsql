//! String concatenation functions for SQL
//!
//! SQL:1999 Section 6.29: String value functions

use crate::errors::ExecutorError;

/// CONCAT(str1, str2, ...) - Concatenate strings
/// SQL:1999 Section 6.29: String value functions
pub(in crate::evaluator::functions) fn concat(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "CONCAT requires at least one argument".to_string(),
        ));
    }

    let mut result = String::new();
    for arg in args {
        match arg {
            types::SqlValue::Null => {
                // SQL standard: NULL in concat makes result NULL
                return Ok(types::SqlValue::Null);
            }
            types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => {
                result.push_str(s);
            }
            types::SqlValue::Integer(n) => result.push_str(&n.to_string()),
            val => {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "CONCAT cannot convert {:?} to string",
                    val
                )))
            }
        }
    }
    Ok(types::SqlValue::Varchar(result))
}
