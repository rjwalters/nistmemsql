//! Case conversion functions for SQL strings
//!
//! SQL:1999 Section 6.29: String value functions

use crate::errors::ExecutorError;

/// UPPER(string) - Convert string to uppercase
/// SQL:1999 Section 6.29: String value functions
pub(in crate::evaluator::functions) fn upper(
    args: &[types::SqlValue],
) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "UPPER requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Varchar(s) => Ok(types::SqlValue::Varchar(s.to_uppercase())),
        types::SqlValue::Character(s) => Ok(types::SqlValue::Varchar(s.to_uppercase())),
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "UPPER requires string argument, got {:?}",
            val
        ))),
    }
}

/// LOWER(string) - Convert string to lowercase
/// SQL:1999 Section 6.29: String value functions
pub(in crate::evaluator::functions) fn lower(
    args: &[types::SqlValue],
) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "LOWER requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Varchar(s) => Ok(types::SqlValue::Varchar(s.to_lowercase())),
        types::SqlValue::Character(s) => Ok(types::SqlValue::Varchar(s.to_lowercase())),
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "LOWER requires string argument, got {:?}",
            val
        ))),
    }
}
