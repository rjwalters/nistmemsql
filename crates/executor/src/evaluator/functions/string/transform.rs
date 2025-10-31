//! String transformation functions for SQL
//!
//! SQL:1999 Section 6.29: String value functions

use crate::errors::ExecutorError;

/// REPLACE(string, from, to) - Replace occurrences
/// SQL:1999 Section 6.29: String value functions
pub(in crate::evaluator::functions) fn replace(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 3 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "REPLACE requires exactly 3 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1], &args[2]) {
        (types::SqlValue::Null, _, _)
        | (_, types::SqlValue::Null, _)
        | (_, _, types::SqlValue::Null) => Ok(types::SqlValue::Null),
        (
            types::SqlValue::Varchar(text) | types::SqlValue::Character(text),
            types::SqlValue::Varchar(from) | types::SqlValue::Character(from),
            types::SqlValue::Varchar(to) | types::SqlValue::Character(to),
        ) => Ok(types::SqlValue::Varchar(text.replace(from.as_str(), to.as_str()))),
        (a, b, c) => Err(ExecutorError::UnsupportedFeature(format!(
            "REPLACE requires string arguments, got {:?}, {:?}, {:?}",
            a, b, c
        ))),
    }
}

/// REVERSE(string) - Reverse a string
pub(in crate::evaluator::functions) fn reverse(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "REVERSE requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => {
            Ok(types::SqlValue::Varchar(s.chars().rev().collect()))
        }
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "REVERSE requires string argument, got {:?}",
            val
        ))),
    }
}
