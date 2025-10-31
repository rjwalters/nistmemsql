//! String search functions for SQL
//!
//! Functions for finding substrings within strings
//! Includes POSITION (SQL standard), INSTR and LOCATE (MySQL/Oracle)

use crate::errors::ExecutorError;

/// POSITION(substring IN string) - Find position (1-indexed)
/// SQL:1999 Section 6.29: String value functions
/// Note: This is called as POSITION('sub', 'string') in our implementation
pub(in crate::evaluator::functions) fn position(
    args: &[types::SqlValue],
) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "POSITION requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => Ok(types::SqlValue::Null),
        (
            types::SqlValue::Varchar(needle) | types::SqlValue::Character(needle),
            types::SqlValue::Varchar(haystack) | types::SqlValue::Character(haystack),
        ) => {
            // Find returns 0-indexed position, SQL needs 1-indexed
            match haystack.find(needle.as_str()) {
                Some(pos) => Ok(types::SqlValue::Integer((pos + 1) as i64)),
                None => Ok(types::SqlValue::Integer(0)),
            }
        }
        (a, b) => Err(ExecutorError::UnsupportedFeature(format!(
            "POSITION requires string arguments, got {:?} and {:?}",
            a, b
        ))),
    }
}

/// INSTR(string, substring) - Find position of substring (1-indexed, 0 if not found)
/// MySQL/Oracle function - returns first occurrence position
pub(in crate::evaluator::functions) fn instr(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "INSTR requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => Ok(types::SqlValue::Null),
        (
            types::SqlValue::Varchar(haystack) | types::SqlValue::Character(haystack),
            types::SqlValue::Varchar(needle) | types::SqlValue::Character(needle),
        ) => {
            // Find returns 0-indexed position, convert to 1-indexed
            // Return 0 if not found (SQL convention)
            let position = haystack.find(needle.as_str()).map(|pos| (pos + 1) as i64).unwrap_or(0);
            Ok(types::SqlValue::Integer(position))
        }
        (haystack, needle) => Err(ExecutorError::UnsupportedFeature(format!(
            "INSTR requires string arguments, got {:?} and {:?}",
            haystack, needle
        ))),
    }
}

/// LOCATE(substring, string, [start]) - Find position of substring with optional start
/// Note: Arguments reversed compared to INSTR (needle, haystack vs haystack, needle)
pub(in crate::evaluator::functions) fn locate(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "LOCATE requires 2 or 3 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => Ok(types::SqlValue::Null),
        (
            types::SqlValue::Varchar(needle) | types::SqlValue::Character(needle),
            types::SqlValue::Varchar(haystack) | types::SqlValue::Character(haystack),
        ) => {
            // Optional start position (1-indexed, default to 1)
            let start_pos = if args.len() == 3 {
                match &args[2] {
                    types::SqlValue::Integer(s) => {
                        // Convert from 1-indexed to 0-indexed, clamp to 0
                        ((*s - 1).max(0) as usize).min(haystack.len())
                    }
                    types::SqlValue::Null => return Ok(types::SqlValue::Null),
                    val => {
                        return Err(ExecutorError::UnsupportedFeature(format!(
                            "LOCATE start position must be integer, got {:?}",
                            val
                        )))
                    }
                }
            } else {
                0
            };

            // Search from start position
            let position = if start_pos >= haystack.len() {
                0 // Start beyond string length -> not found
            } else {
                haystack[start_pos..].find(needle.as_str())
                    .map(|pos| (pos + start_pos + 1) as i64) // Convert to 1-indexed
                    .unwrap_or(0)
            };

            Ok(types::SqlValue::Integer(position))
        }
        (needle, haystack) => Err(ExecutorError::UnsupportedFeature(format!(
            "LOCATE requires string arguments, got {:?} and {:?}",
            needle, haystack
        ))),
    }
}
