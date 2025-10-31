//! String length measurement functions for SQL
//!
//! SQL:1999 Section 6.29: String value functions

use crate::errors::ExecutorError;

/// CHAR_LENGTH(string [USING unit]) / CHARACTER_LENGTH(string [USING unit])
/// Return string length in characters or octets
/// SQL:1999 Section 6.29: String value functions
pub(in crate::evaluator::functions) fn char_length(
    args: &[types::SqlValue],
    name: &str,
    character_unit: &Option<ast::CharacterUnit>,
) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "{} requires exactly 1 argument, got {}",
            name,
            args.len()
        )));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => {
            // Determine unit: CHARACTERS (default) or OCTETS
            let length = match character_unit {
                Some(ast::CharacterUnit::Octets) => {
                    // USING OCTETS - return byte count
                    s.len() as i64
                }
                Some(ast::CharacterUnit::Characters) | None => {
                    // USING CHARACTERS or default - return character count
                    s.chars().count() as i64
                }
            };
            Ok(types::SqlValue::Integer(length))
        }
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "{} requires string argument, got {:?}",
            name, val
        ))),
    }
}

/// OCTET_LENGTH(string) - Return number of octets (bytes) in string
/// SQL:1999 Section 6.29: String value functions
/// Returns byte length, not character count. For UTF-8:
/// - ASCII characters: 1 byte each
/// - Multi-byte characters: 2-4 bytes each
pub(in crate::evaluator::functions) fn octet_length(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "OCTET_LENGTH requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Varchar(s) => Ok(types::SqlValue::Integer(s.len() as i64)),
        types::SqlValue::Character(s) => Ok(types::SqlValue::Integer(s.len() as i64)),
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "OCTET_LENGTH requires string argument, got {:?}",
            val
        ))),
    }
}

/// LENGTH(str) - Alias for byte length (commonly used)
/// Note: In many SQL implementations, LENGTH returns byte count
pub(in crate::evaluator::functions) fn length(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "LENGTH requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => {
            Ok(types::SqlValue::Integer(s.len() as i64))
        }
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "LENGTH requires string argument, got {:?}",
            val
        ))),
    }
}
