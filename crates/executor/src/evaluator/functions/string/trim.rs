//! String trimming functions for SQL
//!
//! SQL:1999 Section 6.29: String value functions

use crate::errors::ExecutorError;

/// TRIM(string) - Remove leading and trailing spaces
/// SQL:1999 Section 6.29: String value functions
#[allow(dead_code)] // Reserved for future use when basic TRIM needs to be exposed
pub(in crate::evaluator::functions) fn trim(
    args: &[types::SqlValue],
) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TRIM requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Varchar(s) => Ok(types::SqlValue::Varchar(s.trim().to_string())),
        types::SqlValue::Character(s) => Ok(types::SqlValue::Varchar(s.trim().to_string())),
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "TRIM requires string argument, got {:?}",
            val
        ))),
    }
}

/// TRIM with position and custom character support
/// Supports TRIM(BOTH/LEADING/TRAILING 'x' FROM 'string')
/// SQL:1999 Section 6.29: String value functions
#[allow(dead_code)] // Currently called via eval_trim method, may be used directly in future
pub(crate) fn trim_advanced(
    string_val: types::SqlValue,
    position: Option<ast::TrimPosition>,
    removal_char: Option<types::SqlValue>,
) -> Result<types::SqlValue, ExecutorError> {
    // Handle NULL string
    if matches!(string_val, types::SqlValue::Null) {
        return Ok(types::SqlValue::Null);
    }

    // Extract string
    let s = match &string_val {
        types::SqlValue::Varchar(s) => s.as_str(),
        types::SqlValue::Character(s) => s.as_str(),
        _ => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "TRIM requires string argument, got {:?}",
                string_val
            )))
        }
    };

    // Determine character to remove (default: space)
    let char_to_remove = if let Some(removal) = removal_char {
        match removal {
            types::SqlValue::Null => return Ok(types::SqlValue::Null),
            types::SqlValue::Varchar(c) | types::SqlValue::Character(c) => {
                if c.len() != 1 {
                    return Err(ExecutorError::UnsupportedFeature(format!(
                        "TRIM removal character must be single character, got '{}'",
                        c
                    )));
                }
                c.chars().next().unwrap()
            }
            _ => {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "TRIM removal character must be string, got {:?}",
                    removal
                )))
            }
        }
    } else {
        ' ' // Default to space
    };

    // Apply trimming based on position
    let result = match position.unwrap_or(ast::TrimPosition::Both) {
        ast::TrimPosition::Both => s.trim_matches(char_to_remove).to_string(),
        ast::TrimPosition::Leading => s.trim_start_matches(char_to_remove).to_string(),
        ast::TrimPosition::Trailing => s.trim_end_matches(char_to_remove).to_string(),
    };

    Ok(types::SqlValue::Varchar(result))
}
