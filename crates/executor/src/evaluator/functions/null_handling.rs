//! NULL handling functions (COALESCE, NULLIF)

use crate::errors::ExecutorError;

/// COALESCE(val1, val2, ..., valN) - returns first non-NULL value
/// SQL:1999 Section 6.12: COALESCE expression
pub(super) fn coalesce(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "COALESCE requires at least one argument".to_string(),
        ));
    }

    // Return first non-NULL value
    for val in args {
        if !matches!(val, types::SqlValue::Null) {
            return Ok(val.clone());
        }
    }

    // All arguments were NULL
    Ok(types::SqlValue::Null)
}

/// NULLIF(val1, val2) - returns NULL if val1 = val2, otherwise val1
/// SQL:1999 Section 6.13: NULLIF expression
pub(super) fn nullif(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "NULLIF requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    let val1 = &args[0];
    let val2 = &args[1];

    // If either is NULL, comparison is undefined - return val1
    if matches!(val1, types::SqlValue::Null) || matches!(val2, types::SqlValue::Null) {
        return Ok(val1.clone());
    }

    // Check equality
    if values_are_equal(val1, val2) {
        Ok(types::SqlValue::Null)
    } else {
        Ok(val1.clone())
    }
}

/// Helper function to check if two SQL values are equal
fn values_are_equal(left: &types::SqlValue, right: &types::SqlValue) -> bool {
    use types::SqlValue::*;

    match (left, right) {
        (Integer(a), Integer(b)) => a == b,
        (Varchar(a), Varchar(b)) => a == b,
        (Character(a), Character(b)) => a == b,
        (Character(a), Varchar(b)) | (Varchar(a), Character(b)) => a == b,
        (Boolean(a), Boolean(b)) => a == b,
        _ => false, // Type mismatch or NULL = not equal
    }
}
