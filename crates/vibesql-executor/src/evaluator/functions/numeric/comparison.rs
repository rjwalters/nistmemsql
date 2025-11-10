//! Comparison functions
//!
//! Implements GREATEST and LEAST functions.

use vibesql_types::SqlValue;

use super::exponential::numeric_to_f64;
use crate::errors::ExecutorError;

/// GREATEST(val1, val2, ...) - Returns greatest value
pub fn greatest(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "GREATEST requires at least one argument".to_string(),
        ));
    }

    let mut max_val = &args[0];
    for arg in &args[1..] {
        // Skip NULL values
        if matches!(arg, SqlValue::Null) {
            continue;
        }
        if matches!(max_val, SqlValue::Null) {
            max_val = arg;
            continue;
        }

        // Compare values
        match (max_val, arg) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => {
                if b > a {
                    max_val = arg;
                }
            }
            (SqlValue::Double(a), SqlValue::Double(b)) => {
                if b > a {
                    max_val = arg;
                }
            }
            (a, b) => {
                let a_f64 = numeric_to_f64(a)?;
                let b_f64 = numeric_to_f64(b)?;
                if b_f64 > a_f64 {
                    max_val = arg;
                }
            }
        }
    }

    Ok(max_val.clone())
}

/// LEAST(val1, val2, ...) - Returns smallest value
pub fn least(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "LEAST requires at least one argument".to_string(),
        ));
    }

    let mut min_val = &args[0];
    for arg in &args[1..] {
        // Skip NULL values
        if matches!(arg, SqlValue::Null) {
            continue;
        }
        if matches!(min_val, SqlValue::Null) {
            min_val = arg;
            continue;
        }

        // Compare values
        match (min_val, arg) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => {
                if b < a {
                    min_val = arg;
                }
            }
            (SqlValue::Double(a), SqlValue::Double(b)) => {
                if b < a {
                    min_val = arg;
                }
            }
            (a, b) => {
                let a_f64 = numeric_to_f64(a)?;
                let b_f64 = numeric_to_f64(b)?;
                if b_f64 < a_f64 {
                    min_val = arg;
                }
            }
        }
    }

    Ok(min_val.clone())
}
