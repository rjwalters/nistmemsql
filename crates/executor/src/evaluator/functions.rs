//! Scalar function evaluation (COALESCE, NULLIF, string functions, etc.)

use crate::errors::ExecutorError;

/// Evaluate a scalar function on given argument values
///
/// This handles SQL scalar functions that don't depend on table schemas
/// (unlike aggregates like COUNT, SUM which are handled elsewhere).
pub(super) fn eval_scalar_function(
    name: &str,
    args: &[types::SqlValue],
) -> Result<types::SqlValue, ExecutorError> {
    match name.to_uppercase().as_str() {
        // COALESCE(val1, val2, ..., valN) - returns first non-NULL value
        // SQL:1999 Section 6.12: COALESCE expression
        "COALESCE" => {
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

        // NULLIF(val1, val2) - returns NULL if val1 = val2, otherwise val1
        // SQL:1999 Section 6.13: NULLIF expression
        "NULLIF" => {
            if args.len() != 2 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("NULLIF requires exactly 2 arguments, got {}", args.len()),
                ));
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

        // UPPER(string) - Convert string to uppercase
        // SQL:1999 Section 6.29: String value functions
        "UPPER" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("UPPER requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                types::SqlValue::Varchar(s) => Ok(types::SqlValue::Varchar(s.to_uppercase())),
                types::SqlValue::Character(s) => Ok(types::SqlValue::Varchar(s.to_uppercase())),
                val => Err(ExecutorError::UnsupportedFeature(
                    format!("UPPER requires string argument, got {:?}", val),
                )),
            }
        }

        // LOWER(string) - Convert string to lowercase
        // SQL:1999 Section 6.29: String value functions
        "LOWER" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("LOWER requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                types::SqlValue::Varchar(s) => Ok(types::SqlValue::Varchar(s.to_lowercase())),
                types::SqlValue::Character(s) => Ok(types::SqlValue::Varchar(s.to_lowercase())),
                val => Err(ExecutorError::UnsupportedFeature(
                    format!("LOWER requires string argument, got {:?}", val),
                )),
            }
        }

        // SUBSTRING(string, start [, length]) - Extract substring
        // SQL:1999 Section 6.29: String value functions
        // start is 1-based (SQL standard), length is optional
        "SUBSTRING" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("SUBSTRING requires 2 or 3 arguments, got {}", args.len()),
                ));
            }

            let string_val = &args[0];
            let start_val = &args[1];
            let length_val = args.get(2);

            // Handle NULL inputs
            if matches!(string_val, types::SqlValue::Null)
                || matches!(start_val, types::SqlValue::Null)
                || (length_val.is_some() && matches!(length_val, Some(types::SqlValue::Null)))
            {
                return Ok(types::SqlValue::Null);
            }

            // Extract string
            let s = match string_val {
                types::SqlValue::Varchar(s) => s.as_str(),
                types::SqlValue::Character(s) => s.as_str(),
                _ => {
                    return Err(ExecutorError::UnsupportedFeature(
                        format!("SUBSTRING requires string argument, got {:?}", string_val),
                    ))
                }
            };

            // Extract start position (1-based in SQL)
            let start = match start_val {
                types::SqlValue::Integer(n) => *n,
                _ => {
                    return Err(ExecutorError::UnsupportedFeature(
                        format!("SUBSTRING start position must be integer, got {:?}", start_val),
                    ))
                }
            };

            // Extract optional length
            let length = if let Some(len_val) = length_val {
                match len_val {
                    types::SqlValue::Integer(n) => Some(*n),
                    _ => {
                        return Err(ExecutorError::UnsupportedFeature(
                            format!("SUBSTRING length must be integer, got {:?}", len_val),
                        ))
                    }
                }
            } else {
                None
            };

            // Convert 1-based SQL index to 0-based Rust index
            // SQL standard: SUBSTRING('hello', 1, 1) = 'h'
            let start_idx = if start > 0 {
                (start - 1) as usize
            } else {
                // SQL:1999 treats start <= 0 as starting from position 1
                0
            };

            // Calculate substring
            let result = if start_idx >= s.len() {
                // Start beyond string length - return empty string
                String::new()
            } else if let Some(len) = length {
                if len <= 0 {
                    // Zero or negative length - return empty string
                    String::new()
                } else {
                    let len_usize = len as usize;
                    let end_idx = std::cmp::min(start_idx + len_usize, s.len());
                    s[start_idx..end_idx].to_string()
                }
            } else {
                // No length specified - extract to end of string
                s[start_idx..].to_string()
            };

            Ok(types::SqlValue::Varchar(result))
        }

        // TRIM(string) - Remove leading and trailing spaces
        // SQL:1999 Section 6.29: String value functions
        "TRIM" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("TRIM requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                types::SqlValue::Varchar(s) => Ok(types::SqlValue::Varchar(s.trim().to_string())),
                types::SqlValue::Character(s) => {
                    Ok(types::SqlValue::Varchar(s.trim().to_string()))
                }
                val => Err(ExecutorError::UnsupportedFeature(
                    format!("TRIM requires string argument, got {:?}", val),
                )),
            }
        }

        // CHAR_LENGTH(string) / CHARACTER_LENGTH(string) - Return string length
        // SQL:1999 Section 6.29: String value functions
        "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("{} requires exactly 1 argument, got {}", name, args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                types::SqlValue::Varchar(s) => Ok(types::SqlValue::Integer(s.len() as i64)),
                types::SqlValue::Character(s) => Ok(types::SqlValue::Integer(s.len() as i64)),
                val => Err(ExecutorError::UnsupportedFeature(
                    format!("{} requires string argument, got {:?}", name, val),
                )),
            }
        }

        // Unknown function
        _ => Err(ExecutorError::UnsupportedFeature(
            format!("Scalar function {} not supported in this context", name),
        )),
    }
}

/// Simple equality check for NULLIF
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
