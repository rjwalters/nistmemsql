//! Substring extraction functions for SQL
//!
//! SQL:1999 Section 6.29: String value functions

use crate::errors::ExecutorError;

/// SUBSTRING(string, start [, length]) - Extract substring
/// SQL:1999 Section 6.29: String value functions
/// start is 1-based (SQL standard), length is optional
pub(in crate::evaluator::functions) fn substring(
    args: &[types::SqlValue],
) -> Result<types::SqlValue, ExecutorError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "SUBSTRING requires 2 or 3 arguments, got {}",
            args.len()
        )));
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
            return Err(ExecutorError::UnsupportedFeature(format!(
                "SUBSTRING requires string argument, got {:?}",
                string_val
            )))
        }
    };

    // Extract start position (1-based in SQL)
    let start = match start_val {
        types::SqlValue::Integer(n) => *n,
        _ => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "SUBSTRING start position must be integer, got {:?}",
                start_val
            )))
        }
    };

    // Extract optional length
    let length = if let Some(len_val) = length_val {
        match len_val {
            types::SqlValue::Integer(n) => Some(*n),
            _ => {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "SUBSTRING length must be integer, got {:?}",
                    len_val
                )))
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

/// LEFT(string, n) - Leftmost n characters
pub(in crate::evaluator::functions) fn left(
    args: &[types::SqlValue],
) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "LEFT requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => Ok(types::SqlValue::Null),
        (
            types::SqlValue::Varchar(s) | types::SqlValue::Character(s),
            types::SqlValue::Integer(n),
        ) => {
            if *n < 0 {
                Ok(types::SqlValue::Varchar(String::new()))
            } else {
                let n_usize = *n as usize;
                let result: String = s.chars().take(n_usize).collect();
                Ok(types::SqlValue::Varchar(result))
            }
        }
        (a, b) => Err(ExecutorError::UnsupportedFeature(format!(
            "LEFT requires string and integer arguments, got {:?} and {:?}",
            a, b
        ))),
    }
}

/// RIGHT(string, n) - Rightmost n characters
pub(in crate::evaluator::functions) fn right(
    args: &[types::SqlValue],
) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "RIGHT requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => Ok(types::SqlValue::Null),
        (
            types::SqlValue::Varchar(s) | types::SqlValue::Character(s),
            types::SqlValue::Integer(n),
        ) => {
            if *n < 0 {
                Ok(types::SqlValue::Varchar(String::new()))
            } else {
                let n_usize = *n as usize;
                let char_count = s.chars().count();
                if n_usize >= char_count {
                    Ok(types::SqlValue::Varchar(s.clone()))
                } else {
                    let skip_count = char_count - n_usize;
                    let result: String = s.chars().skip(skip_count).collect();
                    Ok(types::SqlValue::Varchar(result))
                }
            }
        }
        (a, b) => Err(ExecutorError::UnsupportedFeature(format!(
            "RIGHT requires string and integer arguments, got {:?} and {:?}",
            a, b
        ))),
    }
}
