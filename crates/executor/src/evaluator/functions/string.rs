//! String function implementations for SQL scalar functions
//!
//! This module contains all string manipulation functions including:
//! - Case conversion (UPPER, LOWER)
//! - Substring operations (SUBSTRING, LEFT, RIGHT)
//! - String analysis (LENGTH, CHAR_LENGTH, POSITION)
//! - String manipulation (CONCAT, TRIM, REPLACE, REVERSE)

use crate::errors::ExecutorError;

/// UPPER(string) - Convert string to uppercase
/// SQL:1999 Section 6.29: String value functions
pub(super) fn upper(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// LOWER(string) - Convert string to lowercase
/// SQL:1999 Section 6.29: String value functions
pub(super) fn lower(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// SUBSTRING(string, start [, length]) - Extract substring
/// SQL:1999 Section 6.29: String value functions
/// start is 1-based (SQL standard), length is optional
pub(super) fn substring(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// TRIM(string) - Remove leading and trailing spaces
/// SQL:1999 Section 6.29: String value functions
pub(super) fn trim(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// CHAR_LENGTH(string) / CHARACTER_LENGTH(string) - Return string length
/// SQL:1999 Section 6.29: String value functions
pub(super) fn char_length(args: &[types::SqlValue], name: &str) -> Result<types::SqlValue, ExecutorError> {
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

/// OCTET_LENGTH(string) - Return number of octets (bytes) in string
/// SQL:1999 Section 6.29: String value functions
/// Returns byte length, not character count. For UTF-8:
/// - ASCII characters: 1 byte each
/// - Multi-byte characters: 2-4 bytes each
pub(super) fn octet_length(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("OCTET_LENGTH requires exactly 1 argument, got {}", args.len()),
        ));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Varchar(s) => Ok(types::SqlValue::Integer(s.len() as i64)),
        types::SqlValue::Character(s) => Ok(types::SqlValue::Integer(s.len() as i64)),
        val => Err(ExecutorError::UnsupportedFeature(
            format!("OCTET_LENGTH requires string argument, got {:?}", val),
        )),
    }
}

/// CONCAT(str1, str2, ...) - Concatenate strings
/// SQL:1999 Section 6.29: String value functions
pub(super) fn concat(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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
                return Err(ExecutorError::UnsupportedFeature(
                    format!("CONCAT cannot convert {:?} to string", val),
                ))
            }
        }
    }
    Ok(types::SqlValue::Varchar(result))
}

/// LENGTH(str) - Alias for CHAR_LENGTH
pub(super) fn length(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("LENGTH requires exactly 1 argument, got {}", args.len()),
        ));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => {
            Ok(types::SqlValue::Integer(s.len() as i64))
        }
        val => Err(ExecutorError::UnsupportedFeature(
            format!("LENGTH requires string argument, got {:?}", val),
        )),
    }
}

/// POSITION(substring IN string) - Find position (1-indexed)
/// SQL:1999 Section 6.29: String value functions
/// Note: This is called as POSITION('sub', 'string') in our implementation
pub(super) fn position(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("POSITION requires exactly 2 arguments, got {}", args.len()),
        ));
    }

    match (&args[0], &args[1]) {
        (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => {
            Ok(types::SqlValue::Null)
        }
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
        (a, b) => Err(ExecutorError::UnsupportedFeature(
            format!("POSITION requires string arguments, got {:?} and {:?}", a, b),
        )),
    }
}

/// REPLACE(string, from, to) - Replace occurrences
/// SQL:1999 Section 6.29: String value functions
pub(super) fn replace(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 3 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("REPLACE requires exactly 3 arguments, got {}", args.len()),
        ));
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
        (a, b, c) => Err(ExecutorError::UnsupportedFeature(
            format!(
                "REPLACE requires string arguments, got {:?}, {:?}, {:?}",
                a, b, c
            ),
        )),
    }
}

/// REVERSE(string) - Reverse a string
pub(super) fn reverse(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("REVERSE requires exactly 1 argument, got {}", args.len()),
        ));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => {
            Ok(types::SqlValue::Varchar(s.chars().rev().collect()))
        }
        val => Err(ExecutorError::UnsupportedFeature(
            format!("REVERSE requires string argument, got {:?}", val),
        )),
    }
}

/// LEFT(string, n) - Leftmost n characters
pub(super) fn left(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("LEFT requires exactly 2 arguments, got {}", args.len()),
        ));
    }

    match (&args[0], &args[1]) {
        (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => {
            Ok(types::SqlValue::Null)
        }
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
        (a, b) => Err(ExecutorError::UnsupportedFeature(
            format!("LEFT requires string and integer arguments, got {:?} and {:?}", a, b),
        )),
    }
}

/// RIGHT(string, n) - Rightmost n characters
pub(super) fn right(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("RIGHT requires exactly 2 arguments, got {}", args.len()),
        ));
    }

    match (&args[0], &args[1]) {
        (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => {
            Ok(types::SqlValue::Null)
        }
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
        (a, b) => Err(ExecutorError::UnsupportedFeature(
            format!(
                "RIGHT requires string and integer arguments, got {:?} and {:?}",
                a, b
            ),
        )),
    }
}

/// INSTR(string, substring) - Find position of substring (1-indexed, 0 if not found)
/// MySQL/Oracle function - returns first occurrence position
pub(super) fn instr(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("INSTR requires exactly 2 arguments, got {}", args.len()),
        ));
    }

    match (&args[0], &args[1]) {
        (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => Ok(types::SqlValue::Null),
        (types::SqlValue::Varchar(haystack) | types::SqlValue::Character(haystack),
         types::SqlValue::Varchar(needle) | types::SqlValue::Character(needle)) => {
            // Find returns 0-indexed position, convert to 1-indexed
            // Return 0 if not found (SQL convention)
            let position = haystack.find(needle.as_str())
                .map(|pos| (pos + 1) as i64)
                .unwrap_or(0);
            Ok(types::SqlValue::Integer(position))
        }
        (haystack, needle) => Err(ExecutorError::UnsupportedFeature(
            format!("INSTR requires string arguments, got {:?} and {:?}", haystack, needle),
        )),
    }
}

/// LOCATE(substring, string, [start]) - Find position of substring with optional start
/// Note: Arguments reversed compared to INSTR (needle, haystack vs haystack, needle)
pub(super) fn locate(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("LOCATE requires 2 or 3 arguments, got {}", args.len()),
        ));
    }

    match (&args[0], &args[1]) {
        (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => Ok(types::SqlValue::Null),
        (types::SqlValue::Varchar(needle) | types::SqlValue::Character(needle),
         types::SqlValue::Varchar(haystack) | types::SqlValue::Character(haystack)) => {
            // Optional start position (1-indexed, default to 1)
            let start_pos = if args.len() == 3 {
                match &args[2] {
                    types::SqlValue::Integer(s) => {
                        // Convert from 1-indexed to 0-indexed, clamp to 0
                        ((*s - 1).max(0) as usize).min(haystack.len())
                    }
                    types::SqlValue::Null => return Ok(types::SqlValue::Null),
                    val => {
                        return Err(ExecutorError::UnsupportedFeature(
                            format!("LOCATE start position must be integer, got {:?}", val),
                        ))
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
        (needle, haystack) => Err(ExecutorError::UnsupportedFeature(
            format!("LOCATE requires string arguments, got {:?} and {:?}", needle, haystack),
        )),
    }
}
