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

        // ==================== NUMERIC FUNCTIONS ====================

        // ABS(x) - Absolute value
        // SQL:1999 Section 6.27: Numeric value functions
        "ABS" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("ABS requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                types::SqlValue::Integer(n) => Ok(types::SqlValue::Integer(n.abs())),
                types::SqlValue::Bigint(n) => Ok(types::SqlValue::Bigint(n.abs())),
                types::SqlValue::Smallint(n) => Ok(types::SqlValue::Smallint(n.abs())),
                types::SqlValue::Float(n) => Ok(types::SqlValue::Float(n.abs())),
                types::SqlValue::Double(n) => Ok(types::SqlValue::Double(n.abs())),
                types::SqlValue::Real(n) => Ok(types::SqlValue::Real(n.abs())),
                val => Err(ExecutorError::UnsupportedFeature(
                    format!("ABS requires numeric argument, got {:?}", val),
                )),
            }
        }

        // ROUND(x [, precision]) - Round to nearest integer or decimal places
        // SQL:1999 Section 6.27: Numeric value functions
        "ROUND" => {
            if args.is_empty() || args.len() > 2 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("ROUND requires 1 or 2 arguments, got {}", args.len()),
                ));
            }

            let value = &args[0];
            let precision = if args.len() == 2 {
                match &args[1] {
                    types::SqlValue::Integer(p) => *p as i32,
                    types::SqlValue::Null => return Ok(types::SqlValue::Null),
                    val => {
                        return Err(ExecutorError::UnsupportedFeature(
                            format!("ROUND precision must be integer, got {:?}", val),
                        ))
                    }
                }
            } else {
                0
            };

            match value {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                types::SqlValue::Integer(n) => Ok(types::SqlValue::Integer(*n)),
                types::SqlValue::Float(f) => {
                    let multiplier = 10_f32.powi(precision);
                    Ok(types::SqlValue::Float((f * multiplier).round() / multiplier))
                }
                types::SqlValue::Double(f) => {
                    let multiplier = 10_f64.powi(precision);
                    Ok(types::SqlValue::Double((f * multiplier).round() / multiplier))
                }
                types::SqlValue::Real(f) => {
                    let multiplier = 10_f32.powi(precision);
                    Ok(types::SqlValue::Real((f * multiplier).round() / multiplier))
                }
                val => Err(ExecutorError::UnsupportedFeature(
                    format!("ROUND requires numeric argument, got {:?}", val),
                )),
            }
        }

        // FLOOR(x) - Round down to nearest integer
        // SQL:1999 Section 6.27: Numeric value functions
        "FLOOR" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("FLOOR requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                types::SqlValue::Integer(n) => Ok(types::SqlValue::Integer(*n)),
                types::SqlValue::Float(f) => Ok(types::SqlValue::Float(f.floor())),
                types::SqlValue::Double(f) => Ok(types::SqlValue::Double(f.floor())),
                types::SqlValue::Real(f) => Ok(types::SqlValue::Real(f.floor())),
                val => Err(ExecutorError::UnsupportedFeature(
                    format!("FLOOR requires numeric argument, got {:?}", val),
                )),
            }
        }

        // CEIL/CEILING(x) - Round up to nearest integer
        // SQL:1999 Section 6.27: Numeric value functions
        "CEIL" | "CEILING" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("{} requires exactly 1 argument, got {}", name, args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                types::SqlValue::Integer(n) => Ok(types::SqlValue::Integer(*n)),
                types::SqlValue::Float(f) => Ok(types::SqlValue::Float(f.ceil())),
                types::SqlValue::Double(f) => Ok(types::SqlValue::Double(f.ceil())),
                types::SqlValue::Real(f) => Ok(types::SqlValue::Real(f.ceil())),
                val => Err(ExecutorError::UnsupportedFeature(
                    format!("{} requires numeric argument, got {:?}", name, val),
                )),
            }
        }

        // MOD(x, y) - Modulo (remainder)
        // SQL:1999 Section 6.27: Numeric value functions
        "MOD" => {
            if args.len() != 2 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("MOD requires exactly 2 arguments, got {}", args.len()),
                ));
            }

            match (&args[0], &args[1]) {
                (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => {
                    Ok(types::SqlValue::Null)
                }
                (types::SqlValue::Integer(a), types::SqlValue::Integer(b)) => {
                    if *b == 0 {
                        Err(ExecutorError::DivisionByZero)
                    } else {
                        Ok(types::SqlValue::Integer(a % b))
                    }
                }
                (a, b) => Err(ExecutorError::UnsupportedFeature(
                    format!("MOD requires integer arguments, got {:?} and {:?}", a, b),
                )),
            }
        }

        // POWER(x, y) - x raised to power y
        // SQL:1999 Section 6.27: Numeric value functions
        "POWER" | "POW" => {
            if args.len() != 2 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("{} requires exactly 2 arguments, got {}", name, args.len()),
                ));
            }

            match (&args[0], &args[1]) {
                (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => {
                    Ok(types::SqlValue::Null)
                }
                (types::SqlValue::Integer(base), types::SqlValue::Integer(exp)) => {
                    if *exp >= 0 && *exp <= i32::MAX as i64 {
                        Ok(types::SqlValue::Double((*base as f64).powi(*exp as i32)))
                    } else {
                        Ok(types::SqlValue::Double((*base as f64).powf(*exp as f64)))
                    }
                }
                (types::SqlValue::Float(base), types::SqlValue::Float(exp)) => {
                    Ok(types::SqlValue::Float(base.powf(*exp)))
                }
                (types::SqlValue::Double(base), types::SqlValue::Double(exp)) => {
                    Ok(types::SqlValue::Double(base.powf(*exp)))
                }
                (base, exp) => {
                    // Try to convert to f64
                    let base_f64 = numeric_to_f64(base)?;
                    let exp_f64 = numeric_to_f64(exp)?;
                    Ok(types::SqlValue::Double(base_f64.powf(exp_f64)))
                }
            }
        }

        // SQRT(x) - Square root
        // SQL:1999 Section 6.27: Numeric value functions
        "SQRT" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("SQRT requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                types::SqlValue::Integer(n) => {
                    if *n < 0 {
                        Err(ExecutorError::UnsupportedFeature(
                            "SQRT of negative number".to_string(),
                        ))
                    } else {
                        Ok(types::SqlValue::Double((*n as f64).sqrt()))
                    }
                }
                types::SqlValue::Float(f) => {
                    if *f < 0.0 {
                        Err(ExecutorError::UnsupportedFeature(
                            "SQRT of negative number".to_string(),
                        ))
                    } else {
                        Ok(types::SqlValue::Float(f.sqrt()))
                    }
                }
                types::SqlValue::Double(f) => {
                    if *f < 0.0 {
                        Err(ExecutorError::UnsupportedFeature(
                            "SQRT of negative number".to_string(),
                        ))
                    } else {
                        Ok(types::SqlValue::Double(f.sqrt()))
                    }
                }
                val => {
                    let f = numeric_to_f64(val)?;
                    if f < 0.0 {
                        Err(ExecutorError::UnsupportedFeature(
                            "SQRT of negative number".to_string(),
                        ))
                    } else {
                        Ok(types::SqlValue::Double(f.sqrt()))
                    }
                }
            }
        }

        // EXP(x) - e raised to power x
        // SQL:1999 Section 6.27: Numeric value functions
        "EXP" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("EXP requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                val => {
                    let x = numeric_to_f64(val)?;
                    Ok(types::SqlValue::Double(x.exp()))
                }
            }
        }

        // LN(x) / LOG(x) - Natural logarithm
        // SQL:1999 Section 6.27: Numeric value functions
        "LN" | "LOG" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("{} requires exactly 1 argument, got {}", name, args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                val => {
                    let x = numeric_to_f64(val)?;
                    if x <= 0.0 {
                        Err(ExecutorError::UnsupportedFeature(
                            format!("{} of non-positive number", name),
                        ))
                    } else {
                        Ok(types::SqlValue::Double(x.ln()))
                    }
                }
            }
        }

        // LOG10(x) - Base-10 logarithm
        "LOG10" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("LOG10 requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                val => {
                    let x = numeric_to_f64(val)?;
                    if x <= 0.0 {
                        Err(ExecutorError::UnsupportedFeature(
                            "LOG10 of non-positive number".to_string(),
                        ))
                    } else {
                        Ok(types::SqlValue::Double(x.log10()))
                    }
                }
            }
        }

        // SIGN(x) - Sign of number (-1, 0, or 1)
        "SIGN" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("SIGN requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                types::SqlValue::Integer(n) => {
                    let sign = if *n < 0 {
                        -1
                    } else if *n > 0 {
                        1
                    } else {
                        0
                    };
                    Ok(types::SqlValue::Integer(sign))
                }
                val => {
                    let x = numeric_to_f64(val)?;
                    let sign = if x < 0.0 {
                        -1.0
                    } else if x > 0.0 {
                        1.0
                    } else {
                        0.0
                    };
                    Ok(types::SqlValue::Double(sign))
                }
            }
        }

        // PI() - Mathematical constant π
        "PI" => {
            if !args.is_empty() {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("PI requires no arguments, got {}", args.len()),
                ));
            }
            Ok(types::SqlValue::Double(std::f64::consts::PI))
        }

        // ==================== TRIGONOMETRIC FUNCTIONS ====================

        // SIN(x) - Sine (x in radians)
        "SIN" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("SIN requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                val => {
                    let x = numeric_to_f64(val)?;
                    Ok(types::SqlValue::Double(x.sin()))
                }
            }
        }

        // COS(x) - Cosine (x in radians)
        "COS" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("COS requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                val => {
                    let x = numeric_to_f64(val)?;
                    Ok(types::SqlValue::Double(x.cos()))
                }
            }
        }

        // TAN(x) - Tangent (x in radians)
        "TAN" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("TAN requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                val => {
                    let x = numeric_to_f64(val)?;
                    Ok(types::SqlValue::Double(x.tan()))
                }
            }
        }

        // ASIN(x) - Arcsine (returns radians)
        "ASIN" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("ASIN requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                val => {
                    let x = numeric_to_f64(val)?;
                    if x < -1.0 || x > 1.0 {
                        Err(ExecutorError::UnsupportedFeature(
                            "ASIN requires value between -1 and 1".to_string(),
                        ))
                    } else {
                        Ok(types::SqlValue::Double(x.asin()))
                    }
                }
            }
        }

        // ACOS(x) - Arccosine (returns radians)
        "ACOS" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("ACOS requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                val => {
                    let x = numeric_to_f64(val)?;
                    if x < -1.0 || x > 1.0 {
                        Err(ExecutorError::UnsupportedFeature(
                            "ACOS requires value between -1 and 1".to_string(),
                        ))
                    } else {
                        Ok(types::SqlValue::Double(x.acos()))
                    }
                }
            }
        }

        // ATAN(x) - Arctangent (returns radians)
        "ATAN" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("ATAN requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                val => {
                    let x = numeric_to_f64(val)?;
                    Ok(types::SqlValue::Double(x.atan()))
                }
            }
        }

        // ATAN2(y, x) - Arctangent of y/x (returns radians)
        "ATAN2" => {
            if args.len() != 2 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("ATAN2 requires exactly 2 arguments, got {}", args.len()),
                ));
            }

            match (&args[0], &args[1]) {
                (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => {
                    Ok(types::SqlValue::Null)
                }
                (y_val, x_val) => {
                    let y = numeric_to_f64(y_val)?;
                    let x = numeric_to_f64(x_val)?;
                    Ok(types::SqlValue::Double(y.atan2(x)))
                }
            }
        }

        // RADIANS(x) - Convert degrees to radians
        "RADIANS" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("RADIANS requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                val => {
                    let degrees = numeric_to_f64(val)?;
                    Ok(types::SqlValue::Double(degrees.to_radians()))
                }
            }
        }

        // DEGREES(x) - Convert radians to degrees
        "DEGREES" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("DEGREES requires exactly 1 argument, got {}", args.len()),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                val => {
                    let radians = numeric_to_f64(val)?;
                    Ok(types::SqlValue::Double(radians.to_degrees()))
                }
            }
        }

        // ==================== CONDITIONAL FUNCTIONS ====================

        // GREATEST(val1, val2, ...) - Returns greatest value
        "GREATEST" => {
            if args.is_empty() {
                return Err(ExecutorError::UnsupportedFeature(
                    "GREATEST requires at least one argument".to_string(),
                ));
            }

            let mut max_val = &args[0];
            for arg in &args[1..] {
                // Skip NULL values
                if matches!(arg, types::SqlValue::Null) {
                    continue;
                }
                if matches!(max_val, types::SqlValue::Null) {
                    max_val = arg;
                    continue;
                }

                // Compare values
                match (max_val, arg) {
                    (types::SqlValue::Integer(a), types::SqlValue::Integer(b)) => {
                        if b > a {
                            max_val = arg;
                        }
                    }
                    (types::SqlValue::Double(a), types::SqlValue::Double(b)) => {
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

        // LEAST(val1, val2, ...) - Returns smallest value
        "LEAST" => {
            if args.is_empty() {
                return Err(ExecutorError::UnsupportedFeature(
                    "LEAST requires at least one argument".to_string(),
                ));
            }

            let mut min_val = &args[0];
            for arg in &args[1..] {
                // Skip NULL values
                if matches!(arg, types::SqlValue::Null) {
                    continue;
                }
                if matches!(min_val, types::SqlValue::Null) {
                    min_val = arg;
                    continue;
                }

                // Compare values
                match (min_val, arg) {
                    (types::SqlValue::Integer(a), types::SqlValue::Integer(b)) => {
                        if b < a {
                            min_val = arg;
                        }
                    }
                    (types::SqlValue::Double(a), types::SqlValue::Double(b)) => {
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

        // IF(condition, true_value, false_value) - MySQL-style conditional
        "IF" => {
            if args.len() != 3 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("IF requires exactly 3 arguments, got {}", args.len()),
                ));
            }

            // Evaluate condition
            let condition = &args[0];
            match condition {
                types::SqlValue::Boolean(true) => Ok(args[1].clone()),
                types::SqlValue::Boolean(false) | types::SqlValue::Null => Ok(args[2].clone()),
                _ => Err(ExecutorError::UnsupportedFeature(
                    format!("IF condition must be boolean, got {:?}", condition),
                )),
            }
        }

        // ==================== ADDITIONAL STRING FUNCTIONS ====================

        // CONCAT(str1, str2, ...) - Concatenate strings
        // SQL:1999 Section 6.29: String value functions
        "CONCAT" => {
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

        // LENGTH(str) - Alias for CHAR_LENGTH
        "LENGTH" => {
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

        // POSITION(substring IN string) - Find position (1-indexed)
        // SQL:1999 Section 6.29: String value functions
        // Note: This is called as POSITION('sub', 'string') in our implementation
        "POSITION" => {
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

        // REPLACE(string, from, to) - Replace occurrences
        // SQL:1999 Section 6.29: String value functions
        "REPLACE" => {
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

        // REVERSE(string) - Reverse a string
        "REVERSE" => {
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

        // LEFT(string, n) - Leftmost n characters
        "LEFT" => {
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

        // RIGHT(string, n) - Rightmost n characters
        "RIGHT" => {
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

        // ==================== DATE/TIME FUNCTIONS ====================

        // CURRENT_DATE / CURDATE - Returns current date
        // SQL:1999 Section 6.31: Datetime value functions
        "CURRENT_DATE" | "CURDATE" => {
            if !args.is_empty() {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "{} takes no arguments",
                    name
                )));
            }

            use chrono::Local;
            let now = Local::now();
            let date_str = now.format("%Y-%m-%d").to_string();
            Ok(types::SqlValue::Date(date_str))
        }

        // CURRENT_TIME / CURTIME - Returns current time
        // SQL:1999 Section 6.31: Datetime value functions
        "CURRENT_TIME" | "CURTIME" => {
            if !args.is_empty() {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "{} takes no arguments",
                    name
                )));
            }

            use chrono::Local;
            let now = Local::now();
            let time_str = now.format("%H:%M:%S").to_string();
            Ok(types::SqlValue::Time(time_str))
        }

        // CURRENT_TIMESTAMP / NOW - Returns current timestamp
        // SQL:1999 Section 6.31: Datetime value functions
        "CURRENT_TIMESTAMP" | "NOW" => {
            if !args.is_empty() {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "{} takes no arguments",
                    name
                )));
            }

            use chrono::Local;
            let now = Local::now();
            let timestamp_str = now.format("%Y-%m-%d %H:%M:%S").to_string();
            Ok(types::SqlValue::Timestamp(timestamp_str))
        }

        // YEAR(date) - Extract year from date/timestamp
        // SQL:1999 Section 6.32: Datetime field extraction
        "YEAR" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    "YEAR requires exactly 1 argument".to_string(),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                types::SqlValue::Date(s) | types::SqlValue::Timestamp(s) => {
                    // Parse date string (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
                    let parts: Vec<&str> = s.split(&['-', ' '][..]).collect();
                    if parts.is_empty() {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Invalid date format for YEAR".to_string(),
                        ));
                    }
                    match parts[0].parse::<i64>() {
                        Ok(year) => Ok(types::SqlValue::Integer(year)),
                        Err(_) => Err(ExecutorError::UnsupportedFeature(
                            "Invalid year value".to_string(),
                        )),
                    }
                }
                _ => Err(ExecutorError::UnsupportedFeature(
                    "YEAR requires date or timestamp argument".to_string(),
                )),
            }
        }

        // MONTH(date) - Extract month from date/timestamp
        // SQL:1999 Section 6.32: Datetime field extraction
        "MONTH" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    "MONTH requires exactly 1 argument".to_string(),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                types::SqlValue::Date(s) | types::SqlValue::Timestamp(s) => {
                    // Parse date string (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
                    let date_part = s.split(' ').next().unwrap_or(s);
                    let parts: Vec<&str> = date_part.split('-').collect();
                    if parts.len() < 2 {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Invalid date format for MONTH".to_string(),
                        ));
                    }
                    match parts[1].parse::<i64>() {
                        Ok(month) => Ok(types::SqlValue::Integer(month)),
                        Err(_) => Err(ExecutorError::UnsupportedFeature(
                            "Invalid month value".to_string(),
                        )),
                    }
                }
                _ => Err(ExecutorError::UnsupportedFeature(
                    "MONTH requires date or timestamp argument".to_string(),
                )),
            }
        }

        // DAY(date) - Extract day from date/timestamp
        // SQL:1999 Section 6.32: Datetime field extraction
        "DAY" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    "DAY requires exactly 1 argument".to_string(),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                types::SqlValue::Date(s) | types::SqlValue::Timestamp(s) => {
                    // Parse date string (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
                    let date_part = s.split(' ').next().unwrap_or(s);
                    let parts: Vec<&str> = date_part.split('-').collect();
                    if parts.len() < 3 {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Invalid date format for DAY".to_string(),
                        ));
                    }
                    match parts[2].parse::<i64>() {
                        Ok(day) => Ok(types::SqlValue::Integer(day)),
                        Err(_) => Err(ExecutorError::UnsupportedFeature(
                            "Invalid day value".to_string(),
                        )),
                    }
                }
                _ => Err(ExecutorError::UnsupportedFeature(
                    "DAY requires date or timestamp argument".to_string(),
                )),
            }
        }

        // HOUR(time) - Extract hour from time/timestamp
        // SQL:1999 Section 6.32: Datetime field extraction
        "HOUR" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    "HOUR requires exactly 1 argument".to_string(),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                types::SqlValue::Time(s) => {
                    // Parse time string (HH:MM:SS)
                    let parts: Vec<&str> = s.split(':').collect();
                    if parts.is_empty() {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Invalid time format for HOUR".to_string(),
                        ));
                    }
                    match parts[0].parse::<i64>() {
                        Ok(hour) => Ok(types::SqlValue::Integer(hour)),
                        Err(_) => Err(ExecutorError::UnsupportedFeature(
                            "Invalid hour value".to_string(),
                        )),
                    }
                }
                types::SqlValue::Timestamp(s) => {
                    // Parse timestamp string (YYYY-MM-DD HH:MM:SS)
                    let time_part = s.split(' ').nth(1).unwrap_or("");
                    let parts: Vec<&str> = time_part.split(':').collect();
                    if parts.is_empty() {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Invalid timestamp format for HOUR".to_string(),
                        ));
                    }
                    match parts[0].parse::<i64>() {
                        Ok(hour) => Ok(types::SqlValue::Integer(hour)),
                        Err(_) => Err(ExecutorError::UnsupportedFeature(
                            "Invalid hour value".to_string(),
                        )),
                    }
                }
                _ => Err(ExecutorError::UnsupportedFeature(
                    "HOUR requires time or timestamp argument".to_string(),
                )),
            }
        }

        // MINUTE(time) - Extract minute from time/timestamp
        // SQL:1999 Section 6.32: Datetime field extraction
        "MINUTE" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    "MINUTE requires exactly 1 argument".to_string(),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                types::SqlValue::Time(s) => {
                    // Parse time string (HH:MM:SS)
                    let parts: Vec<&str> = s.split(':').collect();
                    if parts.len() < 2 {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Invalid time format for MINUTE".to_string(),
                        ));
                    }
                    match parts[1].parse::<i64>() {
                        Ok(minute) => Ok(types::SqlValue::Integer(minute)),
                        Err(_) => Err(ExecutorError::UnsupportedFeature(
                            "Invalid minute value".to_string(),
                        )),
                    }
                }
                types::SqlValue::Timestamp(s) => {
                    // Parse timestamp string (YYYY-MM-DD HH:MM:SS)
                    let time_part = s.split(' ').nth(1).unwrap_or("");
                    let parts: Vec<&str> = time_part.split(':').collect();
                    if parts.len() < 2 {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Invalid timestamp format for MINUTE".to_string(),
                        ));
                    }
                    match parts[1].parse::<i64>() {
                        Ok(minute) => Ok(types::SqlValue::Integer(minute)),
                        Err(_) => Err(ExecutorError::UnsupportedFeature(
                            "Invalid minute value".to_string(),
                        )),
                    }
                }
                _ => Err(ExecutorError::UnsupportedFeature(
                    "MINUTE requires time or timestamp argument".to_string(),
                )),
            }
        }

        // SECOND(time) - Extract second from time/timestamp
        // SQL:1999 Section 6.32: Datetime field extraction
        "SECOND" => {
            if args.len() != 1 {
                return Err(ExecutorError::UnsupportedFeature(
                    "SECOND requires exactly 1 argument".to_string(),
                ));
            }

            match &args[0] {
                types::SqlValue::Null => Ok(types::SqlValue::Null),
                types::SqlValue::Time(s) => {
                    // Parse time string (HH:MM:SS)
                    let parts: Vec<&str> = s.split(':').collect();
                    if parts.len() < 3 {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Invalid time format for SECOND".to_string(),
                        ));
                    }
                    match parts[2].parse::<i64>() {
                        Ok(second) => Ok(types::SqlValue::Integer(second)),
                        Err(_) => Err(ExecutorError::UnsupportedFeature(
                            "Invalid second value".to_string(),
                        )),
                    }
                }
                types::SqlValue::Timestamp(s) => {
                    // Parse timestamp string (YYYY-MM-DD HH:MM:SS)
                    let time_part = s.split(' ').nth(1).unwrap_or("");
                    let parts: Vec<&str> = time_part.split(':').collect();
                    if parts.len() < 3 {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Invalid timestamp format for SECOND".to_string(),
                        ));
                    }
                    match parts[2].parse::<i64>() {
                        Ok(second) => Ok(types::SqlValue::Integer(second)),
                        Err(_) => Err(ExecutorError::UnsupportedFeature(
                            "Invalid second value".to_string(),
                        )),
                    }
                }
                _ => Err(ExecutorError::UnsupportedFeature(
                    "SECOND requires time or timestamp argument".to_string(),
                )),
            }
        }

        // ==================== DATE ARITHMETIC FUNCTIONS ====================

        // DATEDIFF(date1, date2) - Calculate day difference between two dates
        // SQL:1999 Core Feature E021-02: Date and time arithmetic
        // Returns: date1 - date2 in days
        "DATEDIFF" => {
            if args.len() != 2 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("DATEDIFF requires exactly 2 arguments, got {}", args.len()),
                ));
            }

            match (&args[0], &args[1]) {
                (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => {
                    Ok(types::SqlValue::Null)
                }
                (types::SqlValue::Date(date1_str), types::SqlValue::Date(date2_str)) |
                (types::SqlValue::Timestamp(date1_str), types::SqlValue::Date(date2_str)) |
                (types::SqlValue::Date(date1_str), types::SqlValue::Timestamp(date2_str)) |
                (types::SqlValue::Timestamp(date1_str), types::SqlValue::Timestamp(date2_str)) => {
                    use chrono::NaiveDate;

                    // Extract date part from timestamps if needed
                    let date1_part = date1_str.split(' ').next().unwrap_or(date1_str);
                    let date2_part = date2_str.split(' ').next().unwrap_or(date2_str);

                    // Parse dates
                    let date1 = NaiveDate::parse_from_str(date1_part, "%Y-%m-%d")
                        .map_err(|_| ExecutorError::UnsupportedFeature(
                            format!("Invalid date format for DATEDIFF: {}", date1_part)
                        ))?;
                    let date2 = NaiveDate::parse_from_str(date2_part, "%Y-%m-%d")
                        .map_err(|_| ExecutorError::UnsupportedFeature(
                            format!("Invalid date format for DATEDIFF: {}", date2_part)
                        ))?;

                    // Calculate difference in days
                    let diff = date1.signed_duration_since(date2).num_days();
                    Ok(types::SqlValue::Integer(diff))
                }
                (a, b) => Err(ExecutorError::UnsupportedFeature(
                    format!("DATEDIFF requires date or timestamp arguments, got {:?} and {:?}", a, b),
                )),
            }
        }

        // DATE_ADD(date, amount, unit) - Add interval to date
        // ADDDATE is an alias for DATE_ADD
        // SQL:1999 Core Feature E021-02: Date and time arithmetic
        // Units: 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND'
        "DATE_ADD" | "ADDDATE" => {
            if args.len() != 3 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("{} requires exactly 3 arguments (date, amount, unit), got {}", name, args.len()),
                ));
            }

            // Handle NULL inputs
            if matches!(&args[0], types::SqlValue::Null) ||
               matches!(&args[1], types::SqlValue::Null) ||
               matches!(&args[2], types::SqlValue::Null) {
                return Ok(types::SqlValue::Null);
            }

            // Extract date string
            let date_str = match &args[0] {
                types::SqlValue::Date(s) | types::SqlValue::Timestamp(s) => s,
                val => return Err(ExecutorError::UnsupportedFeature(
                    format!("{} requires date/timestamp as first argument, got {:?}", name, val)
                )),
            };

            // Extract amount
            let amount = match &args[1] {
                types::SqlValue::Integer(n) => *n,
                val => return Err(ExecutorError::UnsupportedFeature(
                    format!("{} requires integer amount, got {:?}", name, val)
                )),
            };

            // Extract unit
            let unit = match &args[2] {
                types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => s.to_uppercase(),
                val => return Err(ExecutorError::UnsupportedFeature(
                    format!("{} requires string unit, got {:?}", name, val)
                )),
            };

            // Perform date arithmetic based on unit
            date_add_subtract(date_str, amount, &unit, true)
        }

        // DATE_SUB(date, amount, unit) - Subtract interval from date
        // SUBDATE is an alias for DATE_SUB
        // SQL:1999 Core Feature E021-02: Date and time arithmetic
        "DATE_SUB" | "SUBDATE" => {
            if args.len() != 3 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("{} requires exactly 3 arguments (date, amount, unit), got {}", name, args.len()),
                ));
            }

            // Handle NULL inputs
            if matches!(&args[0], types::SqlValue::Null) ||
               matches!(&args[1], types::SqlValue::Null) ||
               matches!(&args[2], types::SqlValue::Null) {
                return Ok(types::SqlValue::Null);
            }

            // Extract date string
            let date_str = match &args[0] {
                types::SqlValue::Date(s) | types::SqlValue::Timestamp(s) => s,
                val => return Err(ExecutorError::UnsupportedFeature(
                    format!("{} requires date/timestamp as first argument, got {:?}", name, val)
                )),
            };

            // Extract amount
            let amount = match &args[1] {
                types::SqlValue::Integer(n) => *n,
                val => return Err(ExecutorError::UnsupportedFeature(
                    format!("{} requires integer amount, got {:?}", name, val)
                )),
            };

            // Extract unit
            let unit = match &args[2] {
                types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => s.to_uppercase(),
                val => return Err(ExecutorError::UnsupportedFeature(
                    format!("{} requires string unit, got {:?}", name, val)
                )),
            };

            // Perform date arithmetic (negate amount for subtraction)
            date_add_subtract(date_str, -amount, &unit, true)
        }

        // EXTRACT(unit, date) - Extract date/time component
        // SQL:1999 Section 6.32: Datetime field extraction
        // Simplified syntax: EXTRACT('YEAR', date) instead of EXTRACT(YEAR FROM date)
        "EXTRACT" => {
            if args.len() != 2 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("EXTRACT requires exactly 2 arguments (unit, date), got {}", args.len()),
                ));
            }

            // Extract unit
            let unit = match &args[0] {
                types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => s.to_uppercase(),
                val => return Err(ExecutorError::UnsupportedFeature(
                    format!("EXTRACT requires string unit as first argument, got {:?}", val)
                )),
            };

            // Delegate to existing extraction functions based on unit
            match unit.as_ref() {
                "YEAR" => eval_scalar_function("YEAR", &[args[1].clone()]),
                "MONTH" => eval_scalar_function("MONTH", &[args[1].clone()]),
                "DAY" => eval_scalar_function("DAY", &[args[1].clone()]),
                "HOUR" => eval_scalar_function("HOUR", &[args[1].clone()]),
                "MINUTE" => eval_scalar_function("MINUTE", &[args[1].clone()]),
                "SECOND" => eval_scalar_function("SECOND", &[args[1].clone()]),
                _ => Err(ExecutorError::UnsupportedFeature(
                    format!("Unsupported EXTRACT unit: {}", unit)
                )),
            }
        }

        // AGE(date1, date2) - Calculate age as interval between two dates
        // AGE(date) - Calculate age from date to current date
        // Returns VARCHAR representation of interval (e.g., "2 years 3 months 5 days")
        "AGE" => {
            if args.is_empty() || args.len() > 2 {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("AGE requires 1 or 2 arguments, got {}", args.len()),
                ));
            }

            use chrono::{Local, NaiveDate};

            // Get the two dates to compare
            let (date1_str, date2_str) = if args.len() == 1 {
                // AGE(date) - compare with current date
                match &args[0] {
                    types::SqlValue::Null => return Ok(types::SqlValue::Null),
                    types::SqlValue::Date(s) | types::SqlValue::Timestamp(s) => {
                        let now = Local::now();
                        let current_date = now.format("%Y-%m-%d").to_string();
                        (current_date, s.clone())
                    }
                    val => return Err(ExecutorError::UnsupportedFeature(
                        format!("AGE requires date/timestamp argument, got {:?}", val)
                    )),
                }
            } else {
                // AGE(date1, date2) - calculate date1 - date2
                match (&args[0], &args[1]) {
                    (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => {
                        return Ok(types::SqlValue::Null);
                    }
                    (
                        types::SqlValue::Date(d1) | types::SqlValue::Timestamp(d1),
                        types::SqlValue::Date(d2) | types::SqlValue::Timestamp(d2),
                    ) => (d1.clone(), d2.clone()),
                    (a, b) => return Err(ExecutorError::UnsupportedFeature(
                        format!("AGE requires date/timestamp arguments, got {:?} and {:?}", a, b)
                    )),
                }
            };

            // Extract date parts
            let date1_part = date1_str.split(' ').next().unwrap_or(&date1_str);
            let date2_part = date2_str.split(' ').next().unwrap_or(&date2_str);

            // Parse dates
            let date1 = NaiveDate::parse_from_str(date1_part, "%Y-%m-%d")
                .map_err(|_| ExecutorError::UnsupportedFeature(
                    format!("Invalid date format for AGE: {}", date1_part)
                ))?;
            let date2 = NaiveDate::parse_from_str(date2_part, "%Y-%m-%d")
                .map_err(|_| ExecutorError::UnsupportedFeature(
                    format!("Invalid date format for AGE: {}", date2_part)
                ))?;

            // Calculate age components
            let (years, months, days) = calculate_age_components(date1, date2);

            // Format as interval string
            let mut parts = Vec::new();
            if years != 0 {
                parts.push(format!("{} year{}", years.abs(), if years.abs() == 1 { "" } else { "s" }));
            }
            if months != 0 {
                parts.push(format!("{} month{}", months.abs(), if months.abs() == 1 { "" } else { "s" }));
            }
            if days != 0 || parts.is_empty() {
                parts.push(format!("{} day{}", days.abs(), if days.abs() == 1 { "" } else { "s" }));
            }

            let result = if date1 < date2 {
                format!("-{}", parts.join(" "))
            } else {
                parts.join(" ")
            };

            Ok(types::SqlValue::Varchar(result))
        }

        // Unknown function
        _ => Err(ExecutorError::UnsupportedFeature(
            format!("Scalar function {} not supported in this context", name),
        )),
    }
}

/// Helper: Convert numeric types to f64
fn numeric_to_f64(val: &types::SqlValue) -> Result<f64, ExecutorError> {
    match val {
        types::SqlValue::Integer(n) => Ok(*n as f64),
        types::SqlValue::Bigint(n) => Ok(*n as f64),
        types::SqlValue::Smallint(n) => Ok(*n as f64),
        types::SqlValue::Float(f) => Ok(*f as f64),
        types::SqlValue::Double(f) => Ok(*f),
        types::SqlValue::Real(f) => Ok(*f as f64),
        _ => Err(ExecutorError::UnsupportedFeature(
            format!("Cannot convert {:?} to numeric", val),
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

/// Helper for DATE_ADD and DATE_SUB functions
/// Adds or subtracts an interval from a date/timestamp
/// preserve_time: if true and input is timestamp, preserve time component
fn date_add_subtract(
    date_str: &str,
    amount: i64,
    unit: &str,
    preserve_time: bool,
) -> Result<types::SqlValue, ExecutorError> {
    use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime};

    // Check if input is a timestamp (has time component)
    let is_timestamp = date_str.contains(' ');

    if is_timestamp && preserve_time {
        // Parse as timestamp and preserve time
        let timestamp = NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S")
            .map_err(|_| ExecutorError::UnsupportedFeature(
                format!("Invalid timestamp format: {}", date_str)
            ))?;

        let new_timestamp = match unit {
            "YEAR" | "YEARS" => {
                let year = timestamp.year() + amount as i32;
                timestamp.with_year(year).ok_or_else(|| ExecutorError::UnsupportedFeature(
                    format!("Invalid year: {}", year)
                ))?
            }
            "MONTH" | "MONTHS" => {
                let months = timestamp.month() as i64 + amount;
                let years_offset = (months - 1) / 12;
                let new_month = ((months - 1) % 12 + 1) as u32;
                let new_month = if new_month == 0 { 12 } else { new_month };
                let new_year = timestamp.year() + years_offset as i32;

                let mut new_ts = timestamp.with_year(new_year).ok_or_else(|| ExecutorError::UnsupportedFeature(
                    format!("Invalid year: {}", new_year)
                ))?;
                new_ts = new_ts.with_month(new_month).ok_or_else(|| ExecutorError::UnsupportedFeature(
                    format!("Invalid month: {}", new_month)
                ))?;
                new_ts
            }
            "DAY" | "DAYS" => timestamp + Duration::days(amount),
            "HOUR" | "HOURS" => timestamp + Duration::hours(amount),
            "MINUTE" | "MINUTES" => timestamp + Duration::minutes(amount),
            "SECOND" | "SECONDS" => timestamp + Duration::seconds(amount),
            _ => return Err(ExecutorError::UnsupportedFeature(
                format!("Unsupported date unit: {}", unit)
            )),
        };

        Ok(types::SqlValue::Timestamp(new_timestamp.format("%Y-%m-%d %H:%M:%S").to_string()))
    } else {
        // Parse as date (extract date part if timestamp)
        let date_part = date_str.split(' ').next().unwrap_or(date_str);
        let date = NaiveDate::parse_from_str(date_part, "%Y-%m-%d")
            .map_err(|_| ExecutorError::UnsupportedFeature(
                format!("Invalid date format: {}", date_part)
            ))?;

        let new_date = match unit {
            "YEAR" | "YEARS" => {
                let year = date.year() + amount as i32;
                date.with_year(year).ok_or_else(|| ExecutorError::UnsupportedFeature(
                    format!("Invalid year: {}", year)
                ))?
            }
            "MONTH" | "MONTHS" => {
                let months = date.month() as i64 + amount;
                let years_offset = (months - 1) / 12;
                let new_month = ((months - 1) % 12 + 1) as u32;
                let new_month = if new_month == 0 { 12 } else { new_month };
                let new_year = date.year() + years_offset as i32;

                let mut new_date = date.with_year(new_year).ok_or_else(|| ExecutorError::UnsupportedFeature(
                    format!("Invalid year: {}", new_year)
                ))?;
                new_date = new_date.with_month(new_month).ok_or_else(|| ExecutorError::UnsupportedFeature(
                    format!("Invalid month: {}", new_month)
                ))?;
                new_date
            }
            "DAY" | "DAYS" => date + Duration::days(amount),
            "HOUR" | "HOURS" | "MINUTE" | "MINUTES" | "SECOND" | "SECONDS" => {
                // Time units on dates don't change the date
                return Ok(types::SqlValue::Date(date.format("%Y-%m-%d").to_string()));
            }
            _ => return Err(ExecutorError::UnsupportedFeature(
                format!("Unsupported date unit: {}", unit)
            )),
        };

        Ok(types::SqlValue::Date(new_date.format("%Y-%m-%d").to_string()))
    }
}

/// Helper for AGE function - calculate years, months, and days between two dates
/// Returns (years, months, days) where all values have the same sign
fn calculate_age_components(date1: chrono::NaiveDate, date2: chrono::NaiveDate) -> (i32, i32, i32) {
    use chrono::Datelike;

    // Determine if result should be negative
    let is_negative = date1 < date2;
    let (later, earlier) = if is_negative {
        (date2, date1)
    } else {
        (date1, date2)
    };

    let mut years = later.year() - earlier.year();
    let mut months = later.month() as i32 - earlier.month() as i32;
    let mut days = later.day() as i32 - earlier.day() as i32;

    // Adjust if days are negative
    if days < 0 {
        months -= 1;
        // Add days in the previous month
        let prev_month = if later.month() == 1 { 12 } else { later.month() - 1 };
        let prev_year = if later.month() == 1 { later.year() - 1 } else { later.year() };

        // Get days in previous month
        let days_in_prev_month = if let Some(date) = chrono::NaiveDate::from_ymd_opt(prev_year, prev_month, 1) {
            let next_month_date = if prev_month == 12 {
                chrono::NaiveDate::from_ymd_opt(prev_year + 1, 1, 1).unwrap()
            } else {
                chrono::NaiveDate::from_ymd_opt(prev_year, prev_month + 1, 1).unwrap()
            };
            (next_month_date - date).num_days() as i32
        } else {
            30 // Fallback to 30 days
        };

        days += days_in_prev_month;
    }

    // Adjust if months are negative
    if months < 0 {
        years -= 1;
        months += 12;
    }

    // Apply sign
    if is_negative {
        (-years, -months, -days)
    } else {
        (years, months, days)
    }
}
