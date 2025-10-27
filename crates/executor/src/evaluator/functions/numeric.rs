//! Numeric and mathematical functions
//!
//! This module implements SQL numeric functions including:
//! - Basic arithmetic: ABS, ROUND, FLOOR, CEIL/CEILING, MOD
//! - Exponential and logarithmic: POWER/POW, SQRT, EXP, LN/LOG, LOG10
//! - Trigonometric: SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2
//! - Angle conversion: RADIANS, DEGREES
//! - Utility: SIGN, PI, GREATEST, LEAST
//!
//! SQL:1999 Section 6.27: Numeric value functions

use crate::errors::ExecutorError;

/// ABS(x) - Absolute value
/// SQL:1999 Section 6.27: Numeric value functions
pub(super) fn abs(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// ROUND(x [, precision]) - Round to nearest integer or decimal places
/// SQL:1999 Section 6.27: Numeric value functions
pub(super) fn round(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// FLOOR(x) - Round down to nearest integer
/// SQL:1999 Section 6.27: Numeric value functions
pub(super) fn floor(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// CEIL/CEILING(x) - Round up to nearest integer
/// SQL:1999 Section 6.27: Numeric value functions
/// Note: CEILING is an alias for CEIL
pub(super) fn ceil(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("CEIL requires exactly 1 argument, got {}", args.len()),
        ));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Integer(n) => Ok(types::SqlValue::Integer(*n)),
        types::SqlValue::Float(f) => Ok(types::SqlValue::Float(f.ceil())),
        types::SqlValue::Double(f) => Ok(types::SqlValue::Double(f.ceil())),
        types::SqlValue::Real(f) => Ok(types::SqlValue::Real(f.ceil())),
        val => Err(ExecutorError::UnsupportedFeature(
            format!("CEIL requires numeric argument, got {:?}", val),
        )),
    }
}

/// MOD(x, y) - Modulo (remainder)
/// SQL:1999 Section 6.27: Numeric value functions
pub(super) fn mod_func(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// POWER(x, y) - x raised to power y
/// SQL:1999 Section 6.27: Numeric value functions
/// Note: POW is an alias for POWER
pub(super) fn power(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("POWER requires exactly 2 arguments, got {}", args.len()),
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

/// SQRT(x) - Square root
/// SQL:1999 Section 6.27: Numeric value functions
pub(super) fn sqrt(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// EXP(x) - e raised to power x
/// SQL:1999 Section 6.27: Numeric value functions
pub(super) fn exp(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// LN(x) - Natural logarithm
/// SQL:1999 Section 6.27: Numeric value functions
/// Note: LOG is an alias for LN
pub(super) fn ln(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("LN requires exactly 1 argument, got {}", args.len()),
        ));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        val => {
            let x = numeric_to_f64(val)?;
            if x <= 0.0 {
                Err(ExecutorError::UnsupportedFeature(
                    "LN of non-positive number".to_string(),
                ))
            } else {
                Ok(types::SqlValue::Double(x.ln()))
            }
        }
    }
}

/// LOG10(x) - Base-10 logarithm
pub(super) fn log10(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// SIGN(x) - Sign of number (-1, 0, or 1)
pub(super) fn sign(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// PI() - Mathematical constant π
pub(super) fn pi(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if !args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            format!("PI requires no arguments, got {}", args.len()),
        ));
    }
    Ok(types::SqlValue::Double(std::f64::consts::PI))
}

/// SIN(x) - Sine (x in radians)
pub(super) fn sin(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// COS(x) - Cosine (x in radians)
pub(super) fn cos(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// TAN(x) - Tangent (x in radians)
pub(super) fn tan(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// ASIN(x) - Arcsine (returns radians)
pub(super) fn asin(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// ACOS(x) - Arccosine (returns radians)
pub(super) fn acos(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// ATAN(x) - Arctangent (returns radians)
pub(super) fn atan(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// ATAN2(y, x) - Arctangent of y/x (returns radians)
pub(super) fn atan2(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// RADIANS(x) - Convert degrees to radians
pub(super) fn radians(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// DEGREES(x) - Convert radians to degrees
pub(super) fn degrees(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// GREATEST(val1, val2, ...) - Returns greatest value
pub(super) fn greatest(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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

/// LEAST(val1, val2, ...) - Returns smallest value
pub(super) fn least(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
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
