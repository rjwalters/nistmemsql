//! Basic numeric functions
//!
//! Implements ABS, SIGN, and MOD functions.

use crate::errors::ExecutorError;
use types::SqlValue;

/// ABS(x) - Absolute value
/// SQL:1999 Section 6.27: Numeric value functions
pub fn abs(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("ABS requires exactly 1 argument, got {}", args.len()),
        ));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Integer(n) => Ok(SqlValue::Integer(n.abs())),
        SqlValue::Bigint(n) => Ok(SqlValue::Bigint(n.abs())),
        SqlValue::Smallint(n) => Ok(SqlValue::Smallint(n.abs())),
        SqlValue::Float(n) => Ok(SqlValue::Float(n.abs())),
        SqlValue::Double(n) => Ok(SqlValue::Double(n.abs())),
        SqlValue::Real(n) => Ok(SqlValue::Real(n.abs())),
        val => Err(ExecutorError::UnsupportedFeature(
            format!("ABS requires numeric argument, got {:?}", val),
        )),
    }
}

/// SIGN(x) - Sign of number (-1, 0, or 1)
/// SQL:1999 Section 6.27: Numeric value functions
pub fn sign(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("SIGN requires exactly 1 argument, got {}", args.len()),
        ));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Integer(n) => {
            let sign = if *n < 0 {
                -1
            } else if *n > 0 {
                1
            } else {
                0
            };
            Ok(SqlValue::Integer(sign))
        }
        SqlValue::Bigint(n) => {
            let sign = if *n < 0 {
                -1
            } else if *n > 0 {
                1
            } else {
                0
            };
            Ok(SqlValue::Bigint(sign))
        }
        SqlValue::Float(n) => {
            let sign = if *n < 0.0 {
                -1.0
            } else if *n > 0.0 {
                1.0
            } else {
                0.0
            };
            Ok(SqlValue::Float(sign))
        }
        SqlValue::Double(n) => {
            let sign = if *n < 0.0 {
                -1.0
            } else if *n > 0.0 {
                1.0
            } else {
                0.0
            };
            Ok(SqlValue::Double(sign))
        }
        SqlValue::Real(n) => {
            let sign = if *n < 0.0 {
                -1.0
            } else if *n > 0.0 {
                1.0
            } else {
                0.0
            };
            Ok(SqlValue::Real(sign))
        }
        val => Err(ExecutorError::UnsupportedFeature(
            format!("SIGN requires numeric argument, got {:?}", val),
        )),
    }
}

/// MOD(x, y) - Modulo (remainder)
/// SQL:1999 Section 6.27: Numeric value functions
pub fn mod_fn(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(
            format!("MOD requires exactly 2 arguments, got {}", args.len()),
        ));
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),

        (SqlValue::Integer(x), SqlValue::Integer(y)) => {
            if *y == 0 {
                return Err(ExecutorError::DivisionByZero);
            }
            Ok(SqlValue::Integer(x % y))
        }

        (SqlValue::Float(x), SqlValue::Float(y)) | (SqlValue::Real(x), SqlValue::Real(y)) => {
            if *y == 0.0 {
                return Err(ExecutorError::DivisionByZero);
            }
            Ok(SqlValue::Float(x % y))
        }

        _ => Err(ExecutorError::UnsupportedFeature(
            "MOD requires numeric arguments of the same type".to_string(),
        )),
    }
}

/// PI() - Mathematical constant π
pub fn pi(_args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    Ok(SqlValue::Double(std::f64::consts::PI))
}
