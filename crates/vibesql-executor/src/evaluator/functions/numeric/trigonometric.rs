//! Trigonometric functions
//!
//! Implements SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2, RADIANS, and DEGREES functions.

use vibesql_types::SqlValue;

use super::exponential::numeric_to_f64;
use crate::errors::ExecutorError;

/// SIN(x) - Sine (x in radians)
pub fn sin(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "SIN requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        val => {
            let x = numeric_to_f64(val)?;
            Ok(SqlValue::Double(x.sin()))
        }
    }
}

/// COS(x) - Cosine (x in radians)
pub fn cos(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "COS requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        val => {
            let x = numeric_to_f64(val)?;
            Ok(SqlValue::Double(x.cos()))
        }
    }
}

/// TAN(x) - Tangent (x in radians)
pub fn tan(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TAN requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        val => {
            let x = numeric_to_f64(val)?;
            Ok(SqlValue::Double(x.tan()))
        }
    }
}

/// ASIN(x) - Arcsine (returns radians)
pub fn asin(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ASIN requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        val => {
            let x = numeric_to_f64(val)?;
            if !(-1.0..=1.0).contains(&x) {
                Err(ExecutorError::UnsupportedFeature(
                    "ASIN requires value between -1 and 1".to_string(),
                ))
            } else {
                Ok(SqlValue::Double(x.asin()))
            }
        }
    }
}

/// ACOS(x) - Arccosine (returns radians)
pub fn acos(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ACOS requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        val => {
            let x = numeric_to_f64(val)?;
            if !(-1.0..=1.0).contains(&x) {
                Err(ExecutorError::UnsupportedFeature(
                    "ACOS requires value between -1 and 1".to_string(),
                ))
            } else {
                Ok(SqlValue::Double(x.acos()))
            }
        }
    }
}

/// ATAN(x) - Arctangent (returns radians)
pub fn atan(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ATAN requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        val => {
            let x = numeric_to_f64(val)?;
            Ok(SqlValue::Double(x.atan()))
        }
    }
}

/// ATAN2(y, x) - Arctangent of y/x (returns radians)
pub fn atan2(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ATAN2 requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
        (y_val, x_val) => {
            let y = numeric_to_f64(y_val)?;
            let x = numeric_to_f64(x_val)?;
            Ok(SqlValue::Double(y.atan2(x)))
        }
    }
}

/// RADIANS(x) - Convert degrees to radians
pub fn radians(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "RADIANS requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        val => {
            let degrees = numeric_to_f64(val)?;
            Ok(SqlValue::Double(degrees.to_radians()))
        }
    }
}

/// DEGREES(x) - Convert radians to degrees
pub fn degrees(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "DEGREES requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        val => {
            let radians = numeric_to_f64(val)?;
            Ok(SqlValue::Double(radians.to_degrees()))
        }
    }
}
