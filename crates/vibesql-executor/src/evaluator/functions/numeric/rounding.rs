//! Rounding functions
//!
//! Implements ROUND, FLOOR, and CEIL/CEILING functions.

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// ROUND(x [, precision]) - Round to nearest integer or decimal places
/// SQL:1999 Section 6.27: Numeric value functions
pub fn round(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "ROUND requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    let value = &args[0];
    let precision = if args.len() == 2 {
        match &args[1] {
            SqlValue::Integer(p) => *p as i32,
            SqlValue::Null => return Ok(SqlValue::Null),
            val => {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "ROUND precision must be integer, got {:?}",
                    val
                )))
            }
        }
    } else {
        0
    };

    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Integer(n) => Ok(SqlValue::Integer(*n)),
        SqlValue::Float(f) => {
            let multiplier = 10_f32.powi(precision);
            Ok(SqlValue::Float((f * multiplier).round() / multiplier))
        }
        SqlValue::Double(f) => {
            let multiplier = 10_f64.powi(precision);
            Ok(SqlValue::Double((f * multiplier).round() / multiplier))
        }
        SqlValue::Real(f) => {
            let multiplier = 10_f32.powi(precision);
            Ok(SqlValue::Real((f * multiplier).round() / multiplier))
        }
        SqlValue::Numeric(n) => {
            let multiplier = 10_f64.powi(precision);
            Ok(SqlValue::Numeric((n * multiplier).round() / multiplier))
        }
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "ROUND requires numeric argument, got {:?}",
            val
        ))),
    }
}

/// FLOOR(x) - Round down to nearest integer
/// SQL:1999 Section 6.27: Numeric value functions
pub fn floor(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "FLOOR requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Integer(n) => Ok(SqlValue::Integer(*n)),
        SqlValue::Float(f) => Ok(SqlValue::Float(f.floor())),
        SqlValue::Double(f) => Ok(SqlValue::Double(f.floor())),
        SqlValue::Real(f) => Ok(SqlValue::Real(f.floor())),
        SqlValue::Numeric(n) => Ok(SqlValue::Numeric(n.floor())),
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "FLOOR requires numeric argument, got {:?}",
            val
        ))),
    }
}

/// CEIL/CEILING(x) - Round up to nearest integer
/// SQL:1999 Section 6.27: Numeric value functions
/// Note: CEILING is an alias for CEIL
pub fn ceil(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "CEIL requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Integer(n) => Ok(SqlValue::Integer(*n)),
        SqlValue::Float(f) => Ok(SqlValue::Float(f.ceil())),
        SqlValue::Double(f) => Ok(SqlValue::Double(f.ceil())),
        SqlValue::Real(f) => Ok(SqlValue::Real(f.ceil())),
        SqlValue::Numeric(n) => Ok(SqlValue::Numeric(n.ceil())),
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "CEIL requires numeric argument, got {:?}",
            val
        ))),
    }
}

/// TRUNCATE(x [, precision]) - Truncate to specified decimal places (towards zero)
/// SQL:1999 Section 6.27: Numeric value functions
pub fn truncate(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TRUNCATE requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    let value = &args[0];
    let precision = if args.len() == 2 {
        match &args[1] {
            SqlValue::Integer(p) => *p as i32,
            SqlValue::Null => return Ok(SqlValue::Null),
            val => {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "TRUNCATE precision must be integer, got {:?}",
                    val
                )))
            }
        }
    } else {
        0
    };

    match value {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Integer(n) => Ok(SqlValue::Integer(*n)),
        SqlValue::Float(f) => {
            let multiplier = 10_f32.powi(precision);
            Ok(SqlValue::Float((f * multiplier).trunc() / multiplier))
        }
        SqlValue::Double(f) => {
            let multiplier = 10_f64.powi(precision);
            Ok(SqlValue::Double((f * multiplier).trunc() / multiplier))
        }
        SqlValue::Real(f) => {
            let multiplier = 10_f32.powi(precision);
            Ok(SqlValue::Real((f * multiplier).trunc() / multiplier))
        }
        SqlValue::Numeric(n) => {
            let multiplier = 10_f64.powi(precision);
            Ok(SqlValue::Numeric((n * multiplier).trunc() / multiplier))
        }
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "TRUNCATE requires numeric argument, got {:?}",
            val
        ))),
    }
}
