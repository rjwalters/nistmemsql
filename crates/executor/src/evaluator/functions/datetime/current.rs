//! Current date/time functions
//!
//! Implements CURRENT_DATE, CURRENT_TIME, and CURRENT_TIMESTAMP functions.

use crate::errors::ExecutorError;
use chrono::{Local, Timelike};
use types::SqlValue;

/// CURRENT_DATE / CURDATE - Returns current date
/// Alias: CURDATE
/// SQL:1999 Section 6.31: Datetime value functions
pub fn current_date(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if !args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "CURRENT_DATE takes no arguments".to_string(),
        ));
    }

    let now = Local::now();
    let date_str = now.format("%Y-%m-%d").to_string();
    Ok(SqlValue::Date(date_str))
}

/// CURRENT_TIME / CURTIME - Returns current time
/// Alias: CURTIME
/// SQL:1999 Section 6.31: Datetime value functions
/// Supports optional precision argument (0-9) for fractional seconds
pub fn current_time(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    // Parse precision argument if provided
    let precision = if args.is_empty() {
        None
    } else if args.len() == 1 {
        match &args[0] {
            SqlValue::Integer(n) if *n >= 0 && *n <= 9 => Some(*n as u32),
            SqlValue::Integer(n) => {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("CURRENT_TIME precision must be 0-9, got {}", n)
                ));
            }
            _ => {
                return Err(ExecutorError::UnsupportedFeature(
                    "CURRENT_TIME precision must be an integer between 0 and 9".to_string()
                ));
            }
        }
    } else {
        return Err(ExecutorError::UnsupportedFeature(
            "CURRENT_TIME takes 0 or 1 arguments".to_string()
        ));
    };

    let now = Local::now();

    let time_str = match precision {
        None => now.format("%H:%M:%S").to_string(),
        Some(prec) => {
            // Format with fractional seconds
            format_time_with_precision(now.time(), prec)
        }
    };

    Ok(SqlValue::Time(time_str))
}

/// CURRENT_TIMESTAMP / NOW - Returns current timestamp
/// Alias: NOW
/// SQL:1999 Section 6.31: Datetime value functions
/// Supports optional precision argument (0-9) for fractional seconds
pub fn current_timestamp(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    // Parse precision argument if provided
    let precision = if args.is_empty() {
        None
    } else if args.len() == 1 {
        match &args[0] {
            SqlValue::Integer(n) if *n >= 0 && *n <= 9 => Some(*n as u32),
            SqlValue::Integer(n) => {
                return Err(ExecutorError::UnsupportedFeature(
                    format!("CURRENT_TIMESTAMP precision must be 0-9, got {}", n)
                ));
            }
            _ => {
                return Err(ExecutorError::UnsupportedFeature(
                    "CURRENT_TIMESTAMP precision must be an integer between 0 and 9".to_string()
                ));
            }
        }
    } else {
        return Err(ExecutorError::UnsupportedFeature(
            "CURRENT_TIMESTAMP takes 0 or 1 arguments".to_string()
        ));
    };

    let now = Local::now();

    let timestamp_str = match precision {
        None => now.format("%Y-%m-%d %H:%M:%S").to_string(),
        Some(prec) => {
            // Format with fractional seconds
            format_timestamp_with_precision(now, prec)
        }
    };

    Ok(SqlValue::Timestamp(timestamp_str))
}

/// Helper function to format time with fractional seconds precision
fn format_time_with_precision(time: chrono::NaiveTime, precision: u32) -> String {
    let base = time.format("%H:%M:%S").to_string();
    if precision == 0 {
        return base;
    }

    // Get fractional seconds
    let nanos = time.nanosecond();
    let divisor = 10_u32.pow(9 - precision);
    let fractional = nanos / divisor;

    format!("{}.{:0width$}", base, fractional, width = precision as usize)
}

/// Helper function to format timestamp with fractional seconds precision
fn format_timestamp_with_precision(dt: chrono::DateTime<Local>, precision: u32) -> String {
    let base = dt.format("%Y-%m-%d %H:%M:%S").to_string();
    if precision == 0 {
        return base;
    }

    let nanos = dt.timestamp_subsec_nanos();
    let divisor = 10_u32.pow(9 - precision);
    let fractional = nanos / divisor;

    format!("{}.{:0width$}", base, fractional, width = precision as usize)
}
