//! Current date/time functions
//!
//! Implements CURRENT_DATE, CURRENT_TIME, and CURRENT_TIMESTAMP functions.

use crate::errors::ExecutorError;
use chrono::Local;
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
pub fn current_time(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if !args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "CURRENT_TIME takes no arguments".to_string(),
        ));
    }

    let now = Local::now();
    let time_str = now.format("%H:%M:%S").to_string();
    Ok(SqlValue::Time(time_str))
}

/// CURRENT_TIMESTAMP / NOW - Returns current timestamp
/// Alias: NOW
/// SQL:1999 Section 6.31: Datetime value functions
pub fn current_timestamp(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if !args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "CURRENT_TIMESTAMP takes no arguments".to_string(),
        ));
    }

    let now = Local::now();
    let timestamp_str = now.format("%Y-%m-%d %H:%M:%S").to_string();
    Ok(SqlValue::Timestamp(timestamp_str))
}
