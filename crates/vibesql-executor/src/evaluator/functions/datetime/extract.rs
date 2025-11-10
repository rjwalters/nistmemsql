//! Date/time field extraction functions
//!
//! Implements YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, and EXTRACT functions.

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// YEAR(date) - Extract year from date/timestamp
/// SQL:1999 Section 6.32: Datetime field extraction
pub fn year(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            "YEAR requires exactly 1 argument".to_string(),
        ));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Date(d) => Ok(SqlValue::Integer(d.year as i64)),
        SqlValue::Timestamp(ts) => Ok(SqlValue::Integer(ts.date.year as i64)),
        _ => Err(ExecutorError::UnsupportedFeature(
            "YEAR requires date or timestamp argument".to_string(),
        )),
    }
}

/// MONTH(date) - Extract month from date/timestamp
/// SQL:1999 Section 6.32: Datetime field extraction
pub fn month(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            "MONTH requires exactly 1 argument".to_string(),
        ));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Date(d) => Ok(SqlValue::Integer(d.month as i64)),
        SqlValue::Timestamp(ts) => Ok(SqlValue::Integer(ts.date.month as i64)),
        _ => Err(ExecutorError::UnsupportedFeature(
            "MONTH requires date or timestamp argument".to_string(),
        )),
    }
}

/// DAY(date) - Extract day from date/timestamp
/// SQL:1999 Section 6.32: Datetime field extraction
pub fn day(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            "DAY requires exactly 1 argument".to_string(),
        ));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Date(d) => Ok(SqlValue::Integer(d.day as i64)),
        SqlValue::Timestamp(ts) => Ok(SqlValue::Integer(ts.date.day as i64)),
        _ => Err(ExecutorError::UnsupportedFeature(
            "DAY requires date or timestamp argument".to_string(),
        )),
    }
}

/// HOUR(time) - Extract hour from time/timestamp
/// SQL:1999 Section 6.32: Datetime field extraction
pub fn hour(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            "HOUR requires exactly 1 argument".to_string(),
        ));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Time(t) => Ok(SqlValue::Integer(t.hour as i64)),
        SqlValue::Timestamp(ts) => Ok(SqlValue::Integer(ts.time.hour as i64)),
        _ => Err(ExecutorError::UnsupportedFeature(
            "HOUR requires time or timestamp argument".to_string(),
        )),
    }
}

/// MINUTE(time) - Extract minute from time/timestamp
/// SQL:1999 Section 6.32: Datetime field extraction
pub fn minute(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            "MINUTE requires exactly 1 argument".to_string(),
        ));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Time(t) => Ok(SqlValue::Integer(t.minute as i64)),
        SqlValue::Timestamp(ts) => Ok(SqlValue::Integer(ts.time.minute as i64)),
        _ => Err(ExecutorError::UnsupportedFeature(
            "MINUTE requires time or timestamp argument".to_string(),
        )),
    }
}

/// SECOND(time) - Extract second from time/timestamp
/// SQL:1999 Section 6.32: Datetime field extraction
pub fn second(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(
            "SECOND requires exactly 1 argument".to_string(),
        ));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Time(t) => Ok(SqlValue::Integer(t.second as i64)),
        SqlValue::Timestamp(ts) => Ok(SqlValue::Integer(ts.time.second as i64)),
        _ => Err(ExecutorError::UnsupportedFeature(
            "SECOND requires time or timestamp argument".to_string(),
        )),
    }
}
