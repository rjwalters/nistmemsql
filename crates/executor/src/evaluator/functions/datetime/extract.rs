//! Date/time field extraction functions
//!
//! Implements YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, and EXTRACT functions.

use crate::errors::ExecutorError;
use types::SqlValue;

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
        SqlValue::Date(s) | SqlValue::Timestamp(s) => {
            // Parse date string (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
            let parts: Vec<&str> = s.split(&['-', ' '][..]).collect();
            if parts.is_empty() {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid date format for YEAR".to_string(),
                ));
            }
            match parts[0].parse::<i64>() {
                Ok(year) => Ok(SqlValue::Integer(year)),
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
        SqlValue::Date(s) | SqlValue::Timestamp(s) => {
            // Parse date string (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
            let date_part = s.split(' ').next().unwrap_or(s);
            let parts: Vec<&str> = date_part.split('-').collect();
            if parts.len() < 2 {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid date format for MONTH".to_string(),
                ));
            }
            match parts[1].parse::<i64>() {
                Ok(month) => Ok(SqlValue::Integer(month)),
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
        SqlValue::Date(s) | SqlValue::Timestamp(s) => {
            // Parse date string (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
            let date_part = s.split(' ').next().unwrap_or(s);
            let parts: Vec<&str> = date_part.split('-').collect();
            if parts.len() < 3 {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid date format for DAY".to_string(),
                ));
            }
            match parts[2].parse::<i64>() {
                Ok(day) => Ok(SqlValue::Integer(day)),
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
        SqlValue::Time(s) => {
            // Parse time string (HH:MM:SS)
            let parts: Vec<&str> = s.split(':').collect();
            if parts.is_empty() {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid time format for HOUR".to_string(),
                ));
            }
            match parts[0].parse::<i64>() {
                Ok(hour) => Ok(SqlValue::Integer(hour)),
                Err(_) => Err(ExecutorError::UnsupportedFeature(
                    "Invalid hour value".to_string(),
                )),
            }
        }
        SqlValue::Timestamp(s) => {
            // Parse timestamp string (YYYY-MM-DD HH:MM:SS)
            let time_part = s.split(' ').nth(1).unwrap_or("");
            let parts: Vec<&str> = time_part.split(':').collect();
            if parts.is_empty() {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid timestamp format for HOUR".to_string(),
                ));
            }
            match parts[0].parse::<i64>() {
                Ok(hour) => Ok(SqlValue::Integer(hour)),
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
        SqlValue::Time(s) => {
            // Parse time string (HH:MM:SS)
            let parts: Vec<&str> = s.split(':').collect();
            if parts.len() < 2 {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid time format for MINUTE".to_string(),
                ));
            }
            match parts[1].parse::<i64>() {
                Ok(minute) => Ok(SqlValue::Integer(minute)),
                Err(_) => Err(ExecutorError::UnsupportedFeature(
                    "Invalid minute value".to_string(),
                )),
            }
        }
        SqlValue::Timestamp(s) => {
            // Parse timestamp string (YYYY-MM-DD HH:MM:SS)
            let time_part = s.split(' ').nth(1).unwrap_or("");
            let parts: Vec<&str> = time_part.split(':').collect();
            if parts.len() < 2 {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid timestamp format for MINUTE".to_string(),
                ));
            }
            match parts[1].parse::<i64>() {
                Ok(minute) => Ok(SqlValue::Integer(minute)),
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
        SqlValue::Time(s) => {
            // Parse time string (HH:MM:SS)
            let parts: Vec<&str> = s.split(':').collect();
            if parts.len() < 3 {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid time format for SECOND".to_string(),
                ));
            }
            match parts[2].parse::<i64>() {
                Ok(second) => Ok(SqlValue::Integer(second)),
                Err(_) => Err(ExecutorError::UnsupportedFeature(
                    "Invalid second value".to_string(),
                )),
            }
        }
        SqlValue::Timestamp(s) => {
            // Parse timestamp string (YYYY-MM-DD HH:MM:SS)
            let time_part = s.split(' ').nth(1).unwrap_or("");
            let parts: Vec<&str> = time_part.split(':').collect();
            if parts.len() < 3 {
                return Err(ExecutorError::UnsupportedFeature(
                    "Invalid timestamp format for SECOND".to_string(),
                ));
            }
            match parts[2].parse::<i64>() {
                Ok(second) => Ok(SqlValue::Integer(second)),
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
