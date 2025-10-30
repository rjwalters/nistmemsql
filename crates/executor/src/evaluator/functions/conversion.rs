//! Type conversion functions (CAST, TO_NUMBER, TO_DATE, TO_TIMESTAMP, TO_CHAR)
//!
//! SQL:1999 Section 6.10-6.12: CAST specification and type conversions

use crate::errors::ExecutorError;

/// TO_NUMBER(string) - Convert string to numeric value
/// SQL:1999 Section 6.12: Type conversions
pub(super) fn to_number(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TO_NUMBER requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),
        types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => {
            // Remove whitespace and commas
            let cleaned = s.trim().replace(",", "");

            // Try parsing as integer first
            if let Ok(n) = cleaned.parse::<i64>() {
                Ok(types::SqlValue::Integer(n))
            } else if let Ok(n) = cleaned.parse::<f64>() {
                // Parse as floating point
                Ok(types::SqlValue::Double(n))
            } else {
                Err(ExecutorError::UnsupportedFeature(format!(
                    "TO_NUMBER: Invalid numeric string '{}'",
                    s
                )))
            }
        }
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "TO_NUMBER requires string argument, got {:?}",
            val
        ))),
    }
}

/// TO_DATE(string, format) - Convert string to date with format
/// SQL:1999 Section 6.12: Type conversions
pub(super) fn to_date(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TO_DATE requires exactly 2 arguments (string, format), got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => Ok(types::SqlValue::Null),
        (
            types::SqlValue::Varchar(input) | types::SqlValue::Character(input),
            types::SqlValue::Varchar(format) | types::SqlValue::Character(format),
        ) => {
            let date = super::super::date_format::parse_date(input, format)?;
            Ok(types::SqlValue::Date(date.format("%Y-%m-%d").to_string()))
        }
        (input, format) => Err(ExecutorError::UnsupportedFeature(format!(
            "TO_DATE requires string arguments, got {:?} and {:?}",
            input, format
        ))),
    }
}

/// TO_TIMESTAMP(string, format) - Convert string to timestamp with format
/// SQL:1999 Section 6.12: Type conversions
pub(super) fn to_timestamp(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TO_TIMESTAMP requires exactly 2 arguments (string, format), got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (types::SqlValue::Null, _) | (_, types::SqlValue::Null) => Ok(types::SqlValue::Null),
        (
            types::SqlValue::Varchar(input) | types::SqlValue::Character(input),
            types::SqlValue::Varchar(format) | types::SqlValue::Character(format),
        ) => {
            let timestamp = super::super::date_format::parse_timestamp(input, format)?;
            Ok(types::SqlValue::Timestamp(timestamp.format("%Y-%m-%d %H:%M:%S").to_string()))
        }
        (input, format) => Err(ExecutorError::UnsupportedFeature(format!(
            "TO_TIMESTAMP requires string arguments, got {:?} and {:?}",
            input, format
        ))),
    }
}

/// TO_CHAR(value, format) - Convert date/number to formatted string
/// SQL:1999 Section 6.12: Type conversions
pub(super) fn to_char(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TO_CHAR requires exactly 2 arguments (value, format), got {}",
            args.len()
        )));
    }

    let format_str = match &args[1] {
        types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => s,
        types::SqlValue::Null => return Ok(types::SqlValue::Null),
        val => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "TO_CHAR format must be string, got {:?}",
                val
            )))
        }
    };

    match &args[0] {
        types::SqlValue::Null => Ok(types::SqlValue::Null),

        // Date/Time formatting
        types::SqlValue::Date(date_str) => {
            use chrono::NaiveDate;
            let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").map_err(|e| {
                ExecutorError::UnsupportedFeature(format!("Invalid date format: {}", e))
            })?;
            let formatted = super::super::date_format::format_date(&date, format_str)?;
            Ok(types::SqlValue::Varchar(formatted))
        }
        types::SqlValue::Timestamp(ts_str) => {
            use chrono::NaiveDateTime;
            let timestamp =
                NaiveDateTime::parse_from_str(ts_str, "%Y-%m-%d %H:%M:%S").map_err(|e| {
                    ExecutorError::UnsupportedFeature(format!("Invalid timestamp format: {}", e))
                })?;
            let formatted = super::super::date_format::format_timestamp(&timestamp, format_str)?;
            Ok(types::SqlValue::Varchar(formatted))
        }
        types::SqlValue::Time(time_str) => {
            use chrono::NaiveTime;
            let time = NaiveTime::parse_from_str(time_str, "%H:%M:%S").map_err(|e| {
                ExecutorError::UnsupportedFeature(format!("Invalid time format: {}", e))
            })?;
            let formatted = super::super::date_format::format_time(&time, format_str)?;
            Ok(types::SqlValue::Varchar(formatted))
        }

        // Number formatting
        types::SqlValue::Integer(n) => {
            let formatted = super::super::date_format::format_number(*n as f64, format_str)?;
            Ok(types::SqlValue::Varchar(formatted))
        }
        types::SqlValue::Smallint(n) => {
            let formatted = super::super::date_format::format_number(*n as f64, format_str)?;
            Ok(types::SqlValue::Varchar(formatted))
        }
        types::SqlValue::Bigint(n) => {
            let formatted = super::super::date_format::format_number(*n as f64, format_str)?;
            Ok(types::SqlValue::Varchar(formatted))
        }
        types::SqlValue::Float(n) => {
            let formatted = super::super::date_format::format_number(*n as f64, format_str)?;
            Ok(types::SqlValue::Varchar(formatted))
        }
        types::SqlValue::Double(n) => {
            let formatted = super::super::date_format::format_number(*n, format_str)?;
            Ok(types::SqlValue::Varchar(formatted))
        }
        types::SqlValue::Real(n) => {
            let formatted = super::super::date_format::format_number(*n as f64, format_str)?;
            Ok(types::SqlValue::Varchar(formatted))
        }

        val => {
            Err(ExecutorError::UnsupportedFeature(format!("TO_CHAR cannot format type {:?}", val)))
        }
    }
}

/// CAST(value AS type) - SQL:1999 standard type conversion
/// SQL:1999 Section 6.10: CAST specification
/// Note: CAST syntax is handled specially by the parser
/// This function receives [value, DataType] as arguments
pub(super) fn cast(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "CAST requires exactly 2 arguments (value, target_type), got {}",
            args.len()
        )));
    }

    // The second argument should be a DataType wrapped in a SqlValue
    // For now, we'll extract the target type information from string representation
    // This is a simplified implementation - ideally the parser would pass DataType directly
    let target_type_str = match &args[1] {
        types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => s.to_uppercase(),
        val => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "CAST target type must be string, got {:?}",
                val
            )))
        }
    };

    // Map string to DataType
    let target_type = match target_type_str.as_str() {
        "INTEGER" | "INT" => types::DataType::Integer,
        "SMALLINT" => types::DataType::Smallint,
        "BIGINT" => types::DataType::Bigint,
        "FLOAT" => types::DataType::Float { precision: 53 },
        "DOUBLE PRECISION" | "DOUBLE" => types::DataType::DoublePrecision,
        "VARCHAR" => types::DataType::Varchar { max_length: Some(255) },
        "DATE" => types::DataType::Date,
        "TIME" => types::DataType::Time { with_timezone: false },
        "TIMESTAMP" => types::DataType::Timestamp { with_timezone: false },
        _ => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "CAST to type '{}' not supported",
                target_type_str
            )))
        }
    };

    // Use the existing cast_value function from casting module
    super::super::casting::cast_value(&args[0], &target_type)
}
