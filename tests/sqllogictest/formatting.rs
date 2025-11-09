//! SQL value formatting utilities for test output.

use sqllogictest::DefaultColumnType;
use types::SqlValue;

/// Format SQL value for test output, applying type-specific formatting rules
pub fn format_sql_value(value: &SqlValue, expected_type: Option<&DefaultColumnType>) -> String {
    match value {
        SqlValue::Integer(i) => {
            if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                format!("{:.3}", *i as f64)
            } else {
                i.to_string()
            }
        }
        SqlValue::Smallint(i) => {
            if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                format!("{:.3}", *i as f64)
            } else {
                i.to_string()
            }
        }
        SqlValue::Bigint(i) => {
            if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                format!("{:.3}", *i as f64)
            } else {
                i.to_string()
            }
        }
        SqlValue::Unsigned(i) => {
            if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                format!("{:.3}", *i as f64)
            } else {
                i.to_string()
            }
        }
        SqlValue::Numeric(_) => value.to_string(), // Use Display trait for consistent formatting
        SqlValue::Float(f) | SqlValue::Real(f) => {
            if f.fract() == 0.0 {
                format!("{:.1}", f)
            } else {
                f.to_string()
            }
        }
        SqlValue::Double(f) => {
            if f.fract() == 0.0 {
                format!("{:.1}", f)
            } else {
                f.to_string()
            }
        }
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.clone(),
        SqlValue::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Date(d)
        | SqlValue::Time(d)
        | SqlValue::Timestamp(d)
        | SqlValue::Interval(d) => d.clone(),
    }
}

/// Format value in canonical form for hashing (plain format without display decorations)
pub fn format_sql_value_canonical(value: &SqlValue, expected_type: Option<&DefaultColumnType>) -> String {
    match value {
        SqlValue::Integer(i) => i.to_string(),
        SqlValue::Smallint(i) => {
            if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                format!("{:.3}", *i as f64)
            } else {
                i.to_string()
            }
        }
        SqlValue::Bigint(i) => {
            if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                format!("{:.3}", *i as f64)
            } else {
                i.to_string()
            }
        }
        SqlValue::Unsigned(i) => {
            if matches!(expected_type, Some(DefaultColumnType::FloatingPoint)) {
                format!("{:.3}", *i as f64)
            } else {
                i.to_string()
            }
        }
        SqlValue::Numeric(_) => value.to_string(),
        SqlValue::Float(f) | SqlValue::Real(f) => {
            if f.fract() == 0.0 {
                format!("{:.1}", f)
            } else {
                f.to_string()
            }
        }
        SqlValue::Double(f) => {
            if f.fract() == 0.0 {
                format!("{:.1}", f)
            } else {
                f.to_string()
            }
        }
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.clone(),
        SqlValue::Boolean(b) => if *b { "1" } else { "0" }.to_string(),
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Date(d)
        | SqlValue::Time(d)
        | SqlValue::Timestamp(d)
        | SqlValue::Interval(d) => d.clone(),
    }
}
