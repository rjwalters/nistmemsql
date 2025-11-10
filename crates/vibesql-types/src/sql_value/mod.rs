//! SQL Value runtime representation

mod comparison;
mod hash;
mod display;

use crate::{DataType, temporal::{Date, Time, Timestamp, Interval, IntervalField}};

/// SQL Values - runtime representation of data
///
/// Represents actual values in SQL, including NULL.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Integer(i64),
    Smallint(i16),
    Bigint(i64),
    Unsigned(u64), // 64-bit unsigned integer (MySQL compatibility)
    Numeric(f64),  // f64 for performance (was: String)

    Float(f32),
    Real(f32),
    Double(f64),

    Character(String),
    Varchar(String),

    Boolean(bool),

    // Date/Time types with proper structured representation
    Date(Date),
    Time(Time),
    Timestamp(Timestamp),

    // Interval type
    Interval(Interval),

    Null,
}

impl SqlValue {
    /// Check if this value is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, SqlValue::Null)
    }

    /// Get the type name as a string (for error messages)
    pub fn type_name(&self) -> &'static str {
        match self {
            SqlValue::Integer(_) => "INTEGER",
            SqlValue::Smallint(_) => "SMALLINT",
            SqlValue::Bigint(_) => "BIGINT",
            SqlValue::Unsigned(_) => "UNSIGNED",
            SqlValue::Numeric(_) => "NUMERIC",
            SqlValue::Float(_) => "FLOAT",
            SqlValue::Real(_) => "REAL",
            SqlValue::Double(_) => "DOUBLE PRECISION",
            SqlValue::Character(_) => "CHAR",
            SqlValue::Varchar(_) => "VARCHAR",
            SqlValue::Boolean(_) => "BOOLEAN",
            SqlValue::Date(_) => "DATE",
            SqlValue::Time(_) => "TIME",
            SqlValue::Timestamp(_) => "TIMESTAMP",
            SqlValue::Interval(_) => "INTERVAL",
            SqlValue::Null => "NULL",
        }
    }

    /// Get the data type of this value
    pub fn get_type(&self) -> DataType {
        match self {
            SqlValue::Integer(_) => DataType::Integer,
            SqlValue::Smallint(_) => DataType::Smallint,
            SqlValue::Bigint(_) => DataType::Bigint,
            SqlValue::Unsigned(_) => DataType::Unsigned,
            SqlValue::Numeric(_) => DataType::Numeric { precision: 38, scale: 0 }, // Default
            SqlValue::Float(_) => DataType::Float { precision: 53 }, // Default to double precision
            SqlValue::Real(_) => DataType::Real,
            SqlValue::Double(_) => DataType::DoublePrecision,
            SqlValue::Character(s) => DataType::Character { length: s.len() },
            SqlValue::Varchar(_) => DataType::Varchar { max_length: None }, /* Unknown/unlimited */
            // length
            SqlValue::Boolean(_) => DataType::Boolean,
            SqlValue::Date(_) => DataType::Date,
            SqlValue::Time(_) => DataType::Time { with_timezone: false },
            SqlValue::Timestamp(_) => DataType::Timestamp { with_timezone: false },
            SqlValue::Interval(_) => DataType::Interval {
                start_field: IntervalField::Day, /* Default - actual type lost in string
                                                  * representation */
                end_field: None,
            },
            SqlValue::Null => DataType::Null,
        }
    }
}
