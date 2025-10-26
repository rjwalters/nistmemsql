use crate::data_type::DataType;
use std::fmt;

/// SQL Values - runtime representation of data
///
/// Represents actual values in SQL, including NULL.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Integer(i64),
    Smallint(i16),
    Bigint(i64),
    Numeric(String), // TODO: Use proper decimal type

    Float(f32),
    Real(f32),
    Double(f64),

    Character(String),
    Varchar(String),

    Boolean(bool),

    // Date/Time (using strings for now)
    Date(String), // TODO: Use proper date types
    Time(String),
    Timestamp(String),

    Null,
}

impl SqlValue {
    /// Check if this value is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, SqlValue::Null)
    }

    /// Get the data type of this value
    pub fn get_type(&self) -> DataType {
        match self {
            SqlValue::Integer(_) => DataType::Integer,
            SqlValue::Smallint(_) => DataType::Smallint,
            SqlValue::Bigint(_) => DataType::Bigint,
            SqlValue::Numeric(_) => DataType::Numeric { precision: 38, scale: 0 }, // Default
            SqlValue::Float(_) => DataType::Float,
            SqlValue::Real(_) => DataType::Real,
            SqlValue::Double(_) => DataType::DoublePrecision,
            SqlValue::Character(s) => DataType::Character { length: s.len() },
            SqlValue::Varchar(_) => DataType::Varchar { max_length: usize::MAX }, // Unknown length
            SqlValue::Boolean(_) => DataType::Boolean,
            SqlValue::Date(_) => DataType::Date,
            SqlValue::Time(_) => DataType::Time { with_timezone: false },
            SqlValue::Timestamp(_) => DataType::Timestamp { with_timezone: false },
            SqlValue::Null => DataType::Null,
        }
    }
}

/// Display implementation for SqlValue (how values are shown to users)
impl fmt::Display for SqlValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlValue::Integer(i) => write!(f, "{}", i),
            SqlValue::Smallint(i) => write!(f, "{}", i),
            SqlValue::Bigint(i) => write!(f, "{}", i),
            SqlValue::Numeric(s) => write!(f, "{}", s),
            SqlValue::Float(n) => write!(f, "{}", n),
            SqlValue::Real(n) => write!(f, "{}", n),
            SqlValue::Double(n) => write!(f, "{}", n),
            SqlValue::Character(s) => write!(f, "{}", s),
            SqlValue::Varchar(s) => write!(f, "{}", s),
            SqlValue::Boolean(true) => write!(f, "TRUE"),
            SqlValue::Boolean(false) => write!(f, "FALSE"),
            SqlValue::Date(s) => write!(f, "{}", s),
            SqlValue::Time(s) => write!(f, "{}", s),
            SqlValue::Timestamp(s) => write!(f, "{}", s),
            SqlValue::Null => write!(f, "NULL"),
        }
    }
}
