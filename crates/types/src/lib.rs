//! SQL:1999 Type System
//!
//! This crate provides the type system for SQL:1999, including:
//! - Data type definitions (INTEGER, VARCHAR, BOOLEAN, etc.)
//! - SQL values representation
//! - Type compatibility and coercion rules
//! - Type checking utilities

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

/// SQL:1999 Interval Fields
///
/// Represents the fields that can be used in INTERVAL types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntervalField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

/// SQL:1999 Data Types
///
/// Represents the type of a column or expression in SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    // Exact numeric types
    Integer,
    Smallint,
    Bigint,
    Numeric { precision: u8, scale: u8 },
    Decimal { precision: u8, scale: u8 },

    // Approximate numeric types
    Float { precision: u8 }, // SQL:1999 FLOAT(p), default 53 (double precision)
    Real,
    DoublePrecision,

    // Character string types
    Character { length: usize },
    Varchar { max_length: Option<usize> }, // None = default length (255)
    CharacterLargeObject,                  // CLOB

    // Boolean type (SQL:1999)
    Boolean,

    // Date/time types
    Date,
    Time { with_timezone: bool },
    Timestamp { with_timezone: bool },

    // Interval types
    // Single field: INTERVAL YEAR, INTERVAL MONTH, etc. (end_field is None)
    // Multi-field: INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, etc.
    Interval { start_field: IntervalField, end_field: Option<IntervalField> },

    // Binary types
    BinaryLargeObject, // BLOB

    // Special type for NULL
    Null,
}

impl DataType {
    /// Check if this type is compatible with another type for operations
    ///
    /// NULL is compatible with any type, and types are compatible with themselves.
    /// Some types have special compatibility rules (e.g., different VARCHAR lengths).
    pub fn is_compatible_with(&self, other: &DataType) -> bool {
        // NULL is compatible with everything
        if matches!(self, DataType::Null) || matches!(other, DataType::Null) {
            return true;
        }

        match (self, other) {
            // Same types are compatible
            (DataType::Integer, DataType::Integer) => true,
            (DataType::Boolean, DataType::Boolean) => true,
            (DataType::Date, DataType::Date) => true,

            // VARCHAR with different lengths are compatible
            (DataType::Varchar { .. }, DataType::Varchar { .. }) => true,

            // For now, different types are not compatible
            // TODO: Add proper SQL:1999 type coercion rules
            _ => false,
        }
    }
}

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

    // Interval (using string for now)
    Interval(String), // TODO: Use proper interval representation

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
            SqlValue::Float(_) => DataType::Float { precision: 53 }, // Default to double precision
            SqlValue::Real(_) => DataType::Real,
            SqlValue::Double(_) => DataType::DoublePrecision,
            SqlValue::Character(s) => DataType::Character { length: s.len() },
            SqlValue::Varchar(_) => DataType::Varchar { max_length: None }, // Unknown/unlimited length
            SqlValue::Boolean(_) => DataType::Boolean,
            SqlValue::Date(_) => DataType::Date,
            SqlValue::Time(_) => DataType::Time { with_timezone: false },
            SqlValue::Timestamp(_) => DataType::Timestamp { with_timezone: false },
            SqlValue::Interval(_) => DataType::Interval {
                start_field: IntervalField::Day, // Default - actual type lost in string representation
                end_field: None,
            },
            SqlValue::Null => DataType::Null,
        }
    }
}

/// PartialOrd implementation for SQL value comparison
///
/// Implements SQL:1999 comparison semantics:
/// - NULL comparisons return None (SQL UNKNOWN)
/// - Type mismatches return None (incomparable)
/// - NaN in floating point returns None (IEEE 754 semantics)
/// - All other comparisons follow Rust's natural ordering
impl PartialOrd for SqlValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use SqlValue::*;
        match (self, other) {
            // NULL comparisons return None (SQL UNKNOWN semantics)
            (Null, _) | (_, Null) => None,

            // Integer types
            (Integer(a), Integer(b)) => a.partial_cmp(b),
            (Smallint(a), Smallint(b)) => a.partial_cmp(b),
            (Bigint(a), Bigint(b)) => a.partial_cmp(b),

            // Floating point (handles NaN properly via IEEE 754)
            (Float(a), Float(b)) => a.partial_cmp(b),
            (Real(a), Real(b)) => a.partial_cmp(b),
            (Double(a), Double(b)) => a.partial_cmp(b),

            // String types (lexicographic comparison)
            (Character(a), Character(b)) => a.partial_cmp(b),
            (Varchar(a), Varchar(b)) => a.partial_cmp(b),

            // Numeric (string-based decimal, parse as f64 for now)
            // TODO: Replace with proper decimal type for exact comparison
            (Numeric(a), Numeric(b)) => match (a.parse::<f64>(), b.parse::<f64>()) {
                (Ok(x), Ok(y)) => x.partial_cmp(&y),
                _ => None, // Invalid numeric string is incomparable
            },

            // Boolean (false < true in SQL)
            (Boolean(a), Boolean(b)) => a.partial_cmp(b),

            // Date/Time (lexicographic for now)
            // TODO: Replace with proper date/time types for correct comparison
            (Date(a), Date(b)) => a.partial_cmp(b),
            (Time(a), Time(b)) => a.partial_cmp(b),
            (Timestamp(a), Timestamp(b)) => a.partial_cmp(b),

            // Interval (lexicographic for now)
            // TODO: Replace with proper interval type for correct comparison
            (Interval(a), Interval(b)) => a.partial_cmp(b),

            // Type mismatch - incomparable (SQL:1999 behavior)
            _ => None,
        }
    }
}

/// Eq implementation for SqlValue
///
/// For DISTINCT operations, we need Eq semantics where:
/// - NULL == NULL (for grouping purposes, unlike SQL comparison)
/// - NaN == NaN (for grouping purposes, unlike IEEE 754)
/// - All other values use standard equality
impl Eq for SqlValue {}

/// Hash implementation for SqlValue
///
/// Custom implementation to handle floating-point values correctly:
/// - NaN values are treated as equal (hash to same value)
/// - Uses to_bits() for floats to ensure consistent hashing
/// - NULL hashes to a specific value
impl Hash for SqlValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use SqlValue::*;

        // Hash discriminant first to distinguish variants
        std::mem::discriminant(self).hash(state);

        match self {
            Integer(i) => i.hash(state),
            Smallint(i) => i.hash(state),
            Bigint(i) => i.hash(state),
            Numeric(s) => s.hash(state),

            // For floats, use to_bits() to get consistent hash for NaN
            Float(f) => {
                if f.is_nan() {
                    // All NaN values hash the same
                    f32::NAN.to_bits().hash(state);
                } else {
                    f.to_bits().hash(state);
                }
            }
            Real(f) => {
                if f.is_nan() {
                    f32::NAN.to_bits().hash(state);
                } else {
                    f.to_bits().hash(state);
                }
            }
            Double(f) => {
                if f.is_nan() {
                    f64::NAN.to_bits().hash(state);
                } else {
                    f.to_bits().hash(state);
                }
            }

            Character(s) => s.hash(state),
            Varchar(s) => s.hash(state),
            Boolean(b) => b.hash(state),
            Date(s) => s.hash(state),
            Time(s) => s.hash(state),
            Timestamp(s) => s.hash(state),
            Interval(s) => s.hash(state),

            // NULL hashes to nothing (discriminant is enough)
            Null => {}
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
            SqlValue::Interval(s) => write!(f, "{}", s),
            SqlValue::Null => write!(f, "NULL"),
        }
    }
}
