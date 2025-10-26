//! SQL:1999 Type System
//!
//! This crate provides the type system for SQL:1999, including:
//! - Data type definitions (INTEGER, VARCHAR, BOOLEAN, etc.)
//! - SQL values representation
//! - Type compatibility and coercion rules
//! - Type checking utilities

use std::fmt;

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
    Float,
    Real,
    DoublePrecision,

    // Character string types
    Character { length: usize },
    Varchar { max_length: usize },
    CharacterLargeObject, // CLOB

    // Boolean type (SQL:1999)
    Boolean,

    // Date/time types
    Date,
    Time { with_timezone: bool },
    Timestamp { with_timezone: bool },

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

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // DataType Tests - Define what we want our type system to do
    // ============================================================================

    #[test]
    fn test_integer_type_creation() {
        let int_type = DataType::Integer;
        assert_eq!(format!("{:?}", int_type), "Integer");
    }

    #[test]
    fn test_varchar_type_with_length() {
        let varchar_type = DataType::Varchar { max_length: 255 };
        assert_eq!(format!("{:?}", varchar_type), "Varchar { max_length: 255 }");
    }

    #[test]
    fn test_boolean_type_creation() {
        let bool_type = DataType::Boolean;
        assert_eq!(format!("{:?}", bool_type), "Boolean");
    }

    #[test]
    fn test_numeric_type_with_precision_scale() {
        let numeric_type = DataType::Numeric { precision: 10, scale: 2 };
        assert_eq!(format!("{:?}", numeric_type), "Numeric { precision: 10, scale: 2 }");
    }

    #[test]
    fn test_date_type_creation() {
        let date_type = DataType::Date;
        assert_eq!(format!("{:?}", date_type), "Date");
    }

    // ============================================================================
    // SqlValue Tests - Test SQL value representation
    // ============================================================================

    #[test]
    fn test_integer_value_creation() {
        let value = SqlValue::Integer(42);
        assert_eq!(format!("{:?}", value), "Integer(42)");
    }

    #[test]
    fn test_varchar_value_creation() {
        let value = SqlValue::Varchar("hello".to_string());
        assert_eq!(format!("{:?}", value), "Varchar(\"hello\")");
    }

    #[test]
    fn test_boolean_true_value() {
        let value = SqlValue::Boolean(true);
        assert_eq!(format!("{:?}", value), "Boolean(true)");
    }

    #[test]
    fn test_boolean_false_value() {
        let value = SqlValue::Boolean(false);
        assert_eq!(format!("{:?}", value), "Boolean(false)");
    }

    #[test]
    fn test_null_value() {
        let value = SqlValue::Null;
        assert_eq!(format!("{:?}", value), "Null");
    }

    // ============================================================================
    // SqlValue::is_null() Tests - Check if value is NULL
    // ============================================================================

    #[test]
    fn test_null_is_null() {
        let value = SqlValue::Null;
        assert!(value.is_null());
    }

    #[test]
    fn test_integer_is_not_null() {
        let value = SqlValue::Integer(42);
        assert!(!value.is_null());
    }

    #[test]
    fn test_varchar_is_not_null() {
        let value = SqlValue::Varchar("test".to_string());
        assert!(!value.is_null());
    }

    // ============================================================================
    // SqlValue::get_type() Tests - Get the type of a value
    // ============================================================================

    #[test]
    fn test_integer_value_has_integer_type() {
        let value = SqlValue::Integer(42);
        assert_eq!(value.get_type(), DataType::Integer);
    }

    #[test]
    fn test_varchar_value_has_varchar_type() {
        let value = SqlValue::Varchar("test".to_string());
        // Note: VARCHAR values don't have a specific max_length in the value itself
        // We'll handle this properly when we implement it
        match value.get_type() {
            DataType::Varchar { .. } => {} // Success
            _ => panic!("Expected Varchar type"),
        }
    }

    #[test]
    fn test_boolean_value_has_boolean_type() {
        let value = SqlValue::Boolean(true);
        assert_eq!(value.get_type(), DataType::Boolean);
    }

    #[test]
    fn test_null_value_has_null_type() {
        let value = SqlValue::Null;
        assert_eq!(value.get_type(), DataType::Null);
    }

    // ============================================================================
    // Type Compatibility Tests - Can we assign/compare values?
    // ============================================================================

    #[test]
    fn test_integer_compatible_with_integer() {
        assert!(DataType::Integer.is_compatible_with(&DataType::Integer));
    }

    #[test]
    fn test_integer_not_compatible_with_varchar() {
        assert!(!DataType::Integer.is_compatible_with(&DataType::Varchar { max_length: 10 }));
    }

    #[test]
    fn test_null_compatible_with_any_type() {
        assert!(DataType::Null.is_compatible_with(&DataType::Integer));
        assert!(DataType::Null.is_compatible_with(&DataType::Boolean));
        assert!(DataType::Null.is_compatible_with(&DataType::Varchar { max_length: 10 }));
    }

    #[test]
    fn test_any_type_compatible_with_null() {
        assert!(DataType::Integer.is_compatible_with(&DataType::Null));
        assert!(DataType::Boolean.is_compatible_with(&DataType::Null));
        assert!(DataType::Varchar { max_length: 10 }.is_compatible_with(&DataType::Null));
    }

    #[test]
    fn test_varchar_different_lengths_compatible() {
        // VARCHAR(10) and VARCHAR(20) should be compatible for comparison
        let v1 = DataType::Varchar { max_length: 10 };
        let v2 = DataType::Varchar { max_length: 20 };
        assert!(v1.is_compatible_with(&v2));
    }

    // ============================================================================
    // Display/Format Tests - How types are displayed
    // ============================================================================

    #[test]
    fn test_integer_display() {
        let value = SqlValue::Integer(42);
        assert_eq!(format!("{}", value), "42");
    }

    #[test]
    fn test_varchar_display() {
        let value = SqlValue::Varchar("hello".to_string());
        assert_eq!(format!("{}", value), "hello");
    }

    #[test]
    fn test_boolean_true_display() {
        let value = SqlValue::Boolean(true);
        assert_eq!(format!("{}", value), "TRUE");
    }

    #[test]
    fn test_boolean_false_display() {
        let value = SqlValue::Boolean(false);
        assert_eq!(format!("{}", value), "FALSE");
    }

    #[test]
    fn test_null_display() {
        let value = SqlValue::Null;
        assert_eq!(format!("{}", value), "NULL");
    }
}
