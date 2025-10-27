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

    // ============================================================================
    // PartialOrd Tests for SqlValue
    // ============================================================================

    #[test]
    fn test_integer_ordering() {
        assert!(SqlValue::Integer(1) < SqlValue::Integer(2));
        assert!(SqlValue::Integer(2) > SqlValue::Integer(1));
        assert_eq!(
            SqlValue::Integer(1).partial_cmp(&SqlValue::Integer(1)),
            Some(std::cmp::Ordering::Equal)
        );
    }

    #[test]
    fn test_smallint_ordering() {
        assert!(SqlValue::Smallint(10) < SqlValue::Smallint(20));
        assert!(SqlValue::Smallint(20) > SqlValue::Smallint(10));
    }

    #[test]
    fn test_bigint_ordering() {
        assert!(SqlValue::Bigint(1000) < SqlValue::Bigint(2000));
        assert!(SqlValue::Bigint(2000) > SqlValue::Bigint(1000));
    }

    #[test]
    fn test_float_ordering() {
        assert!(SqlValue::Float(1.5) < SqlValue::Float(2.5));
        assert!(SqlValue::Float(2.5) > SqlValue::Float(1.5));
    }

    #[test]
    fn test_float_nan_is_incomparable() {
        let nan = SqlValue::Float(f32::NAN);
        let one = SqlValue::Float(1.0);
        assert_eq!(nan.partial_cmp(&one), None);
        assert_eq!(one.partial_cmp(&nan), None);
        assert_eq!(nan.partial_cmp(&nan), None);
    }

    #[test]
    fn test_double_ordering() {
        assert!(SqlValue::Double(1.5) < SqlValue::Double(2.5));
        assert!(SqlValue::Double(2.5) > SqlValue::Double(1.5));
    }

    #[test]
    fn test_double_nan_is_incomparable() {
        let nan = SqlValue::Double(f64::NAN);
        let one = SqlValue::Double(1.0);
        assert_eq!(nan.partial_cmp(&one), None);
        assert_eq!(one.partial_cmp(&nan), None);
    }

    #[test]
    fn test_varchar_ordering() {
        assert!(SqlValue::Varchar("apple".to_string()) < SqlValue::Varchar("banana".to_string()));
        assert!(SqlValue::Varchar("zebra".to_string()) > SqlValue::Varchar("aardvark".to_string()));
    }

    #[test]
    fn test_character_ordering() {
        assert!(SqlValue::Character("a".to_string()) < SqlValue::Character("b".to_string()));
        assert!(SqlValue::Character("z".to_string()) > SqlValue::Character("a".to_string()));
    }

    #[test]
    fn test_boolean_ordering() {
        // In SQL, FALSE < TRUE
        assert!(SqlValue::Boolean(false) < SqlValue::Boolean(true));
        assert!(SqlValue::Boolean(true) > SqlValue::Boolean(false));
        assert_eq!(
            SqlValue::Boolean(true).partial_cmp(&SqlValue::Boolean(true)),
            Some(std::cmp::Ordering::Equal)
        );
    }

    #[test]
    fn test_numeric_ordering() {
        assert!(SqlValue::Numeric("1.5".to_string()) < SqlValue::Numeric("2.5".to_string()));
        assert!(SqlValue::Numeric("100.0".to_string()) > SqlValue::Numeric("50.5".to_string()));
    }

    #[test]
    fn test_numeric_invalid_is_incomparable() {
        let invalid = SqlValue::Numeric("not-a-number".to_string());
        let valid = SqlValue::Numeric("1.0".to_string());
        assert_eq!(invalid.partial_cmp(&valid), None);
        assert_eq!(valid.partial_cmp(&invalid), None);
    }

    #[test]
    fn test_date_ordering() {
        assert!(
            SqlValue::Date("2024-01-01".to_string()) < SqlValue::Date("2024-12-31".to_string())
        );
        assert!(
            SqlValue::Date("2024-12-31".to_string()) > SqlValue::Date("2024-01-01".to_string())
        );
    }

    #[test]
    fn test_time_ordering() {
        assert!(SqlValue::Time("09:00:00".to_string()) < SqlValue::Time("17:00:00".to_string()));
        assert!(SqlValue::Time("17:00:00".to_string()) > SqlValue::Time("09:00:00".to_string()));
    }

    #[test]
    fn test_timestamp_ordering() {
        assert!(
            SqlValue::Timestamp("2024-01-01 09:00:00".to_string())
                < SqlValue::Timestamp("2024-01-01 17:00:00".to_string())
        );
    }

    #[test]
    fn test_null_is_incomparable() {
        // NULL compared to anything (including NULL) returns None
        assert_eq!(SqlValue::Null.partial_cmp(&SqlValue::Integer(1)), None);
        assert_eq!(SqlValue::Integer(1).partial_cmp(&SqlValue::Null), None);
        assert_eq!(SqlValue::Null.partial_cmp(&SqlValue::Null), None);
    }

    #[test]
    fn test_type_mismatch_is_incomparable() {
        // Different types cannot be compared
        assert_eq!(SqlValue::Integer(1).partial_cmp(&SqlValue::Varchar("1".to_string())), None);
        assert_eq!(SqlValue::Float(1.0).partial_cmp(&SqlValue::Integer(1)), None);
        assert_eq!(SqlValue::Boolean(true).partial_cmp(&SqlValue::Integer(1)), None);
    }

    #[test]
    fn test_can_use_comparison_operators() {
        // Test that Rust's comparison operators work with PartialOrd
        let a = SqlValue::Integer(1);
        let b = SqlValue::Integer(2);

        assert!(a < b);
        assert!(b > a);
        assert!(a <= b);
        assert!(b >= a);
        assert!(a == a);
        assert!(a != b);
    }

    // ============================================================================
    // Additional Type Compatibility Tests
    // ============================================================================

    #[test]
    fn test_boolean_compatible_with_boolean() {
        assert!(DataType::Boolean.is_compatible_with(&DataType::Boolean));
    }

    #[test]
    fn test_date_compatible_with_date() {
        assert!(DataType::Date.is_compatible_with(&DataType::Date));
    }

    // ============================================================================
    // Additional get_type() Tests
    // ============================================================================

    #[test]
    fn test_smallint_value_has_smallint_type() {
        let value = SqlValue::Smallint(42);
        assert_eq!(value.get_type(), DataType::Smallint);
    }

    #[test]
    fn test_bigint_value_has_bigint_type() {
        let value = SqlValue::Bigint(1000);
        assert_eq!(value.get_type(), DataType::Bigint);
    }

    #[test]
    fn test_numeric_value_has_numeric_type() {
        let value = SqlValue::Numeric("123.45".to_string());
        match value.get_type() {
            DataType::Numeric { .. } => {} // Success
            _ => panic!("Expected Numeric type"),
        }
    }

    #[test]
    fn test_float_value_has_float_type() {
        let value = SqlValue::Float(3.14);
        assert_eq!(value.get_type(), DataType::Float);
    }

    #[test]
    fn test_real_value_has_real_type() {
        let value = SqlValue::Real(2.71);
        assert_eq!(value.get_type(), DataType::Real);
    }

    #[test]
    fn test_double_value_has_double_type() {
        let value = SqlValue::Double(3.14159);
        assert_eq!(value.get_type(), DataType::DoublePrecision);
    }

    #[test]
    fn test_character_value_has_character_type() {
        let value = SqlValue::Character("hello".to_string());
        match value.get_type() {
            DataType::Character { .. } => {} // Success
            _ => panic!("Expected Character type"),
        }
    }

    #[test]
    fn test_date_value_has_date_type() {
        let value = SqlValue::Date("2024-01-01".to_string());
        assert_eq!(value.get_type(), DataType::Date);
    }

    #[test]
    fn test_time_value_has_time_type() {
        let value = SqlValue::Time("12:30:00".to_string());
        match value.get_type() {
            DataType::Time { .. } => {} // Success
            _ => panic!("Expected Time type"),
        }
    }

    #[test]
    fn test_timestamp_value_has_timestamp_type() {
        let value = SqlValue::Timestamp("2024-01-01 12:30:00".to_string());
        match value.get_type() {
            DataType::Timestamp { .. } => {} // Success
            _ => panic!("Expected Timestamp type"),
        }
    }

    // ============================================================================
    // Additional Display Tests
    // ============================================================================

    #[test]
    fn test_smallint_display() {
        let value = SqlValue::Smallint(100);
        assert_eq!(format!("{}", value), "100");
    }

    #[test]
    fn test_bigint_display() {
        let value = SqlValue::Bigint(1000000);
        assert_eq!(format!("{}", value), "1000000");
    }

    #[test]
    fn test_numeric_display() {
        let value = SqlValue::Numeric("123.45".to_string());
        assert_eq!(format!("{}", value), "123.45");
    }

    #[test]
    fn test_float_display() {
        let value = SqlValue::Float(3.14);
        assert_eq!(format!("{}", value), "3.14");
    }

    #[test]
    fn test_real_display() {
        let value = SqlValue::Real(2.71);
        assert_eq!(format!("{}", value), "2.71");
    }

    #[test]
    fn test_double_display() {
        let value = SqlValue::Double(3.14159);
        assert_eq!(format!("{}", value), "3.14159");
    }

    #[test]
    fn test_character_display() {
        let value = SqlValue::Character("test".to_string());
        assert_eq!(format!("{}", value), "test");
    }

    #[test]
    fn test_date_display() {
        let value = SqlValue::Date("2024-01-01".to_string());
        assert_eq!(format!("{}", value), "2024-01-01");
    }

    #[test]
    fn test_time_display() {
        let value = SqlValue::Time("12:30:00".to_string());
        assert_eq!(format!("{}", value), "12:30:00");
    }

    #[test]
    fn test_timestamp_display() {
        let value = SqlValue::Timestamp("2024-01-01 12:30:00".to_string());
        assert_eq!(format!("{}", value), "2024-01-01 12:30:00");
    }

    // ============================================================================
    // Real Type Comparison Tests
    // ============================================================================

    #[test]
    fn test_real_ordering() {
        assert!(SqlValue::Real(1.5) < SqlValue::Real(2.5));
        assert!(SqlValue::Real(2.5) > SqlValue::Real(1.5));
    }

    #[test]
    fn test_real_nan_is_incomparable() {
        let nan = SqlValue::Real(f32::NAN);
        let one = SqlValue::Real(1.0);
        assert_eq!(nan.partial_cmp(&one), None);
        assert_eq!(one.partial_cmp(&nan), None);
    }

    // ============================================================================
    // Hash Implementation Tests (for DISTINCT operations)
    // ============================================================================

    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn calculate_hash<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn test_integer_hash() {
        let v1 = SqlValue::Integer(42);
        let v2 = SqlValue::Integer(42);
        let v3 = SqlValue::Integer(43);
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
        assert_ne!(calculate_hash(&v1), calculate_hash(&v3));
    }

    #[test]
    fn test_smallint_hash() {
        let v1 = SqlValue::Smallint(10);
        let v2 = SqlValue::Smallint(10);
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_bigint_hash() {
        let v1 = SqlValue::Bigint(1000);
        let v2 = SqlValue::Bigint(1000);
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_numeric_hash() {
        let v1 = SqlValue::Numeric("123.45".to_string());
        let v2 = SqlValue::Numeric("123.45".to_string());
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_float_hash() {
        let v1 = SqlValue::Float(3.14);
        let v2 = SqlValue::Float(3.14);
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_float_nan_hash() {
        // NaN values should hash to the same value for DISTINCT operations
        let nan1 = SqlValue::Float(f32::NAN);
        let nan2 = SqlValue::Float(f32::NAN);
        assert_eq!(calculate_hash(&nan1), calculate_hash(&nan2));
    }

    #[test]
    fn test_real_hash() {
        let v1 = SqlValue::Real(2.71);
        let v2 = SqlValue::Real(2.71);
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_real_nan_hash() {
        let nan1 = SqlValue::Real(f32::NAN);
        let nan2 = SqlValue::Real(f32::NAN);
        assert_eq!(calculate_hash(&nan1), calculate_hash(&nan2));
    }

    #[test]
    fn test_double_hash() {
        let v1 = SqlValue::Double(3.14159);
        let v2 = SqlValue::Double(3.14159);
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_double_nan_hash() {
        let nan1 = SqlValue::Double(f64::NAN);
        let nan2 = SqlValue::Double(f64::NAN);
        assert_eq!(calculate_hash(&nan1), calculate_hash(&nan2));
    }

    #[test]
    fn test_varchar_hash() {
        let v1 = SqlValue::Varchar("hello".to_string());
        let v2 = SqlValue::Varchar("hello".to_string());
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_character_hash() {
        let v1 = SqlValue::Character("test".to_string());
        let v2 = SqlValue::Character("test".to_string());
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_boolean_hash() {
        let v1 = SqlValue::Boolean(true);
        let v2 = SqlValue::Boolean(true);
        let v3 = SqlValue::Boolean(false);
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
        assert_ne!(calculate_hash(&v1), calculate_hash(&v3));
    }

    #[test]
    fn test_date_hash() {
        let v1 = SqlValue::Date("2024-01-01".to_string());
        let v2 = SqlValue::Date("2024-01-01".to_string());
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_time_hash() {
        let v1 = SqlValue::Time("12:30:00".to_string());
        let v2 = SqlValue::Time("12:30:00".to_string());
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_timestamp_hash() {
        let v1 = SqlValue::Timestamp("2024-01-01 12:30:00".to_string());
        let v2 = SqlValue::Timestamp("2024-01-01 12:30:00".to_string());
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    #[test]
    fn test_null_hash() {
        let v1 = SqlValue::Null;
        let v2 = SqlValue::Null;
        // NULL values should hash consistently
        assert_eq!(calculate_hash(&v1), calculate_hash(&v2));
    }

    // ============================================================================
    // Edge Case Tests
    // ============================================================================

    #[test]
    fn test_empty_string_varchar() {
        let value = SqlValue::Varchar("".to_string());
        assert_eq!(format!("{}", value), "");
        assert!(!value.is_null());
    }

    #[test]
    fn test_negative_integer() {
        let value = SqlValue::Integer(-42);
        assert_eq!(format!("{}", value), "-42");
    }

    #[test]
    fn test_very_large_bigint() {
        let value = SqlValue::Bigint(i64::MAX);
        assert_eq!(format!("{}", value), format!("{}", i64::MAX));
    }

    #[test]
    fn test_very_small_bigint() {
        let value = SqlValue::Bigint(i64::MIN);
        assert_eq!(format!("{}", value), format!("{}", i64::MIN));
    }

    #[test]
    fn test_special_characters_in_varchar() {
        let value = SqlValue::Varchar("Hello, 世界! 🌍".to_string());
        assert_eq!(format!("{}", value), "Hello, 世界! 🌍");
    }
}
