//! SQL Data Type definitions

use crate::temporal::IntervalField;

/// SQL:1999 Data Types
///
/// Represents the type of a column or expression in SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    // Exact numeric types
    Integer,
    Smallint,
    Bigint,
    Unsigned, // 64-bit unsigned integer (MySQL compatibility)
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
    Name,                                  /* NAME type for SQL identifiers (SQL:1999), maps to
                                            * VARCHAR(128) */

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

    // User-defined types (SQL:1999)
    UserDefined { type_name: String },

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
            (DataType::Unsigned, DataType::Unsigned) => true,
            (DataType::Boolean, DataType::Boolean) => true,
            (DataType::Date, DataType::Date) => true,

            // VARCHAR with different lengths are compatible
            (DataType::Varchar { .. }, DataType::Varchar { .. }) => true,

            // NAME is compatible with VARCHAR and other NAME types (both are strings)
            (DataType::Name, DataType::Name) => true,
            (DataType::Name, DataType::Varchar { .. }) => true,
            (DataType::Varchar { .. }, DataType::Name) => true,

            // User-defined types are only compatible with the same type name
            (DataType::UserDefined { type_name: t1 }, DataType::UserDefined { type_name: t2 }) => {
                t1 == t2
            }

            // For now, different types are not compatible
            // TODO: Add proper SQL:1999 type coercion rules
            _ => false,
        }
    }
}
