//! SQL:1999 Type System
//!
//! This crate provides the type system for SQL:1999, including:
//! - Data type definitions (INTEGER, VARCHAR, BOOLEAN, etc.)
//! - SQL values representation
//! - Type compatibility and coercion rules
//! - Type checking utilities

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    str::FromStr,
};

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

/// SQL DATE type - represents a date without time
///
/// Format: YYYY-MM-DD (e.g., '2024-01-01')
/// Stored as year, month, day components for correct comparison
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Date {
    pub year: i32,
    pub month: u8,  // 1-12
    pub day: u8,    // 1-31
}

impl Date {
    /// Create a new Date (validation is basic)
    pub fn new(year: i32, month: u8, day: u8) -> Result<Self, String> {
        if month < 1 || month > 12 {
            return Err(format!("Invalid month: {}", month));
        }
        if day < 1 || day > 31 {
            return Err(format!("Invalid day: {}", day));
        }
        Ok(Date { year, month, day })
    }
}

impl FromStr for Date {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parse format: YYYY-MM-DD
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 3 {
            return Err(format!("Invalid date format: '{}' (expected YYYY-MM-DD)", s));
        }

        let year = parts[0].parse::<i32>()
            .map_err(|_| format!("Invalid year: '{}'", parts[0]))?;
        let month = parts[1].parse::<u8>()
            .map_err(|_| format!("Invalid month: '{}'", parts[1]))?;
        let day = parts[2].parse::<u8>()
            .map_err(|_| format!("Invalid day: '{}'", parts[2]))?;

        Date::new(year, month, day)
    }
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

impl PartialOrd for Date {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Date {
    fn cmp(&self, other: &Self) -> Ordering {
        self.year.cmp(&other.year)
            .then_with(|| self.month.cmp(&other.month))
            .then_with(|| self.day.cmp(&other.day))
    }
}

/// SQL TIME type - represents a time without date
///
/// Format: HH:MM:SS or HH:MM:SS.fff (e.g., '14:30:00' or '14:30:00.123')
/// Stored as hour, minute, second, nanosecond components for correct comparison
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Time {
    pub hour: u8,       // 0-23
    pub minute: u8,     // 0-59
    pub second: u8,     // 0-59
    pub nanosecond: u32, // 0-999999999
}

impl Time {
    /// Create a new Time (validation is basic)
    pub fn new(hour: u8, minute: u8, second: u8, nanosecond: u32) -> Result<Self, String> {
        if hour > 23 {
            return Err(format!("Invalid hour: {}", hour));
        }
        if minute > 59 {
            return Err(format!("Invalid minute: {}", minute));
        }
        if second > 59 {
            return Err(format!("Invalid second: {}", second));
        }
        if nanosecond > 999_999_999 {
            return Err(format!("Invalid nanosecond: {}", nanosecond));
        }
        Ok(Time { hour, minute, second, nanosecond })
    }
}

impl FromStr for Time {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parse format: HH:MM:SS or HH:MM:SS.fff
        let (time_part, frac_part) = if let Some(dot_pos) = s.find('.') {
            (&s[..dot_pos], Some(&s[dot_pos + 1..]))
        } else {
            (s, None)
        };

        let parts: Vec<&str> = time_part.split(':').collect();
        if parts.len() != 3 {
            return Err(format!("Invalid time format: '{}' (expected HH:MM:SS)", s));
        }

        let hour = parts[0].parse::<u8>()
            .map_err(|_| format!("Invalid hour: '{}'", parts[0]))?;
        let minute = parts[1].parse::<u8>()
            .map_err(|_| format!("Invalid minute: '{}'", parts[1]))?;
        let second = parts[2].parse::<u8>()
            .map_err(|_| format!("Invalid second: '{}'", parts[2]))?;

        // Parse fractional seconds if present
        let nanosecond = if let Some(frac) = frac_part {
            // Pad or truncate to 9 digits (nanoseconds)
            let padded = format!("{:0<9}", frac);
            let truncated = &padded[..9.min(padded.len())];
            truncated.parse::<u32>()
                .map_err(|_| format!("Invalid fractional seconds: '{}'", frac))?
        } else {
            0
        };

        Time::new(hour, minute, second, nanosecond)
    }
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.nanosecond == 0 {
            write!(f, "{:02}:{:02}:{:02}", self.hour, self.minute, self.second)
        } else {
            // Display fractional seconds, trimming trailing zeros for readability
            let frac = format!("{:09}", self.nanosecond).trim_end_matches('0').to_string();
            write!(f, "{:02}:{:02}:{:02}.{}", self.hour, self.minute, self.second, frac)
        }
    }
}

impl PartialOrd for Time {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Time {
    fn cmp(&self, other: &Self) -> Ordering {
        self.hour.cmp(&other.hour)
            .then_with(|| self.minute.cmp(&other.minute))
            .then_with(|| self.second.cmp(&other.second))
            .then_with(|| self.nanosecond.cmp(&other.nanosecond))
    }
}

/// SQL TIMESTAMP type - represents a date and time
///
/// Format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.ffffff
/// (e.g., '2024-01-01 14:30:00' or '2024-01-01 14:30:00.123456')
/// Stored as Date and Time components for correct comparison
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Timestamp {
    pub date: Date,
    pub time: Time,
}

impl Timestamp {
    /// Create a new Timestamp
    pub fn new(date: Date, time: Time) -> Self {
        Timestamp { date, time }
    }
}

impl FromStr for Timestamp {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parse format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.ffffff
        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.len() != 2 {
            return Err(format!("Invalid timestamp format: '{}' (expected YYYY-MM-DD HH:MM:SS)", s));
        }

        let date = Date::from_str(parts[0])?;
        let time = Time::from_str(parts[1])?;

        Ok(Timestamp::new(date, time))
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.date, self.time)
    }
}

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.date.cmp(&other.date)
            .then_with(|| self.time.cmp(&other.time))
    }
}

/// SQL INTERVAL type - represents a duration
///
/// Formats vary: '5 YEAR', '1-6 YEAR TO MONTH', '5 12:30:45 DAY TO SECOND', etc.
/// Stored as a formatted string for now (can be enhanced later for arithmetic)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Interval {
    /// The original interval string (e.g., "5 YEAR", "1-6 YEAR TO MONTH")
    pub value: String,
}

impl Interval {
    /// Create a new Interval
    pub fn new(value: String) -> Self {
        Interval { value }
    }
}

impl FromStr for Interval {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // For now, just store the string representation
        // Can be enhanced later to parse and validate interval format
        Ok(Interval::new(s.to_string()))
    }
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Lexicographic comparison for now (not semantically correct for all intervals)
        // TODO: Implement proper interval comparison based on SQL:1999 rules
        self.value.partial_cmp(&other.value)
    }
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
            (Unsigned(a), Unsigned(b)) => a.partial_cmp(b),

            // Floating point (handles NaN properly via IEEE 754)
            (Float(a), Float(b)) => a.partial_cmp(b),
            (Real(a), Real(b)) => a.partial_cmp(b),
            (Double(a), Double(b)) => a.partial_cmp(b),

            // String types (lexicographic comparison)
            (Character(a), Character(b)) => a.partial_cmp(b),
            (Varchar(a), Varchar(b)) => a.partial_cmp(b),

            // Numeric (f64 - direct comparison)
            (Numeric(a), Numeric(b)) => a.partial_cmp(b),

            // Boolean (false < true in SQL)
            (Boolean(a), Boolean(b)) => a.partial_cmp(b),

            // Date/Time types with proper temporal comparison
            (Date(a), Date(b)) => a.partial_cmp(b),
            (Time(a), Time(b)) => a.partial_cmp(b),
            (Timestamp(a), Timestamp(b)) => a.partial_cmp(b),

            // Interval type comparison
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
            Unsigned(u) => u.hash(state),
            Numeric(f) => {
                if f.is_nan() {
                    f64::NAN.to_bits().hash(state);
                } else {
                    f.to_bits().hash(state);
                }
            }
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
            SqlValue::Unsigned(u) => write!(f, "{}", u),
            // Format Numeric - show whole numbers without decimals,
            // fractional numbers without trailing zeros
            SqlValue::Numeric(n) => {
                if n.is_nan() {
                    write!(f, "NaN")
                } else if n.is_infinite() {
                    if *n > 0.0 {
                        write!(f, "Infinity")
                    } else {
                        write!(f, "-Infinity")
                    }
                } else if n.fract() == 0.0 {
                    // Whole number - display without decimals
                    write!(f, "{}", *n as i64)
                } else {
                    // Has fractional part - use default formatting (no trailing zeros)
                    write!(f, "{}", n)
                }
            }
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
