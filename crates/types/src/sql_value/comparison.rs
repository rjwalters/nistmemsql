//! Comparison implementations for SqlValue

use crate::sql_value::SqlValue;
use std::cmp::Ordering;

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
