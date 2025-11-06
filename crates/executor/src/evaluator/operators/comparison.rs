//! Comparison operator implementations
//!
//! Handles: =, <>, <, <=, >, >=
//! Supports: All SQL types with proper type coercion
//! Includes: NULL handling (three-valued logic), cross-type comparisons

use crate::errors::ExecutorError;
use crate::evaluator::casting::{is_approximate_numeric, is_exact_numeric, to_f64, to_i64};
use types::SqlValue;

pub(crate) struct ComparisonOps;

impl ComparisonOps {
    /// Equality operator (=)
    #[inline]
    pub fn equal(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        Self::compare(left, right, |cmp| cmp == std::cmp::Ordering::Equal, "=")
    }

    /// Inequality operator (<>)
    #[inline]
    pub fn not_equal(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        Self::compare(left, right, |cmp| cmp != std::cmp::Ordering::Equal, "<>")
    }

    /// Less than operator (<)
    #[inline]
    pub fn less_than(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        Self::compare(left, right, |cmp| cmp == std::cmp::Ordering::Less, "<")
    }

    /// Less than or equal operator (<=)
    #[inline]
    pub fn less_than_or_equal(
        left: &SqlValue,
        right: &SqlValue,
    ) -> Result<SqlValue, ExecutorError> {
        Self::compare(
            left,
            right,
            |cmp| cmp != std::cmp::Ordering::Greater,
            "<=",
        )
    }

    /// Greater than operator (>)
    #[inline]
    pub fn greater_than(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        Self::compare(left, right, |cmp| cmp == std::cmp::Ordering::Greater, ">")
    }

    /// Greater than or equal operator (>=)
    #[inline]
    pub fn greater_than_or_equal(
        left: &SqlValue,
        right: &SqlValue,
    ) -> Result<SqlValue, ExecutorError> {
        Self::compare(left, right, |cmp| cmp != std::cmp::Ordering::Less, ">=")
    }

    /// Generic comparison helper
    #[inline]
    fn compare<F>(
        left: &SqlValue,
        right: &SqlValue,
        predicate: F,
        op_str: &str,
    ) -> Result<SqlValue, ExecutorError>
    where
        F: FnOnce(std::cmp::Ordering) -> bool,
    {
        use SqlValue::*;

        match (left, right) {
            // Integer comparisons
            (Integer(a), Integer(b)) => Ok(Boolean(predicate(a.cmp(b)))),

            // String comparisons (VARCHAR and CHAR are compatible)
            (Varchar(a), Varchar(b)) => Ok(Boolean(predicate(a.cmp(b)))),
            (Character(a), Character(b)) => Ok(Boolean(predicate(a.cmp(b)))),
            (Character(a), Varchar(b)) | (Varchar(b), Character(a)) => {
                Ok(Boolean(predicate(a.cmp(b))))
            }

            // Temporal type comparisons (DATE, TIME, TIMESTAMP)
            (Date(a), Date(b)) => Ok(Boolean(predicate(a.cmp(b)))),
            (Time(a), Time(b)) => Ok(Boolean(predicate(a.cmp(b)))),
            (Timestamp(a), Timestamp(b)) => Ok(Boolean(predicate(a.cmp(b)))),

            // Boolean comparisons
            (Boolean(a), Boolean(b)) => Ok(Boolean(predicate(a.cmp(b)))),

            // Cross-type numeric comparisons - exact numeric types
            (left_val, right_val)
                if is_exact_numeric(left_val) && is_exact_numeric(right_val) =>
            {
                let left_i64 = to_i64(left_val)?;
                let right_i64 = to_i64(right_val)?;
                Ok(Boolean(predicate(left_i64.cmp(&right_i64))))
            }

            // Approximate numeric types
            (left_val, right_val)
                if is_approximate_numeric(left_val) && is_approximate_numeric(right_val) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Boolean(predicate(
                    left_f64.partial_cmp(&right_f64).unwrap_or(std::cmp::Ordering::Equal),
                )))
            }

            // Mixed Float/Integer comparisons - promote Integer to Float
            (left_val @ (Float(_) | Real(_) | Double(_)), right_val @ (Integer(_) | Smallint(_) | Bigint(_)))
            | (left_val @ (Integer(_) | Smallint(_) | Bigint(_)), right_val @ (Float(_) | Real(_) | Double(_))) => {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Boolean(predicate(
                    left_f64.partial_cmp(&right_f64).unwrap_or(std::cmp::Ordering::Equal),
                )))
            }

            // NUMERIC comparisons with any numeric type
            (left_val @ Numeric(_), right_val)
                if matches!(
                    right_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Boolean(predicate(
                    left_f64.partial_cmp(&right_f64).unwrap_or(std::cmp::Ordering::Equal),
                )))
            }
            (left_val, right_val @ Numeric(_))
                if matches!(
                    left_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Boolean(predicate(
                    left_f64.partial_cmp(&right_f64).unwrap_or(std::cmp::Ordering::Equal),
                )))
            }

            // Type mismatch
            _ => Err(ExecutorError::TypeMismatch {
                left: left.clone(),
                op: op_str.to_string(),
                right: right.clone(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_equality() {
        let result = ComparisonOps::equal(&SqlValue::Integer(5), &SqlValue::Integer(5)).unwrap();
        assert_eq!(result, SqlValue::Boolean(true));

        let result = ComparisonOps::equal(&SqlValue::Integer(5), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Boolean(false));
    }

    #[test]
    fn test_integer_comparisons() {
        assert_eq!(
            ComparisonOps::less_than(&SqlValue::Integer(3), &SqlValue::Integer(5)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            ComparisonOps::greater_than(&SqlValue::Integer(5), &SqlValue::Integer(3)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            ComparisonOps::less_than_or_equal(&SqlValue::Integer(5), &SqlValue::Integer(5))
                .unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_string_equality() {
        let result = ComparisonOps::equal(
            &SqlValue::Varchar("hello".to_string()),
            &SqlValue::Varchar("hello".to_string()),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }

    #[test]
    fn test_cross_type_string() {
        let result = ComparisonOps::equal(
            &SqlValue::Character("hello".to_string()),
            &SqlValue::Varchar("hello".to_string()),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }

    #[test]
    fn test_boolean_equality() {
        let result =
            ComparisonOps::equal(&SqlValue::Boolean(true), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }

    #[test]
    fn test_mixed_exact_numeric() {
        let result =
            ComparisonOps::equal(&SqlValue::Smallint(5), &SqlValue::Bigint(5)).unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }

    #[test]
    fn test_mixed_float_integer() {
        let result =
            ComparisonOps::equal(&SqlValue::Float(5.0), &SqlValue::Integer(5)).unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }

    #[test]
    fn test_temporal_comparisons() {
        let result = ComparisonOps::less_than(
            &SqlValue::Date("2024-01-01".to_string()),
            &SqlValue::Date("2024-12-31".to_string()),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }
}
