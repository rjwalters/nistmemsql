//! Comparison operator implementations
//!
//! Handles: =, <>, <, <=, >, >=
//! Supports: All SQL types with proper type coercion
//! Includes: NULL handling (three-valued logic), cross-type comparisons

use vibesql_types::SqlValue;

use crate::{
    errors::ExecutorError,
    evaluator::casting::{boolean_to_i64, is_approximate_numeric, is_exact_numeric, to_f64, to_i64},
};

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
        Self::compare(left, right, |cmp| cmp != std::cmp::Ordering::Greater, "<=")
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

        // Boolean coercion for comparisons
        // If either operand is Boolean and the other is numeric, coerce boolean to i64
        match (left, right) {
            // Boolean compared to any numeric type
            (Boolean(_), right_val)
                if is_exact_numeric(right_val)
                    || is_approximate_numeric(right_val)
                    || matches!(right_val, Numeric(_)) =>
            {
                let left_i64 = boolean_to_i64(left).unwrap(); // Safe: we know left is Boolean

                // For exact numeric, compare as i64
                if is_exact_numeric(right_val) {
                    let right_i64 = to_i64(right_val)?;
                    return Ok(Boolean(predicate(left_i64.cmp(&right_i64))));
                }

                // For approximate numeric or Numeric, compare as f64
                let left_f64 = left_i64 as f64;
                let right_f64 = to_f64(right_val)?;
                return Ok(Boolean(predicate(
                    left_f64
                        .partial_cmp(&right_f64)
                        .unwrap_or(std::cmp::Ordering::Equal),
                )));
            }

            // Numeric compared to Boolean (symmetric case)
            (left_val, Boolean(_))
                if is_exact_numeric(left_val)
                    || is_approximate_numeric(left_val)
                    || matches!(left_val, Numeric(_)) =>
            {
                let right_i64 = boolean_to_i64(right).unwrap(); // Safe: we know right is Boolean

                // For exact numeric, compare as i64
                if is_exact_numeric(left_val) {
                    let left_i64 = to_i64(left_val)?;
                    return Ok(Boolean(predicate(left_i64.cmp(&right_i64))));
                }

                // For approximate numeric or Numeric, compare as f64
                let left_f64 = to_f64(left_val)?;
                let right_f64 = right_i64 as f64;
                return Ok(Boolean(predicate(
                    left_f64
                        .partial_cmp(&right_f64)
                        .unwrap_or(std::cmp::Ordering::Equal),
                )));
            }

            _ => {} // Fall through to existing comparison logic
        }

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
            (left_val, right_val) if is_exact_numeric(left_val) && is_exact_numeric(right_val) => {
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
            (
                left_val @ (Float(_) | Real(_) | Double(_)),
                right_val @ (Integer(_) | Smallint(_) | Bigint(_)),
            )
            | (
                left_val @ (Integer(_) | Smallint(_) | Bigint(_)),
                right_val @ (Float(_) | Real(_) | Double(_)),
            ) => {
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
                    Integer(_)
                        | Smallint(_)
                        | Bigint(_)
                        | Float(_)
                        | Real(_)
                        | Double(_)
                        | Numeric(_)
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
                    Integer(_)
                        | Smallint(_)
                        | Bigint(_)
                        | Float(_)
                        | Real(_)
                        | Double(_)
                        | Numeric(_)
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
        let result = ComparisonOps::equal(&SqlValue::Smallint(5), &SqlValue::Bigint(5)).unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }

    #[test]
    fn test_mixed_float_integer() {
        let result = ComparisonOps::equal(&SqlValue::Float(5.0), &SqlValue::Integer(5)).unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }

    #[test]
    fn test_temporal_comparisons() {
        let result = ComparisonOps::less_than(
            &SqlValue::Date("2024-01-01".parse().unwrap()),
            &SqlValue::Date("2024-12-31".parse().unwrap()),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }

    #[test]
    fn test_integer_vs_numeric() {
        let result = ComparisonOps::greater_than(
            &SqlValue::Integer(200),
            &SqlValue::Numeric(174.36666666666667),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Boolean(true));

        let result = ComparisonOps::greater_than(
            &SqlValue::Integer(150),
            &SqlValue::Numeric(174.36666666666667),
        )
        .unwrap();
        assert_eq!(result, SqlValue::Boolean(false));
    }

    // Boolean-to-Integer Comparison Tests
    #[test]
    fn test_boolean_equals_integer() {
        // TRUE (1) = 1 → true
        assert_eq!(
            ComparisonOps::equal(&SqlValue::Boolean(true), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(true)
        );
        // FALSE (0) = 0 → true
        assert_eq!(
            ComparisonOps::equal(&SqlValue::Boolean(false), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(true)
        );
        // TRUE (1) = 0 → false
        assert_eq!(
            ComparisonOps::equal(&SqlValue::Boolean(true), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(false)
        );
        // FALSE (0) = 1 → false
        assert_eq!(
            ComparisonOps::equal(&SqlValue::Boolean(false), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_boolean_less_than_integer() {
        // FALSE (0) < 5 = true
        assert_eq!(
            ComparisonOps::less_than(&SqlValue::Boolean(false), &SqlValue::Integer(5)).unwrap(),
            SqlValue::Boolean(true)
        );
        // TRUE (1) < 5 = true
        assert_eq!(
            ComparisonOps::less_than(&SqlValue::Boolean(true), &SqlValue::Integer(5)).unwrap(),
            SqlValue::Boolean(true)
        );
        // TRUE (1) < 1 = false
        assert_eq!(
            ComparisonOps::less_than(&SqlValue::Boolean(true), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(false)
        );
        // TRUE (1) < 0 = false
        assert_eq!(
            ComparisonOps::less_than(&SqlValue::Boolean(true), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_integer_greater_than_boolean() {
        // 5 > TRUE (1) = true
        assert_eq!(
            ComparisonOps::greater_than(&SqlValue::Integer(5), &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(true)
        );
        // 0 > FALSE (0) = false
        assert_eq!(
            ComparisonOps::greater_than(&SqlValue::Integer(0), &SqlValue::Boolean(false))
                .unwrap(),
            SqlValue::Boolean(false)
        );
        // 1 > TRUE (1) = false
        assert_eq!(
            ComparisonOps::greater_than(&SqlValue::Integer(1), &SqlValue::Boolean(true)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_boolean_comparison_with_float() {
        // TRUE (1.0) = 1.0 (as float)
        assert_eq!(
            ComparisonOps::equal(&SqlValue::Boolean(true), &SqlValue::Float(1.0)).unwrap(),
            SqlValue::Boolean(true)
        );
        // FALSE (0.0) < 3.14
        assert_eq!(
            ComparisonOps::less_than(&SqlValue::Boolean(false), &SqlValue::Float(3.14)).unwrap(),
            SqlValue::Boolean(true)
        );
        // TRUE (1.0) > 0.5
        assert_eq!(
            ComparisonOps::greater_than(&SqlValue::Boolean(true), &SqlValue::Float(0.5)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_boolean_comparison_with_numeric() {
        // TRUE (1) compared to NUMERIC
        assert_eq!(
            ComparisonOps::equal(&SqlValue::Boolean(true), &SqlValue::Numeric(1.0)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            ComparisonOps::less_than(&SqlValue::Boolean(false), &SqlValue::Numeric(0.5)).unwrap(),
            SqlValue::Boolean(true)
        );
        // FALSE (0) >= NUMERIC(0)
        assert_eq!(
            ComparisonOps::greater_than_or_equal(
                &SqlValue::Boolean(false),
                &SqlValue::Numeric(0.0)
            )
            .unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_boolean_equality_symmetric() {
        // Test symmetry: a = b should equal b = a
        assert_eq!(
            ComparisonOps::equal(&SqlValue::Boolean(true), &SqlValue::Integer(1)).unwrap(),
            ComparisonOps::equal(&SqlValue::Integer(1), &SqlValue::Boolean(true)).unwrap()
        );
        assert_eq!(
            ComparisonOps::equal(&SqlValue::Boolean(false), &SqlValue::Integer(0)).unwrap(),
            ComparisonOps::equal(&SqlValue::Integer(0), &SqlValue::Boolean(false)).unwrap()
        );
    }

    #[test]
    fn test_boolean_comparison_all_numeric_types() {
        // Test with Smallint, Bigint, Float, Real, Double, Numeric
        let true_val = SqlValue::Boolean(true);

        assert_eq!(
            ComparisonOps::equal(&true_val, &SqlValue::Smallint(1)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            ComparisonOps::equal(&true_val, &SqlValue::Bigint(1)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            ComparisonOps::equal(&true_val, &SqlValue::Real(1.0)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            ComparisonOps::equal(&true_val, &SqlValue::Double(1.0)).unwrap(),
            SqlValue::Boolean(true)
        );
        assert_eq!(
            ComparisonOps::equal(&true_val, &SqlValue::Numeric(1.0)).unwrap(),
            SqlValue::Boolean(true)
        );
    }

    #[test]
    fn test_boolean_not_equal() {
        // TRUE <> 0
        assert_eq!(
            ComparisonOps::not_equal(&SqlValue::Boolean(true), &SqlValue::Integer(0)).unwrap(),
            SqlValue::Boolean(true)
        );
        // FALSE <> 1
        assert_eq!(
            ComparisonOps::not_equal(&SqlValue::Boolean(false), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(true)
        );
        // TRUE <> 1 = false
        assert_eq!(
            ComparisonOps::not_equal(&SqlValue::Boolean(true), &SqlValue::Integer(1)).unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_boolean_less_than_or_equal() {
        // TRUE (1) <= 1
        assert_eq!(
            ComparisonOps::less_than_or_equal(&SqlValue::Boolean(true), &SqlValue::Integer(1))
                .unwrap(),
            SqlValue::Boolean(true)
        );
        // FALSE (0) <= 0
        assert_eq!(
            ComparisonOps::less_than_or_equal(&SqlValue::Boolean(false), &SqlValue::Integer(0))
                .unwrap(),
            SqlValue::Boolean(true)
        );
        // TRUE (1) <= 0 = false
        assert_eq!(
            ComparisonOps::less_than_or_equal(&SqlValue::Boolean(true), &SqlValue::Integer(0))
                .unwrap(),
            SqlValue::Boolean(false)
        );
    }

    #[test]
    fn test_boolean_greater_than_or_equal() {
        // TRUE (1) >= 1
        assert_eq!(
            ComparisonOps::greater_than_or_equal(&SqlValue::Boolean(true), &SqlValue::Integer(1))
                .unwrap(),
            SqlValue::Boolean(true)
        );
        // TRUE (1) >= 0
        assert_eq!(
            ComparisonOps::greater_than_or_equal(&SqlValue::Boolean(true), &SqlValue::Integer(0))
                .unwrap(),
            SqlValue::Boolean(true)
        );
        // FALSE (0) >= 1 = false
        assert_eq!(
            ComparisonOps::greater_than_or_equal(&SqlValue::Boolean(false), &SqlValue::Integer(1))
                .unwrap(),
            SqlValue::Boolean(false)
        );
    }
}
