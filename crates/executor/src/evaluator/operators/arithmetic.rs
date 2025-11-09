//! Arithmetic operator implementations
//!
//! Handles: +, -, *, /
//! Supports: Integer, Smallint, Bigint, Float, Real, Double, Numeric types
//! Includes: Type coercion, mixed-type arithmetic, division-by-zero handling

use types::SqlValue;

use crate::{
    errors::ExecutorError,
    evaluator::casting::{
        boolean_to_i64, is_approximate_numeric, is_exact_numeric, to_f64, to_i64,
    },
};

/// Result of type coercion for arithmetic operations
enum CoercedValues {
    ExactNumeric(i64, i64),
    ApproximateNumeric(f64, f64),
}

/// Helper function to coerce two values to a common numeric type
fn coerce_numeric_values(
    left: &SqlValue,
    right: &SqlValue,
    op: &str,
) -> Result<CoercedValues, ExecutorError> {
    use SqlValue::*;

    // Handle Boolean coercion first
    if matches!(left, Boolean(_)) || matches!(right, Boolean(_)) {
        let left_i64 = boolean_to_i64(left).or_else(|| to_i64(left).ok()).ok_or_else(|| {
            ExecutorError::TypeMismatch {
                left: left.clone(),
                op: op.to_string(),
                right: right.clone(),
            }
        })?;

        let right_i64 = boolean_to_i64(right).or_else(|| to_i64(right).ok()).ok_or_else(|| {
            ExecutorError::TypeMismatch {
                left: left.clone(),
                op: op.to_string(),
                right: right.clone(),
            }
        })?;

        return Ok(CoercedValues::ExactNumeric(left_i64, right_i64));
    }

    // Mixed exact numeric types - promote to i64
    if is_exact_numeric(left) && is_exact_numeric(right) {
        let left_i64 = to_i64(left)?;
        let right_i64 = to_i64(right)?;
        return Ok(CoercedValues::ExactNumeric(left_i64, right_i64));
    }

    // Approximate numeric types - promote to f64
    if is_approximate_numeric(left) && is_approximate_numeric(right) {
        let left_f64 = to_f64(left)?;
        let right_f64 = to_f64(right)?;
        return Ok(CoercedValues::ApproximateNumeric(left_f64, right_f64));
    }

    // Mixed Float/Integer - promote to f64
    if (matches!(left, Float(_) | Real(_) | Double(_))
        && matches!(right, Integer(_) | Smallint(_) | Bigint(_)))
        || (matches!(left, Integer(_) | Smallint(_) | Bigint(_))
            && matches!(right, Float(_) | Real(_) | Double(_)))
    {
        let left_f64 = to_f64(left)?;
        let right_f64 = to_f64(right)?;
        return Ok(CoercedValues::ApproximateNumeric(left_f64, right_f64));
    }

    // NUMERIC with any numeric type - promote to f64
    if (matches!(left, Numeric(_))
        && matches!(
            right,
            Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
        ))
        || (matches!(left, Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_))
            && matches!(right, Numeric(_)))
    {
        let left_f64 = to_f64(left)?;
        let right_f64 = to_f64(right)?;
        return Ok(CoercedValues::ApproximateNumeric(left_f64, right_f64));
    }

    // Type mismatch
    Err(ExecutorError::TypeMismatch {
        left: left.clone(),
        op: op.to_string(),
        right: right.clone(),
    })
}

/// Helper function to check for division by zero
fn check_division_by_zero(value: &CoercedValues) -> Result<(), ExecutorError> {
    match value {
        CoercedValues::ExactNumeric(_, right) if *right == 0 => Err(ExecutorError::DivisionByZero),
        CoercedValues::ApproximateNumeric(_, right) if *right == 0.0 => {
            Err(ExecutorError::DivisionByZero)
        }
        _ => Ok(()),
    }
}

pub(crate) struct ArithmeticOps;

impl ArithmeticOps {
    /// Addition operator (+)
    #[inline]
    pub fn add(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // Fast path for common case
        if let (Integer(a), Integer(b)) = (left, right) {
            return Ok(Integer(a + b));
        }

        // Use helper for type coercion
        match coerce_numeric_values(left, right, "+")? {
            CoercedValues::ExactNumeric(a, b) => Ok(Integer(a + b)),
            CoercedValues::ApproximateNumeric(a, b) => Ok(Float((a + b) as f32)),
        }
    }

    /// Subtraction operator (-)
    #[inline]
    pub fn subtract(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // Fast path for common case
        if let (Integer(a), Integer(b)) = (left, right) {
            return Ok(Integer(a - b));
        }

        // Use helper for type coercion
        match coerce_numeric_values(left, right, "-")? {
            CoercedValues::ExactNumeric(a, b) => Ok(Integer(a - b)),
            CoercedValues::ApproximateNumeric(a, b) => Ok(Float((a - b) as f32)),
        }
    }

    /// Multiplication operator (*)
    #[inline]
    pub fn multiply(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // Fast path for common case
        if let (Integer(a), Integer(b)) = (left, right) {
            return Ok(Integer(a * b));
        }

        // Use helper for type coercion
        match coerce_numeric_values(left, right, "*")? {
            CoercedValues::ExactNumeric(a, b) => Ok(Integer(a * b)),
            CoercedValues::ApproximateNumeric(a, b) => Ok(Float((a * b) as f32)),
        }
    }

    /// Division operator (/)
    #[inline]
    pub fn divide(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // Fast path for common case
        if let (Integer(a), Integer(b)) = (left, right) {
            if *b == 0 {
                return Err(ExecutorError::DivisionByZero);
            }
            return Ok(Float((*a as f64 / *b as f64) as f32));
        }

        // Use helper for type coercion
        let coerced = coerce_numeric_values(left, right, "/")?;
        check_division_by_zero(&coerced)?;

        // Division always returns Float for precision
        match coerced {
            CoercedValues::ExactNumeric(a, b) => Ok(Float((a as f64 / b as f64) as f32)),
            CoercedValues::ApproximateNumeric(a, b) => Ok(Float((a / b) as f32)),
        }
    }

    /// Integer division operator (DIV) - MySQL-specific
    /// Returns integer result, truncating fractional part (truncates toward zero)
    #[inline]
    pub fn integer_divide(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // Fast path for common case
        if let (Integer(a), Integer(b)) = (left, right) {
            if *b == 0 {
                return Err(ExecutorError::DivisionByZero);
            }
            return Ok(Integer(a / b));
        }

        // Use helper for type coercion
        let coerced = coerce_numeric_values(left, right, "DIV")?;
        check_division_by_zero(&coerced)?;

        // Integer division truncates toward zero
        match coerced {
            CoercedValues::ExactNumeric(a, b) => Ok(Integer(a / b)),
            CoercedValues::ApproximateNumeric(a, b) => Ok(Integer((a / b) as i64)),
        }
    }

    /// Modulo operator (%)
    /// Returns the remainder of division
    #[inline]
    pub fn modulo(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // Fast path for common case
        if let (Integer(a), Integer(b)) = (left, right) {
            if *b == 0 {
                return Err(ExecutorError::DivisionByZero);
            }
            return Ok(Integer(a % b));
        }

        // Use helper for type coercion
        let coerced = coerce_numeric_values(left, right, "%")?;
        check_division_by_zero(&coerced)?;

        match coerced {
            CoercedValues::ExactNumeric(a, b) => Ok(Integer(a % b)),
            CoercedValues::ApproximateNumeric(a, b) => Ok(Float((a % b) as f32)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_addition() {
        let result = ArithmeticOps::add(&SqlValue::Integer(5), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Integer(8));
    }

    #[test]
    fn test_integer_subtraction() {
        let result = ArithmeticOps::subtract(&SqlValue::Integer(5), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Integer(2));
    }

    #[test]
    fn test_integer_multiplication() {
        let result = ArithmeticOps::multiply(&SqlValue::Integer(5), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Integer(15));
    }

    #[test]
    fn test_integer_division() {
        let result = ArithmeticOps::divide(&SqlValue::Integer(15), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Float(5.0));
    }

    #[test]
    fn test_division_by_zero() {
        let result = ArithmeticOps::divide(&SqlValue::Integer(5), &SqlValue::Integer(0));
        assert!(matches!(result, Err(ExecutorError::DivisionByZero)));
    }

    #[test]
    fn test_mixed_exact_numeric() {
        let result = ArithmeticOps::add(&SqlValue::Smallint(5), &SqlValue::Bigint(3)).unwrap();
        assert_eq!(result, SqlValue::Integer(8));
    }

    #[test]
    fn test_float_arithmetic() {
        let result = ArithmeticOps::add(&SqlValue::Float(5.5), &SqlValue::Float(3.2)).unwrap();
        match result {
            SqlValue::Float(f) => assert!((f - 8.7).abs() < 0.01),
            _ => panic!("Expected Float result"),
        }
    }

    #[test]
    fn test_mixed_float_integer() {
        let result = ArithmeticOps::add(&SqlValue::Float(5.5), &SqlValue::Integer(3)).unwrap();
        match result {
            SqlValue::Float(f) => assert!((f - 8.5).abs() < 0.01),
            _ => panic!("Expected Float result"),
        }
    }

    // Boolean coercion tests
    #[test]
    fn test_boolean_true_addition() {
        // TRUE + 40 = 41
        let result = ArithmeticOps::add(&SqlValue::Boolean(true), &SqlValue::Integer(40)).unwrap();
        assert_eq!(result, SqlValue::Integer(41));
    }

    #[test]
    fn test_boolean_false_addition() {
        // FALSE + 40 = 40
        let result = ArithmeticOps::add(&SqlValue::Boolean(false), &SqlValue::Integer(40)).unwrap();
        assert_eq!(result, SqlValue::Integer(40));
    }

    #[test]
    fn test_integer_plus_boolean() {
        // 40 + TRUE = 41
        let result = ArithmeticOps::add(&SqlValue::Integer(40), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Integer(41));
    }

    #[test]
    fn test_boolean_multiplication() {
        // 97 * TRUE = 97
        let result =
            ArithmeticOps::multiply(&SqlValue::Integer(97), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Integer(97));

        // 97 * FALSE = 0
        let result =
            ArithmeticOps::multiply(&SqlValue::Integer(97), &SqlValue::Boolean(false)).unwrap();
        assert_eq!(result, SqlValue::Integer(0));
    }

    #[test]
    fn test_boolean_subtraction() {
        // TRUE - FALSE = 1
        let result =
            ArithmeticOps::subtract(&SqlValue::Boolean(true), &SqlValue::Boolean(false)).unwrap();
        assert_eq!(result, SqlValue::Integer(1));

        // 5 - TRUE = 4
        let result =
            ArithmeticOps::subtract(&SqlValue::Integer(5), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Integer(4));
    }

    #[test]
    fn test_boolean_division() {
        // 10 / TRUE = 10.0
        let result =
            ArithmeticOps::divide(&SqlValue::Integer(10), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Float(10.0));

        // TRUE / TRUE = 1.0
        let result =
            ArithmeticOps::divide(&SqlValue::Boolean(true), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Float(1.0));
    }

    #[test]
    fn test_boolean_division_by_false() {
        // 10 / FALSE should return DivisionByZero error
        let result = ArithmeticOps::divide(&SqlValue::Integer(10), &SqlValue::Boolean(false));
        assert!(matches!(result, Err(ExecutorError::DivisionByZero)));
    }

    #[test]
    fn test_boolean_modulo() {
        // 10 % TRUE = 0 (10 % 1 = 0)
        let result =
            ArithmeticOps::modulo(&SqlValue::Integer(10), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Integer(0));

        // TRUE % TRUE = 0 (1 % 1 = 0)
        let result =
            ArithmeticOps::modulo(&SqlValue::Boolean(true), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Integer(0));
    }

    #[test]
    fn test_boolean_integer_divide() {
        // 10 DIV TRUE = 10
        let result =
            ArithmeticOps::integer_divide(&SqlValue::Integer(10), &SqlValue::Boolean(true))
                .unwrap();
        assert_eq!(result, SqlValue::Integer(10));

        // TRUE DIV TRUE = 1
        let result =
            ArithmeticOps::integer_divide(&SqlValue::Boolean(true), &SqlValue::Boolean(true))
                .unwrap();
        assert_eq!(result, SqlValue::Integer(1));
    }
}
