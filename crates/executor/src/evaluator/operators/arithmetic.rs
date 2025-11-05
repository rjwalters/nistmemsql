//! Arithmetic operator implementations
//!
//! Handles: +, -, *, /
//! Supports: Integer, Smallint, Bigint, Float, Real, Double, Numeric types
//! Includes: Type coercion, mixed-type arithmetic, division-by-zero handling

use crate::errors::ExecutorError;
use crate::evaluator::casting::{is_approximate_numeric, is_exact_numeric, to_f64, to_i64};
use types::SqlValue;

pub(crate) struct ArithmeticOps;

impl ArithmeticOps {
    /// Addition operator (+)
    #[inline]
    pub fn add(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        match (left, right) {
            // Integer arithmetic
            (Integer(a), Integer(b)) => Ok(Integer(a + b)),

            // Mixed exact numeric types - promote to i64
            (left_val, right_val)
                if is_exact_numeric(left_val) && is_exact_numeric(right_val) =>
            {
                let left_i64 = to_i64(left_val)?;
                let right_i64 = to_i64(right_val)?;
                Ok(Integer(left_i64 + right_i64))
            }

            // Approximate numeric types - promote to f64
            (left_val, right_val)
                if is_approximate_numeric(left_val) && is_approximate_numeric(right_val) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Float((left_f64 + right_f64) as f32))
            }

            // Mixed Float/Integer - promote Integer to Float
            (left_val @ (Float(_) | Real(_) | Double(_)), right_val @ (Integer(_) | Smallint(_) | Bigint(_)))
            | (left_val @ (Integer(_) | Smallint(_) | Bigint(_)), right_val @ (Float(_) | Real(_) | Double(_))) => {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Float((left_f64 + right_f64) as f32))
            }

            // NUMERIC with any numeric type
            (left_val @ Numeric(_), right_val)
                if matches!(
                    right_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Float((left_f64 + right_f64) as f32))
            }
            (left_val, right_val @ Numeric(_))
                if matches!(
                    left_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Float((left_f64 + right_f64) as f32))
            }

            // Type mismatch
            _ => Err(ExecutorError::TypeMismatch {
                left: left.clone(),
                op: "+".to_string(),
                right: right.clone(),
            }),
        }
    }

    /// Subtraction operator (-)
    #[inline]
    pub fn subtract(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        match (left, right) {
            // Integer arithmetic
            (Integer(a), Integer(b)) => Ok(Integer(a - b)),

            // Mixed exact numeric types - promote to i64
            (left_val, right_val)
                if is_exact_numeric(left_val) && is_exact_numeric(right_val) =>
            {
                let left_i64 = to_i64(left_val)?;
                let right_i64 = to_i64(right_val)?;
                Ok(Integer(left_i64 - right_i64))
            }

            // Approximate numeric types - promote to f64
            (left_val, right_val)
                if is_approximate_numeric(left_val) && is_approximate_numeric(right_val) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Float((left_f64 - right_f64) as f32))
            }

            // Mixed Float/Integer - promote Integer to Float
            (left_val @ (Float(_) | Real(_) | Double(_)), right_val @ (Integer(_) | Smallint(_) | Bigint(_)))
            | (left_val @ (Integer(_) | Smallint(_) | Bigint(_)), right_val @ (Float(_) | Real(_) | Double(_))) => {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Float((left_f64 - right_f64) as f32))
            }

            // NUMERIC with any numeric type
            (left_val @ Numeric(_), right_val)
                if matches!(
                    right_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Float((left_f64 - right_f64) as f32))
            }
            (left_val, right_val @ Numeric(_))
                if matches!(
                    left_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Float((left_f64 - right_f64) as f32))
            }

            // Type mismatch
            _ => Err(ExecutorError::TypeMismatch {
                left: left.clone(),
                op: "-".to_string(),
                right: right.clone(),
            }),
        }
    }

    /// Multiplication operator (*)
    #[inline]
    pub fn multiply(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        match (left, right) {
            // Integer arithmetic
            (Integer(a), Integer(b)) => Ok(Integer(a * b)),

            // Mixed exact numeric types - promote to i64
            (left_val, right_val)
                if is_exact_numeric(left_val) && is_exact_numeric(right_val) =>
            {
                let left_i64 = to_i64(left_val)?;
                let right_i64 = to_i64(right_val)?;
                Ok(Integer(left_i64 * right_i64))
            }

            // Approximate numeric types - promote to f64
            (left_val, right_val)
                if is_approximate_numeric(left_val) && is_approximate_numeric(right_val) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Float((left_f64 * right_f64) as f32))
            }

            // Mixed Float/Integer - promote Integer to Float
            (left_val @ (Float(_) | Real(_) | Double(_)), right_val @ (Integer(_) | Smallint(_) | Bigint(_)))
            | (left_val @ (Integer(_) | Smallint(_) | Bigint(_)), right_val @ (Float(_) | Real(_) | Double(_))) => {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Float((left_f64 * right_f64) as f32))
            }

            // NUMERIC with any numeric type
            (left_val @ Numeric(_), right_val)
                if matches!(
                    right_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Float((left_f64 * right_f64) as f32))
            }
            (left_val, right_val @ Numeric(_))
                if matches!(
                    left_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Float((left_f64 * right_f64) as f32))
            }

            // Type mismatch
            _ => Err(ExecutorError::TypeMismatch {
                left: left.clone(),
                op: "*".to_string(),
                right: right.clone(),
            }),
        }
    }

    /// Division operator (/)
    #[inline]
    pub fn divide(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        match (left, right) {
            // Integer division with zero check
            (Integer(a), Integer(b)) => {
                if *b == 0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Integer(a / b))
            }

            // Mixed exact numeric types - promote to i64
            (left_val, right_val)
                if is_exact_numeric(left_val) && is_exact_numeric(right_val) =>
            {
                let left_i64 = to_i64(left_val)?;
                let right_i64 = to_i64(right_val)?;
                if right_i64 == 0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Integer(left_i64 / right_i64))
            }

            // Approximate numeric types - promote to f64
            (left_val, right_val)
                if is_approximate_numeric(left_val) && is_approximate_numeric(right_val) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                if right_f64 == 0.0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Float((left_f64 / right_f64) as f32))
            }

            // Mixed Float/Integer - promote Integer to Float
            (left_val @ (Float(_) | Real(_) | Double(_)), right_val @ (Integer(_) | Smallint(_) | Bigint(_)))
            | (left_val @ (Integer(_) | Smallint(_) | Bigint(_)), right_val @ (Float(_) | Real(_) | Double(_))) => {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                if right_f64 == 0.0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Float((left_f64 / right_f64) as f32))
            }

            // NUMERIC with any numeric type
            (left_val @ Numeric(_), right_val)
                if matches!(
                    right_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                if right_f64 == 0.0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Float((left_f64 / right_f64) as f32))
            }
            (left_val, right_val @ Numeric(_))
                if matches!(
                    left_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                if right_f64 == 0.0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Float((left_f64 / right_f64) as f32))
            }

            // Type mismatch
            _ => Err(ExecutorError::TypeMismatch {
                left: left.clone(),
                op: "/".to_string(),
                right: right.clone(),
            }),
        }
    }

    /// Integer division operator (DIV) - MySQL-specific
    /// Returns integer result, truncating fractional part (truncates toward zero)
    #[inline]
    pub fn integer_divide(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        match (left, right) {
            // Integer division - already returns integer
            (Integer(a), Integer(b)) => {
                if *b == 0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Integer(a / b))
            }

            // Mixed exact numeric types - promote to i64, then divide
            (left_val, right_val)
                if is_exact_numeric(left_val) && is_exact_numeric(right_val) =>
            {
                let left_i64 = to_i64(left_val)?;
                let right_i64 = to_i64(right_val)?;
                if right_i64 == 0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Integer(left_i64 / right_i64))
            }

            // Approximate numeric types - convert to float, divide, truncate to int
            (left_val, right_val)
                if is_approximate_numeric(left_val) && is_approximate_numeric(right_val) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                if right_f64 == 0.0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                // Truncate toward zero (same as Rust's as i64 cast)
                Ok(Integer((left_f64 / right_f64) as i64))
            }

            // Mixed Float/Integer - promote to float, divide, truncate to int
            (left_val @ (Float(_) | Real(_) | Double(_)), right_val @ (Integer(_) | Smallint(_) | Bigint(_)))
            | (left_val @ (Integer(_) | Smallint(_) | Bigint(_)), right_val @ (Float(_) | Real(_) | Double(_))) => {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                if right_f64 == 0.0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Integer((left_f64 / right_f64) as i64))
            }

            // NUMERIC with any numeric type
            (left_val @ Numeric(_), right_val)
                if matches!(
                    right_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                if right_f64 == 0.0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Integer((left_f64 / right_f64) as i64))
            }
            (left_val, right_val @ Numeric(_))
                if matches!(
                    left_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                if right_f64 == 0.0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Integer((left_f64 / right_f64) as i64))
            }

            // Type mismatch
            _ => Err(ExecutorError::TypeMismatch {
                left: left.clone(),
                op: "DIV".to_string(),
                right: right.clone(),
            }),
        }
    }

    /// Modulo operator (%)
    /// Returns the remainder of division
    #[inline]
    pub fn modulo(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        match (left, right) {
            // Integer modulo
            (Integer(a), Integer(b)) => {
                if *b == 0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Integer(a % b))
            }

            // Mixed exact numeric types - promote to i64
            (left_val, right_val)
                if is_exact_numeric(left_val) && is_exact_numeric(right_val) =>
            {
                let left_i64 = to_i64(left_val)?;
                let right_i64 = to_i64(right_val)?;
                if right_i64 == 0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Integer(left_i64 % right_i64))
            }

            // Approximate numeric types - use fmod
            (left_val, right_val)
                if is_approximate_numeric(left_val) && is_approximate_numeric(right_val) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                if right_f64 == 0.0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Float((left_f64 % right_f64) as f32))
            }

            // Mixed Float/Integer - promote to float
            (left_val @ (Float(_) | Real(_) | Double(_)), right_val @ (Integer(_) | Smallint(_) | Bigint(_)))
            | (left_val @ (Integer(_) | Smallint(_) | Bigint(_)), right_val @ (Float(_) | Real(_) | Double(_))) => {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                if right_f64 == 0.0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Float((left_f64 % right_f64) as f32))
            }

            // NUMERIC with any numeric type
            (left_val @ Numeric(_), right_val)
                if matches!(
                    right_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                if right_f64 == 0.0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Float((left_f64 % right_f64) as f32))
            }
            (left_val, right_val @ Numeric(_))
                if matches!(
                    left_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                if right_f64 == 0.0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Float((left_f64 % right_f64) as f32))
            }

            // Type mismatch
            _ => Err(ExecutorError::TypeMismatch {
                left: left.clone(),
                op: "%".to_string(),
                right: right.clone(),
            }),
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
        let result =
            ArithmeticOps::subtract(&SqlValue::Integer(5), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Integer(2));
    }

    #[test]
    fn test_integer_multiplication() {
        let result =
            ArithmeticOps::multiply(&SqlValue::Integer(5), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Integer(15));
    }

    #[test]
    fn test_integer_division() {
        let result = ArithmeticOps::divide(&SqlValue::Integer(15), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Integer(5));
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
}
