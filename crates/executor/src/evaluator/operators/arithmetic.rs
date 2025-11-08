//! Arithmetic operator implementations
//!
//! Handles: +, -, *, /
//! Supports: Integer, Smallint, Bigint, Float, Real, Double, Numeric types
//! Includes: Type coercion, mixed-type arithmetic, division-by-zero handling

use crate::errors::ExecutorError;
use crate::evaluator::casting::{boolean_to_i64, is_approximate_numeric, is_exact_numeric, to_f64, to_i64};
use types::SqlValue;

pub(crate) struct ArithmeticOps;

impl ArithmeticOps {
    /// Addition operator (+)
    #[inline]
    pub fn add(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        match (left, right) {
            // Integer arithmetic - return Numeric for SQLLogicTest compatibility
            (Integer(a), Integer(b)) => Ok(Numeric((*a + *b) as f64)),

            // Mixed exact numeric types - promote to i64, return Numeric
            (left_val, right_val)
                if is_exact_numeric(left_val) && is_exact_numeric(right_val) =>
            {
                let left_i64 = to_i64(left_val)?;
                let right_i64 = to_i64(right_val)?;
                Ok(Numeric((left_i64 + right_i64) as f64))
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

            // NUMERIC with any numeric type - return Numeric for SQLLogicTest compatibility
            (left_val @ Numeric(_), right_val)
                if matches!(
                    right_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Numeric(left_f64 + right_f64))
            }
            (left_val, right_val @ Numeric(_))
                if matches!(
                    left_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Numeric(left_f64 + right_f64))
            }

            // Boolean coercion - treat TRUE as 1, FALSE as 0
            (left_val, right_val @ Boolean(_))
            | (left_val @ Boolean(_), right_val) => {
                let left_i64 = boolean_to_i64(left_val)
                    .or_else(|| to_i64(left_val).ok())
                    .ok_or_else(|| ExecutorError::TypeMismatch {
                        left: left_val.clone(),
                        op: "+".to_string(),
                        right: right_val.clone(),
                    })?;

                let right_i64 = boolean_to_i64(right_val)
                    .or_else(|| to_i64(right_val).ok())
                    .ok_or_else(|| ExecutorError::TypeMismatch {
                        left: left_val.clone(),
                        op: "+".to_string(),
                        right: right_val.clone(),
                    })?;

                Ok(Numeric((left_i64 + right_i64) as f64))
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
            // Integer arithmetic - return Numeric for SQLLogicTest compatibility
            (Integer(a), Integer(b)) => Ok(Numeric((*a - *b) as f64)),

            // Mixed exact numeric types - promote to i64, return Numeric
            (left_val, right_val)
                if is_exact_numeric(left_val) && is_exact_numeric(right_val) =>
            {
                let left_i64 = to_i64(left_val)?;
                let right_i64 = to_i64(right_val)?;
                Ok(Numeric((left_i64 - right_i64) as f64))
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

            // NUMERIC with any numeric type - return Numeric for SQLLogicTest compatibility
            (left_val @ Numeric(_), right_val)
                if matches!(
                    right_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Numeric(left_f64 - right_f64))
            }
            (left_val, right_val @ Numeric(_))
                if matches!(
                    left_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Numeric(left_f64 - right_f64))
            }

            // Boolean coercion - treat TRUE as 1, FALSE as 0
            (left_val, right_val @ Boolean(_))
            | (left_val @ Boolean(_), right_val) => {
                let left_i64 = boolean_to_i64(left_val)
                    .or_else(|| to_i64(left_val).ok())
                    .ok_or_else(|| ExecutorError::TypeMismatch {
                        left: left_val.clone(),
                        op: "-".to_string(),
                        right: right_val.clone(),
                    })?;

                let right_i64 = boolean_to_i64(right_val)
                    .or_else(|| to_i64(right_val).ok())
                    .ok_or_else(|| ExecutorError::TypeMismatch {
                        left: left_val.clone(),
                        op: "-".to_string(),
                        right: right_val.clone(),
                    })?;

                Ok(Numeric((left_i64 - right_i64) as f64))
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
            // Integer arithmetic - return Numeric for SQLLogicTest compatibility
            (Integer(a), Integer(b)) => Ok(Numeric((*a * *b) as f64)),

            // Mixed exact numeric types - promote to i64, return Numeric
            (left_val, right_val)
                if is_exact_numeric(left_val) && is_exact_numeric(right_val) =>
            {
                let left_i64 = to_i64(left_val)?;
                let right_i64 = to_i64(right_val)?;
                Ok(Numeric((left_i64 * right_i64) as f64))
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

            // NUMERIC with any numeric type - return Numeric for SQLLogicTest compatibility
            (left_val @ Numeric(_), right_val)
                if matches!(
                    right_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Numeric(left_f64 * right_f64))
            }
            (left_val, right_val @ Numeric(_))
                if matches!(
                    left_val,
                    Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_)
                ) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                Ok(Numeric(left_f64 * right_f64))
            }

            // Boolean coercion - treat TRUE as 1, FALSE as 0
            (left_val, right_val @ Boolean(_))
            | (left_val @ Boolean(_), right_val) => {
                let left_i64 = boolean_to_i64(left_val)
                    .or_else(|| to_i64(left_val).ok())
                    .ok_or_else(|| ExecutorError::TypeMismatch {
                        left: left_val.clone(),
                        op: "*".to_string(),
                        right: right_val.clone(),
                    })?;

                let right_i64 = boolean_to_i64(right_val)
                    .or_else(|| to_i64(right_val).ok())
                    .ok_or_else(|| ExecutorError::TypeMismatch {
                        left: left_val.clone(),
                        op: "*".to_string(),
                        right: right_val.clone(),
                    })?;

                Ok(Numeric((left_i64 * right_i64) as f64))
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
            // Integer division with zero check - returns Float to preserve precision
            (Integer(a), Integer(b)) => {
                if *b == 0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Float((*a as f64 / *b as f64) as f32))
            }

            // Mixed exact numeric types - promote to i64, return Float for precision
            (left_val, right_val)
                if is_exact_numeric(left_val) && is_exact_numeric(right_val) =>
            {
                let left_i64 = to_i64(left_val)?;
                let right_i64 = to_i64(right_val)?;
                if right_i64 == 0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Float((left_i64 as f64 / right_i64 as f64) as f32))
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

            // NUMERIC with any numeric type - return Numeric for SQLLogicTest compatibility
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
                Ok(Numeric(left_f64 / right_f64))
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
                Ok(Numeric(left_f64 / right_f64))
            }

            // Boolean coercion - treat TRUE as 1, FALSE as 0
            (left_val, right_val @ Boolean(_))
            | (left_val @ Boolean(_), right_val) => {
                let left_i64 = boolean_to_i64(left_val)
                    .or_else(|| to_i64(left_val).ok())
                    .ok_or_else(|| ExecutorError::TypeMismatch {
                        left: left_val.clone(),
                        op: "/".to_string(),
                        right: right_val.clone(),
                    })?;

                let right_i64 = boolean_to_i64(right_val)
                    .or_else(|| to_i64(right_val).ok())
                    .ok_or_else(|| ExecutorError::TypeMismatch {
                        left: left_val.clone(),
                        op: "/".to_string(),
                        right: right_val.clone(),
                    })?;

                if right_i64 == 0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Float((left_i64 as f64 / right_i64 as f64) as f32))
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
            // Integer division - return Numeric for SQLLogicTest compatibility
            (Integer(a), Integer(b)) => {
                if *b == 0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Numeric((*a / *b) as f64))
            }

            // Mixed exact numeric types - promote to i64, then divide, return Numeric
            (left_val, right_val)
                if is_exact_numeric(left_val) && is_exact_numeric(right_val) =>
            {
                let left_i64 = to_i64(left_val)?;
                let right_i64 = to_i64(right_val)?;
                if right_i64 == 0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Numeric((left_i64 / right_i64) as f64))
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
                // Truncate toward zero (same as Rust's as i64 cast) - return Numeric for SQLLogicTest compatibility
                Ok(Numeric(((left_f64 / right_f64) as i64) as f64))
            }

            // Mixed Float/Integer - promote to float, divide, truncate to int
            (left_val @ (Float(_) | Real(_) | Double(_)), right_val @ (Integer(_) | Smallint(_) | Bigint(_)))
            | (left_val @ (Integer(_) | Smallint(_) | Bigint(_)), right_val @ (Float(_) | Real(_) | Double(_))) => {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                if right_f64 == 0.0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                // Return Numeric for SQLLogicTest compatibility
                Ok(Numeric(((left_f64 / right_f64) as i64) as f64))
            }

            // NUMERIC with any numeric type - return Numeric for SQLLogicTest compatibility
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
                Ok(Numeric((left_f64 / right_f64).trunc()))
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
                Ok(Numeric((left_f64 / right_f64).trunc()))
            }

            // Boolean coercion - treat TRUE as 1, FALSE as 0
            (left_val, right_val @ Boolean(_))
            | (left_val @ Boolean(_), right_val) => {
                let left_i64 = boolean_to_i64(left_val)
                    .or_else(|| to_i64(left_val).ok())
                    .ok_or_else(|| ExecutorError::TypeMismatch {
                        left: left_val.clone(),
                        op: "DIV".to_string(),
                        right: right_val.clone(),
                    })?;

                let right_i64 = boolean_to_i64(right_val)
                    .or_else(|| to_i64(right_val).ok())
                    .ok_or_else(|| ExecutorError::TypeMismatch {
                        left: left_val.clone(),
                        op: "DIV".to_string(),
                        right: right_val.clone(),
                    })?;

                if right_i64 == 0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                // Return Numeric for SQLLogicTest compatibility
                Ok(Numeric((left_i64 / right_i64) as f64))
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
            // Integer modulo - return Numeric for SQLLogicTest compatibility
            (Integer(a), Integer(b)) => {
                if *b == 0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Numeric((*a % *b) as f64))
            }

            // Mixed exact numeric types - promote to i64, return Numeric
            (left_val, right_val)
                if is_exact_numeric(left_val) && is_exact_numeric(right_val) =>
            {
                let left_i64 = to_i64(left_val)?;
                let right_i64 = to_i64(right_val)?;
                if right_i64 == 0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                Ok(Numeric((left_i64 % right_i64) as f64))
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

            // NUMERIC with any numeric type - return Numeric for SQLLogicTest compatibility
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
                Ok(Numeric(left_f64 % right_f64))
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
                Ok(Numeric(left_f64 % right_f64))
            }

            // Boolean coercion - treat TRUE as 1, FALSE as 0
            (left_val, right_val @ Boolean(_))
            | (left_val @ Boolean(_), right_val) => {
                let left_i64 = boolean_to_i64(left_val)
                    .or_else(|| to_i64(left_val).ok())
                    .ok_or_else(|| ExecutorError::TypeMismatch {
                        left: left_val.clone(),
                        op: "%".to_string(),
                        right: right_val.clone(),
                    })?;

                let right_i64 = boolean_to_i64(right_val)
                    .or_else(|| to_i64(right_val).ok())
                    .ok_or_else(|| ExecutorError::TypeMismatch {
                        left: left_val.clone(),
                        op: "%".to_string(),
                        right: right_val.clone(),
                    })?;

                if right_i64 == 0 {
                    return Err(ExecutorError::DivisionByZero);
                }
                // Return Numeric for SQLLogicTest compatibility
                Ok(Numeric((left_i64 % right_i64) as f64))
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
        assert_eq!(result, SqlValue::Numeric(8.0));
    }

    #[test]
    fn test_integer_subtraction() {
        let result =
            ArithmeticOps::subtract(&SqlValue::Integer(5), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Numeric(2.0));
    }

    #[test]
    fn test_integer_multiplication() {
        let result =
            ArithmeticOps::multiply(&SqlValue::Integer(5), &SqlValue::Integer(3)).unwrap();
        assert_eq!(result, SqlValue::Numeric(15.0));
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
        assert_eq!(result, SqlValue::Numeric(8.0));
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
        assert_eq!(result, SqlValue::Numeric(41.0));
    }

    #[test]
    fn test_boolean_false_addition() {
        // FALSE + 40 = 40
        let result = ArithmeticOps::add(&SqlValue::Boolean(false), &SqlValue::Integer(40)).unwrap();
        assert_eq!(result, SqlValue::Numeric(40.0));
    }

    #[test]
    fn test_integer_plus_boolean() {
        // 40 + TRUE = 41
        let result = ArithmeticOps::add(&SqlValue::Integer(40), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Numeric(41.0));
    }

    #[test]
    fn test_boolean_multiplication() {
        // 97 * TRUE = 97
        let result = ArithmeticOps::multiply(&SqlValue::Integer(97), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Numeric(97.0));

        // 97 * FALSE = 0
        let result = ArithmeticOps::multiply(&SqlValue::Integer(97), &SqlValue::Boolean(false)).unwrap();
        assert_eq!(result, SqlValue::Numeric(0.0));
    }

    #[test]
    fn test_boolean_subtraction() {
        // TRUE - FALSE = 1
        let result = ArithmeticOps::subtract(&SqlValue::Boolean(true), &SqlValue::Boolean(false)).unwrap();
        assert_eq!(result, SqlValue::Numeric(1.0));

        // 5 - TRUE = 4
        let result = ArithmeticOps::subtract(&SqlValue::Integer(5), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Numeric(4.0));
    }

    #[test]
    fn test_boolean_division() {
        // 10 / TRUE = 10.0
        let result = ArithmeticOps::divide(&SqlValue::Integer(10), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Float(10.0));

        // TRUE / TRUE = 1.0
        let result = ArithmeticOps::divide(&SqlValue::Boolean(true), &SqlValue::Boolean(true)).unwrap();
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
        let result = ArithmeticOps::modulo(&SqlValue::Integer(10), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Numeric(0.0));

        // TRUE % TRUE = 0 (1 % 1 = 0)
        let result = ArithmeticOps::modulo(&SqlValue::Boolean(true), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Numeric(0.0));
    }

    #[test]
    fn test_boolean_integer_divide() {
        // 10 DIV TRUE = 10
        let result = ArithmeticOps::integer_divide(&SqlValue::Integer(10), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Numeric(10.0));

        // TRUE DIV TRUE = 1
        let result = ArithmeticOps::integer_divide(&SqlValue::Boolean(true), &SqlValue::Boolean(true)).unwrap();
        assert_eq!(result, SqlValue::Numeric(1.0));
    }
}
