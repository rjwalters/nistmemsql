//! Division operators (/, //) implementation

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

use super::coerce_numeric_values;

pub struct Division;

impl Division {
    /// Division operator (/)
    #[inline]
    pub fn divide(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // NULL propagation - SQL standard semantics
        if matches!(left, Null) || matches!(right, Null) {
            return Ok(Null);
        }

        // Fast path for integers - SQLLogicTest expects floating-point results
        // INTEGER / INTEGER → FLOAT (floating-point division)
        // This matches SQLLogicTest expectations for standard division operator
        if let (Integer(a), Integer(b)) = (left, right) {
            if *b == 0 {
                return Ok(SqlValue::Null);
            }
            // Perform floating-point division
            let result = (*a as f64) / (*b as f64);
            return Ok(Float(result as f32));
        }

        // Use helper for type coercion
        let coerced = coerce_numeric_values(left, right, "/")?;

        // Check for division by zero and return NULL (SQL standard behavior)
        let is_zero = match &coerced {
            super::CoercedValues::ExactNumeric(_, right) => *right == 0,
            super::CoercedValues::ApproximateNumeric(_, right) => *right == 0.0,
            super::CoercedValues::Numeric(_, right) => *right == 0.0,
        };

        if is_zero {
            return Ok(SqlValue::Null);
        }

        // Division returns floating-point results
        // - ExactNumeric: INTEGER / INTEGER → FLOAT (floating-point division)
        // - ApproximateNumeric: FLOAT / FLOAT → FLOAT
        // - Numeric: NUMERIC / NUMERIC → NUMERIC
        match coerced {
            super::CoercedValues::ExactNumeric(a, b) => {
                // Perform floating-point division for integers
                let result = (a as f64) / (b as f64);
                Ok(Float(result as f32))
            }
            super::CoercedValues::ApproximateNumeric(a, b) => Ok(Float((a / b) as f32)),
            super::CoercedValues::Numeric(a, b) => Ok(Numeric(a / b)),
        }
    }

    /// Integer division operator (DIV) - MySQL-specific
    /// Returns integer result, truncating fractional part (truncates toward zero)
    #[inline]
    pub fn integer_divide(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // NULL propagation - SQL standard semantics
        if matches!(left, Null) || matches!(right, Null) {
            return Ok(Null);
        }

        // Fast path for integers (both modes)
        if let (Integer(a), Integer(b)) = (left, right) {
            if *b == 0 {
                return Ok(SqlValue::Null);
            }
            // Integer division truncates toward zero (not floor division)
            let result = ((*a as f64) / (*b as f64)).trunc() as i64;
            return Ok(Integer(result));
        }

        // Use helper for type coercion
        let coerced = coerce_numeric_values(left, right, "DIV")?;

        // Check for division by zero and return NULL (SQL standard behavior)
        let is_zero = match &coerced {
            super::CoercedValues::ExactNumeric(_, right) => *right == 0,
            super::CoercedValues::ApproximateNumeric(_, right) => *right == 0.0,
            super::CoercedValues::Numeric(_, right) => *right == 0.0,
        };

        if is_zero {
            return Ok(SqlValue::Null);
        }

        // Integer division truncates toward zero
        match coerced {
            super::CoercedValues::ExactNumeric(a, b) => {
                let result = ((a as f64) / (b as f64)).trunc() as i64;
                Ok(Integer(result))
            }
            super::CoercedValues::ApproximateNumeric(a, b) => Ok(Integer((a / b).trunc() as i64)),
            super::CoercedValues::Numeric(a, b) => Ok(Integer((a / b).trunc() as i64)),
        }
    }
}
