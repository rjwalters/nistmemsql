//! Modulo operator (%) implementation

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

use super::{check_division_by_zero, coerce_numeric_values};

pub struct Modulo;

impl Modulo {
    /// Modulo operator (%)
    /// Returns the remainder of division
    #[inline]
    pub fn modulo(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // Fast path for integers (both modes)
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
            super::CoercedValues::ExactNumeric(a, b) => Ok(Integer(a % b)),
            super::CoercedValues::ApproximateNumeric(a, b) => Ok(Float((a % b) as f32)),
            super::CoercedValues::Numeric(a, b) => Ok(Numeric(a % b)),
        }
    }
}
