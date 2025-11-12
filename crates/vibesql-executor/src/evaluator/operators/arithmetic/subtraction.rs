//! Subtraction operator (-) implementation

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

use super::coerce_numeric_values;

pub struct Subtraction;

impl Subtraction {
    /// Subtraction operator (-)
    #[inline]
    pub fn subtract(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        use SqlValue::*;

        // Fast path for integers (both modes)
        if let (Integer(a), Integer(b)) = (left, right) {
            return Ok(Integer(a - b));
        }

        // Use helper for type coercion
        match coerce_numeric_values(left, right, "-")? {
            super::CoercedValues::ExactNumeric(a, b) => Ok(Integer(a - b)),
            super::CoercedValues::ApproximateNumeric(a, b) => Ok(Float((a - b) as f32)),
            super::CoercedValues::Numeric(a, b) => Ok(Numeric(a - b)),
        }
    }
}
