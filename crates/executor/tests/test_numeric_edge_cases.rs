//! Edge case tests for numeric functions
//!
//! This module tests numeric functions with edge cases including:
//! - NULL handling
//! - Overflow/underflow conditions
//! - Domain errors (negative sqrt, log of zero/negative)
//! - Type coercion and precision issues
//! - Boundary values (MIN_INT, MAX_INT, etc.)
//!
//! Tests are organized into logical modules for maintainability.

mod common;
mod test_numeric_helpers;

mod test_numeric_basic;
mod test_numeric_rounding;
mod test_numeric_exponential;
mod test_numeric_trigonometric;
mod test_numeric_type_coercion;
mod test_numeric_domain_range;
mod test_numeric_precision;
