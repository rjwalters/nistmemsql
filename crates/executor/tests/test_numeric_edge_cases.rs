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

#[path = "test_numeric_edge_cases/helpers.rs"]
mod helpers;

#[path = "test_numeric_edge_cases/basic.rs"]
mod basic;

#[path = "test_numeric_edge_cases/rounding.rs"]
mod rounding;

#[path = "test_numeric_edge_cases/exponential.rs"]
mod exponential;

#[path = "test_numeric_edge_cases/trigonometric.rs"]
mod trigonometric;

#[path = "test_numeric_edge_cases/type_coercion.rs"]
mod type_coercion;

#[path = "test_numeric_edge_cases/domain_range.rs"]
mod domain_range;

#[path = "test_numeric_edge_cases/precision.rs"]
mod precision;
