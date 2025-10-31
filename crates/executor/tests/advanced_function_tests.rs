//! Tests for advanced scalar functions (math, trigonometry, and conditional functions)
//!
//! This test suite is organized into focused sub-modules for better organization:
//! - math_functions: EXP, LN, LOG, SIGN, PI functions
//! - trig_functions: SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2, RADIANS, DEGREES
//! - conditional_functions: GREATEST, LEAST, IF functions
//! - nested_expressions: Complex expressions combining multiple functions
//! - truncate_functions: TRUNCATE function with various precisions
//! - fixtures: Common constants and helper functions

// Allow approximate constants in tests - these are test data values, not mathematical constants
#![allow(clippy::approx_constant)]

mod common;

// Include the sub-modules from the advanced_function_tests directory
#[path = "advanced_function_tests/math_functions.rs"]
mod math_functions;

#[path = "advanced_function_tests/trig_functions.rs"]
mod trig_functions;

#[path = "advanced_function_tests/conditional_functions.rs"]
mod conditional_functions;

#[path = "advanced_function_tests/nested_expressions.rs"]
mod nested_expressions;

#[path = "advanced_function_tests/truncate_functions.rs"]
mod truncate_functions;

#[path = "advanced_function_tests/fixtures.rs"]
mod fixtures;
