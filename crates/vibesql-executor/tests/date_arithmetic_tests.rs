//! Tests for date arithmetic functions (DATEDIFF, DATE_ADD, DATE_SUB, EXTRACT, AGE)
//!
//! This test suite is organized into focused sub-modules for better organization:
//! - date_operations: DATEDIFF, DATE_ADD, DATE_SUB with DATE types
//! - time_operations: Time-specific calculations (HOUR, MINUTE, SECOND)
//! - timestamp_operations: Timestamp with time component operations
//! - interval_operations: EXTRACT and AGE functions
//! - edge_cases: Leap years, month/year boundaries, null handling, errors
//! - type_coercion: VARCHAR to DATE coercion, INTERVAL syntax tests

// Re-export common test utilities
#[path = "common/mod.rs"]
mod common;

// Include the sub-modules from the date_arithmetic_tests directory
#[path = "date_arithmetic_tests/date_operations.rs"]
mod date_operations;

#[path = "date_arithmetic_tests/time_operations.rs"]
mod time_operations;

#[path = "date_arithmetic_tests/timestamp_operations.rs"]
mod timestamp_operations;

#[path = "date_arithmetic_tests/interval_operations.rs"]
mod interval_operations;

#[path = "date_arithmetic_tests/edge_cases.rs"]
mod edge_cases;

#[path = "date_arithmetic_tests/type_coercion.rs"]
mod type_coercion;
