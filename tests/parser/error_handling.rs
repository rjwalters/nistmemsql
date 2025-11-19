//! Comprehensive error handling tests
//!
//! This test suite verifies error paths and error message formatting across
//! catalog, executor, and storage layers. Each test triggers a specific error
//! condition and validates the error variant and message.
//!
//! ## Module Organization
//!
//! - `catalog_errors`: Catalog-level error tests (tables, columns, schemas, views)
//! - `executor_errors`: Executor-level error tests (division by zero, type mismatches, etc.)
//! - `storage_errors`: Storage-level error tests (column count, index bounds, etc.)
//! - `display_and_conversion`: Error display formatting and type conversion tests

#[path = "../test_error_handling/catalog_errors.rs"]
mod catalog_errors;

#[path = "../test_error_handling/executor_errors.rs"]
mod executor_errors;

#[path = "../test_error_handling/storage_errors.rs"]
mod storage_errors;

#[path = "../test_error_handling/display_and_conversion.rs"]
mod display_and_conversion;
