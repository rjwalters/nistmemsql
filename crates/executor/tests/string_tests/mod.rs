//! Comprehensive edge case tests for SQL string functions
//!
//! This test suite provides extensive coverage of SQL string functions,
//! organized into focused modules for better maintainability.
//!
//! ## Test Modules
//!
//! - **case_conversion** - UPPER, LOWER functions
//! - **length_functions** - LENGTH, CHAR_LENGTH, OCTET_LENGTH functions
//! - **substring_operations** - SUBSTRING, LEFT, RIGHT functions
//! - **search_functions** - POSITION, INSTR, LOCATE functions
//! - **transformations** - CONCAT, REPLACE, REVERSE, TRIM functions
//!
//! ## Test Coverage
//!
//! Each function is tested for:
//! - NULL handling
//! - Empty strings
//! - Unicode/multi-byte UTF-8 characters
//! - Boundary conditions
//! - Error conditions (wrong argument count/types)
//! - Type conversions (VARCHAR vs CHARACTER)
//!
//! Total: 116 comprehensive edge case tests

mod case_conversion;
mod length_functions;
mod search_functions;
mod substring_operations;
mod transformations;
