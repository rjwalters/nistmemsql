//! Comprehensive edge case tests for string functions
//!
//! This test suite provides extensive coverage of SQL string functions,
//! organized into focused modules for better maintainability.
//!
//! Test modules:
//! - `string_tests::basic_functions` - UPPER, LOWER, LENGTH, CHAR_LENGTH, OCTET_LENGTH, REVERSE
//! - `string_tests::substring_operations` - SUBSTRING, LEFT, RIGHT
//! - `string_tests::search_functions` - POSITION, INSTR, LOCATE
//! - `string_tests::transformations` - CONCAT, REPLACE, TRIM
//!
//! Each function is tested for:
//! - NULL handling
//! - Empty strings
//! - Unicode/multi-byte UTF-8 characters
//! - Boundary conditions
//! - Error conditions (wrong argument count/types)
//! - Type conversions (VARCHAR vs CHARACTER)

mod common;
mod string_tests;
