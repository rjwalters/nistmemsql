//! String function edge case tests organized by category
//!
//! This module contains comprehensive edge case tests for SQL string functions,
//! split into focused sub-modules for better maintainability:
//!
//! - `basic_functions` - UPPER, LOWER, LENGTH, CHAR_LENGTH, OCTET_LENGTH, REVERSE
//! - `substring_operations` - SUBSTRING, LEFT, RIGHT
//! - `search_functions` - POSITION, INSTR, LOCATE
//! - `transformations` - CONCAT, REPLACE, TRIM
//!
//! Each sub-module tests:
//! - NULL handling
//! - Empty strings
//! - Unicode/multi-byte UTF-8 characters
//! - Boundary conditions
//! - Error conditions (wrong argument count/types)
//! - Type conversions (VARCHAR vs CHARACTER)

mod basic_functions;
mod search_functions;
mod substring_operations;
mod transformations;
