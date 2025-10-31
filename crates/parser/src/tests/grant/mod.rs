//! Tests for GRANT statement parsing
//!
//! This module contains tests organized by privilege type and object category:
//! - `grant_test_utils`: Shared helper functions for AST/schema builders and assertions
//! - `table_privileges`: Table-level privileges (SELECT, INSERT, UPDATE, DELETE, REFERENCES, ALL)
//! - `schema_privileges`: Schema privileges (USAGE, CREATE)
//! - `column_privileges`: Column-level privileges (UPDATE(cols), REFERENCES(cols))
//! - `routine_privileges`: Routine/function/procedure privileges (EXECUTE)
//! - `grant_options`: WITH GRANT OPTION clause tests
//! - `parsing_edge_cases`: Edge cases and implicit defaults

mod column_privileges;
mod grant_options;
mod grant_test_utils;
mod parsing_edge_cases;
mod routine_privileges;
mod schema_privileges;
mod table_privileges;
