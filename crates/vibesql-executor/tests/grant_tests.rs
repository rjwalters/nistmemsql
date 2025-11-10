//! Integration tests for GRANT statement execution
//!
//! This test suite is organized into focused modules:
//! - table_privileges: Table-level privilege tests
//! - schema_privileges: Schema-level privilege tests
//! - grant_option: WITH GRANT OPTION tests
//! - edge_cases: Error conditions and validation

// Re-export all tests from submodules
mod grant_tests {
    pub mod edge_cases;
    pub mod grant_option;
    pub mod schema_privileges;
    pub mod table_privileges;
}
