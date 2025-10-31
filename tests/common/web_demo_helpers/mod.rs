//! Common helpers for web demo SQL example tests
//!
//! This module provides shared functionality for testing web demo SQL examples,
//! broken down into focused submodules for better organization and maintainability.

// Public submodules
pub mod database_fixtures;
pub mod example_parsing;

// Re-export commonly used types and functions to maintain backward compatibility
pub use database_fixtures::{
    create_employees_db, create_empty_db, create_northwind_db, create_university_db,
    load_database,
};
pub use example_parsing::{extract_query, parse_example_files, parse_expected_results, WebDemoExample};
