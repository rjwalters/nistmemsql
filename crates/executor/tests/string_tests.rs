//! Focused string function integration tests grouped by category.
//!
//! This test crate serves as the entry point for modular string function suites
//! that keep individual files concise while sharing common fixtures.

mod common;
#[path = "string_tests/mod.rs"]
mod suites;
