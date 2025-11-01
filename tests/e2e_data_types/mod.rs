//! End-to-end integration tests for SQL data types.
//!
//! Tests numeric types, character types, and basic query features like DISTINCT, GROUP BY, LIMIT.

// Allow approximate constants in tests - these are test data values, not mathematical constants
#![allow(clippy::approx_constant)]

// Shared fixtures for data type tests
mod fixtures;

// Test modules for different data type categories
mod basic_queries;
mod character_types;
mod numeric_types;
