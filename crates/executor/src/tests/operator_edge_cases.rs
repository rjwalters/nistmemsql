//! Operator edge case tests
//!
//! Comprehensive tests for operator edge cases organized by operator category:
//! - Unary operators (Plus, Minus)
//! - Binary arithmetic operators
//! - Comparison operators
//! - NULL propagation in all operators
//! - String concatenation edge cases
//!
//! Tests are split into focused modules with shared utilities to eliminate
//! duplication and improve maintainability.

// Shared test utilities
mod operator_test_utils;

// Operator category modules
mod binary_arithmetic;
mod comparison_operators;
mod null_propagation;
mod string_concatenation;
mod unary_operators;
