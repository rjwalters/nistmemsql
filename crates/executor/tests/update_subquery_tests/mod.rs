//! UPDATE statement subquery tests
//!
//! This module contains comprehensive tests for UPDATE statements with subqueries,
//! organized into focused sub-modules for maintainability.
//!
//! ## Test Organization
//!
//! Tests are split by subquery usage pattern:
//!
//! - **simple_scalar_subqueries**: Tests for scalar subqueries in SET clauses (single value
//!   returns)
//! - **error_cases**: Tests for error handling (multiple rows, multiple columns, type mismatches)
//! - **where_in_subqueries**: Tests for IN/NOT IN subqueries in WHERE clauses
//! - **where_comparison_subqueries**: Tests for comparison operators with scalar subqueries in
//!   WHERE clauses
//!
//! ## Test Coverage
//!
//! ### Simple Scalar Subqueries (SET clause)
//! - Single value subqueries
//! - Aggregate functions (MAX, MIN, AVG)
//! - NULL handling
//! - Empty result sets
//! - Multiple subqueries in one UPDATE
//! - Subqueries with WHERE clause filtering
//!
//! ### Error Cases
//! - Subquery returning multiple rows (should error)
//! - Subquery returning multiple columns (should error)
//!
//! ### WHERE Clause Subqueries
//! - IN operator with subqueries
//! - NOT IN operator with subqueries
//! - Comparison operators (=, <, >, etc.) with scalar subqueries
//! - Empty result sets in subqueries
//! - NULL handling in subqueries
//! - Aggregate functions in subqueries
//! - Complex conditions with subqueries
//! - Combined SET and WHERE subqueries
//!
//! ## Related Issues
//! - Issue #353: UPDATE WHERE with subquery support
//! - Issue #619: Refactor large test file into focused modules

mod error_cases;
mod simple_scalar_subqueries;
mod where_comparison_subqueries;
mod where_in_subqueries;
