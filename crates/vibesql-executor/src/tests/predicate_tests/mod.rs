//! Predicate and expression tests organized by category
//!
//! This module contains comprehensive tests for SQL predicates and expressions,
//! split into focused sub-modules for maintainability.
//!
//! # Test Coverage
//!
//! ## Predicates (SQL:1999 compliant)
//! - **IN/NOT IN** (`in_predicate.rs`) - List membership with NULL handling
//! - **LIKE/NOT LIKE** (`like_predicate.rs`) - Pattern matching with wildcards
//! - **BETWEEN/NOT BETWEEN** (`between_predicate.rs`) - Range predicates with boundaries
//!
//! ## String Functions (SQL:1999 standard)
//! - **POSITION** (`position_function.rs`) - Substring position with 1-based indexing
//! - **TRIM** (`trim_function.rs`) - Leading/trailing/both character removal
//!
//! ## Type Conversion
//! - **CAST** (`cast_expression.rs`) - Type coercion and NULL preservation
//!
//! # SQL Standard Compliance
//!
//! All tests verify compliance with SQL:1999 standards including:
//! - Three-valued logic (TRUE, FALSE, UNKNOWN)
//! - NULL propagation in expressions
//! - Standard predicate semantics
//! - Character string operations
//!
//! # Module Organization
//!
//! Each sub-module is self-contained with:
//! - Focused test coverage (< 250 lines per module)
//! - Clear documentation of SQL features tested
//! - Edge case coverage (NULL, boundaries, negation)

mod between_predicate;
mod cast_expression;
mod in_predicate;
mod in_subquery_multi_column_validation;
mod like_predicate;
mod position_function;
mod trim_function;
