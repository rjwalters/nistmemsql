//! Test modules for executor crate
//!
//! Tests are organized by feature area:
//! - `expression_eval`: Expression evaluator tests (literals, column refs, binary ops)
//! - `limit_offset`: LIMIT/OFFSET pagination tests
//! - `select_basic`: Basic SELECT tests (wildcards, columns, ORDER BY)
//! - `select_where`: WHERE clause filtering tests
//! - `select_distinct`: DISTINCT keyword tests for duplicate removal
//! - `aggregates`: Aggregate functions (COUNT, SUM, GROUP BY, HAVING)
//! - `select_joins`: JOIN operation tests
//! - `scalar_subqueries`: Scalar subquery execution tests
//! - `error_display`: ExecutorError Display implementation tests
//! - `comparison_ops`: Comparison operator tests
//! - `between_predicates`: BETWEEN predicate execution tests

mod aggregates;
mod between_predicates;
mod comparison_ops;
mod error_display;
mod expression_eval;
mod limit_offset;
mod scalar_subqueries;
mod select_basic;
mod select_distinct;
mod select_joins;
mod select_where;
