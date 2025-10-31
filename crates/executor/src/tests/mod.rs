//! Test modules for executor crate
//!
//! Tests are organized by feature area:
//! - `expression_eval`: Expression evaluator tests (literals, column refs, binary ops)
//! - `limit_offset`: LIMIT/OFFSET pagination tests
//! - `select_basic`: Basic SELECT tests (wildcards, columns, ORDER BY)
//! - `select_where`: WHERE clause filtering tests
//! - `select_distinct`: DISTINCT keyword tests for duplicate removal
//! - `aggregate_count_sum_avg_tests`: COUNT, SUM, AVG functions with NULL handling
//! - `aggregate_min_max_tests`: MIN, MAX functions on integers and strings
//! - `aggregate_group_by_tests`: GROUP BY clause with aggregates
//! - `aggregate_having_tests`: HAVING clause filtering
//! - `aggregate_edge_case_tests`: Decimal precision, mixed types, CASE expressions
//! - `aggregate_distinct`: DISTINCT aggregation tests
//! - `select_joins`: JOIN operation tests
//! - `scalar_subquery_basic_tests`: Basic scalar subquery execution tests
//! - `scalar_subquery_error_tests`: Scalar subquery error handling tests
//! - `scalar_subquery_correlated_tests`: Correlated scalar subquery tests
//! - `error_display`: ExecutorError Display implementation tests
//! - `comparison_ops`: Comparison operator tests
//! - `between_predicates`: BETWEEN predicate execution tests
//! - `operator_edge_cases`: Unary operators, NULL propagation, complex nested expressions
//! - `predicate_variations`: IN/NOT IN, LIKE/NOT LIKE, POSITION, TRIM, CAST tests
//! - `privilege_checker_tests`: Privilege enforcement tests

mod aggregate_count_sum_avg_tests;
mod aggregate_distinct;
mod aggregate_edge_case_tests;
mod aggregate_group_by_tests;
mod aggregate_having_tests;
mod aggregate_min_max_tests;
mod between_predicates;
mod case_bug;
mod comparison_ops;
mod error_display;
mod expression_eval;
mod join_aggregation;
mod limit_offset;
mod operator_edge_cases;
mod predicate_variations;
mod scalar_subquery_basic_tests;
mod scalar_subquery_correlated_tests;
mod scalar_subquery_error_tests;
mod select_basic;
mod select_distinct;
mod select_into_tests;
mod select_joins;
mod select_where;
mod select_window_aggregate;
mod select_without_from;
mod transaction_tests;
mod privilege_checker_tests;
