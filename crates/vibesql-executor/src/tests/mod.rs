//! Test modules for executor crate
//!
//! Tests are organized by feature area:
//! - `expression_eval`: Expression evaluator tests (literals, column refs, binary ops)
//! - `limit_offset`: LIMIT/OFFSET pagination tests
//! - `select_basic_projection`: Basic SELECT projection tests (wildcards, specific columns)
//! - `select_order_by`: ORDER BY clause tests
//! - `select_derived_columns`: Derived column lists (SQL:1999 E051-07/08) tests
//! - `select_where`: WHERE clause filtering tests
//! - `select_distinct`: DISTINCT keyword tests for duplicate removal
//! - `aggregate_count_sum_avg_tests`: COUNT, SUM, AVG functions with NULL handling
//! - `aggregate_min_max_tests`: MIN, MAX functions on integers and strings
//! - `aggregate_group_by_tests`: GROUP BY clause with aggregates
//! - `aggregate_having_tests`: HAVING clause filtering
//! - `aggregate_edge_case_tests`: Decimal precision, mixed types, CASE expressions
//! - `aggregate_distinct`: DISTINCT aggregation tests
//! - `aggregate_without_from`: Aggregate functions without FROM clause (issue #937)
//! - `aggregate_caching`: Aggregate result caching tests (Phase 2 of issue #1038)
//! - `select_joins`: JOIN operation tests
//! - `scalar_subquery_basic_tests`: Basic scalar subquery execution tests
//! - `scalar_subquery_error_tests`: Scalar subquery error handling tests
//! - `scalar_subquery_correlated_tests`: Correlated scalar subquery tests
//! - `error_display`: ExecutorError Display implementation tests
//! - `comparison_ops`: Comparison operator tests
//! - `between_predicates`: BETWEEN predicate execution tests
//! - `operator_edge_cases`: Unary operators, NULL propagation, complex nested expressions
//! - `predicate_tests`: IN/NOT IN, LIKE/NOT LIKE, BETWEEN, POSITION, TRIM, CAST tests (organized by
//!   type)
//! - `predicate_pushdown`: Table-local predicate pushdown optimization tests (Phase 2)
//! - `privilege_checker_tests`: Privilege enforcement tests
//! - `query_timeout_tests`: Query timeout enforcement tests (issue #1014)
//! - `create_table_tests`: CREATE TABLE executor tests (basic table creation, data types, spatial
//!   types)
//! - `fulltext_search`: Full-text search integration tests (MATCH...AGAINST natural language, boolean mode)

mod aggregate_caching;
mod aggregate_count_sum_avg_tests;
mod aggregate_distinct;
mod aggregate_edge_case_tests;
mod aggregate_group_by_tests;
mod aggregate_having_tests;
mod aggregate_min_max_tests;
mod aggregate_without_from;
mod between_predicates;
mod case_bug;
mod comparison_ops;
mod count_star_fast_path;
mod create_table_constraints;
mod create_table_tests;
mod error_display;
mod expression_eval;
mod fulltext_search;
mod issue_938_integer_type_preservation;
mod join_aggregation;
mod lazy_evaluation_tests;
mod limit_offset;
mod operator_edge_cases;
mod phase3_join_optimization;
mod predicate_pushdown;
mod predicate_tests;
mod privilege_checker_tests;
mod query_timeout_tests;
mod scalar_subquery_basic_tests;
mod scalar_subquery_correlated_tests;
mod scalar_subquery_error_tests;
mod select_basic_projection;
mod select_derived_columns;
mod select_distinct;
mod select_into_tests;
mod select_joins;
mod select_order_by;
mod select_where;
mod select_window_aggregate;
mod select_without_from;
mod timeout_enforcement;
mod transaction_tests;
mod trigger_tests;
