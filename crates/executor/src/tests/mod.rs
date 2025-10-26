//! Test modules for executor crate
//!
//! Tests are organized by feature area:
//! - `expression_eval`: Expression evaluator tests (literals, column refs, binary ops)
//! - `select_basic`: Basic SELECT tests (no JOINs, aggregates, or ORDER BY)
//! - `select_executor`: Advanced SELECT tests (ORDER BY, JOIN, GROUP BY, aggregates)
//! - `limit_offset`: LIMIT/OFFSET pagination tests

mod expression_eval;
mod limit_offset;
mod select_basic;
mod select_executor;
