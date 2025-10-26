//! Test modules for executor crate
//!
//! Tests are organized by feature area:
//! - `expression_eval`: Expression evaluator tests (literals, column refs, binary ops)
//! - `select_executor`: SELECT statement execution tests
//! - `limit_offset`: LIMIT/OFFSET pagination tests

mod expression_eval;
mod limit_offset;
mod select_executor;
