//! Chunk-based WHERE clause predicate evaluation
//!
//! Processes rows in chunks of 256 for better instruction cache locality.
//! Single-pass evaluation avoids the overhead of bitmap allocation and double iteration.
//! Falls back to row-by-row evaluation for small row counts (< 100).

use crate::{
    errors::ExecutorError,
    evaluator::CombinedExpressionEvaluator,
};
use vibesql_storage::Row;
use vibesql_ast::Expression;

use super::{DEFAULT_CHUNK_SIZE, VECTORIZE_THRESHOLD};

/// Apply WHERE clause filter using chunk-based evaluation
///
/// Processes rows in chunks of DEFAULT_CHUNK_SIZE (256 rows) for improved:
/// - Instruction cache locality: Keeps predicate evaluation function hot
/// - Code locality: Tight inner loop enables better CPU pipeline efficiency
/// - Reduced overhead: Single-pass evaluation and filtering
///
/// This is NOT true vectorization/SIMD - it's chunking for cache benefits.
/// Automatically falls back to row-by-row for small datasets (< VECTORIZE_THRESHOLD).
pub fn apply_where_filter_vectorized<'a>(
    rows: Vec<Row>,
    where_expr: Option<&Expression>,
    evaluator: &CombinedExpressionEvaluator,
    executor: &crate::SelectExecutor<'a>,
) -> Result<Vec<Row>, ExecutorError> {
    // Early return if no WHERE clause
    if where_expr.is_none() {
        return Ok(rows);
    }

    let where_expr = where_expr.unwrap();
    let row_count = rows.len();

    // For small datasets, row-by-row is more efficient (less overhead)
    if row_count < VECTORIZE_THRESHOLD {
        return super::super::filter::apply_where_filter_combined(
            rows,
            Some(where_expr),
            evaluator,
            executor,
        );
    }

    // Use pooled buffer for result
    let mut filtered_rows = executor.query_buffer_pool().get_row_buffer(row_count);
    let mut rows_processed = 0;
    const CHECK_INTERVAL: usize = 1000;

    // Process rows in chunks (single-pass for cache efficiency)
    for chunk in rows.chunks(DEFAULT_CHUNK_SIZE) {
        // Check timeout periodically
        rows_processed += chunk.len();
        if rows_processed % CHECK_INTERVAL < DEFAULT_CHUNK_SIZE {
            executor.check_timeout()?;
        }

        // Single-pass evaluation and filtering within chunk
        // This keeps the predicate expression hot in instruction cache
        // while processing the chunk in one pass
        for row in chunk {
            let include_row = evaluate_predicate(row, where_expr, evaluator)?;

            if include_row {
                filtered_rows.push(row.clone());
            }
        }
    }

    // Clear CSE cache at end of query to prevent cross-query pollution
    // Cache can persist within a single query for performance, but must be
    // cleared between different SQL statements to avoid stale values
    evaluator.clear_cse_cache();

    // Return pooled buffer and extract result
    let result = std::mem::take(&mut filtered_rows);
    executor.query_buffer_pool().return_row_buffer(filtered_rows);
    Ok(result)
}

/// Evaluate WHERE predicate on a single row
///
/// Extracted as a helper function to keep it hot in instruction cache
/// when processing chunks. The tight loop in the caller enables better
/// CPU pipeline efficiency.
#[inline]
fn evaluate_predicate(
    row: &Row,
    where_expr: &Expression,
    evaluator: &CombinedExpressionEvaluator,
) -> Result<bool, ExecutorError> {
    // CSE cache is NOT cleared between rows because only deterministic expressions
    // (those without column references) are cached. Column values cannot be cached
    // since is_deterministic() returns false for expressions containing column refs.
    // This allows constant sub-expressions like (1 + 2) to be cached across all rows,
    // significantly improving performance for expression-heavy queries.

    match evaluator.eval(where_expr, row)? {
        vibesql_types::SqlValue::Boolean(true) => Ok(true),
        vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Null => Ok(false),
        // SQLLogicTest compatibility: treat integers as truthy/falsy (C-like behavior)
        vibesql_types::SqlValue::Integer(0) => Ok(false),
        vibesql_types::SqlValue::Integer(_) => Ok(true),
        vibesql_types::SqlValue::Smallint(0) => Ok(false),
        vibesql_types::SqlValue::Smallint(_) => Ok(true),
        vibesql_types::SqlValue::Bigint(0) => Ok(false),
        vibesql_types::SqlValue::Bigint(_) => Ok(true),
        vibesql_types::SqlValue::Float(0.0) => Ok(false),
        vibesql_types::SqlValue::Float(_) => Ok(true),
        vibesql_types::SqlValue::Real(0.0) => Ok(false),
        vibesql_types::SqlValue::Real(_) => Ok(true),
        vibesql_types::SqlValue::Double(0.0) => Ok(false),
        vibesql_types::SqlValue::Double(_) => Ok(true),
        other => Err(ExecutorError::InvalidWhereClause(format!(
            "WHERE clause must evaluate to boolean, got: {:?}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_size_threshold() {
        // Verify constants are reasonable for cache optimization
        assert!(VECTORIZE_THRESHOLD > 0);
        assert!(VECTORIZE_THRESHOLD < DEFAULT_CHUNK_SIZE);
        assert!(DEFAULT_CHUNK_SIZE > 0);
        assert!(DEFAULT_CHUNK_SIZE <= 1024); // Reasonable upper bound for L1 cache
    }

    #[test]
    fn test_chunk_size_cache_friendly() {
        // 256 rows * ~64 bytes/row = ~16KB, fits in typical L1 cache (32KB)
        assert_eq!(DEFAULT_CHUNK_SIZE, 256);

        // Threshold should be high enough to amortize chunking overhead
        assert!(VECTORIZE_THRESHOLD >= 100);
    }
}
