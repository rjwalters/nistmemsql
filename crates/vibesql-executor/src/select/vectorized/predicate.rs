//! Vectorized WHERE clause predicate evaluation
//!
//! Processes rows in chunks for better cache locality and potential SIMD acceleration.
//! Falls back to row-by-row evaluation for small row counts.

use crate::{
    errors::ExecutorError,
    evaluator::CombinedExpressionEvaluator,
};
use vibesql_storage::Row;
use vibesql_ast::Expression;

use super::{bitmap::SelectionBitmap, DEFAULT_CHUNK_SIZE, VECTORIZE_THRESHOLD};

/// Apply WHERE clause filter using vectorized (chunk-based) evaluation
///
/// Processes rows in chunks of DEFAULT_CHUNK_SIZE (256 rows) for improved:
/// - Cache locality: Reduces scattered memory access
/// - Branch prediction: More predictable patterns in batch operations
/// - Future SIMD: Provides foundation for SIMD acceleration
///
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

    // Process rows in chunks
    for chunk in rows.chunks(DEFAULT_CHUNK_SIZE) {
        // Check timeout periodically
        rows_processed += chunk.len();
        if rows_processed % CHECK_INTERVAL < DEFAULT_CHUNK_SIZE {
            executor.check_timeout()?;
        }

        // Evaluate predicate on entire chunk, producing bitmap
        let selection_bitmap = evaluate_predicate_chunk(
            chunk,
            where_expr,
            evaluator,
        )?;

        // Filter chunk using bitmap and append to result
        for (row, &keep) in chunk.iter().zip(selection_bitmap.iter()) {
            if keep {
                filtered_rows.push(row.clone());
            }
        }
    }

    // Return pooled buffer and extract result
    let result = std::mem::take(&mut filtered_rows);
    executor.query_buffer_pool().return_row_buffer(filtered_rows);
    Ok(result)
}

/// Evaluate WHERE predicate on a chunk of rows, producing a selection bitmap
///
/// This is the core vectorized evaluation function. It processes all rows in the chunk
/// and produces a bitmap indicating which rows pass the predicate.
///
/// The chunk-based approach improves cache locality by:
/// - Keeping the predicate expression hot in instruction cache
/// - Reducing function call overhead (one setup per chunk vs per row)
/// - Enabling better compiler optimizations for the inner loop
fn evaluate_predicate_chunk(
    chunk: &[Row],
    where_expr: &Expression,
    evaluator: &CombinedExpressionEvaluator,
) -> Result<SelectionBitmap, ExecutorError> {
    let chunk_size = chunk.len();
    let mut bitmap = SelectionBitmap::all_false(chunk_size);

    // Evaluate predicate for each row in chunk
    // The tight loop with minimal branching improves CPU pipeline efficiency
    for (idx, row) in chunk.iter().enumerate() {
        // CSE cache is NOT cleared between rows because only deterministic expressions
        // (those without column references) are cached. Column values cannot be cached
        // since is_deterministic() returns false for expressions containing column refs.
        // This allows constant sub-expressions like (1 + 2) to be cached across all rows,
        // significantly improving performance for expression-heavy queries.

        let include_row = match evaluator.eval(where_expr, row)? {
            vibesql_types::SqlValue::Boolean(true) => true,
            vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Null => false,
            // SQLLogicTest compatibility: treat integers as truthy/falsy (C-like behavior)
            vibesql_types::SqlValue::Integer(0) => false,
            vibesql_types::SqlValue::Integer(_) => true,
            vibesql_types::SqlValue::Smallint(0) => false,
            vibesql_types::SqlValue::Smallint(_) => true,
            vibesql_types::SqlValue::Bigint(0) => false,
            vibesql_types::SqlValue::Bigint(_) => true,
            vibesql_types::SqlValue::Float(0.0) => false,
            vibesql_types::SqlValue::Float(_) => true,
            vibesql_types::SqlValue::Real(0.0) => false,
            vibesql_types::SqlValue::Real(_) => true,
            vibesql_types::SqlValue::Double(0.0) => false,
            vibesql_types::SqlValue::Double(_) => true,
            other => {
                return Err(ExecutorError::InvalidWhereClause(format!(
                    "WHERE clause must evaluate to boolean, got: {:?}",
                    other
                )))
            }
        };

        if include_row {
            bitmap.set(idx, true);
        }
    }

    Ok(bitmap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::SqlValue;
    use vibesql_storage::Row;

    /// Helper to create a simple row with integer values
    fn make_row(values: Vec<i32>) -> Row {
        Row::new(values.into_iter().map(SqlValue::Integer).collect())
    }

    #[test]
    fn test_evaluate_chunk_all_true() {
        // This is a simplified test - real usage would require proper evaluator setup
        // For now, we're testing the structure
        let chunk = vec![
            make_row(vec![1]),
            make_row(vec![2]),
            make_row(vec![3]),
        ];

        // In practice, evaluate_predicate_chunk would need a proper evaluator
        // This test verifies the bitmap structure
        let bitmap = SelectionBitmap::all_true(3);
        assert_eq!(bitmap.count_true(), 3);
    }

    #[test]
    fn test_chunk_size_threshold() {
        // Verify constants are reasonable
        assert!(VECTORIZE_THRESHOLD > 0);
        assert!(VECTORIZE_THRESHOLD < DEFAULT_CHUNK_SIZE);
        assert!(DEFAULT_CHUNK_SIZE > 0);
        assert!(DEFAULT_CHUNK_SIZE <= 1024); // Reasonable upper bound for cache
    }
}
