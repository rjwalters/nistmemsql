//! Index scan execution
//!
//! Executes index scans to retrieve rows from tables using indexes.

use vibesql_ast::Expression;
use vibesql_storage::{Database, Row};

use crate::{errors::ExecutorError, schema::CombinedSchema};

use super::predicate::{extract_index_predicate, IndexPredicate};

/// Execute an index scan
///
/// Uses the specified index to retrieve matching rows, then fetches full rows from the table.
/// This implements the "index scan + fetch" strategy with optimized range scans.
///
/// If sorted_columns is provided, the function preserves index order and returns results
/// marked as pre-sorted, allowing the caller to skip ORDER BY sorting.
///
/// # Performance Optimization
/// When the WHERE clause can be fully satisfied by the index predicate (e.g., simple
/// predicates like `WHERE col = 5` or `WHERE col BETWEEN 10 AND 20`), we skip redundant
/// WHERE clause re-evaluation, significantly improving performance for large result sets.
pub(crate) fn execute_index_scan(
    table_name: &str,
    index_name: &str,
    alias: Option<&String>,
    where_clause: Option<&Expression>,
    sorted_columns: Option<Vec<(String, vibesql_ast::OrderDirection)>>,
    database: &Database,
) -> Result<super::super::FromResult, ExecutorError> {
    // Get table and index
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let index_metadata = database
        .get_index(index_name)
        .ok_or_else(|| ExecutorError::IndexNotFound(index_name.to_string()))?;

    let index_data = database
        .get_index_data(index_name)
        .ok_or_else(|| ExecutorError::IndexNotFound(index_name.to_string()))?;

    // Get the first indexed column (for predicate extraction)
    let indexed_column = index_metadata
        .columns
        .first()
        .map(|col| col.column_name.as_str())
        .unwrap_or("");

    // Try to extract index predicate (range or IN) for the indexed column
    let index_predicate = where_clause.and_then(|expr| extract_index_predicate(expr, indexed_column));

    // DISABLED: Performance optimization was buggy (see issue #1867)
    // The `where_clause_fully_satisfied_by_index()` function incorrectly determined
    // when WHERE filtering could be skipped, causing wrong results.
    // For now, always apply WHERE filtering when present for correctness.
    // TODO: Reimplement this optimization correctly in a future PR
    let need_where_filter = where_clause.is_some();

    // Get row indices using the appropriate index operation
    let matching_row_indices: Vec<usize> = match index_predicate {
        Some(IndexPredicate::Range(range)) => {
            // Validate bounds: if start > end, the range is empty
            if let (Some(start_val), Some(end_val)) = (&range.start, &range.end) {
                let gt_result = crate::evaluator::ExpressionEvaluator::eval_binary_op_static(
                    start_val,
                    &vibesql_ast::BinaryOperator::GreaterThan,
                    end_val,
                )?;
                if let vibesql_types::SqlValue::Boolean(true) = gt_result {
                    // start_val > end_val: empty range, return no rows
                    Vec::new()
                } else {
                    // Valid range, use storage layer's optimized range_scan
                    index_data.range_scan(
                        range.start.as_ref(),
                        range.end.as_ref(),
                        range.inclusive_start,
                        range.inclusive_end,
                    )
                }
            } else {
                // Use storage layer's optimized range_scan for >, <, >=, <=, BETWEEN
                index_data.range_scan(
                    range.start.as_ref(),
                    range.end.as_ref(),
                    range.inclusive_start,
                    range.inclusive_end,
                )
            }
        }
        Some(IndexPredicate::In(values)) => {
            // Use storage layer's multi_lookup for IN predicates
            index_data.multi_lookup(&values)
        }
        None => {
            // Full index scan - collect all row indices from the index in index key order
            // (Will be sorted by row index later if needed, see lines 425-427)
            index_data
                .values()
                .flatten()
                .copied()
                .collect()
        }
    };

    // If we're not returning sorted results, ensure rows are in table order (by row index)
    // This is important when the index doesn't satisfy the ORDER BY clause.
    // Without this, rows would be returned in index key order, which would cause
    // incorrect results when ORDER BY specifies a different column.
    let mut matching_row_indices = matching_row_indices;
    if sorted_columns.is_none() {
        matching_row_indices.sort_unstable();
    }

    // Fetch rows from table
    let all_rows = table.scan();
    let mut rows: Vec<Row> = matching_row_indices
        .into_iter()
        .filter_map(|idx| all_rows.get(idx).cloned())
        .collect();

    // Reverse rows if needed for DESC index ordering
    // BTreeMap iteration is always ascending, but for DESC indexes we need descending order
    // Check if we're using this index for ORDER BY and if the first column is DESC
    if sorted_columns.is_some() {
        if let Some(first_index_col) = index_metadata.columns.first() {
            if first_index_col.direction == vibesql_ast::OrderDirection::Desc {
                rows.reverse();
            }
        }
    }

    // Build schema
    let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
    let schema = CombinedSchema::from_table(effective_name, table.schema.clone());

    // Apply WHERE clause predicates if needed
    // Performance optimization: Skip WHERE clause evaluation if the index already
    // guarantees all rows satisfy the predicate (e.g., simple predicates like
    // `WHERE col = 5` or `WHERE col BETWEEN 10 AND 20`).
    //
    // We still need to filter when:
    // - Predicates involve non-indexed columns
    // - Complex predicates that couldn't be fully pushed to index
    // - OR predicates (not yet optimized for index pushdown)
    // - Multi-column predicates where only first column was indexed
    if need_where_filter {
        if let Some(where_expr) = where_clause {
            use crate::evaluator::CombinedExpressionEvaluator;

            // Create evaluator for filtering
            let evaluator = CombinedExpressionEvaluator::with_database(&schema, database);

            // Apply full WHERE clause to each row
            let mut filtered_rows = Vec::new();
            for row in rows {
                // Clear CSE cache before evaluating each row
                evaluator.clear_cse_cache();

                let eval_result = evaluator.eval(where_expr, &row)?;
                let include_row = match eval_result {
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
                    filtered_rows.push(row);
                }
            }
            rows = filtered_rows;
        }
    }

    // Return results with sorting metadata if available
    // If WHERE clause was fully handled by index (!need_where_filter), indicate this
    // so the executor doesn't redundantly re-apply WHERE filtering
    if !need_where_filter {
        Ok(super::super::FromResult::from_rows_where_filtered(schema, rows, sorted_columns))
    } else {
        match sorted_columns {
            Some(sorted) => Ok(super::super::FromResult::from_rows_sorted(schema, rows, sorted)),
            None => Ok(super::super::FromResult::from_rows(schema, rows)),
        }
    }
}
