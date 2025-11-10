//! Helper functions for SELECT query execution

use indexmap::IndexSet;

/// Apply DISTINCT to remove duplicate rows
///
/// Uses an IndexSet to track unique rows while preserving insertion order.
/// This ensures deterministic results that match SQLite's behavior.
/// This requires SqlValue to implement Hash and Eq, which we've implemented
/// with SQL semantics:
/// - NULL == NULL for grouping
/// - NaN == NaN for grouping
pub(super) fn apply_distinct(rows: Vec<vibesql_storage::Row>) -> Vec<vibesql_storage::Row> {
    let mut seen = IndexSet::new();
    let mut result = Vec::new();

    for row in rows {
        // Try to insert the row's values into the set
        // If insertion succeeds (wasn't already present), keep the row
        if seen.insert(row.values.clone()) {
            result.push(row);
        }
    }

    result
}

/// Apply LIMIT and OFFSET to a result set
pub(super) fn apply_limit_offset(
    rows: Vec<vibesql_storage::Row>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Vec<vibesql_storage::Row> {
    let start = offset.unwrap_or(0);
    if start >= rows.len() {
        return Vec::new();
    }

    let max_take = rows.len() - start;
    let take = limit.unwrap_or(max_take).min(max_take);

    rows.into_iter().skip(start).take(take).collect()
}
