//! Set operations (UNION, INTERSECT, EXCEPT) for SELECT queries

use std::collections::{HashMap, HashSet};
use crate::errors::ExecutorError;
use super::helpers::apply_distinct;

/// Apply a set operation (UNION, INTERSECT, EXCEPT) to two result sets
pub(super) fn apply_set_operation(
    left: Vec<storage::Row>,
    right: Vec<storage::Row>,
    set_op: &ast::SetOperation,
) -> Result<Vec<storage::Row>, ExecutorError> {
    // Validate that both result sets have the same number of columns
    if !left.is_empty() && !right.is_empty() {
        let left_cols = left[0].values.len();
        let right_cols = right[0].values.len();
        if left_cols != right_cols {
            return Err(ExecutorError::SubqueryColumnCountMismatch {
                expected: left_cols,
                actual: right_cols,
            });
        }
    }

    match set_op.op {
        ast::SetOperator::Union => {
            if set_op.all {
                // UNION ALL: combine all rows from both queries
                let mut result = left;
                result.extend(right);
                Ok(result)
            } else {
                // UNION (DISTINCT): combine and remove duplicates
                let mut result = left;
                result.extend(right);
                Ok(apply_distinct(result))
            }
        }

        ast::SetOperator::Intersect => {
            if set_op.all {
                // INTERSECT ALL: return rows that appear in both (with multiplicity)
                // Count occurrences in right side
                let mut right_counts = HashMap::new();
                for row in &right {
                    *right_counts.entry(row.values.clone()).or_insert(0) += 1;
                }

                // For each left row, if it appears in right, include it and decrement count
                let mut result = Vec::new();
                for row in left {
                    if let Some(count) = right_counts.get_mut(&row.values) {
                        if *count > 0 {
                            result.push(row);
                            *count -= 1;
                        }
                    }
                }
                Ok(result)
            } else {
                // INTERSECT (DISTINCT): return unique rows that appear in both
                let right_set: HashSet<_> =
                    right.iter().map(|row| row.values.clone()).collect();

                let mut result = Vec::new();
                let mut seen = HashSet::new();
                for row in left {
                    if right_set.contains(&row.values) && seen.insert(row.values.clone()) {
                        result.push(row);
                    }
                }
                Ok(result)
            }
        }

        ast::SetOperator::Except => {
            if set_op.all {
                // EXCEPT ALL: return rows from left that don't appear in right (with multiplicity)
                // Count occurrences in right side
                let mut right_counts = HashMap::new();
                for row in &right {
                    *right_counts.entry(row.values.clone()).or_insert(0) += 1;
                }

                // For each left row, if it doesn't appear in right (or count exhausted), include it
                let mut result = Vec::new();
                for row in left {
                    match right_counts.get_mut(&row.values) {
                        None => {
                            // Row not in right side, include it
                            result.push(row);
                        }
                        Some(count) if *count == 0 => {
                            // All instances from right side already used, include it
                            result.push(row);
                        }
                        Some(count) => {
                            // Row exists in right side, decrement count (exclude this instance)
                            *count -= 1;
                        }
                    }
                }
                Ok(result)
            } else {
                // EXCEPT (DISTINCT): return unique rows from left that don't appear in right
                let right_set: HashSet<_> =
                    right.iter().map(|row| row.values.clone()).collect();

                let mut result = Vec::new();
                let mut seen = HashSet::new();
                for row in left {
                    if !right_set.contains(&row.values) && seen.insert(row.values.clone()) {
                        result.push(row);
                    }
                }
                Ok(result)
            }
        }
    }
}
