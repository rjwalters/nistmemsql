//! Columnar filtering - efficient predicate evaluation on column data

use crate::errors::ExecutorError;
use vibesql_types::SqlValue;

/// Apply a filter to row indices based on column predicates
///
/// Returns a bitmap of which rows pass the filter.
/// This avoids creating intermediate Row objects.
///
/// # Arguments
///
/// * `row_count` - Total number of rows
/// * `predicates` - Column-based predicates to evaluate
///
/// # Returns
///
/// A Vec<bool> where true means the row passes the filter
pub fn create_filter_bitmap(
    row_count: usize,
    _predicates: &[ColumnPredicate],
) -> Result<Vec<bool>, ExecutorError> {
    // For now, include all rows (no filtering)
    // TODO: Implement actual predicate evaluation
    Ok(vec![true; row_count])
}

/// Apply a columnar filter using a pre-computed bitmap
///
/// This is a convenience function that creates a filter bitmap
/// and returns the indices of rows that pass.
pub fn apply_columnar_filter(
    row_count: usize,
    predicates: &[ColumnPredicate],
) -> Result<Vec<usize>, ExecutorError> {
    let bitmap = create_filter_bitmap(row_count, predicates)?;
    Ok(bitmap
        .iter()
        .enumerate()
        .filter_map(|(idx, &pass)| if pass { Some(idx) } else { None })
        .collect())
}

/// A predicate on a single column
///
/// Represents filters like: `column_idx < 24` or `column_idx BETWEEN 0.05 AND 0.07`
#[derive(Debug, Clone)]
pub enum ColumnPredicate {
    /// column < value
    LessThan { column_idx: usize, value: SqlValue },

    /// column > value
    GreaterThan { column_idx: usize, value: SqlValue },

    /// column >= value
    GreaterThanOrEqual { column_idx: usize, value: SqlValue },

    /// column <= value
    LessThanOrEqual { column_idx: usize, value: SqlValue },

    /// column = value
    Equal { column_idx: usize, value: SqlValue },

    /// column BETWEEN low AND high
    Between {
        column_idx: usize,
        low: SqlValue,
        high: SqlValue,
    },
}

/// Evaluate a column predicate on a specific value
///
/// Returns true if the value satisfies the predicate
pub fn evaluate_predicate(predicate: &ColumnPredicate, value: &SqlValue) -> bool {
    match predicate {
        ColumnPredicate::LessThan { value: threshold, .. } => {
            compare_values(value, threshold) == std::cmp::Ordering::Less
        }
        ColumnPredicate::GreaterThan { value: threshold, .. } => {
            compare_values(value, threshold) == std::cmp::Ordering::Greater
        }
        ColumnPredicate::GreaterThanOrEqual { value: threshold, .. } => {
            matches!(
                compare_values(value, threshold),
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
            )
        }
        ColumnPredicate::LessThanOrEqual { value: threshold, .. } => {
            matches!(
                compare_values(value, threshold),
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal
            )
        }
        ColumnPredicate::Equal { value: target, .. } => {
            compare_values(value, target) == std::cmp::Ordering::Equal
        }
        ColumnPredicate::Between { low, high, .. } => {
            matches!(
                compare_values(value, low),
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
            ) && matches!(
                compare_values(value, high),
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal
            )
        }
    }
}

/// Compare two SqlValues for ordering
///
/// Simplified comparison for common numeric types
fn compare_values(a: &SqlValue, b: &SqlValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match (a, b) {
        (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
        (SqlValue::Bigint(a), SqlValue::Bigint(b)) => a.cmp(b),
        (SqlValue::Smallint(a), SqlValue::Smallint(b)) => a.cmp(b),
        (SqlValue::Float(a), SqlValue::Float(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Double(a), SqlValue::Double(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Numeric(a), SqlValue::Numeric(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Varchar(a), SqlValue::Varchar(b)) => a.cmp(b),
        (SqlValue::Character(a), SqlValue::Character(b)) => a.cmp(b),
        (SqlValue::Date(a), SqlValue::Date(b)) => a.cmp(b),
        _ => Ordering::Equal, // Fallback for mixed types
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_less_than_predicate() {
        let pred = ColumnPredicate::LessThan {
            column_idx: 0,
            value: SqlValue::Integer(10),
        };

        assert!(evaluate_predicate(&pred, &SqlValue::Integer(5)));
        assert!(!evaluate_predicate(&pred, &SqlValue::Integer(10)));
        assert!(!evaluate_predicate(&pred, &SqlValue::Integer(15)));
    }

    #[test]
    fn test_between_predicate() {
        let pred = ColumnPredicate::Between {
            column_idx: 0,
            low: SqlValue::Double(0.05),
            high: SqlValue::Double(0.07),
        };

        assert!(evaluate_predicate(&pred, &SqlValue::Double(0.06)));
        assert!(evaluate_predicate(&pred, &SqlValue::Double(0.05)));
        assert!(evaluate_predicate(&pred, &SqlValue::Double(0.07)));
        assert!(!evaluate_predicate(&pred, &SqlValue::Double(0.04)));
        assert!(!evaluate_predicate(&pred, &SqlValue::Double(0.08)));
    }

    #[test]
    fn test_filter_bitmap() {
        let row_count = 5;
        let predicates = vec![];

        let bitmap = create_filter_bitmap(row_count, &predicates).unwrap();
        assert_eq!(bitmap.len(), 5);
        assert!(bitmap.iter().all(|&x| x)); // All true for now
    }
}
