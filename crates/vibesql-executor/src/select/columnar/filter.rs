//! Columnar filtering - efficient predicate evaluation on column data

use crate::{errors::ExecutorError, schema::CombinedSchema};
use vibesql_ast::{BinaryOperator, Expression};
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
/// * `get_value` - Closure to get a value at (row_index, column_index)
///
/// # Returns
///
/// A Vec<bool> where true means the row passes the filter
pub fn create_filter_bitmap<'a, F>(
    row_count: usize,
    predicates: &[ColumnPredicate],
    mut get_value: F,
) -> Result<Vec<bool>, ExecutorError>
where
    F: FnMut(usize, usize) -> Option<&'a SqlValue>,
{
    // If no predicates, all rows pass
    if predicates.is_empty() {
        return Ok(vec![true; row_count]);
    }

    let mut bitmap = vec![true; row_count];

    // Evaluate each row against all predicates (AND logic)
    for row_idx in 0..row_count {
        for predicate in predicates {
            let column_idx = match predicate {
                ColumnPredicate::LessThan { column_idx, .. } => *column_idx,
                ColumnPredicate::GreaterThan { column_idx, .. } => *column_idx,
                ColumnPredicate::GreaterThanOrEqual { column_idx, .. } => *column_idx,
                ColumnPredicate::LessThanOrEqual { column_idx, .. } => *column_idx,
                ColumnPredicate::Equal { column_idx, .. } => *column_idx,
                ColumnPredicate::Between { column_idx, .. } => *column_idx,
            };

            if let Some(value) = get_value(row_idx, column_idx) {
                if !evaluate_predicate(predicate, value) {
                    bitmap[row_idx] = false;
                    break; // Short-circuit: row failed, skip remaining predicates
                }
            } else {
                // NULL values fail all predicates
                bitmap[row_idx] = false;
                break;
            }
        }
    }

    Ok(bitmap)
}

/// Apply a columnar filter using a pre-computed bitmap
///
/// This is a convenience function that creates a filter bitmap
/// and returns the indices of rows that pass.
///
/// # Arguments
///
/// * `rows` - The rows to filter
/// * `predicates` - Column-based predicates to evaluate
///
/// # Returns
///
/// Indices of rows that pass all predicates
pub fn apply_columnar_filter(
    rows: &[vibesql_storage::Row],
    predicates: &[ColumnPredicate],
) -> Result<Vec<usize>, ExecutorError> {
    let bitmap = create_filter_bitmap(rows.len(), predicates, |row_idx, col_idx| {
        rows.get(row_idx).and_then(|row| row.get(col_idx))
    })?;
    Ok(bitmap
        .iter()
        .enumerate()
        .filter_map(|(idx, &pass)| if pass { Some(idx) } else { None })
        .collect())
}

/// Filter rows in place using columnar predicates
///
/// Returns a new Vec containing only the rows that pass all predicates.
/// This is the main entry point for columnar filtering.
///
/// # Arguments
///
/// * `rows` - The rows to filter
/// * `predicates` - Column-based predicates to evaluate
///
/// # Returns
///
/// Filtered rows
pub fn filter_rows(
    rows: Vec<vibesql_storage::Row>,
    predicates: &[ColumnPredicate],
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    if predicates.is_empty() {
        return Ok(rows);
    }

    let indices = apply_columnar_filter(&rows, predicates)?;
    Ok(indices.into_iter().filter_map(|idx| rows.get(idx).cloned()).collect())
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

/// Extract simple column predicates from a WHERE clause expression
///
/// This attempts to convert complex AST expressions into simple column predicates
/// that can be evaluated efficiently. Returns None if the expression is too complex
/// for columnar optimization.
///
/// Currently supports:
/// - Simple comparisons: column op literal (where op is <, >, <=, >=, =)
/// - BETWEEN: column BETWEEN literal AND literal
/// - AND combinations of the above
///
/// # Arguments
///
/// * `expr` - The WHERE clause expression
/// * `schema` - The schema to resolve column names to indices
///
/// # Returns
///
/// Some(predicates) if the expression can be converted to simple column predicates,
/// None if the expression is too complex for columnar optimization.
pub fn extract_column_predicates(
    expr: &Expression,
    schema: &CombinedSchema,
) -> Option<Vec<ColumnPredicate>> {
    let mut predicates = Vec::new();
    extract_predicates_recursive(expr, schema, &mut predicates)?;
    Some(predicates)
}

/// Recursively extract predicates from an expression
fn extract_predicates_recursive(
    expr: &Expression,
    schema: &CombinedSchema,
    predicates: &mut Vec<ColumnPredicate>,
) -> Option<()> {
    match expr {
        // AND: extract predicates from both sides
        Expression::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            extract_predicates_recursive(left, schema, predicates)?;
            extract_predicates_recursive(right, schema, predicates)?;
            Some(())
        }

        // Binary comparison: column op literal
        Expression::BinaryOp { left, op, right } => {
            // Try: column op literal
            if let (Expression::ColumnRef { table, column }, Expression::Literal(value)) =
                (left.as_ref(), right.as_ref())
            {
                let column_idx = schema.get_column_index(table.as_deref(), column)?;
                let predicate = match op {
                    BinaryOperator::LessThan => ColumnPredicate::LessThan {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::GreaterThan => ColumnPredicate::GreaterThan {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::LessThanOrEqual => ColumnPredicate::LessThanOrEqual {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::GreaterThanOrEqual => ColumnPredicate::GreaterThanOrEqual {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::Equal => ColumnPredicate::Equal {
                        column_idx,
                        value: value.clone(),
                    },
                    _ => return None, // Unsupported operator
                };
                predicates.push(predicate);
                return Some(());
            }

            // Try: literal op column (reverse the comparison)
            if let (Expression::Literal(value), Expression::ColumnRef { table, column }) =
                (left.as_ref(), right.as_ref())
            {
                let column_idx = schema.get_column_index(table.as_deref(), column)?;
                let predicate = match op {
                    // Reverse the comparison: literal < column => column > literal
                    BinaryOperator::LessThan => ColumnPredicate::GreaterThan {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::GreaterThan => ColumnPredicate::LessThan {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::LessThanOrEqual => ColumnPredicate::GreaterThanOrEqual {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::GreaterThanOrEqual => ColumnPredicate::LessThanOrEqual {
                        column_idx,
                        value: value.clone(),
                    },
                    BinaryOperator::Equal => ColumnPredicate::Equal {
                        column_idx,
                        value: value.clone(),
                    },
                    _ => return None, // Unsupported operator
                };
                predicates.push(predicate);
                return Some(());
            }

            None
        }

        // BETWEEN: column BETWEEN low AND high
        Expression::Between {
            expr: inner,
            low,
            high,
            negated: false,
            symmetric: _,
        } => {
            if let Expression::ColumnRef { table, column } = inner.as_ref() {
                if let (Expression::Literal(low_val), Expression::Literal(high_val)) =
                    (low.as_ref(), high.as_ref())
                {
                    let column_idx = schema.get_column_index(table.as_deref(), column)?;
                    predicates.push(ColumnPredicate::Between {
                        column_idx,
                        low: low_val.clone(),
                        high: high_val.clone(),
                    });
                    return Some(());
                }
            }
            None
        }

        // Any other expression is too complex
        _ => None,
    }
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
        use vibesql_storage::Row;

        let rows = vec![
            Row::new(vec![SqlValue::Integer(5)]),
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(15)]),
            Row::new(vec![SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(25)]),
        ];

        // Test with no predicates - all rows should pass
        let bitmap = create_filter_bitmap(rows.len(), &[], |row_idx, col_idx| {
            rows.get(row_idx).and_then(|row| row.get(col_idx))
        })
        .unwrap();
        assert_eq!(bitmap.len(), 5);
        assert!(bitmap.iter().all(|&x| x));

        // Test with LessThan predicate
        let predicates = vec![ColumnPredicate::LessThan {
            column_idx: 0,
            value: SqlValue::Integer(18),
        }];
        let bitmap = create_filter_bitmap(rows.len(), &predicates, |row_idx, col_idx| {
            rows.get(row_idx).and_then(|row| row.get(col_idx))
        })
        .unwrap();
        assert_eq!(bitmap, vec![true, true, true, false, false]);
    }
}
