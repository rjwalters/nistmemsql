use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

/// Accumulator for aggregate functions
#[derive(Debug, Clone)]
pub(super) enum AggregateAccumulator {
    Count { count: i64, distinct: bool, seen: Option<HashSet<types::SqlValue>> },
    Sum { sum: types::SqlValue, distinct: bool, seen: Option<HashSet<types::SqlValue>> },
    Avg { sum: types::SqlValue, count: i64, distinct: bool, seen: Option<HashSet<types::SqlValue>> },
    Min { value: Option<types::SqlValue>, distinct: bool, seen: Option<HashSet<types::SqlValue>> },
    Max { value: Option<types::SqlValue>, distinct: bool, seen: Option<HashSet<types::SqlValue>> },
}

impl AggregateAccumulator {
    pub(super) fn new(
        function_name: &str,
        distinct: bool,
    ) -> Result<Self, crate::errors::ExecutorError> {
        let seen = if distinct { Some(HashSet::new()) } else { None };
        match function_name.to_uppercase().as_str() {
            "COUNT" => Ok(AggregateAccumulator::Count { count: 0, distinct, seen }),
            "SUM" => {
                Ok(AggregateAccumulator::Sum { sum: types::SqlValue::Integer(0), distinct, seen })
            }
            "AVG" => Ok(AggregateAccumulator::Avg {
                sum: types::SqlValue::Integer(0),
                count: 0,
                distinct,
                seen,
            }),
            "MIN" => Ok(AggregateAccumulator::Min { value: None, distinct, seen }),
            "MAX" => Ok(AggregateAccumulator::Max { value: None, distinct, seen }),
            _ => Err(crate::errors::ExecutorError::UnsupportedExpression(format!(
                "Unknown aggregate function: {}",
                function_name
            ))),
        }
    }

    pub(super) fn accumulate(&mut self, value: &types::SqlValue) {
        match self {
            // COUNT - counts non-NULL values
            AggregateAccumulator::Count { ref mut count, distinct, seen } => {
                if value.is_null() {
                    return; // Skip NULL values
                }

                if *distinct {
                    // Only count if we haven't seen this value before
                    if seen.as_mut().unwrap().insert(value.clone()) {
                        *count += 1;
                    }
                } else {
                    *count += 1;
                }
            }

            // SUM - sums numeric values (all numeric types), ignores NULLs
            AggregateAccumulator::Sum { ref mut sum, distinct, seen } => {
                match value {
                    types::SqlValue::Null => {} // Skip NULL
                    types::SqlValue::Integer(_)
                    | types::SqlValue::Smallint(_)
                    | types::SqlValue::Bigint(_)
                    | types::SqlValue::Numeric(_)
                    | types::SqlValue::Float(_)
                    | types::SqlValue::Real(_)
                    | types::SqlValue::Double(_) => {
                        if *distinct {
                            // Only sum if we haven't seen this value before
                            if seen.as_mut().unwrap().insert(value.clone()) {
                                *sum = add_sql_values(sum, value);
                            }
                        } else {
                            *sum = add_sql_values(sum, value);
                        }
                    }
                    _ => {} // Type mismatch - ignore
                }
            }

            // AVG - computes average of numeric values (all numeric types), ignores NULLs
            AggregateAccumulator::Avg { ref mut sum, ref mut count, distinct, seen } => {
                match value {
                    types::SqlValue::Null => {} // Skip NULL
                    types::SqlValue::Integer(_)
                    | types::SqlValue::Smallint(_)
                    | types::SqlValue::Bigint(_)
                    | types::SqlValue::Numeric(_)
                    | types::SqlValue::Float(_)
                    | types::SqlValue::Real(_)
                    | types::SqlValue::Double(_) => {
                        if *distinct {
                            // Only include if we haven't seen this value before
                            if seen.as_mut().unwrap().insert(value.clone()) {
                                *sum = add_sql_values(sum, value);
                                *count += 1;
                            }
                        } else {
                            *sum = add_sql_values(sum, value);
                            *count += 1;
                        }
                    }
                    _ => {} // Type mismatch - ignore
                }
            }

            // MIN - finds minimum value, ignores NULLs
            AggregateAccumulator::Min { value: ref mut current_min, distinct, seen } => {
                if value.is_null() {
                    return; // Skip NULL
                }

                // For MIN with DISTINCT, we still need to consider all unique values
                // but the result is the same as without DISTINCT
                if *distinct && !seen.as_mut().unwrap().insert(value.clone()) {
                    return; // Already seen this value
                }

                match value {
                    types::SqlValue::Integer(_)
                    | types::SqlValue::Smallint(_)
                    | types::SqlValue::Bigint(_)
                    | types::SqlValue::Numeric(_)
                    | types::SqlValue::Float(_)
                    | types::SqlValue::Real(_)
                    | types::SqlValue::Double(_)
                    | types::SqlValue::Varchar(_)
                    | types::SqlValue::Character(_)
                    | types::SqlValue::Boolean(_)
                    | types::SqlValue::Date(_)
                    | types::SqlValue::Time(_)
                    | types::SqlValue::Timestamp(_) => {
                        if let Some(ref current) = current_min {
                            if compare_sql_values(value, current) == Ordering::Less {
                                *current_min = Some(value.clone());
                            }
                        } else {
                            *current_min = Some(value.clone());
                        }
                    }
                    _ => {} // Unsupported type
                }
            }

            // MAX - finds maximum value, ignores NULLs
            AggregateAccumulator::Max { value: ref mut current_max, distinct, seen } => {
                if value.is_null() {
                    return; // Skip NULL
                }

                // For MAX with DISTINCT, we still need to consider all unique values
                // but the result is the same as without DISTINCT
                if *distinct && !seen.as_mut().unwrap().insert(value.clone()) {
                    return; // Already seen this value
                }

                match value {
                    types::SqlValue::Integer(_)
                    | types::SqlValue::Smallint(_)
                    | types::SqlValue::Bigint(_)
                    | types::SqlValue::Numeric(_)
                    | types::SqlValue::Float(_)
                    | types::SqlValue::Real(_)
                    | types::SqlValue::Double(_)
                    | types::SqlValue::Varchar(_)
                    | types::SqlValue::Character(_)
                    | types::SqlValue::Boolean(_)
                    | types::SqlValue::Date(_)
                    | types::SqlValue::Time(_)
                    | types::SqlValue::Timestamp(_) => {
                        if let Some(ref current) = current_max {
                            if compare_sql_values(value, current) == Ordering::Greater {
                                *current_max = Some(value.clone());
                            }
                        } else {
                            *current_max = Some(value.clone());
                        }
                    }
                    _ => {} // Unsupported type
                }
            }
        }
    }

    pub(super) fn finalize(&self) -> types::SqlValue {
        match self {
            AggregateAccumulator::Count { count, .. } => types::SqlValue::Integer(*count),
            AggregateAccumulator::Sum { sum, .. } => sum.clone(),
            AggregateAccumulator::Avg { sum, count, .. } => {
                if *count == 0 {
                    types::SqlValue::Null
                } else {
                    divide_sql_value(sum, *count)
                }
            }
            AggregateAccumulator::Min { value, .. } => {
                value.clone().unwrap_or(types::SqlValue::Null)
            }
            AggregateAccumulator::Max { value, .. } => {
                value.clone().unwrap_or(types::SqlValue::Null)
            }
        }
    }
}

/// Add two SqlValues together, handling all numeric types with type coercion to Numeric
fn add_sql_values(a: &types::SqlValue, b: &types::SqlValue) -> types::SqlValue {
    // Convert both values to f64 for addition, then return as Numeric
    let a_f64 = sql_value_to_f64(a);
    let b_f64 = sql_value_to_f64(b);

    match (a_f64, b_f64) {
        (Some(x), Some(y)) => types::SqlValue::Numeric(x + y),
        _ => types::SqlValue::Null, // If either is not numeric, return NULL
    }
}

/// Convert SqlValue to f64 for numeric operations
fn sql_value_to_f64(value: &types::SqlValue) -> Option<f64> {
    match value {
        types::SqlValue::Integer(x) => Some(*x as f64),
        types::SqlValue::Smallint(x) => Some(*x as f64),
        types::SqlValue::Bigint(x) => Some(*x as f64),
        types::SqlValue::Numeric(x) => Some(*x),
        types::SqlValue::Float(x) => Some(*x as f64),
        types::SqlValue::Real(x) => Some(*x as f64),
        types::SqlValue::Double(x) => Some(*x),
        _ => None,
    }
}

/// Divide a SqlValue by an integer count, handling all numeric types
fn divide_sql_value(value: &types::SqlValue, count: i64) -> types::SqlValue {
    if let Some(sum_f64) = sql_value_to_f64(value) {
        types::SqlValue::Numeric(sum_f64 / count as f64)
    } else {
        types::SqlValue::Null
    }
}

/// Compare two SqlValues for ordering purposes (SQL ORDER BY semantics)
///
/// Uses the PartialOrd trait implementation with SQL-specific NULL handling:
/// - NULL values sort last (NULLS LAST - SQL:1999 default for ASC)
/// - Incomparable values (type mismatches, NaN) default to Equal for sort stability
pub(super) fn compare_sql_values(a: &types::SqlValue, b: &types::SqlValue) -> Ordering {
    match (a.is_null(), b.is_null()) {
        // Both NULL - equal
        (true, true) => Ordering::Equal,
        // First is NULL - sorts last (greater)
        (true, false) => Ordering::Greater,
        // Second is NULL - first sorts first (less)
        (false, true) => Ordering::Less,
        // Neither NULL - use PartialOrd trait
        (false, false) => {
            // partial_cmp returns None for incomparable values (type mismatch, NaN)
            // Default to Equal to maintain sort stability
            PartialOrd::partial_cmp(a, b).unwrap_or(Ordering::Equal)
        }
    }
}

/// Grouped rows: (group key values, rows in group)
pub(super) type GroupedRows = Vec<(Vec<types::SqlValue>, Vec<storage::Row>)>;

/// Group rows by GROUP BY expressions
///
/// Optimized implementation using HashMap for O(1) group lookups instead of O(n) linear search.
/// This significantly improves performance for queries with many groups.
/// Timeout is checked every 1000 rows.
pub(super) fn group_rows<'a>(
    rows: &[storage::Row],
    group_by_exprs: &[ast::Expression],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
    executor: &crate::SelectExecutor<'a>,
) -> Result<GroupedRows, crate::errors::ExecutorError> {
    // Use HashMap for O(1) group lookups
    let mut groups_map: HashMap<Vec<types::SqlValue>, Vec<storage::Row>> = HashMap::new();
    let mut rows_processed = 0;
    const CHECK_INTERVAL: usize = 1000;

    for row in rows {
        // Check timeout every 1000 rows
        rows_processed += 1;
        if rows_processed % CHECK_INTERVAL == 0 {
            executor.check_timeout()?;
        }

        // Evaluate GROUP BY expressions to get the group key
        let mut key = Vec::new();
        for expr in group_by_exprs {
            let value = evaluator.eval(expr, row)?;
            key.push(value);
        }

        // Insert or update group using HashMap (O(1) lookup)
        groups_map.entry(key).or_insert_with(Vec::new).push(row.clone());
    }

    // Convert HashMap back to Vec for compatibility with existing code
    Ok(groups_map.into_iter().collect())
}
