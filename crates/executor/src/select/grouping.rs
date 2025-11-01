use std::cmp::Ordering;
use std::collections::HashSet;

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

            // SUM - sums numeric values (Integer or Numeric), ignores NULLs
            AggregateAccumulator::Sum { ref mut sum, distinct, seen } => {
                match value {
                    types::SqlValue::Null => {} // Skip NULL
                    types::SqlValue::Integer(_) | types::SqlValue::Numeric(_) => {
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

            // AVG - computes average of numeric values (Integer or Numeric), ignores NULLs
            AggregateAccumulator::Avg { ref mut sum, ref mut count, distinct, seen } => {
                match value {
                    types::SqlValue::Null => {} // Skip NULL
                    types::SqlValue::Integer(_) | types::SqlValue::Numeric(_) => {
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
                    | types::SqlValue::Varchar(_)
                    | types::SqlValue::Boolean(_) => {
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
                    | types::SqlValue::Varchar(_)
                    | types::SqlValue::Boolean(_) => {
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

/// Add two SqlValues together, handling Integer and Numeric types with type coercion
fn add_sql_values(a: &types::SqlValue, b: &types::SqlValue) -> types::SqlValue {
    match (a, b) {
        // Integer + Integer => Integer
        (types::SqlValue::Integer(x), types::SqlValue::Integer(y)) => {
            types::SqlValue::Integer(x + y)
        }
        // Integer + Numeric => Numeric (promote Integer to Numeric)
        (types::SqlValue::Integer(x), types::SqlValue::Numeric(y)) => {
            types::SqlValue::Numeric(*x as f64 + *y)
        }
        // Numeric + Integer => Numeric (promote Integer to Numeric)
        (types::SqlValue::Numeric(x), types::SqlValue::Integer(y)) => {
            types::SqlValue::Numeric(*x + *y as f64)
        }
        // Numeric + Numeric => Numeric
        (types::SqlValue::Numeric(x), types::SqlValue::Numeric(y)) => {
            types::SqlValue::Numeric(*x + *y)
        }
        // Default: return first value unchanged
        _ => a.clone(),
    }
}

/// Divide a SqlValue by an integer count, handling Integer and Numeric types
fn divide_sql_value(value: &types::SqlValue, count: i64) -> types::SqlValue {
    match value {
        types::SqlValue::Integer(sum) => types::SqlValue::Integer(sum / count),
        types::SqlValue::Numeric(sum) => types::SqlValue::Numeric(*sum / count as f64),
        // Default: return NULL for unsupported types
        _ => types::SqlValue::Null,
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
pub(super) fn group_rows(
    rows: &[storage::Row],
    group_by_exprs: &[ast::Expression],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
) -> Result<GroupedRows, crate::errors::ExecutorError> {
    let mut groups: GroupedRows = Vec::new();

    for row in rows {
        // Evaluate GROUP BY expressions to get the group key
        let mut key = Vec::new();
        for expr in group_by_exprs {
            let value = evaluator.eval(expr, row)?;
            key.push(value);
        }

        // Find existing group or create new one
        let mut found = false;
        for (existing_key, group_rows) in &mut groups {
            if existing_key == &key {
                group_rows.push(row.clone());
                found = true;
                break;
            }
        }

        if !found {
            groups.push((key, vec![row.clone()]));
        }
    }

    Ok(groups)
}
