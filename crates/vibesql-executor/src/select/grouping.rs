use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

/// Accumulator for aggregate functions
#[derive(Debug, Clone)]
pub(super) enum AggregateAccumulator {
    Count { count: i64, distinct: bool, seen: Option<HashSet<vibesql_types::SqlValue>> },
    Sum { sum: vibesql_types::SqlValue, distinct: bool, seen: Option<HashSet<vibesql_types::SqlValue>> },
    Avg { sum: vibesql_types::SqlValue, count: i64, distinct: bool, seen: Option<HashSet<vibesql_types::SqlValue>> },
    Min { value: Option<vibesql_types::SqlValue>, distinct: bool, seen: Option<HashSet<vibesql_types::SqlValue>> },
    Max { value: Option<vibesql_types::SqlValue>, distinct: bool, seen: Option<HashSet<vibesql_types::SqlValue>> },
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
                Ok(AggregateAccumulator::Sum { sum: vibesql_types::SqlValue::Integer(0), distinct, seen })
            }
            "AVG" => Ok(AggregateAccumulator::Avg {
                sum: vibesql_types::SqlValue::Integer(0),
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

    pub(super) fn accumulate(&mut self, value: &vibesql_types::SqlValue) {
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
                    vibesql_types::SqlValue::Null => {} // Skip NULL
                    vibesql_types::SqlValue::Integer(_)
                    | vibesql_types::SqlValue::Smallint(_)
                    | vibesql_types::SqlValue::Bigint(_)
                    | vibesql_types::SqlValue::Numeric(_)
                    | vibesql_types::SqlValue::Float(_)
                    | vibesql_types::SqlValue::Real(_)
                    | vibesql_types::SqlValue::Double(_) => {
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
                    vibesql_types::SqlValue::Null => {} // Skip NULL
                    vibesql_types::SqlValue::Integer(_)
                    | vibesql_types::SqlValue::Smallint(_)
                    | vibesql_types::SqlValue::Bigint(_)
                    | vibesql_types::SqlValue::Numeric(_)
                    | vibesql_types::SqlValue::Float(_)
                    | vibesql_types::SqlValue::Real(_)
                    | vibesql_types::SqlValue::Double(_) => {
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
                    vibesql_types::SqlValue::Integer(_)
                    | vibesql_types::SqlValue::Smallint(_)
                    | vibesql_types::SqlValue::Bigint(_)
                    | vibesql_types::SqlValue::Numeric(_)
                    | vibesql_types::SqlValue::Float(_)
                    | vibesql_types::SqlValue::Real(_)
                    | vibesql_types::SqlValue::Double(_)
                    | vibesql_types::SqlValue::Varchar(_)
                    | vibesql_types::SqlValue::Character(_)
                    | vibesql_types::SqlValue::Boolean(_)
                    | vibesql_types::SqlValue::Date(_)
                    | vibesql_types::SqlValue::Time(_)
                    | vibesql_types::SqlValue::Timestamp(_) => {
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
                    vibesql_types::SqlValue::Integer(_)
                    | vibesql_types::SqlValue::Smallint(_)
                    | vibesql_types::SqlValue::Bigint(_)
                    | vibesql_types::SqlValue::Numeric(_)
                    | vibesql_types::SqlValue::Float(_)
                    | vibesql_types::SqlValue::Real(_)
                    | vibesql_types::SqlValue::Double(_)
                    | vibesql_types::SqlValue::Varchar(_)
                    | vibesql_types::SqlValue::Character(_)
                    | vibesql_types::SqlValue::Boolean(_)
                    | vibesql_types::SqlValue::Date(_)
                    | vibesql_types::SqlValue::Time(_)
                    | vibesql_types::SqlValue::Timestamp(_) => {
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

    pub(super) fn finalize(&self) -> vibesql_types::SqlValue {
        match self {
            AggregateAccumulator::Count { count, .. } => vibesql_types::SqlValue::Numeric(*count as f64),
            AggregateAccumulator::Sum { sum, .. } => sum.clone(),
            AggregateAccumulator::Avg { sum, count, .. } => {
                if *count == 0 {
                    vibesql_types::SqlValue::Null
                } else {
                    divide_sql_value(sum, *count)
                }
            }
            AggregateAccumulator::Min { value, .. } => {
                value.clone().unwrap_or(vibesql_types::SqlValue::Null)
            }
            AggregateAccumulator::Max { value, .. } => {
                value.clone().unwrap_or(vibesql_types::SqlValue::Null)
            }
        }
    }
}

/// Add two SqlValues together, handling all numeric types with type coercion to Numeric
///
/// **Design Decision**: Always returns Numeric (f64) for aggregate operations
///
/// This behavior was established in commit 0aa09d8a (#871) to align with SQLLogicTest
/// expectations, which requires NUMERIC return types for aggregate functions.
///
/// **SQL Standard Notes**:
/// - Different databases handle SUM return types differently:
///   - PostgreSQL: SUM(INTEGER) → BIGINT
///   - MySQL: SUM(INTEGER) → DECIMAL
///   - SQL Server: SUM(INTEGER) → INTEGER
///   - Oracle: Same type as input
/// - SQLLogicTest (the canonical SQL conformance suite) expects NUMERIC
/// - This choice prevents integer overflow and aligns with SQLLogicTest requirements
///
/// See: https://github.com/rjwalters/vibesql/pull/871
fn add_sql_values(a: &vibesql_types::SqlValue, b: &vibesql_types::SqlValue) -> vibesql_types::SqlValue {
    // Convert both values to f64 for addition, then return as Numeric
    let a_f64 = sql_value_to_f64(a);
    let b_f64 = sql_value_to_f64(b);

    match (a_f64, b_f64) {
        (Some(x), Some(y)) => vibesql_types::SqlValue::Numeric(x + y),
        _ => vibesql_types::SqlValue::Null, // If either is not numeric, return NULL
    }
}

/// Convert SqlValue to f64 for numeric operations
fn sql_value_to_f64(value: &vibesql_types::SqlValue) -> Option<f64> {
    match value {
        vibesql_types::SqlValue::Integer(x) => Some(*x as f64),
        vibesql_types::SqlValue::Smallint(x) => Some(*x as f64),
        vibesql_types::SqlValue::Bigint(x) => Some(*x as f64),
        vibesql_types::SqlValue::Numeric(x) => Some(*x),
        vibesql_types::SqlValue::Float(x) => Some(*x as f64),
        vibesql_types::SqlValue::Real(x) => Some(*x as f64),
        vibesql_types::SqlValue::Double(x) => Some(*x),
        _ => None,
    }
}

/// Divide a SqlValue by an integer count, handling all numeric types
///
/// **Design Decision**: Always returns Numeric (f64) for AVG aggregate function
///
/// This matches the behavior of add_sql_values() and aligns with SQLLogicTest expectations.
/// See add_sql_values() documentation for rationale.
fn divide_sql_value(value: &vibesql_types::SqlValue, count: i64) -> vibesql_types::SqlValue {
    if let Some(sum_f64) = sql_value_to_f64(value) {
        vibesql_types::SqlValue::Numeric(sum_f64 / count as f64)
    } else {
        vibesql_types::SqlValue::Null
    }
}

/// Compare two SqlValues for ordering purposes (SQL ORDER BY semantics)
///
/// Uses the PartialOrd trait implementation with SQL-specific NULL handling:
/// - NULL values sort last (NULLS LAST - SQL:1999 default for ASC)
/// - Incomparable values (type mismatches, NaN) default to Equal for sort stability
pub(super) fn compare_sql_values(a: &vibesql_types::SqlValue, b: &vibesql_types::SqlValue) -> Ordering {
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
pub(super) type GroupedRows = Vec<(Vec<vibesql_types::SqlValue>, Vec<vibesql_storage::Row>)>;

/// Group rows by GROUP BY expressions
///
/// Optimized implementation using HashMap for O(1) group lookups instead of O(n) linear search.
/// This significantly improves performance for queries with many groups.
/// Timeout is checked every 1000 rows.
pub(super) fn group_rows<'a>(
    rows: &[vibesql_storage::Row],
    group_by_exprs: &[vibesql_ast::Expression],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
    executor: &crate::SelectExecutor<'a>,
) -> Result<GroupedRows, crate::errors::ExecutorError> {
    // Use HashMap for O(1) group lookups
    let mut groups_map: HashMap<Vec<vibesql_types::SqlValue>, Vec<vibesql_storage::Row>> = HashMap::new();
    let mut rows_processed = 0;
    const CHECK_INTERVAL: usize = 1000;

    for row in rows {
        // Check timeout every 1000 rows
        rows_processed += 1;
        if rows_processed % CHECK_INTERVAL == 0 {
            executor.check_timeout()?;
        }

        // Clear CSE cache before evaluating each row to prevent column values
        // from being incorrectly cached across different rows
        evaluator.clear_cse_cache();

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
