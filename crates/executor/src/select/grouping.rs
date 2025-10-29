use std::cmp::Ordering;

/// Accumulator for aggregate functions
#[derive(Debug, Clone)]
pub(super) enum AggregateAccumulator {
    Count(i64),
    Sum(types::SqlValue),
    Avg { sum: types::SqlValue, count: i64 },
    Min(Option<types::SqlValue>),
    Max(Option<types::SqlValue>),
}

impl AggregateAccumulator {
    pub(super) fn new(function_name: &str) -> Result<Self, crate::errors::ExecutorError> {
        match function_name.to_uppercase().as_str() {
            "COUNT" => Ok(AggregateAccumulator::Count(0)),
            "SUM" => Ok(AggregateAccumulator::Sum(types::SqlValue::Integer(0))),
            "AVG" => Ok(AggregateAccumulator::Avg { sum: types::SqlValue::Integer(0), count: 0 }),
            "MIN" => Ok(AggregateAccumulator::Min(None)),
            "MAX" => Ok(AggregateAccumulator::Max(None)),
            _ => Err(crate::errors::ExecutorError::UnsupportedExpression(format!(
                "Unknown aggregate function: {}",
                function_name
            ))),
        }
    }

    pub(super) fn accumulate(&mut self, value: &types::SqlValue) {
        match (self, value) {
            // COUNT - counts non-NULL values
            (AggregateAccumulator::Count(_), types::SqlValue::Null) => {}
            (AggregateAccumulator::Count(ref mut count), _) => {
                *count += 1;
            }

            // SUM - sums numeric values (Integer or Numeric), ignores NULLs
            (AggregateAccumulator::Sum(ref mut sum), types::SqlValue::Integer(val)) => {
                *sum = add_sql_values(sum, &types::SqlValue::Integer(*val));
            }
            (AggregateAccumulator::Sum(ref mut sum), types::SqlValue::Numeric(val)) => {
                *sum = add_sql_values(sum, &types::SqlValue::Numeric(val.clone()));
            }
            (AggregateAccumulator::Sum(_), types::SqlValue::Null) => {}

            // AVG - computes average of numeric values (Integer or Numeric), ignores NULLs
            (
                AggregateAccumulator::Avg { ref mut sum, ref mut count },
                types::SqlValue::Integer(val),
            ) => {
                *sum = add_sql_values(sum, &types::SqlValue::Integer(*val));
                *count += 1;
            }
            (
                AggregateAccumulator::Avg { ref mut sum, ref mut count },
                types::SqlValue::Numeric(val),
            ) => {
                *sum = add_sql_values(sum, &types::SqlValue::Numeric(val.clone()));
                *count += 1;
            }
            (AggregateAccumulator::Avg { .. }, types::SqlValue::Null) => {}

            // MIN - finds minimum value, ignores NULLs
            (AggregateAccumulator::Min(ref mut current_min), val @ types::SqlValue::Integer(_))
            | (AggregateAccumulator::Min(ref mut current_min), val @ types::SqlValue::Varchar(_))
            | (AggregateAccumulator::Min(ref mut current_min), val @ types::SqlValue::Boolean(_)) => {
                if let Some(ref current) = current_min {
                    if compare_sql_values(val, current) == Ordering::Less {
                        *current_min = Some(val.clone());
                    }
                } else {
                    *current_min = Some(val.clone());
                }
            }
            (AggregateAccumulator::Min(_), types::SqlValue::Null) => {}

            // MAX - finds maximum value, ignores NULLs
            (AggregateAccumulator::Max(ref mut current_max), val @ types::SqlValue::Integer(_))
            | (AggregateAccumulator::Max(ref mut current_max), val @ types::SqlValue::Varchar(_))
            | (AggregateAccumulator::Max(ref mut current_max), val @ types::SqlValue::Boolean(_)) => {
                if let Some(ref current) = current_max {
                    if compare_sql_values(val, current) == Ordering::Greater {
                        *current_max = Some(val.clone());
                    }
                } else {
                    *current_max = Some(val.clone());
                }
            }
            (AggregateAccumulator::Max(_), types::SqlValue::Null) => {}

            _ => {
                // Type mismatch or unsupported type - ignore for now
            }
        }
    }

    pub(super) fn finalize(&self) -> types::SqlValue {
        match self {
            AggregateAccumulator::Count(count) => types::SqlValue::Integer(*count),
            AggregateAccumulator::Sum(sum) => sum.clone(),
            AggregateAccumulator::Avg { sum, count } => {
                if *count == 0 {
                    types::SqlValue::Null
                } else {
                    divide_sql_value(sum, *count)
                }
            }
            AggregateAccumulator::Min(val) => val.clone().unwrap_or(types::SqlValue::Null),
            AggregateAccumulator::Max(val) => val.clone().unwrap_or(types::SqlValue::Null),
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
            let x_f64 = *x as f64;
            let y_f64 = y.parse::<f64>().unwrap_or(0.0);
            types::SqlValue::Numeric((x_f64 + y_f64).to_string())
        }
        // Numeric + Integer => Numeric (promote Integer to Numeric)
        (types::SqlValue::Numeric(x), types::SqlValue::Integer(y)) => {
            let x_f64 = x.parse::<f64>().unwrap_or(0.0);
            let y_f64 = *y as f64;
            types::SqlValue::Numeric((x_f64 + y_f64).to_string())
        }
        // Numeric + Numeric => Numeric
        (types::SqlValue::Numeric(x), types::SqlValue::Numeric(y)) => {
            let x_f64 = x.parse::<f64>().unwrap_or(0.0);
            let y_f64 = y.parse::<f64>().unwrap_or(0.0);
            types::SqlValue::Numeric((x_f64 + y_f64).to_string())
        }
        // Default: return first value unchanged
        _ => a.clone(),
    }
}

/// Divide a SqlValue by an integer count, handling Integer and Numeric types
fn divide_sql_value(value: &types::SqlValue, count: i64) -> types::SqlValue {
    match value {
        types::SqlValue::Integer(sum) => {
            types::SqlValue::Integer(sum / count)
        }
        types::SqlValue::Numeric(sum) => {
            let sum_f64 = sum.parse::<f64>().unwrap_or(0.0);
            let avg = sum_f64 / (count as f64);
            types::SqlValue::Numeric(avg.to_string())
        }
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
