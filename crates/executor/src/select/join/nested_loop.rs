use super::{combine_rows, FromResult};
use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator, limits::MAX_MEMORY_BYTES,
    schema::CombinedSchema,
};

/// Maximum number of rows allowed in a join result to prevent memory exhaustion
/// With average row size of ~100 bytes, this allows up to ~10GB
const MAX_JOIN_RESULT_ROWS: usize = 100_000_000;

/// Check if a join would exceed memory limits based on estimated result size
/// Accounts for join selectivity when equijoin conditions are present
fn check_join_size_limit(
    left_count: usize,
    right_count: usize,
    condition: &Option<ast::Expression>,
) -> Result<(), ExecutorError> {
    // Estimate result size based on join condition type
    let estimated_result_rows = if is_equijoin_condition(condition) {
        // For equijoins (a.col = b.col), estimate based on join selectivity
        // With equijoins on indexed columns, expect 1:1 or 1:N selectivity
        // Conservative estimate: use the size of the larger input
        // This prevents exponential blowup in cascading joins
        std::cmp::max(left_count, right_count)
    } else {
        // For non-equijoin or missing condition, assume cartesian product
        left_count.saturating_mul(right_count)
    };

    if estimated_result_rows > MAX_JOIN_RESULT_ROWS {
        // Estimate memory usage (conservative: 100 bytes per row average)
        let estimated_bytes = estimated_result_rows.saturating_mul(100);
        return Err(ExecutorError::MemoryLimitExceeded {
            used_bytes: estimated_bytes,
            max_bytes: MAX_MEMORY_BYTES,
        });
    }

    Ok(())
}

/// Check if a condition is a simple equijoin (a.col = b.col)
fn is_equijoin_condition(condition: &Option<ast::Expression>) -> bool {
    match condition {
        Some(ast::Expression::BinaryOp { op: ast::BinaryOperator::Equal, .. }) => true,
        Some(ast::Expression::BinaryOp { op: ast::BinaryOperator::And, left, right }) => {
            // For AND conditions, check if at least one is an equijoin
            is_equijoin_condition(&Some(left.as_ref().clone()))
                || is_equijoin_condition(&Some(right.as_ref().clone()))
        }
        _ => false,
    }
}

/// Nested loop INNER JOIN implementation
pub(super) fn nested_loop_inner_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<ast::Expression>,
    database: &storage::Database,
) -> Result<FromResult, ExecutorError> {
    // Check if join would exceed memory limits before executing
    check_join_size_limit(left.rows.len(), right.rows.len(), condition)?;

    // Extract right table name (assume single table for now)
    let right_table_name = right
        .schema
        .table_schemas
        .keys()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .clone();

    let right_schema = right
        .schema
        .table_schemas
        .get(&right_table_name)
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .clone();

    // Combine schemas
    let combined_schema =
        CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);
    let evaluator = CombinedExpressionEvaluator::with_database(&combined_schema, database);

    // Nested loop join algorithm
    let mut result_rows = Vec::new();
    for left_row in &left.rows {
        for right_row in &right.rows {
            // Combine rows using optimized helper (single allocation)
            let combined_row = combine_rows(left_row, right_row);

            // Clear CSE cache before evaluating join condition for this row combination
            // to prevent stale cached column values from previous combinations
            evaluator.clear_cse_cache();

            // Evaluate join condition
            let matches = if let Some(cond) = condition {
                match evaluator.eval(cond, &combined_row)? {
                    types::SqlValue::Boolean(true) => true,
                    types::SqlValue::Boolean(false) => false,
                    types::SqlValue::Null => false,
                    other => {
                        return Err(ExecutorError::InvalidWhereClause(format!(
                            "JOIN condition must evaluate to boolean, got: {:?}",
                            other
                        )))
                    }
                }
            } else {
                true // No condition = CROSS JOIN
            };

            if matches {
                result_rows.push(combined_row);
            }
        }
    }

    Ok(FromResult { schema: combined_schema, rows: result_rows })
}

/// Nested loop LEFT OUTER JOIN implementation
pub(super) fn nested_loop_left_outer_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<ast::Expression>,
    database: &storage::Database,
) -> Result<FromResult, ExecutorError> {
    // Check if join would exceed memory limits before executing
    check_join_size_limit(left.rows.len(), right.rows.len(), condition)?;

    // Extract right table name and schema
    let right_table_name = right
        .schema
        .table_schemas
        .keys()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .clone();

    let right_schema = right
        .schema
        .table_schemas
        .get(&right_table_name)
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .clone();

    let right_column_count = right_schema.columns.len();

    // Combine schemas
    let combined_schema = CombinedSchema::combine(left.schema, right_table_name, right_schema);
    let evaluator = CombinedExpressionEvaluator::with_database(&combined_schema, database);

    // Nested loop LEFT OUTER JOIN algorithm
    let mut result_rows = Vec::new();
    for left_row in &left.rows {
        let mut matched = false;

        for right_row in &right.rows {
            // Combine rows using optimized helper (single allocation)
            let combined_row = combine_rows(left_row, right_row);

            // Clear CSE cache before evaluating join condition for this row combination
            // to prevent stale cached column values from previous combinations
            evaluator.clear_cse_cache();

            // Evaluate join condition
            let matches = if let Some(cond) = condition {
                match evaluator.eval(cond, &combined_row)? {
                    types::SqlValue::Boolean(true) => true,
                    types::SqlValue::Boolean(false) => false,
                    types::SqlValue::Null => false,
                    other => {
                        return Err(ExecutorError::InvalidWhereClause(format!(
                            "JOIN condition must evaluate to boolean, got: {:?}",
                            other
                        )))
                    }
                }
            } else {
                true // No condition = CROSS JOIN
            };

            if matches {
                result_rows.push(combined_row);
                matched = true;
            }
        }

        // If no match found, add left row with NULLs for right columns
        if !matched {
            let mut combined_values =
                Vec::with_capacity(left_row.values.len() + right_column_count);
            combined_values.extend_from_slice(&left_row.values);
            combined_values.extend(vec![types::SqlValue::Null; right_column_count]);
            result_rows.push(storage::Row::new(combined_values));
        }
    }

    Ok(FromResult { schema: combined_schema, rows: result_rows })
}

/// Nested loop RIGHT OUTER JOIN implementation
pub(super) fn nested_loop_right_outer_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<ast::Expression>,
    database: &storage::Database,
) -> Result<FromResult, ExecutorError> {
    // Check if join would exceed memory limits before executing
    check_join_size_limit(left.rows.len(), right.rows.len(), condition)?;

    // RIGHT OUTER JOIN = LEFT OUTER JOIN with sides swapped
    // Then we need to reorder columns to put left first, right second

    // Get the right column count before moving
    let right_col_count = right
        .schema
        .table_schemas
        .values()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .columns
        .len();

    // Do LEFT OUTER JOIN with swapped sides
    let swapped_result = nested_loop_left_outer_join(right, left, condition, database)?;

    // Now we need to reorder the columns in the result
    // The swapped result has right columns first, then left columns
    // We need to reverse this to left first, then right

    // Reorder rows: move left columns (currently at positions right_col_count..) to front
    let reordered_rows: Vec<storage::Row> = swapped_result
        .rows
        .iter()
        .map(|row| {
            let mut new_values = Vec::new();
            // Add left columns (currently at end)
            new_values.extend_from_slice(&row.values[right_col_count..]);
            // Add right columns (currently at start)
            new_values.extend_from_slice(&row.values[0..right_col_count]);
            storage::Row::new(new_values)
        })
        .collect();

    Ok(FromResult { schema: swapped_result.schema, rows: reordered_rows })
}

/// Nested loop FULL OUTER JOIN implementation
pub(super) fn nested_loop_full_outer_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<ast::Expression>,
    database: &storage::Database,
) -> Result<FromResult, ExecutorError> {
    // Check if join would exceed memory limits before executing
    check_join_size_limit(left.rows.len(), right.rows.len(), condition)?;

    // Extract right table name and schema
    let right_table_name = right
        .schema
        .table_schemas
        .keys()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .clone();

    let right_schema = right
        .schema
        .table_schemas
        .get(&right_table_name)
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .clone();

    let left_column_count = left
        .schema
        .table_schemas
        .values()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .columns
        .len();
    let right_column_count = right_schema.columns.len();

    // Combine schemas
    let combined_schema = CombinedSchema::combine(left.schema, right_table_name, right_schema);
    let evaluator = CombinedExpressionEvaluator::with_database(&combined_schema, database);

    // FULL OUTER JOIN = LEFT OUTER JOIN + unmatched rows from right
    let mut result_rows = Vec::new();
    let mut right_matched = vec![false; right.rows.len()];

    // First pass: LEFT OUTER JOIN logic
    for left_row in &left.rows {
        let mut matched = false;

        for (right_idx, right_row) in right.rows.iter().enumerate() {
            // Combine rows using optimized helper (single allocation)
            let combined_row = combine_rows(left_row, right_row);

            // Clear CSE cache before evaluating join condition for this row combination
            // to prevent stale cached column values from previous combinations
            evaluator.clear_cse_cache();

            // Evaluate join condition
            let matches = if let Some(cond) = condition {
                match evaluator.eval(cond, &combined_row)? {
                    types::SqlValue::Boolean(true) => true,
                    types::SqlValue::Boolean(false) => false,
                    types::SqlValue::Null => false,
                    other => {
                        return Err(ExecutorError::InvalidWhereClause(format!(
                            "JOIN condition must evaluate to boolean, got: {:?}",
                            other
                        )))
                    }
                }
            } else {
                true
            };

            if matches {
                result_rows.push(combined_row);
                matched = true;
                right_matched[right_idx] = true;
            }
        }

        // If no match found, add left row with NULLs for right columns
        if !matched {
            let mut combined_values =
                Vec::with_capacity(left_row.values.len() + right_column_count);
            combined_values.extend_from_slice(&left_row.values);
            combined_values.extend(vec![types::SqlValue::Null; right_column_count]);
            result_rows.push(storage::Row::new(combined_values));
        }
    }

    // Second pass: Add unmatched right rows with NULLs for left columns
    for (right_idx, right_row) in right.rows.iter().enumerate() {
        if !right_matched[right_idx] {
            let mut combined_values =
                Vec::with_capacity(left_column_count + right_row.values.len());
            combined_values.extend(vec![types::SqlValue::Null; left_column_count]);
            combined_values.extend_from_slice(&right_row.values);
            result_rows.push(storage::Row::new(combined_values));
        }
    }

    Ok(FromResult { schema: combined_schema, rows: result_rows })
}

/// Nested loop CROSS JOIN implementation (Cartesian product)
pub(super) fn nested_loop_cross_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<ast::Expression>,
    _database: &storage::Database,
) -> Result<FromResult, ExecutorError> {
    // CROSS JOIN should not have a condition
    if condition.is_some() {
        return Err(ExecutorError::UnsupportedFeature(
            "CROSS JOIN does not support ON clause".to_string(),
        ));
    }

    // Check if cross join would exceed memory limits before executing
    check_join_size_limit(left.rows.len(), right.rows.len(), condition)?;

    // Extract right table name and schema
    let right_table_name = right
        .schema
        .table_schemas
        .keys()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .clone();

    let right_schema = right
        .schema
        .table_schemas
        .get(&right_table_name)
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .clone();

    // Combine schemas
    let combined_schema = CombinedSchema::combine(left.schema, right_table_name, right_schema);

    // CROSS JOIN = Cartesian product (every row from left × every row from right)
    let mut result_rows = Vec::new();
    for left_row in &left.rows {
        for right_row in &right.rows {
            result_rows.push(combine_rows(left_row, right_row));
        }
    }

    Ok(FromResult { schema: combined_schema, rows: result_rows })
}
