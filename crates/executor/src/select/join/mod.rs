use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use crate::optimizer::combine_with_and;
use crate::evaluator::CombinedExpressionEvaluator;

mod hash_join;
mod join_analyzer;
mod nested_loop;
pub mod reorder;

// Re-export hash_join functions for internal use
use hash_join::hash_join_inner;
// Re-export nested loop join variants for internal use
use nested_loop::{
    nested_loop_cross_join, nested_loop_full_outer_join, nested_loop_inner_join,
    nested_loop_left_outer_join, nested_loop_right_outer_join,
};

/// Result of executing a FROM clause
#[derive(Clone)]
pub(super) struct FromResult {
    pub(super) schema: CombinedSchema,
    pub(super) rows: Vec<storage::Row>,
}

/// Helper function to combine two rows without unnecessary cloning
/// Only creates a single combined row, avoiding intermediate clones
#[inline]
fn combine_rows(left_row: &storage::Row, right_row: &storage::Row) -> storage::Row {
    let mut combined_values = Vec::with_capacity(left_row.values.len() + right_row.values.len());
    combined_values.extend_from_slice(&left_row.values);
    combined_values.extend_from_slice(&right_row.values);
    storage::Row::new(combined_values)
}

/// Apply a post-join filter expression to join result rows
/// 
/// This is used to filter rows produced by hash join with additional conditions
/// from the WHERE clause that weren't used in the hash join itself.
fn apply_post_join_filter(
    mut result: FromResult,
    filter_expr: &ast::Expression,
    database: &storage::Database,
) -> Result<FromResult, ExecutorError> {
    let evaluator = CombinedExpressionEvaluator::with_database(&result.schema, database);
    
    // Filter rows based on the expression
    let mut filtered_rows = Vec::new();
    for row in result.rows {
        match evaluator.eval(filter_expr, &row)? {
            types::SqlValue::Boolean(true) => filtered_rows.push(row),
            types::SqlValue::Boolean(false) => {} // Skip this row
            types::SqlValue::Null => {} // Skip NULL results
            // SQLLogicTest compatibility: treat integers as truthy/falsy
            types::SqlValue::Integer(0) => {} // Skip 0
            types::SqlValue::Integer(_) => filtered_rows.push(row),
            types::SqlValue::Smallint(0) => {} // Skip 0
            types::SqlValue::Smallint(_) => filtered_rows.push(row),
            types::SqlValue::Bigint(0) => {} // Skip 0
            types::SqlValue::Bigint(_) => filtered_rows.push(row),
            types::SqlValue::Float(f) if f == 0.0 => {} // Skip 0.0
            types::SqlValue::Float(_) => filtered_rows.push(row),
            types::SqlValue::Real(f) if f == 0.0 => {} // Skip 0.0
            types::SqlValue::Real(_) => filtered_rows.push(row),
            types::SqlValue::Double(f) if f == 0.0 => {} // Skip 0.0
            types::SqlValue::Double(_) => filtered_rows.push(row),
            other => {
                return Err(ExecutorError::InvalidWhereClause(format!(
                    "Filter expression must evaluate to boolean, got: {:?}",
                    other
                )))
            }
        }
    }
    
    result.rows = filtered_rows;
    Ok(result)
}

/// Perform join between two FROM results, optimizing with hash join when possible
///
/// This function now supports predicate pushdown from WHERE clauses. Additional equijoin
/// predicates from WHERE can be passed to optimize hash join selection and execution.
///
/// Note: This function combines rows from left and right according to the join type
/// and join condition. For queries with many tables and large intermediate results,
/// consider applying WHERE filters earlier to reduce memory usage.
pub(super) fn nested_loop_join(
    left: FromResult,
    right: FromResult,
    join_type: &ast::JoinType,
    condition: &Option<ast::Expression>,
    database: &storage::Database,
    additional_equijoins: &[ast::Expression],
) -> Result<FromResult, ExecutorError> {
    // Try to use hash join for INNER JOINs with simple equi-join conditions
    if let ast::JoinType::Inner = join_type {
        // Get column count and right table info once for analysis
        let left_col_count = left
            .schema
            .table_schemas
            .values()
            .next()
            .map(|(_, schema)| schema.columns.len())
            .unwrap_or(0);

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

        let temp_schema =
            CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);

        // Phase 3.1: Try ON condition first (preferred for hash join)
        if let Some(cond) = condition {
            if let Some(equi_join_info) =
                join_analyzer::analyze_equi_join(cond, &temp_schema, left_col_count)
            {
                return hash_join_inner(
                    left,
                    right,
                    equi_join_info.left_col_idx,
                    equi_join_info.right_col_idx,
                );
            }
        }

        // Phase 3.1: If no ON condition hash join, try WHERE clause equijoins
        // Iterate through all additional equijoins to find one suitable for hash join
        for (idx, equijoin) in additional_equijoins.iter().enumerate() {
            if let Some(equi_join_info) =
                join_analyzer::analyze_equi_join(equijoin, &temp_schema, left_col_count)
            {
                // Found a WHERE clause equijoin suitable for hash join!
                let mut result = hash_join_inner(
                    left,
                    right,
                    equi_join_info.left_col_idx,
                    equi_join_info.right_col_idx,
                )?;

                // Apply remaining equijoins and conditions as post-join filters
                let remaining_conditions: Vec<_> = additional_equijoins
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != idx)
                    .map(|(_, e)| e.clone())
                    .collect();

                if !remaining_conditions.is_empty() {
                    if let Some(filter_expr) = combine_with_and(remaining_conditions) {
                        result = apply_post_join_filter(result, &filter_expr, database)?;
                    }
                }

                return Ok(result);
            }
        }
    }

    // Prepare combined join condition including additional equijoins from WHERE clause
    let mut all_join_conditions = Vec::new();
    if let Some(cond) = condition {
        all_join_conditions.push(cond.clone());
    }
    all_join_conditions.extend_from_slice(additional_equijoins);

    // Combine all join conditions with AND
    let combined_condition = combine_with_and(all_join_conditions);

    // Fall back to nested loop join for all other cases
    match join_type {
        ast::JoinType::Inner => nested_loop_inner_join(left, right, &combined_condition, database),
        ast::JoinType::LeftOuter => nested_loop_left_outer_join(left, right, &combined_condition, database),
        ast::JoinType::RightOuter => nested_loop_right_outer_join(left, right, &combined_condition, database),
        ast::JoinType::FullOuter => nested_loop_full_outer_join(left, right, &combined_condition, database),
        ast::JoinType::Cross => nested_loop_cross_join(left, right, &combined_condition, database),
    }
}
