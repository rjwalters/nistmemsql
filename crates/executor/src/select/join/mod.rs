use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;

mod hash_join;
mod join_analyzer;
mod nested_loop;

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

/// Perform join between two FROM results, optimizing with hash join when possible
pub(super) fn nested_loop_join(
    left: FromResult,
    right: FromResult,
    join_type: &ast::JoinType,
    condition: &Option<ast::Expression>,
    database: &storage::Database,
) -> Result<FromResult, ExecutorError> {
    // Try to use hash join for INNER JOINs with simple equi-join conditions
    if let ast::JoinType::Inner = join_type {
        if let Some(cond) = condition {
            let left_col_count = left
                .schema
                .table_schemas
                .values()
                .next()
                .map(|(_, schema)| schema.columns.len())
                .unwrap_or(0);

            // Create a temporary combined schema to analyze the join condition
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

            // Check if this is a simple equi-join that can use hash join
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
    }

    // Fall back to nested loop join for all other cases
    match join_type {
        ast::JoinType::Inner => nested_loop_inner_join(left, right, condition, database),
        ast::JoinType::LeftOuter => nested_loop_left_outer_join(left, right, condition, database),
        ast::JoinType::RightOuter => nested_loop_right_outer_join(left, right, condition, database),
        ast::JoinType::FullOuter => nested_loop_full_outer_join(left, right, condition, database),
        ast::JoinType::Cross => nested_loop_cross_join(left, right, condition, database),
    }
}
