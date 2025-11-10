use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator, optimizer::combine_with_and,
    schema::CombinedSchema,
};
use super::from_iterator::FromIterator;

mod expression_mapper;
mod hash_join;
mod hash_join_iterator;
mod join_analyzer;
mod nested_loop;
pub mod reorder;
pub mod search;

#[cfg(test)]
mod tests;

// Re-export join reorder analyzer for public tests
// Re-export hash_join functions for internal use
use hash_join::hash_join_inner;
// Re-export hash join iterator for public use
pub use hash_join_iterator::HashJoinIterator;
// Re-export nested loop join variants for internal use
use nested_loop::{
    nested_loop_cross_join, nested_loop_full_outer_join, nested_loop_inner_join,
    nested_loop_left_outer_join, nested_loop_right_outer_join,
};
pub use reorder::JoinOrderAnalyzer;
// Re-export join order search for public tests
pub use search::JoinOrderSearch;

/// Data source for FROM clause results
///
/// This enum allows FROM results to be either materialized (Vec<Row>) or lazy (iterator).
/// Materialized results are used for JOINs, CTEs, and operations that need multiple passes.
/// Lazy results are used for simple table scans to enable streaming execution.
pub(super) enum FromData {
    /// Materialized rows (for JOINs, CTEs, operations needing multiple passes)
    Materialized(Vec<storage::Row>),

    /// Lazy iterator (for streaming table scans)
    Iterator(FromIterator),
}

impl FromData {
    /// Get rows, materializing if needed
    pub fn into_rows(self) -> Vec<storage::Row> {
        match self {
            Self::Materialized(rows) => rows,
            Self::Iterator(iter) => iter.collect_vec(),
        }
    }

    /// Get a reference to materialized rows, or materialize if iterator
    pub fn as_rows(&mut self) -> &Vec<storage::Row> {
        // If we have an iterator, materialize it
        if let Self::Iterator(iter) = self {
            let rows = std::mem::replace(iter, FromIterator::from_vec(vec![])).collect_vec();
            *self = Self::Materialized(rows);
        }

        // Now we're guaranteed to have materialized rows
        match self {
            Self::Materialized(rows) => rows,
            Self::Iterator(_) => unreachable!(),
        }
    }
}

/// Result of executing a FROM clause
///
/// Contains the combined schema and data (either materialized or lazy).
pub(super) struct FromResult {
    pub(super) schema: CombinedSchema,
    pub(super) data: FromData,
}

impl FromResult {
    /// Create a FromResult from materialized rows
    pub(super) fn from_rows(schema: CombinedSchema, rows: Vec<storage::Row>) -> Self {
        Self { schema, data: FromData::Materialized(rows) }
    }

    /// Create a FromResult from an iterator
    pub(super) fn from_iterator(schema: CombinedSchema, iterator: FromIterator) -> Self {
        Self { schema, data: FromData::Iterator(iterator) }
    }

    /// Get the rows, materializing if needed
    pub(super) fn into_rows(self) -> Vec<storage::Row> {
        self.data.into_rows()
    }

    /// Get a mutable reference to the rows, materializing if needed
    #[allow(dead_code)]
    pub(super) fn rows_mut(&mut self) -> &mut Vec<storage::Row> {
        // First materialize if needed
        self.data.as_rows();

        // Now we're guaranteed to have materialized rows
        match &mut self.data {
            FromData::Materialized(rows) => rows,
            FromData::Iterator(_) => unreachable!(),
        }
    }

    /// Get a reference to rows, materializing if needed
    pub(super) fn rows(&mut self) -> &Vec<storage::Row> {
        self.data.as_rows()
    }
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
    result: FromResult,
    filter_expr: &ast::Expression,
    database: &storage::Database,
) -> Result<FromResult, ExecutorError> {
    // Extract schema before moving result
    let schema = result.schema.clone();
    let evaluator = CombinedExpressionEvaluator::with_database(&schema, database);

    // Filter rows based on the expression
    let mut filtered_rows = Vec::new();
    for row in result.into_rows() {
        match evaluator.eval(filter_expr, &row)? {
            types::SqlValue::Boolean(true) => filtered_rows.push(row),
            types::SqlValue::Boolean(false) => {} // Skip this row
            types::SqlValue::Null => {}           // Skip NULL results
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

    Ok(FromResult::from_rows(schema, filtered_rows))
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
        // IMPORTANT: Sum up columns from ALL tables in the left schema,
        // not just the first table, to handle accumulated multi-table joins
        let left_col_count: usize =
            left.schema.table_schemas.values().map(|(_, schema)| schema.columns.len()).sum();

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
        ast::JoinType::LeftOuter => {
            nested_loop_left_outer_join(left, right, &combined_condition, database)
        }
        ast::JoinType::RightOuter => {
            nested_loop_right_outer_join(left, right, &combined_condition, database)
        }
        ast::JoinType::FullOuter => {
            nested_loop_full_outer_join(left, right, &combined_condition, database)
        }
        ast::JoinType::Cross => nested_loop_cross_join(left, right, &combined_condition, database),
    }
}
