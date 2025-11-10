//! Join condition analyzer for detecting equi-join opportunities
//!
//! This module analyzes join conditions to identify equi-joins (equality-based joins)
//! which can be optimized using hash join algorithms instead of nested loop joins.

use vibesql_ast::{BinaryOperator, Expression};

use crate::schema::CombinedSchema;

/// Information about an equi-join condition
#[derive(Debug, Clone)]
pub struct EquiJoinInfo {
    /// Column index in the left table
    pub left_col_idx: usize,
    /// Column index in the right table
    pub right_col_idx: usize,
}

/// Analyze a join condition to detect if it's a simple equi-join
///
/// Returns Some(EquiJoinInfo) if the condition is a simple equi-join like:
/// - `t1.col = t2.col`
/// - `t2.col = t1.col`
///
/// Returns None if:
/// - The condition is more complex (AND/OR of multiple conditions)
/// - The condition uses operators other than equality
/// - The condition doesn't reference columns from both sides
pub fn analyze_equi_join(
    condition: &Expression,
    schema: &CombinedSchema,
    left_column_count: usize,
) -> Option<EquiJoinInfo> {
    match condition {
        Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
            // Try to extract column references from both sides
            if let (Some(left_idx), Some(right_idx)) =
                (extract_column_index(left, schema), extract_column_index(right, schema))
            {
                // Check if one is from left table and one is from right table
                if left_idx < left_column_count && right_idx >= left_column_count {
                    // Left column from left table, right column from right table
                    return Some(EquiJoinInfo {
                        left_col_idx: left_idx,
                        right_col_idx: right_idx - left_column_count,
                    });
                } else if right_idx < left_column_count && left_idx >= left_column_count {
                    // Left column from right table, right column from left table (swapped)
                    return Some(EquiJoinInfo {
                        left_col_idx: right_idx,
                        right_col_idx: left_idx - left_column_count,
                    });
                }
            }
            None
        }
        // For complex conditions (AND, OR, etc.), we could potentially optimize
        // multiple equi-join conditions, but for now we fall back to nested loop
        _ => None,
    }
}

/// Extract column index from an expression if it's a simple column reference
fn extract_column_index(expr: &Expression, schema: &CombinedSchema) -> Option<usize> {
    match expr {
        Expression::ColumnRef { table, column } => {
            // Resolve column to index in combined schema
            schema.get_column_index(table.as_deref(), column)
        }
        _ => None,
    }
}
