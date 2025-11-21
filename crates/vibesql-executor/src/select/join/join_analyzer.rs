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
    analyze_single_equi_join(condition, schema, left_column_count)
}

/// Analyze a single equality expression for equi-join
fn analyze_single_equi_join(
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
        _ => None,
    }
}

/// Result of analyzing a compound condition for equi-join opportunities
#[derive(Debug)]
pub struct CompoundEquiJoinResult {
    /// The equi-join info for the hash join
    pub equi_join: EquiJoinInfo,
    /// Remaining conditions to apply as post-join filter
    pub remaining_conditions: Vec<Expression>,
}

/// Analyze a potentially compound (AND) condition to extract equi-join opportunities
///
/// For compound conditions like `a.x = b.x AND a.y > 5 AND b.z = 10`, this will:
/// 1. Extract the first equi-join condition (`a.x = b.x`) for hash join
/// 2. Return remaining conditions (`a.y > 5 AND b.z = 10`) as post-join filters
///
/// This enables hash join optimization for complex WHERE clauses in queries like TPC-H Q3.
pub fn analyze_compound_equi_join(
    condition: &Expression,
    schema: &CombinedSchema,
    left_column_count: usize,
) -> Option<CompoundEquiJoinResult> {
    // First try as a simple equi-join
    if let Some(equi_join) = analyze_single_equi_join(condition, schema, left_column_count) {
        return Some(CompoundEquiJoinResult {
            equi_join,
            remaining_conditions: vec![],
        });
    }

    // Try to extract from AND conditions
    match condition {
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            // Flatten all AND conditions
            let mut conditions = Vec::new();
            flatten_and_conditions(condition, &mut conditions);

            // Find the first equi-join condition
            for (i, cond) in conditions.iter().enumerate() {
                if let Some(equi_join) = analyze_single_equi_join(cond, schema, left_column_count) {
                    // Build remaining conditions
                    let remaining: Vec<Expression> = conditions
                        .iter()
                        .enumerate()
                        .filter(|(j, _)| *j != i)
                        .map(|(_, c)| (*c).clone())
                        .collect();

                    return Some(CompoundEquiJoinResult {
                        equi_join,
                        remaining_conditions: remaining,
                    });
                }
            }

            // No equi-join found in AND conditions
            None
        }
        _ => None,
    }
}

/// Flatten nested AND conditions into a vector
fn flatten_and_conditions<'a>(expr: &'a Expression, out: &mut Vec<&'a Expression>) {
    match expr {
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            flatten_and_conditions(left, out);
            flatten_and_conditions(right, out);
        }
        _ => out.push(expr),
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
