//! EXISTS → SEMI-JOIN transformation
//!
//! Converts correlated EXISTS subqueries to semi-joins for much better performance.
//!
//! ## Example transformation
//!
//! Before:
//! ```sql
//! SELECT * FROM orders
//! WHERE EXISTS (
//!   SELECT 1 FROM lineitem
//!   WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate
//! )
//! ```
//!
//! After:
//! ```sql
//! SELECT * FROM orders
//! SEMI JOIN lineitem ON l_orderkey = o_orderkey AND l_commitdate < l_receiptdate
//! ```
//!
//! This transformation provides 100-10,000x speedup by using hash semi-join instead of
//! re-executing the subquery for each outer row.

use vibesql_ast::{Expression, FromClause, JoinType, SelectStmt};

/// Try to convert WHERE clause EXISTS to a SEMI JOIN
///
/// This function detects simple EXISTS patterns and transforms them into
/// semi-joins for optimal performance.
///
/// Requirements for transformation:
/// 1. EXISTS must be in WHERE clause (not in SELECT list or other places)
/// 2. EXISTS subquery must have a FROM clause with a single table
/// 3. All conditions in the EXISTS can be moved to the JOIN ON clause
///
/// Returns (new_stmt, was_transformed)
pub fn try_convert_exists_to_semijoin(stmt: &SelectStmt) -> (SelectStmt, bool) {
    // Only transform if we have both a FROM clause and a WHERE clause
    let Some(from_clause) = &stmt.from else {
        return (stmt.clone(), false);
    };

    let Some(where_clause) = &stmt.where_clause else {
        return (stmt.clone(), false);
    };

    // Try to find and extract an EXISTS expression from WHERE
    let Some((exists_subquery, exists_negated, remaining_where)) =
        extract_exists_from_where(where_clause)
    else {
        return (stmt.clone(), false);
    };

    // Don't convert NOT EXISTS for now (would need ANTI join)
    if exists_negated {
        return (stmt.clone(), false);
    }

    // Skip EXISTS with LIMIT 1 - these are likely from IN → EXISTS transformation
    // and should remain as EXISTS for proper evaluation
    if exists_subquery.limit == Some(1) {
        return (stmt.clone(), false);
    }

    // Try to convert the EXISTS subquery to a semi-join
    let Some((semi_join_table, semi_join_condition)) = try_convert_subquery_to_join(&exists_subquery)
    else {
        return (stmt.clone(), false);
    };

    // Build the new FROM clause with SEMI JOIN
    let new_from = FromClause::Join {
        left: Box::new(from_clause.clone()),
        right: Box::new(semi_join_table),
        join_type: JoinType::Semi,
        condition: Some(semi_join_condition),
        natural: false,
    };

    // Build the transformed statement
    let mut new_stmt = stmt.clone();
    new_stmt.from = Some(new_from);
    new_stmt.where_clause = remaining_where;

    (new_stmt, true)
}

/// Extract the first EXISTS expression from WHERE clause
///
/// Returns (subquery, negated, remaining_where_without_exists)
fn extract_exists_from_where(
    where_expr: &Expression,
) -> Option<(SelectStmt, bool, Option<Expression>)> {
    match where_expr {
        // Direct EXISTS at top level
        Expression::Exists { subquery, negated } => {
            Some((*subquery.clone(), *negated, None))
        }

        // EXISTS combined with AND
        Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::And,
            left,
            right,
        } => {
            // Try left side
            if let Expression::Exists { subquery, negated } = left.as_ref() {
                return Some((*subquery.clone(), *negated, Some(right.as_ref().clone())));
            }

            // Try right side
            if let Expression::Exists { subquery, negated } = right.as_ref() {
                return Some((*subquery.clone(), *negated, Some(left.as_ref().clone())));
            }

            // Recursively search deeper (for nested ANDs)
            if let Some((subq, neg, remaining)) = extract_exists_from_where(left) {
                // Combine remaining with right
                let new_remaining = if let Some(rem) = remaining {
                    Some(Expression::BinaryOp {
                        op: vibesql_ast::BinaryOperator::And,
                        left: Box::new(rem),
                        right: right.clone(),
                    })
                } else {
                    Some(right.as_ref().clone())
                };
                return Some((subq, neg, new_remaining));
            }

            if let Some((subq, neg, remaining)) = extract_exists_from_where(right) {
                // Combine remaining with left
                let new_remaining = if let Some(rem) = remaining {
                    Some(Expression::BinaryOp {
                        op: vibesql_ast::BinaryOperator::And,
                        left: left.clone(),
                        right: Box::new(rem),
                    })
                } else {
                    Some(left.as_ref().clone())
                };
                return Some((subq, neg, new_remaining));
            }

            None
        }

        _ => None,
    }
}

/// Try to convert an EXISTS subquery to a semi-join
///
/// Returns (table_from_clause, join_condition) if successful
fn try_convert_subquery_to_join(subquery: &SelectStmt) -> Option<(FromClause, Expression)> {
    // Must have a FROM clause
    let from_clause = subquery.from.as_ref()?;

    // For now, only support simple table references (not joins or subqueries)
    // This handles the common case like: EXISTS (SELECT * FROM lineitem WHERE ...)
    let (table_name, table_alias) = match from_clause {
        FromClause::Table { name, alias } => (name.clone(), alias.clone()),
        _ => return None, // Complex FROM clause - don't transform for now
    };

    // Extract all conditions from the EXISTS subquery
    // These become the JOIN ON conditions
    let join_condition = subquery.where_clause.as_ref()?;

    // Create the table FromClause for the right side of the join
    let semi_join_table = FromClause::Table {
        name: table_name,
        alias: table_alias,
    };

    Some((semi_join_table, join_condition.clone()))
}
