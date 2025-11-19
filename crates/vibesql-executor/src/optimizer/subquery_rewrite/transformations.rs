//! IN subquery transformation strategies
//!
//! This module provides the core transformation logic for optimizing IN subqueries:
//! - Correlated IN → EXISTS with LIMIT 1 for early termination
//! - Uncorrelated IN → IN with DISTINCT to reduce duplicates

use vibesql_ast::{BinaryOperator, Expression, SelectItem, SelectStmt};

/// Rewrite correlated IN subquery to EXISTS with correlation predicate
///
/// Transforms:
/// ```sql
/// expr IN (SELECT col FROM table WHERE ...)
/// ```
///
/// To:
/// ```sql
/// EXISTS (SELECT 1 FROM table WHERE ... AND col = expr LIMIT 1)
/// ```
///
/// This allows the database to:
/// - Stop after finding first match (LIMIT 1 enables early termination)
/// - Better leverage indexes on the correlation column
/// - Potentially use better query plans
pub(super) fn rewrite_in_to_exists(
    in_expr: &Expression,
    subquery: &SelectStmt,
    negated: bool,
) -> Expression {
    // Create rewritten subquery
    let mut exists_subquery = subquery.clone();

    // Change SELECT list to SELECT 1 (EXISTS doesn't care about actual values)
    exists_subquery.select_list = vec![SelectItem::Expression {
        expr: Expression::Literal(vibesql_types::SqlValue::Integer(1)),
        alias: None,
    }];

    // Add LIMIT 1 for early termination after first match
    // EXISTS only cares if ANY row matches, not all rows
    exists_subquery.limit = Some(1);

    // Add correlation predicate: subquery_col = outer_expr
    // We need to extract the subquery column from the original SELECT list
    // For now, we'll add the correlation predicate to the WHERE clause
    // The correlation will reference the original SELECT column

    // Extract the correlation column expression from original SELECT list
    // This assumes single-column subquery (already validated by executor)
    if let Some(SelectItem::Expression { expr: subquery_col, .. }) = subquery.select_list.first() {
        // Create correlation predicate: subquery_col = in_expr
        let correlation_predicate = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(subquery_col.clone()),
            right: Box::new(in_expr.clone()),
        };

        // Combine with existing WHERE clause using AND
        exists_subquery.where_clause = if let Some(existing_where) = &subquery.where_clause {
            Some(Expression::BinaryOp {
                op: BinaryOperator::And,
                left: Box::new(existing_where.clone()),
                right: Box::new(correlation_predicate),
            })
        } else {
            Some(correlation_predicate)
        };
    }

    // Create EXISTS expression
    Expression::Exists {
        subquery: Box::new(exists_subquery),
        negated,
    }
}

/// Add DISTINCT to uncorrelated IN subquery to eliminate duplicates
///
/// Transforms:
/// ```sql
/// expr IN (SELECT col FROM table)
/// ```
///
/// To:
/// ```sql
/// expr IN (SELECT DISTINCT col FROM table)
/// ```
///
/// This reduces the number of comparisons by eliminating duplicate values
/// from the subquery result set.
pub(super) fn add_distinct_to_in_subquery(subquery: &SelectStmt) -> SelectStmt {
    let mut optimized = subquery.clone();

    // Only add DISTINCT if not already present
    if !subquery.distinct {
        optimized.distinct = true;
    }

    // Note: Recursive optimization is handled by the expression rewriter
    // to avoid circular dependencies between modules

    optimized
}
