//! Subquery rewriting optimization for IN predicates
//!
//! This module implements Phase 2 of IN subquery optimization (issue #2138):
//! - Rewrites correlated IN subqueries to EXISTS with LIMIT 1 for early termination
//! - Adds DISTINCT to uncorrelated IN subqueries to reduce duplicate processing
//!
//! These optimizations work in conjunction with Phase 1 (HashSet optimization, #2136)
//! to provide 5-50x speedup for IN subqueries.

use vibesql_ast::{BinaryOperator, Expression, SelectItem, SelectStmt};

/// Rewrite a SELECT statement to optimize IN subqueries
///
/// Applies two optimizations:
/// 1. Correlated IN → EXISTS: Better leverages indexes and allows early termination
/// 2. Uncorrelated IN → IN with DISTINCT: Reduces duplicate comparisons
///
/// # Examples
///
/// ```sql
/// -- Before: Correlated IN
/// SELECT * FROM orders WHERE customer_id IN (
///   SELECT customer_id FROM customers WHERE region = 'APAC'
/// )
///
/// -- After: Rewritten to EXISTS
/// SELECT * FROM orders WHERE EXISTS (
///   SELECT 1 FROM customers
///   WHERE region = 'APAC' AND customers.customer_id = orders.customer_id
///   LIMIT 1
/// )
/// ```
///
/// ```sql
/// -- Before: Uncorrelated IN without DISTINCT
/// SELECT * FROM orders WHERE status IN (SELECT status FROM valid_statuses)
///
/// -- After: Added DISTINCT to reduce comparisons
/// SELECT * FROM orders WHERE status IN (SELECT DISTINCT status FROM valid_statuses)
/// ```
pub fn rewrite_subquery_optimizations(stmt: &SelectStmt) -> SelectStmt {
    let mut rewritten = stmt.clone();

    // Rewrite WHERE clause
    if let Some(where_clause) = &stmt.where_clause {
        rewritten.where_clause = Some(rewrite_expression(where_clause));
    }

    // Rewrite HAVING clause
    if let Some(having) = &stmt.having {
        rewritten.having = Some(rewrite_expression(having));
    }

    // Rewrite SELECT list expressions
    rewritten.select_list = stmt
        .select_list
        .iter()
        .map(|item| match item {
            SelectItem::Expression { expr, alias } => SelectItem::Expression {
                expr: rewrite_expression(expr),
                alias: alias.clone(),
            },
            other => other.clone(),
        })
        .collect();

    // Recursively rewrite subqueries in FROM clause
    if let Some(from) = &stmt.from {
        rewritten.from = Some(rewrite_from_clause(from));
    }

    // Recursively rewrite set operations
    if let Some(set_op) = &stmt.set_operation {
        let mut new_set_op = set_op.clone();
        new_set_op.right = Box::new(rewrite_subquery_optimizations(&set_op.right));
        rewritten.set_operation = Some(new_set_op);
    }

    rewritten
}

/// Rewrite an expression to optimize IN subqueries
fn rewrite_expression(expr: &Expression) -> Expression {
    match expr {
        // Optimize IN subquery
        Expression::In {
            expr: in_expr,
            subquery,
            negated,
        } => {
            // Check if subquery is correlated
            if is_correlated(subquery) {
                // Correlated subquery: Rewrite IN → EXISTS
                // This allows database to stop after first match and better leverage indexes
                rewrite_in_to_exists(in_expr, subquery, *negated)
            } else {
                // Uncorrelated subquery: Add DISTINCT to reduce duplicate processing
                let optimized_subquery = add_distinct_to_in_subquery(subquery);
                Expression::In {
                    expr: Box::new(rewrite_expression(in_expr)),
                    subquery: Box::new(optimized_subquery),
                    negated: *negated,
                }
            }
        }

        // Recursively rewrite nested expressions
        Expression::BinaryOp { op, left, right } => Expression::BinaryOp {
            op: *op,
            left: Box::new(rewrite_expression(left)),
            right: Box::new(rewrite_expression(right)),
        },

        Expression::UnaryOp { op, expr } => Expression::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_expression(expr)),
        },

        Expression::IsNull { expr, negated } => Expression::IsNull {
            expr: Box::new(rewrite_expression(expr)),
            negated: *negated,
        },

        Expression::Case {
            operand,
            when_clauses,
            else_result,
        } => Expression::Case {
            operand: operand.as_ref().map(|e| Box::new(rewrite_expression(e))),
            when_clauses: when_clauses
                .iter()
                .map(|clause| vibesql_ast::CaseWhen {
                    conditions: clause.conditions.iter().map(rewrite_expression).collect(),
                    result: rewrite_expression(&clause.result),
                })
                .collect(),
            else_result: else_result.as_ref().map(|e| Box::new(rewrite_expression(e))),
        },

        Expression::ScalarSubquery(subquery) => {
            Expression::ScalarSubquery(Box::new(rewrite_subquery_optimizations(subquery)))
        }

        Expression::Exists { subquery, negated } => Expression::Exists {
            subquery: Box::new(rewrite_subquery_optimizations(subquery)),
            negated: *negated,
        },

        Expression::QuantifiedComparison {
            expr,
            op,
            quantifier,
            subquery,
        } => Expression::QuantifiedComparison {
            expr: Box::new(rewrite_expression(expr)),
            op: *op,
            quantifier: quantifier.clone(),
            subquery: Box::new(rewrite_subquery_optimizations(subquery)),
        },

        Expression::InList {
            expr,
            values,
            negated,
        } => Expression::InList {
            expr: Box::new(rewrite_expression(expr)),
            values: values.iter().map(rewrite_expression).collect(),
            negated: *negated,
        },

        Expression::Between {
            expr,
            low,
            high,
            negated,
            symmetric,
        } => Expression::Between {
            expr: Box::new(rewrite_expression(expr)),
            low: Box::new(rewrite_expression(low)),
            high: Box::new(rewrite_expression(high)),
            negated: *negated,
            symmetric: *symmetric,
        },

        Expression::Cast { expr, data_type } => Expression::Cast {
            expr: Box::new(rewrite_expression(expr)),
            data_type: data_type.clone(),
        },

        Expression::Function {
            name,
            args,
            character_unit,
        } => Expression::Function {
            name: name.clone(),
            args: args.iter().map(rewrite_expression).collect(),
            character_unit: character_unit.clone(),
        },

        Expression::AggregateFunction {
            name,
            distinct,
            args,
        } => Expression::AggregateFunction {
            name: name.clone(),
            distinct: *distinct,
            args: args.iter().map(rewrite_expression).collect(),
        },

        Expression::Position {
            substring,
            string,
            character_unit,
        } => Expression::Position {
            substring: Box::new(rewrite_expression(substring)),
            string: Box::new(rewrite_expression(string)),
            character_unit: character_unit.clone(),
        },

        Expression::Trim {
            position,
            removal_char,
            string,
        } => Expression::Trim {
            position: position.clone(),
            removal_char: removal_char.as_ref().map(|e| Box::new(rewrite_expression(e))),
            string: Box::new(rewrite_expression(string)),
        },

        Expression::Like {
            expr,
            pattern,
            negated,
        } => Expression::Like {
            expr: Box::new(rewrite_expression(expr)),
            pattern: Box::new(rewrite_expression(pattern)),
            negated: *negated,
        },

        Expression::Interval {
            value,
            unit,
            leading_precision,
            fractional_precision,
        } => Expression::Interval {
            value: Box::new(rewrite_expression(value)),
            unit: unit.clone(),
            leading_precision: *leading_precision,
            fractional_precision: *fractional_precision,
        },

        // Literals, column refs, and special expressions don't need rewriting
        Expression::Literal(_)
        | Expression::ColumnRef { .. }
        | Expression::Wildcard
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Default
        | Expression::DuplicateKeyValue { .. }
        | Expression::WindowFunction { .. }
        | Expression::NextValue { .. }
        | Expression::MatchAgainst { .. }
        | Expression::PseudoVariable { .. }
        | Expression::SessionVariable { .. } => expr.clone(),
    }
}

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
fn rewrite_in_to_exists(
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
fn add_distinct_to_in_subquery(subquery: &SelectStmt) -> SelectStmt {
    let mut optimized = subquery.clone();

    // Only add DISTINCT if not already present
    if !subquery.distinct {
        optimized.distinct = true;
    }

    // Recursively optimize nested subqueries
    optimized = rewrite_subquery_optimizations(&optimized);

    optimized
}

/// Check if a subquery is correlated (references outer query columns)
///
/// A subquery is correlated if it contains column references that are not
/// bound within the subquery itself (i.e., they reference columns from the
/// outer query).
///
/// # Examples
///
/// Correlated:
/// ```sql
/// SELECT * FROM orders WHERE customer_id IN (
///   SELECT customer_id FROM customers WHERE customers.region = orders.region
/// )
/// ```
///
/// Uncorrelated:
/// ```sql
/// SELECT * FROM orders WHERE status IN (
///   SELECT status FROM valid_statuses
/// )
/// ```
fn is_correlated(subquery: &SelectStmt) -> bool {
    // Check WHERE clause for external column references
    if let Some(where_clause) = &subquery.where_clause {
        if has_external_column_refs(where_clause, subquery) {
            return true;
        }
    }

    // Check SELECT list for external column references
    for item in &subquery.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            if has_external_column_refs(expr, subquery) {
                return true;
            }
        }
    }

    // Check HAVING clause for external column references
    if let Some(having) = &subquery.having {
        if has_external_column_refs(having, subquery) {
            return true;
        }
    }

    false
}

/// Check if an expression contains column references external to the subquery
///
/// This is a heuristic check: a qualified column reference (table.column)
/// that doesn't match the subquery's FROM clause tables is likely external.
///
/// Note: This is a conservative heuristic that may miss some correlations
/// or incorrectly identify some as correlated. A more accurate implementation
/// would require full symbol table analysis.
fn has_external_column_refs(expr: &Expression, subquery: &SelectStmt) -> bool {
    match expr {
        Expression::ColumnRef { table: Some(table), .. } => {
            // If column is qualified, check if table is in subquery's FROM clause
            !subquery_references_table(subquery, table)
        }

        Expression::ColumnRef { table: None, .. } => {
            // Unqualified column refs might be external, but we can't tell for sure
            // Conservative approach: assume they're internal (to avoid over-optimizing)
            false
        }

        // Recursively check nested expressions
        Expression::BinaryOp { left, right, .. } => {
            has_external_column_refs(left, subquery) || has_external_column_refs(right, subquery)
        }

        Expression::UnaryOp { expr, .. } => has_external_column_refs(expr, subquery),

        Expression::IsNull { expr, .. } => has_external_column_refs(expr, subquery),

        Expression::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            operand.as_ref().map_or(false, |e| has_external_column_refs(e, subquery))
                || when_clauses.iter().any(|clause| {
                    clause.conditions.iter().any(|cond| has_external_column_refs(cond, subquery))
                        || has_external_column_refs(&clause.result, subquery)
                })
                || else_result.as_ref().map_or(false, |e| has_external_column_refs(e, subquery))
        }

        Expression::ScalarSubquery(_)
        | Expression::In { .. }
        | Expression::Exists { .. }
        | Expression::QuantifiedComparison { .. } => {
            // Nested subqueries are handled separately
            false
        }

        Expression::InList { expr, values, .. } => {
            has_external_column_refs(expr, subquery)
                || values.iter().any(|v| has_external_column_refs(v, subquery))
        }

        Expression::Between { expr, low, high, .. } => {
            has_external_column_refs(expr, subquery)
                || has_external_column_refs(low, subquery)
                || has_external_column_refs(high, subquery)
        }

        Expression::Cast { expr, .. } => has_external_column_refs(expr, subquery),

        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            args.iter().any(|arg| has_external_column_refs(arg, subquery))
        }

        Expression::Position { substring, string, .. } => {
            has_external_column_refs(substring, subquery) || has_external_column_refs(string, subquery)
        }

        Expression::Trim {
            removal_char,
            string,
            ..
        } => {
            removal_char.as_ref().map_or(false, |e| has_external_column_refs(e, subquery))
                || has_external_column_refs(string, subquery)
        }

        Expression::Like { expr, pattern, .. } => {
            has_external_column_refs(expr, subquery) || has_external_column_refs(pattern, subquery)
        }

        Expression::Interval { value, .. } => has_external_column_refs(value, subquery),

        // Literals and special expressions don't reference columns
        Expression::Literal(_)
        | Expression::Wildcard
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Default
        | Expression::DuplicateKeyValue { .. }
        | Expression::WindowFunction { .. }
        | Expression::NextValue { .. }
        | Expression::MatchAgainst { .. }
        | Expression::PseudoVariable { .. }
        | Expression::SessionVariable { .. } => false,
    }
}

/// Check if subquery's FROM clause references a specific table
fn subquery_references_table(subquery: &SelectStmt, table_name: &str) -> bool {
    if let Some(from) = &subquery.from {
        from_clause_contains_table(from, table_name)
    } else {
        false
    }
}

/// Recursively check if FROM clause contains a table reference
fn from_clause_contains_table(from: &vibesql_ast::FromClause, table_name: &str) -> bool {
    match from {
        vibesql_ast::FromClause::Table { name, alias } => {
            name == table_name || alias.as_ref().map_or(false, |a| a == table_name)
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            from_clause_contains_table(left, table_name)
                || from_clause_contains_table(right, table_name)
        }
        vibesql_ast::FromClause::Subquery { alias, .. } => alias == table_name,
    }
}

/// Recursively rewrite FROM clause subqueries
fn rewrite_from_clause(from: &vibesql_ast::FromClause) -> vibesql_ast::FromClause {
    match from {
        vibesql_ast::FromClause::Table { name, alias } => vibesql_ast::FromClause::Table {
            name: name.clone(),
            alias: alias.clone(),
        },
        vibesql_ast::FromClause::Join {
            left,
            right,
            join_type,
            condition,
            natural,
        } => vibesql_ast::FromClause::Join {
            left: Box::new(rewrite_from_clause(left)),
            right: Box::new(rewrite_from_clause(right)),
            join_type: join_type.clone(),
            condition: condition.as_ref().map(rewrite_expression),
            natural: *natural,
        },
        vibesql_ast::FromClause::Subquery { query, alias } => vibesql_ast::FromClause::Subquery {
            query: Box::new(rewrite_subquery_optimizations(query)),
            alias: alias.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::SqlValue;

    /// Helper to create a simple SELECT statement for testing
    fn simple_select(table: &str, column: &str) -> SelectStmt {
        SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::ColumnRef {
                    table: None,
                    column: column.to_string(),
                },
                alias: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(vibesql_ast::FromClause::Table {
                name: table.to_string(),
                alias: None,
            }),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
        }
    }

    #[test]
    fn test_add_distinct_to_uncorrelated_in_subquery() {
        let subquery = simple_select("customers", "region");
        let optimized = add_distinct_to_in_subquery(&subquery);

        assert!(optimized.distinct, "DISTINCT should be added to uncorrelated subquery");
    }

    #[test]
    fn test_distinct_not_duplicated() {
        let mut subquery = simple_select("customers", "region");
        subquery.distinct = true;

        let optimized = add_distinct_to_in_subquery(&subquery);

        assert!(optimized.distinct, "DISTINCT should remain true");
    }

    #[test]
    fn test_uncorrelated_subquery_detection() {
        let subquery = simple_select("customers", "region");

        assert!(!is_correlated(&subquery), "Simple subquery should be uncorrelated");
    }

    #[test]
    fn test_correlated_subquery_detection() {
        let mut subquery = simple_select("customers", "customer_id");

        // Add WHERE clause with qualified column reference to outer table
        subquery.where_clause = Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("customers".to_string()),
                column: "region".to_string(),
            }),
            right: Box::new(Expression::ColumnRef {
                table: Some("orders".to_string()),
                column: "region".to_string(),
            }),
        });

        assert!(
            is_correlated(&subquery),
            "Subquery with qualified external column ref should be correlated"
        );
    }

    #[test]
    fn test_in_to_exists_rewrite() {
        let in_expr = Expression::ColumnRef {
            table: None,
            column: "customer_id".to_string(),
        };

        let subquery = simple_select("customers", "customer_id");

        let rewritten = rewrite_in_to_exists(&in_expr, &subquery, false);

        match rewritten {
            Expression::Exists { subquery: exists_subquery, negated } => {
                assert!(!negated, "Negation should match input");
                assert_eq!(exists_subquery.limit, Some(1), "LIMIT 1 should be added");
                assert!(
                    exists_subquery.where_clause.is_some(),
                    "Correlation predicate should be added"
                );

                // Check that SELECT list is rewritten to SELECT 1
                assert_eq!(exists_subquery.select_list.len(), 1);
                if let SelectItem::Expression { expr, .. } = &exists_subquery.select_list[0] {
                    assert!(
                        matches!(expr, Expression::Literal(SqlValue::Integer(1))),
                        "SELECT list should be rewritten to SELECT 1"
                    );
                }
            }
            _ => panic!("Expected EXISTS expression"),
        }
    }
}
