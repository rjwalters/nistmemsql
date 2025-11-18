//! Predicate reordering optimizer for WHERE clauses
//!
//! Analyzes complex predicates and reorders AND chains to evaluate
//! cheaper and more selective predicates first. This can dramatically
//! improve performance for queries with many nested conditions by
//! short-circuiting early on false predicates.
//!
//! Cost Model:
//! - IS NULL checks: cost 1 (very cheap, often very selective)
//! - Literal comparisons: cost 1 (always cheap)
//! - Simple column comparisons: cost 2 (cheap)
//! - BETWEEN: cost 3 (moderate - two comparisons)
//! - IN with literals: cost 4 (moderate - list traversal)
//! - OR expressions: cost 10+ (expensive - less short-circuit benefit)
//! - Function calls: cost 20+ (expensive)
//! - Subqueries: cost 100+ (very expensive)

use vibesql_ast::{BinaryOperator, Expression};

/// Cost score for a predicate (lower is better/cheaper)
type PredicateCost = u32;

/// Reorder AND predicates in a WHERE clause for optimal evaluation order
pub fn optimize_predicate(expr: &Expression) -> Expression {
    match expr {
        Expression::BinaryOp { left, op, right } if matches!(op, BinaryOperator::And) => {
            // Extract all AND-connected predicates into a flat list
            let mut predicates = Vec::new();
            extract_and_chain(expr, &mut predicates);

            // Score each predicate
            let mut scored_predicates: Vec<(PredicateCost, Expression)> = predicates
                .into_iter()
                .map(|pred| {
                    let cost = estimate_predicate_cost(&pred);
                    (cost, pred)
                })
                .collect();

            // Sort by cost (ascending - cheapest first)
            scored_predicates.sort_by_key(|(cost, _)| *cost);

            // Rebuild AND chain with optimized order
            let predicates: Vec<Expression> = scored_predicates
                .into_iter()
                .map(|(_, pred)| pred)
                .collect();

            rebuild_and_chain(predicates)
        }
        // For OR expressions, recursively optimize each side but don't reorder
        // (reordering OR branches doesn't help as much since we can't short-circuit)
        Expression::BinaryOp { left, op, right } if matches!(op, BinaryOperator::Or) => {
            Expression::BinaryOp {
                left: Box::new(optimize_predicate(left)),
                op: op.clone(),
                right: Box::new(optimize_predicate(right)),
            }
        }
        // For other binary ops, recursively optimize both sides
        Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
            left: Box::new(optimize_predicate(left)),
            op: op.clone(),
            right: Box::new(optimize_predicate(right)),
        },
        // For other expression types, return as-is
        _ => expr.clone(),
    }
}

/// Extract all predicates connected by AND into a flat list
fn extract_and_chain(expr: &Expression, predicates: &mut Vec<Expression>) {
    match expr {
        Expression::BinaryOp { left, op, right } if matches!(op, BinaryOperator::And) => {
            // Recursively extract from left and right
            extract_and_chain(left, predicates);
            extract_and_chain(right, predicates);
        }
        // Not an AND - this is a leaf predicate
        _ => {
            // Recursively optimize nested expressions (like OR branches)
            predicates.push(optimize_predicate(expr));
        }
    }
}

/// Rebuild an AND chain from a list of predicates
fn rebuild_and_chain(mut predicates: Vec<Expression>) -> Expression {
    if predicates.is_empty() {
        // Should never happen, but return TRUE if it does
        Expression::Literal(vibesql_types::SqlValue::Boolean(true))
    } else if predicates.len() == 1 {
        predicates.pop().unwrap()
    } else {
        // Build right-associative tree: a AND (b AND (c AND d))
        let mut result = predicates.pop().unwrap();
        while let Some(pred) = predicates.pop() {
            result = Expression::BinaryOp {
                left: Box::new(pred),
                op: BinaryOperator::And,
                right: Box::new(result),
            };
        }
        result
    }
}

/// Estimate the cost of evaluating a predicate
fn estimate_predicate_cost(expr: &Expression) -> PredicateCost {
    match expr {
        // Literals are free (will be constant-folded)
        Expression::Literal(_) => 1,

        // IS NULL / IS NOT NULL - very cheap and often very selective
        Expression::UnaryOp {
            op: vibesql_ast::UnaryOperator::IsNull | vibesql_ast::UnaryOperator::IsNotNull,
            ..
        } => 1,

        // Simple column comparisons - cheap
        Expression::BinaryOp { left, op, right }
            if is_simple_comparison(op)
                && (is_column_or_literal(left) && is_column_or_literal(right)) =>
        {
            2
        }

        // BETWEEN - two comparisons, moderate cost
        Expression::Between { .. } => 3,

        // IN with literal list - moderate cost
        Expression::InList { values, .. } => {
            // Cost scales with list size, but cap at 20
            std::cmp::min(4 + (values.len() as u32 / 10), 20)
        }

        // IN with subquery - very expensive
        Expression::In { .. } => 100,

        // OR expressions - expensive because less opportunity for short-circuit
        // Cost is sum of both branches since we might need to evaluate both
        Expression::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => 10 + estimate_predicate_cost(left) + estimate_predicate_cost(right),

        // AND expressions - recursively estimate (shouldn't normally hit this
        // since we extract AND chains, but handle it anyway)
        Expression::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => estimate_predicate_cost(left) + estimate_predicate_cost(right),

        // Other binary operations - moderate cost
        Expression::BinaryOp { left, right, .. } => {
            2 + estimate_predicate_cost(left) + estimate_predicate_cost(right)
        }

        // Function calls - expensive
        Expression::Function { args, .. } => {
            20 + args.iter().map(estimate_predicate_cost).sum::<u32>()
        }

        // Aggregate functions - very expensive (though shouldn't appear in WHERE)
        Expression::AggregateFunction { .. } => 50,

        // Window functions - very expensive (though shouldn't appear in WHERE)
        Expression::WindowFunction { .. } => 50,

        // Subqueries - very expensive
        Expression::ScalarSubquery(_) => 100,

        // CASE expressions - cost depends on number of branches
        Expression::Case { when_clauses, .. } => {
            10 + (when_clauses.len() as u32 * 5)
        }

        // CAST - relatively cheap
        Expression::Cast { expr, .. } => 3 + estimate_predicate_cost(expr),

        // Column references - cheap
        Expression::ColumnRef { .. } => 1,

        // Other expressions - default moderate cost
        _ => 5,
    }
}

/// Check if operator is a simple comparison (=, <, >, <=, >=, !=)
fn is_simple_comparison(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Equal
            | BinaryOperator::NotEqual
            | BinaryOperator::LessThan
            | BinaryOperator::LessThanOrEqual
            | BinaryOperator::GreaterThan
            | BinaryOperator::GreaterThanOrEqual
    )
}

/// Check if expression is a column reference or literal (simple expressions)
fn is_column_or_literal(expr: &Expression) -> bool {
    matches!(expr, Expression::ColumnRef { .. } | Expression::Literal(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::SqlValue;

    fn col(name: &str) -> Expression {
        Expression::ColumnRef {
            table: None,
            column: name.to_string(),
        }
    }

    fn lit(val: SqlValue) -> Expression {
        Expression::Literal(val)
    }

    fn and(left: Expression, right: Expression) -> Expression {
        Expression::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        }
    }

    fn eq(left: Expression, right: Expression) -> Expression {
        Expression::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Equal,
            right: Box::new(right),
        }
    }

    fn is_null(expr: Expression) -> Expression {
        Expression::UnaryOp {
            op: vibesql_ast::UnaryOperator::IsNull,
            expr: Box::new(expr),
        }
    }

    #[test]
    fn test_extract_and_chain() {
        // a AND b AND c should extract to [a, b, c]
        let expr = and(
            col("a"),
            and(col("b"), col("c")),
        );

        let mut predicates = Vec::new();
        extract_and_chain(&expr, &mut predicates);

        assert_eq!(predicates.len(), 3);
    }

    #[test]
    fn test_rebuild_and_chain() {
        let predicates = vec![col("a"), col("b"), col("c")];
        let result = rebuild_and_chain(predicates);

        // Should rebuild as nested AND
        match result {
            Expression::BinaryOp { op: BinaryOperator::And, .. } => {},
            _ => panic!("Expected AND expression"),
        }
    }

    #[test]
    fn test_cost_estimation() {
        // IS NULL should be cheapest
        assert_eq!(estimate_predicate_cost(&is_null(col("a"))), 1);

        // Simple comparison should be cheap
        let simple_cmp = eq(col("a"), lit(SqlValue::Integer(42)));
        assert_eq!(estimate_predicate_cost(&simple_cmp), 2);

        // BETWEEN should be moderate
        let between = Expression::Between {
            expr: Box::new(col("a")),
            low: Box::new(lit(SqlValue::Integer(1))),
            high: Box::new(lit(SqlValue::Integer(100))),
            negated: false,
            symmetric: false,
        };
        assert_eq!(estimate_predicate_cost(&between), 3);
    }

    #[test]
    fn test_optimize_reorders_by_cost() {
        // Create: expensive AND cheap AND moderate
        // Expected: cheap AND moderate AND expensive
        let expensive = Expression::Function {
            name: "EXPENSIVE".to_string(),
            args: vec![col("a")],
            character_unit: None,
        };

        let cheap = is_null(col("b"));
        let moderate = eq(col("c"), lit(SqlValue::Integer(42)));

        let expr = and(and(expensive.clone(), cheap.clone()), moderate.clone());
        let optimized = optimize_predicate(&expr);

        // Extract predicates from optimized expression
        let mut predicates = Vec::new();
        extract_and_chain(&optimized, &mut predicates);

        // First should be cheapest (IS NULL)
        match &predicates[0] {
            Expression::UnaryOp { op: vibesql_ast::UnaryOperator::IsNull, .. } => {},
            _ => panic!("Expected IS NULL as first predicate after optimization"),
        }
    }
}
