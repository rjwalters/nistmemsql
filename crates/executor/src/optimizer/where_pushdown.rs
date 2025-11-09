//! Predicate pushdown optimization to avoid memory exhaustion in multi-table joins
//!
//! This module implements WHERE clause decomposition to enable filtering rows
//! before building Cartesian products in multi-table JOINs.
//!
//! Example: SELECT * FROM t1 JOIN t2 ON t1.a = t2.b WHERE t1.c > 5
//! Instead of: Cartesian product of t1×t2, then filter by WHERE
//! This does:  Filter t1 by c > 5, then join with t2 on a = b

use ast::Expression;
use std::collections::HashSet;

/// Classification of a WHERE clause predicate
#[derive(Debug, Clone, PartialEq)]
pub enum PredicateType {
    /// Condition references only a single table (can be pushed down)
    TableLocal { table_name: String },
    /// Equijoin condition between two tables (a.col = b.col)
    Equijoin { left_table: String, left_col: String, right_table: String, right_col: String },
    /// Complex predicate (multiple tables or non-equality)
    Complex,
}

/// Result of analyzing a WHERE condition
#[derive(Debug, Clone)]
pub struct PredicateInfo {
    pub predicate_type: PredicateType,
    pub expression: Expression,
}

/// Analyze a WHERE clause and decompose it into pushdown-friendly predicates
///
/// Returns a list of predicates that can be applied at different stages of execution:
/// - Table-local predicates can be applied during table scan
/// - Equijoin predicates can be pushed into join operators
/// - Complex predicates must be applied after joins
pub fn decompose_where_predicates(where_expr: &Expression) -> Vec<PredicateInfo> {
    let mut predicates = Vec::new();
    decompose_conjunctions(where_expr, &mut predicates);
    predicates
}

/// Helper to decompose AND-separated conditions (CNF - Conjunctive Normal Form)
fn decompose_conjunctions(expr: &Expression, predicates: &mut Vec<PredicateInfo>) {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            // AND operator - split into left and right
            if *op == ast::BinaryOperator::And {
                decompose_conjunctions(left, predicates);
                decompose_conjunctions(right, predicates);
                return;
            }
            
            // Not an AND, analyze this expression
            let pred_info = analyze_predicate(expr);
            predicates.push(pred_info);
        }
        _ => {
            // Not a binary op, analyze directly
            let pred_info = analyze_predicate(expr);
            predicates.push(pred_info);
        }
    }
}

/// Analyze a single predicate to determine if it can be pushed down
fn analyze_predicate(expr: &Expression) -> PredicateInfo {
    let pred_type = classify_predicate(expr);
    PredicateInfo { predicate_type: pred_type, expression: expr.clone() }
}

/// Classify a predicate based on which tables it references
fn classify_predicate(expr: &Expression) -> PredicateType {
    let referenced_tables = extract_table_references(expr);

    match referenced_tables.len() {
        0 => {
            // No table references - treat as complex (e.g., WHERE 1=1)
            PredicateType::Complex
        }
        1 => {
            // Single table - can be pushed down
            let table_name = referenced_tables.iter().next().unwrap().clone();
            PredicateType::TableLocal { table_name }
        }
        2 => {
            // Check if this is a simple equijoin condition
            if let Some(equijoin) = try_extract_equijoin(expr, &referenced_tables) {
                equijoin
            } else {
                PredicateType::Complex
            }
        }
        _ => {
            // Multiple tables - complex predicate
            PredicateType::Complex
        }
    }
}

/// Try to extract equijoin information from a binary operation
fn try_extract_equijoin(
    expr: &Expression,
    tables: &HashSet<String>,
) -> Option<PredicateType> {
    match expr {
        Expression::BinaryOp { left, op, right } if *op == ast::BinaryOperator::Equal => {
            // Check for pattern: t1.col = t2.col
            let (left_table, left_col) = extract_column_reference(left)?;
            let (right_table, right_col) = extract_column_reference(right)?;

            // Verify both tables are in our set and they're different
            if tables.contains(&left_table)
                && tables.contains(&right_table)
                && left_table != right_table
            {
                return Some(PredicateType::Equijoin {
                    left_table,
                    left_col,
                    right_table,
                    right_col,
                });
            }

            // Try reversed order: t2.col = t1.col
            if right_table != left_table && tables.contains(&left_table) {
                return Some(PredicateType::Equijoin {
                    left_table: right_table,
                    left_col: right_col,
                    right_table: left_table,
                    right_col: left_col,
                });
            }
        }
        _ => {}
    }
    None
}

/// Extract table and column name from a column reference expression
fn extract_column_reference(expr: &Expression) -> Option<(String, String)> {
    match expr {
        Expression::ColumnRef { table: Some(table), column } => Some((table.clone(), column.clone())),
        Expression::ColumnRef { table: None, column } => {
            // Unqualified column reference - we'll handle this at execution time
            Some(("".to_string(), column.clone()))
        }
        _ => None,
    }
}

/// Extract all table names referenced in an expression
fn extract_table_references(expr: &Expression) -> HashSet<String> {
    let mut tables = HashSet::new();
    extract_table_references_recursive(expr, &mut tables);
    tables
}

/// Recursive helper to extract table references
fn extract_table_references_recursive(expr: &Expression, tables: &mut HashSet<String>) {
    match expr {
        Expression::ColumnRef { table: Some(table), .. } => {
            tables.insert(table.clone());
        }
        Expression::ColumnRef { table: None, .. } => {
            // Unqualified reference - will need to be resolved at execution time
            // For now, we can't push this down without knowing which table it belongs to
        }
        Expression::BinaryOp { left, op: _, right } => {
            extract_table_references_recursive(left, tables);
            extract_table_references_recursive(right, tables);
        }
        Expression::UnaryOp { expr: inner, .. } => {
            extract_table_references_recursive(inner, tables);
        }
        Expression::Function { args, .. } => {
            for arg in args {
                extract_table_references_recursive(arg, tables);
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                extract_table_references_recursive(op, tables);
            }
            for when in when_clauses {
                for cond in &when.conditions {
                    extract_table_references_recursive(cond, tables);
                }
                extract_table_references_recursive(&when.result, tables);
            }
            if let Some(else_expr) = else_result {
                extract_table_references_recursive(else_expr, tables);
            }
        }
        Expression::Between { expr, low, high, .. } => {
            extract_table_references_recursive(expr, tables);
            extract_table_references_recursive(low, tables);
            extract_table_references_recursive(high, tables);
        }
        Expression::IsNull { expr: inner, .. } => {
            extract_table_references_recursive(inner, tables);
        }
        _ => {
            // Other expression types don't reference tables
        }
    }
}

/// Filter predicates that can be applied during table scan
pub fn get_table_local_predicates(
    predicates: &[PredicateInfo],
    table_name: &str,
) -> Vec<Expression> {
    predicates
        .iter()
        .filter_map(|p| {
            if let PredicateType::TableLocal { table_name: ref t } = p.predicate_type {
                if t == table_name {
                    return Some(p.expression.clone());
                }
            }
            None
        })
        .collect()
}

/// Get predicates that must be applied after all joins (complex predicates)
#[allow(dead_code)]
pub fn get_post_join_predicates(predicates: &[PredicateInfo]) -> Vec<Expression> {
    predicates
        .iter()
        .filter_map(|p| {
            if matches!(p.predicate_type, PredicateType::Complex) {
                return Some(p.expression.clone());
            }
            None
        })
        .collect()
}

/// Get equijoin predicates that should be applied during join operations
pub fn get_equijoin_predicates(predicates: &[PredicateInfo]) -> Vec<Expression> {
    predicates
        .iter()
        .filter_map(|p| {
            if matches!(p.predicate_type, PredicateType::Equijoin { .. }) {
                return Some(p.expression.clone());
            }
            None
        })
        .collect()
}

/// Get predicates relevant only to a specific set of tables
///
/// This filters the WHERE clause to only include predicates that reference
/// tables in the given set. Useful for filtering WHERE clauses for specific
/// branches of a join tree.
pub fn get_predicates_for_tables(
    predicates: &[PredicateInfo],
    table_names: &std::collections::HashSet<String>,
) -> Vec<Expression> {
    predicates
        .iter()
        .filter_map(|p| {
            match &p.predicate_type {
                PredicateType::TableLocal { table_name } => {
                    if table_names.contains(table_name) {
                        Some(p.expression.clone())
                    } else {
                        None
                    }
                }
                PredicateType::Equijoin { left_table, right_table, .. } => {
                    // Include equijoin if both tables are in this branch
                    if table_names.contains(left_table) && table_names.contains(right_table) {
                        Some(p.expression.clone())
                    } else {
                        None
                    }
                }
                PredicateType::Complex => {
                    // Check if all referenced tables in this complex predicate are in our set
                    let referenced = extract_table_references(&p.expression);
                    if !referenced.is_empty() && referenced.iter().all(|t| table_names.contains(t)) {
                        Some(p.expression.clone())
                    } else {
                        None
                    }
                }
            }
        })
        .collect()
}

/// Combine multiple expressions with AND operator
pub fn combine_with_and(expressions: Vec<Expression>) -> Option<Expression> {
    match expressions.len() {
        0 => None,
        1 => Some(expressions[0].clone()),
        _ => {
            let mut result = expressions[0].clone();
            for expr in expressions.iter().skip(1) {
                result = Expression::BinaryOp {
                    left: Box::new(result),
                    op: ast::BinaryOperator::And,
                    right: Box::new(expr.clone()),
                };
            }
            Some(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decompose_simple_conjunctions() {
        // WHERE a.x > 5 AND b.y < 10
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: Some("a".to_string()), column: "x".to_string() }),
                op: ast::BinaryOperator::GreaterThan,
                right: Box::new(Expression::Literal(types::SqlValue::Integer(5))),
            }),
            op: ast::BinaryOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: Some("b".to_string()), column: "y".to_string() }),
                op: ast::BinaryOperator::LessThan,
                right: Box::new(Expression::Literal(types::SqlValue::Integer(10))),
            }),
        };

        let predicates = decompose_where_predicates(&expr);
        assert_eq!(predicates.len(), 2);
    }

    #[test]
    fn test_classify_table_local_predicate() {
        // WHERE a.x > 5
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: Some("a".to_string()), column: "x".to_string() }),
            op: ast::BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(types::SqlValue::Integer(5))),
        };

        let pred_type = classify_predicate(&expr);
        match pred_type {
            PredicateType::TableLocal { table_name } => {
                assert_eq!(table_name, "a");
            }
            _ => panic!("Expected TableLocal predicate"),
        }
    }

    #[test]
    fn test_classify_equijoin_predicate() {
        // WHERE a.x = b.y
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: Some("a".to_string()), column: "x".to_string() }),
            op: ast::BinaryOperator::Equal,
            right: Box::new(Expression::ColumnRef { table: Some("b".to_string()), column: "y".to_string() }),
        };

        let pred_type = classify_predicate(&expr);
        match pred_type {
            PredicateType::Equijoin { left_table, left_col, right_table, right_col } => {
                assert_eq!(left_table, "a");
                assert_eq!(left_col, "x");
                assert_eq!(right_table, "b");
                assert_eq!(right_col, "y");
            }
            _ => panic!("Expected Equijoin predicate"),
        }
    }

    #[test]
    fn test_get_table_local_predicates() {
        let pred_a = PredicateInfo {
            predicate_type: PredicateType::TableLocal { table_name: "a".to_string() },
            expression: Expression::Literal(types::SqlValue::Boolean(true)),
        };

        let pred_b = PredicateInfo {
            predicate_type: PredicateType::TableLocal { table_name: "b".to_string() },
            expression: Expression::Literal(types::SqlValue::Boolean(true)),
        };

        let predicates = vec![pred_a, pred_b];
        let a_predicates = get_table_local_predicates(&predicates, "a");
        assert_eq!(a_predicates.len(), 1);
    }
}
