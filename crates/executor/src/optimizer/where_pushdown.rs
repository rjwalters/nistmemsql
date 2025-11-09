//! WHERE clause predicate pushdown optimizer
//!
//! This module decomposes WHERE clauses into three categories of predicates:
//! 1. **Table-local predicates**: Reference only one table (can be applied at scan time)
//! 2. **Equijoin conditions**: Simple equality between two tables (apply during join)
//! 3. **Complex predicates**: Multiple tables or non-equality (apply after all joins)
//!
//! The goal is to filter rows as early as possible before intermediate Cartesian products
//! can explode in memory.
//!
//! ## Example
//!
//! Given query:
//! ```sql
//! SELECT * FROM t1, t2, t3
//! WHERE t1.a = 5 AND t1.a = t2.b AND t2.c > 10 AND t3.d IS NOT NULL
//! ```
//!
//! Decomposition:
//! - Table-local: `t1.a = 5`, `t2.c > 10`, `t3.d IS NOT NULL`
//! - Equijoin: `t1.a = t2.b`
//! - Complex: (none)
//!
//! Execution order:
//! 1. Scan t1, apply `t1.a = 5` → 2 rows
//! 2. Scan t2, apply `t2.c > 10` → 3 rows
//! 3. Join t1 with t2 on `t1.a = t2.b` → 5 rows (not 6)
//! 4. Scan t3, apply `t3.d IS NOT NULL` → 8 rows
//! 5. Join result with t3 → 40 rows (not 48)

use std::collections::{HashMap, HashSet};

use ast::Expression;

use crate::schema::CombinedSchema;

// ============================================================================
// Main branch API (used by scan.rs for predicate pushdown)
// ============================================================================

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
            if let Some(equijoin) = try_extract_equijoin_simple(expr, &referenced_tables) {
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
fn try_extract_equijoin_simple(
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
        Expression::ColumnRef { table: Some(table), column } => {
            Some((table.clone(), column.clone()))
        }
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
                    if !referenced.is_empty() && referenced.iter().all(|t| table_names.contains(t))
                    {
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

// ============================================================================
// Branch-specific API (used by join reordering)
// ============================================================================

/// Classification of a WHERE clause predicate (branch version)
#[derive(Debug, Clone, PartialEq)]
pub enum PredicateCategory {
    /// Predicate references only a single table - can be pushed to table scan
    /// Example: `t1.a = 5` or `t2.name LIKE 'foo%'`
    TableLocal {
        /// Name of the referenced table
        table_name: String,
        /// The predicate expression
        predicate: ast::Expression,
    },

    /// Simple equality between columns of two tables - can be used as join condition
    /// Example: `t1.a = t2.b`
    EquiJoin {
        /// Left table and column
        left_table: String,
        left_column: String,
        /// Right table and column
        right_table: String,
        right_column: String,
        /// The full equality expression
        predicate: ast::Expression,
    },

    /// Complex predicate - must be applied after joins
    /// Examples: `t1.a + t2.b > 10`, `t1.a = t2.b OR t1.c = t3.d`, etc.
    Complex {
        /// The full predicate expression
        predicate: ast::Expression,
        /// Table names referenced in this predicate
        referenced_tables: HashSet<String>,
    },
}

/// Result of decomposing a WHERE clause (branch version)
#[derive(Debug, Clone)]
pub struct PredicateDecomposition {
    /// Table-local predicates grouped by table name
    pub table_local_predicates: HashMap<String, Vec<ast::Expression>>,
    /// Equijoin conditions
    pub equijoin_conditions: Vec<(String, String, String, String, ast::Expression)>,
    /// Complex predicates that cannot be pushed down
    pub complex_predicates: Vec<ast::Expression>,
}

impl PredicateDecomposition {
    /// Create an empty decomposition
    pub fn empty() -> Self {
        Self {
            table_local_predicates: HashMap::new(),
            equijoin_conditions: Vec::new(),
            complex_predicates: Vec::new(),
        }
    }

    /// Check if this decomposition has any predicates
    pub fn is_empty(&self) -> bool {
        self.table_local_predicates.is_empty()
            && self.equijoin_conditions.is_empty()
            && self.complex_predicates.is_empty()
    }

    /// Rebuild a WHERE clause from all categories (for validation/debugging)
    pub fn rebuild_where_clause(&self) -> Option<ast::Expression> {
        let mut all_predicates = Vec::new();

        // Add table-local predicates
        for predicates in self.table_local_predicates.values() {
            all_predicates.extend(predicates.clone());
        }

        // Add equijoin conditions
        for (_, _, _, _, expr) in &self.equijoin_conditions {
            all_predicates.push(expr.clone());
        }

        // Add complex predicates
        all_predicates.extend(self.complex_predicates.clone());

        if all_predicates.is_empty() {
            return None;
        }

        // Combine all predicates with AND
        Some(combine_predicates_with_and(all_predicates))
    }
}

/// Decompose a WHERE clause into pushable and non-pushable predicates (branch version)
pub fn decompose_where_clause(
    where_expr: Option<&ast::Expression>,
    from_schema: &CombinedSchema,
) -> Result<PredicateDecomposition, String> {
    let mut decomposition = PredicateDecomposition::empty();

    let Some(expr) = where_expr else {
        return Ok(decomposition);
    };

    // Flatten the WHERE clause into CNF (Conjunctive Normal Form)
    // i.e., split on top-level AND operators
    let conjuncts = flatten_conjuncts(expr);

    for conjunct in conjuncts {
        classify_predicate_branch(&conjunct, from_schema, &mut decomposition)?;
    }

    Ok(decomposition)
}

/// Classify a single predicate (one of the AND-separated clauses) - branch version
fn classify_predicate_branch(
    expr: &ast::Expression,
    from_schema: &CombinedSchema,
    decomposition: &mut PredicateDecomposition,
) -> Result<(), String> {
    // Extract tables referenced by this predicate
    let referenced_tables = extract_referenced_tables_branch(expr, from_schema);

    match referenced_tables.len() {
        0 => {
            // Predicate references no tables (e.g., constant expression)
            // This is complex but we can still defer it
            decomposition.complex_predicates.push(expr.clone());
            Ok(())
        }
        1 => {
            // Single table - can be pushed to table scan
            let table_name = referenced_tables.iter().next().unwrap().clone();
            decomposition
                .table_local_predicates
                .entry(table_name)
                .or_insert_with(Vec::new)
                .push(expr.clone());
            Ok(())
        }
        2 => {
            // Two tables - check if it's a simple equijoin
            if let Some((left_table, left_col, right_table, right_col)) =
                try_extract_equijoin_branch(expr, from_schema)
            {
                decomposition.equijoin_conditions.push((
                    left_table,
                    left_col,
                    right_table,
                    right_col,
                    expr.clone(),
                ));
                Ok(())
            } else {
                // Complex predicate involving two tables
                decomposition.complex_predicates.push(expr.clone());
                Ok(())
            }
        }
        _ => {
            // Multiple tables - always complex
            decomposition.complex_predicates.push(expr.clone());
            Ok(())
        }
    }
}

/// Flatten a WHERE clause into conjunction (AND-separated) clauses
/// Only splits on top-level AND, not on OR or nested AND
fn flatten_conjuncts(expr: &ast::Expression) -> Vec<ast::Expression> {
    match expr {
        ast::Expression::BinaryOp { left, op, right } if matches!(op, ast::BinaryOperator::And) => {
            let mut conjuncts = flatten_conjuncts(left);
            conjuncts.extend(flatten_conjuncts(right));
            conjuncts
        }
        other => vec![other.clone()],
    }
}

/// Extract all table names referenced in a predicate (branch version with schema)
fn extract_referenced_tables_branch(
    expr: &ast::Expression,
    schema: &CombinedSchema,
) -> HashSet<String> {
    let mut tables = HashSet::new();
    extract_tables_recursive_branch(expr, schema, &mut tables);
    tables
}

/// Recursive helper to extract table references (branch version)
fn extract_tables_recursive_branch(
    expr: &ast::Expression,
    schema: &CombinedSchema,
    tables: &mut HashSet<String>,
) {
    match expr {
        ast::Expression::ColumnRef { table, .. } => {
            if let Some(table_name) = table {
                let normalized = table_name.to_lowercase();
                if schema.table_schemas.contains_key(&normalized) {
                    tables.insert(normalized);
                }
            }
        }
        ast::Expression::BinaryOp { left, op: _, right } => {
            extract_tables_recursive_branch(left, schema, tables);
            extract_tables_recursive_branch(right, schema, tables);
        }
        ast::Expression::UnaryOp { expr: inner, .. } => {
            extract_tables_recursive_branch(inner, schema, tables);
        }
        ast::Expression::Function { args, .. } => {
            for arg in args {
                extract_tables_recursive_branch(arg, schema, tables);
            }
        }
        ast::Expression::Between { expr, low, high, .. } => {
            extract_tables_recursive_branch(expr, schema, tables);
            extract_tables_recursive_branch(low, schema, tables);
            extract_tables_recursive_branch(high, schema, tables);
        }
        ast::Expression::InList { expr, values, .. } => {
            extract_tables_recursive_branch(expr, schema, tables);
            for val in values {
                extract_tables_recursive_branch(val, schema, tables);
            }
        }
        ast::Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                extract_tables_recursive_branch(op, schema, tables);
            }
            for when_clause in when_clauses {
                for condition in &when_clause.conditions {
                    extract_tables_recursive_branch(condition, schema, tables);
                }
                extract_tables_recursive_branch(&when_clause.result, schema, tables);
            }
            if let Some(else_res) = else_result {
                extract_tables_recursive_branch(else_res, schema, tables);
            }
        }
        ast::Expression::In { expr, .. } => {
            extract_tables_recursive_branch(expr, schema, tables);
        }
        ast::Expression::ScalarSubquery(_) => {
            // Scalar subqueries are complex and handled after joins
        }
        _ => {
            // Other expression types: Literal, Wildcard, IsNull, Like, etc.
        }
    }
}

/// Try to extract an equijoin condition from an expression (branch version)
/// Returns (left_table, left_col, right_table, right_col) if successful
fn try_extract_equijoin_branch(
    expr: &ast::Expression,
    schema: &CombinedSchema,
) -> Option<(String, String, String, String)> {
    match expr {
        ast::Expression::BinaryOp { left, op: ast::BinaryOperator::Equal, right } => {
            // Try to extract column references from both sides
            let (left_table, left_col) = extract_column_reference_branch(left, schema)?;
            let (right_table, right_col) = extract_column_reference_branch(right, schema)?;

            // Ensure they reference different tables
            if left_table != right_table {
                return Some((left_table, left_col, right_table, right_col));
            }
        }
        _ => {}
    }
    None
}

/// Extract (table_name, column_name) from a column reference expression (branch version)
fn extract_column_reference_branch(
    expr: &ast::Expression,
    schema: &CombinedSchema,
) -> Option<(String, String)> {
    match expr {
        ast::Expression::ColumnRef { table, column } => {
            if let Some(table_name) = table {
                let normalized_table = table_name.to_lowercase();
                if schema.table_schemas.contains_key(&normalized_table) {
                    return Some((normalized_table, column.to_lowercase()));
                }
            }
            None
        }
        _ => None,
    }
}

/// Combine a list of predicates into a single expression using AND
fn combine_predicates_with_and(mut predicates: Vec<ast::Expression>) -> ast::Expression {
    if predicates.is_empty() {
        // This shouldn't happen, but default to TRUE
        ast::Expression::Literal(types::SqlValue::Boolean(true))
    } else if predicates.len() == 1 {
        predicates.pop().unwrap()
    } else {
        let mut result = predicates.remove(0);
        for predicate in predicates {
            result = ast::Expression::BinaryOp {
                op: ast::BinaryOperator::And,
                left: Box::new(result),
                right: Box::new(predicate),
            };
        }
        result
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
                left: Box::new(Expression::ColumnRef {
                    table: Some("a".to_string()),
                    column: "x".to_string(),
                }),
                op: ast::BinaryOperator::GreaterThan,
                right: Box::new(Expression::Literal(types::SqlValue::Integer(5))),
            }),
            op: ast::BinaryOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: Some("b".to_string()),
                    column: "y".to_string(),
                }),
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
            left: Box::new(Expression::ColumnRef {
                table: Some("a".to_string()),
                column: "x".to_string(),
            }),
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
            left: Box::new(Expression::ColumnRef {
                table: Some("a".to_string()),
                column: "x".to_string(),
            }),
            op: ast::BinaryOperator::Equal,
            right: Box::new(Expression::ColumnRef {
                table: Some("b".to_string()),
                column: "y".to_string(),
            }),
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

    #[test]
    fn test_flatten_conjuncts_single() {
        let expr = ast::Expression::Literal(types::SqlValue::Boolean(true));
        let conjuncts = flatten_conjuncts(&expr);
        assert_eq!(conjuncts.len(), 1);
    }

    #[test]
    fn test_flatten_conjuncts_multiple() {
        // (a AND b) AND c should flatten to 3 conjuncts
        let a = ast::Expression::Literal(types::SqlValue::Boolean(true));
        let b = ast::Expression::Literal(types::SqlValue::Boolean(true));
        let ab = ast::Expression::BinaryOp {
            op: ast::BinaryOperator::And,
            left: Box::new(a),
            right: Box::new(b),
        };
        let c = ast::Expression::Literal(types::SqlValue::Boolean(false));
        let abc = ast::Expression::BinaryOp {
            op: ast::BinaryOperator::And,
            left: Box::new(ab),
            right: Box::new(c),
        };

        let conjuncts = flatten_conjuncts(&abc);
        assert_eq!(conjuncts.len(), 3);
    }

    #[test]
    fn test_decomposition_empty() {
        let decomp = PredicateDecomposition::empty();
        assert!(decomp.is_empty());
    }

    #[test]
    fn test_rebuild_empty() {
        let decomp = PredicateDecomposition::empty();
        assert!(decomp.rebuild_where_clause().is_none());
    }
}
