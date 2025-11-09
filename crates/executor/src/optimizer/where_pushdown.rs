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

use crate::schema::CombinedSchema;
use std::collections::{HashMap, HashSet};

/// Classification of a WHERE clause predicate
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

/// Result of decomposing a WHERE clause
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

/// Decompose a WHERE clause into pushable and non-pushable predicates
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
        classify_predicate(&conjunct, from_schema, &mut decomposition)?;
    }

    Ok(decomposition)
}

/// Classify a single predicate (one of the AND-separated clauses)
fn classify_predicate(
    expr: &ast::Expression,
    from_schema: &CombinedSchema,
    decomposition: &mut PredicateDecomposition,
) -> Result<(), String> {
    // Extract tables referenced by this predicate
    let referenced_tables = extract_referenced_tables(expr, from_schema);

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
                try_extract_equijoin(expr, from_schema)
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

/// Extract all table names referenced in a predicate
fn extract_referenced_tables(expr: &ast::Expression, schema: &CombinedSchema) -> HashSet<String> {
    let mut tables = HashSet::new();
    extract_tables_recursive(expr, schema, &mut tables);
    tables
}

/// Recursive helper to extract table references
fn extract_tables_recursive(
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
            extract_tables_recursive(left, schema, tables);
            extract_tables_recursive(right, schema, tables);
        }
        ast::Expression::UnaryOp { expr: inner, .. } => {
            extract_tables_recursive(inner, schema, tables);
        }
        ast::Expression::Function { args, .. } => {
            for arg in args {
                extract_tables_recursive(arg, schema, tables);
            }
        }
        ast::Expression::Between { expr, low, high, .. } => {
            extract_tables_recursive(expr, schema, tables);
            extract_tables_recursive(low, schema, tables);
            extract_tables_recursive(high, schema, tables);
        }
        ast::Expression::InList { expr, values, .. } => {
            extract_tables_recursive(expr, schema, tables);
            for val in values {
                extract_tables_recursive(val, schema, tables);
            }
        }
        ast::Expression::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            if let Some(op) = operand {
                extract_tables_recursive(op, schema, tables);
            }
            for when_clause in when_clauses {
                for condition in &when_clause.conditions {
                    extract_tables_recursive(condition, schema, tables);
                }
                extract_tables_recursive(&when_clause.result, schema, tables);
            }
            if let Some(else_res) = else_result {
                extract_tables_recursive(else_res, schema, tables);
            }
        }
        ast::Expression::In { expr, .. } => {
            extract_tables_recursive(expr, schema, tables);
        }
        ast::Expression::ScalarSubquery(_) => {
            // Scalar subqueries are complex and handled after joins
        }
        _ => {
            // Other expression types: Literal, Wildcard, IsNull, Like, etc.
            // Handle common ones:
        }
    }
}

/// Try to extract an equijoin condition from an expression
/// Returns (left_table, left_col, right_table, right_col) if successful
fn try_extract_equijoin(
    expr: &ast::Expression,
    schema: &CombinedSchema,
) -> Option<(String, String, String, String)> {
    match expr {
        ast::Expression::BinaryOp {
            left,
            op: ast::BinaryOperator::Equal,
            right,
        } => {
            // Try to extract column references from both sides
            let (left_table, left_col) = extract_column_reference(left, schema)?;
            let (right_table, right_col) = extract_column_reference(right, schema)?;

            // Ensure they reference different tables
            if left_table != right_table {
                return Some((left_table, left_col, right_table, right_col));
            }
        }
        _ => {}
    }
    None
}

/// Extract (table_name, column_name) from a column reference expression
fn extract_column_reference(expr: &ast::Expression, schema: &CombinedSchema) -> Option<(String, String)> {
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
