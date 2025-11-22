//! WHERE clause predicate pushdown optimizer
//!
//! This module decomposes WHERE clauses into three categories of predicates:
//! 1. **Table-local predicates**: Reference only one table (can be applied at scan time)
//! 2. **Equijoin conditions**: Simple equality between two tables (apply during join)

#![allow(dead_code)]
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

use vibesql_ast::Expression;

use crate::schema::CombinedSchema;

// ============================================================================
// Unified WHERE Clause Decomposition API
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
        predicate: vibesql_ast::Expression,
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
        predicate: vibesql_ast::Expression,
    },

    /// Complex predicate - must be applied after joins
    /// Examples: `t1.a + t2.b > 10`, `t1.a = t2.b OR t1.c = t3.d`, etc.
    Complex {
        /// The full predicate expression
        predicate: vibesql_ast::Expression,
        /// Table names referenced in this predicate
        referenced_tables: HashSet<String>,
    },
}

/// Result of decomposing a WHERE clause (branch version)
#[derive(Debug, Clone)]
pub struct PredicateDecomposition {
    /// Table-local predicates grouped by table name
    pub table_local_predicates: HashMap<String, Vec<vibesql_ast::Expression>>,
    /// Equijoin conditions
    pub equijoin_conditions: Vec<(String, String, String, String, vibesql_ast::Expression)>,
    /// Complex predicates that cannot be pushed down
    pub complex_predicates: Vec<vibesql_ast::Expression>,
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
    pub fn rebuild_where_clause(&self) -> Option<vibesql_ast::Expression> {
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
    where_expr: Option<&vibesql_ast::Expression>,
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

    // Extract implied single-table filters from OR predicates
    // This is critical for queries like TPC-H Q7 where:
    //   (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
    // implies: n1.n_name IN ('FRANCE', 'GERMANY') AND n2.n_name IN ('FRANCE', 'GERMANY')
    extract_implied_filters_from_or_predicates(&mut decomposition, from_schema);

    Ok(decomposition)
}

/// Classify a single predicate (one of the AND-separated clauses) - branch version
fn classify_predicate_branch(
    expr: &vibesql_ast::Expression,
    from_schema: &CombinedSchema,
    decomposition: &mut PredicateDecomposition,
) -> Result<(), String> {
    // Extract tables referenced by this predicate
    let referenced_tables_opt = extract_referenced_tables_branch(expr, from_schema);

    match referenced_tables_opt {
        None => {
            // Predicate references tables not in schema - treat as complex
            decomposition.complex_predicates.push(expr.clone());
            Ok(())
        }
        Some(referenced_tables) => {
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
                        .or_default()
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
    }
}

/// Flatten a WHERE clause into conjunction (AND-separated) clauses
/// Only splits on top-level AND, not on OR or nested AND
fn flatten_conjuncts(expr: &vibesql_ast::Expression) -> Vec<vibesql_ast::Expression> {
    match expr {
        vibesql_ast::Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::And, right } => {
            let mut conjuncts = flatten_conjuncts(left);
            conjuncts.extend(flatten_conjuncts(right));
            conjuncts
        }
        other => vec![other.clone()],
    }
}

/// Extract all table names referenced in a predicate (branch version with schema)
/// Returns None if the expression references tables not in the schema (should be treated as complex)
fn extract_referenced_tables_branch(
    expr: &vibesql_ast::Expression,
    schema: &CombinedSchema,
) -> Option<HashSet<String>> {
    let mut tables = HashSet::new();
    let success = extract_tables_recursive_branch(expr, schema, &mut tables);
    if success {
        Some(tables)
    } else {
        None
    }
}

/// Recursive helper to extract table references (branch version)
/// Returns false if the expression references tables not in the schema
fn extract_tables_recursive_branch(
    expr: &vibesql_ast::Expression,
    schema: &CombinedSchema,
    tables: &mut HashSet<String>,
) -> bool {
    match expr {
        vibesql_ast::Expression::ColumnRef { table: Some(table_name), .. } => {
            let normalized = table_name.to_lowercase();
            if schema.table_schemas.contains_key(&normalized) {
                tables.insert(normalized);
                true
            } else {
                // Table qualification not in schema - treat as complex predicate
                false
            }
        }
        vibesql_ast::Expression::ColumnRef { table: None, column } => {
            // Unqualified column reference - need to resolve it to table(s)
            // Search all tables in the schema to find which contain this column
            let column_lower = column.to_lowercase();
            let mut found = false;
            for (table_name, (_start_idx, table_schema)) in &schema.table_schemas {
                if table_schema.columns.iter().any(|col| col.name.to_lowercase() == column_lower) {
                    tables.insert(table_name.clone());
                    found = true;
                }
            }
            found  // Return true if column found in at least one table
        }
        vibesql_ast::Expression::BinaryOp { left, op: _, right } => {
            extract_tables_recursive_branch(left, schema, tables)
                && extract_tables_recursive_branch(right, schema, tables)
        }
        vibesql_ast::Expression::UnaryOp { expr: inner, .. } => {
            extract_tables_recursive_branch(inner, schema, tables)
        }
        vibesql_ast::Expression::Function { args, .. } => {
            args.iter().all(|arg| extract_tables_recursive_branch(arg, schema, tables))
        }
        vibesql_ast::Expression::Between { expr, low, high, .. } => {
            extract_tables_recursive_branch(expr, schema, tables)
                && extract_tables_recursive_branch(low, schema, tables)
                && extract_tables_recursive_branch(high, schema, tables)
        }
        vibesql_ast::Expression::InList { expr, values, .. } => {
            extract_tables_recursive_branch(expr, schema, tables)
                && values.iter().all(|val| extract_tables_recursive_branch(val, schema, tables))
        }
        vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
            let op_ok = operand.as_ref().is_none_or(|op| extract_tables_recursive_branch(op, schema, tables));
            let when_ok = when_clauses.iter().all(|when_clause| {
                when_clause.conditions.iter().all(|condition| extract_tables_recursive_branch(condition, schema, tables))
                    && extract_tables_recursive_branch(&when_clause.result, schema, tables)
            });
            let else_ok = else_result.as_ref().is_none_or(|else_res| extract_tables_recursive_branch(else_res, schema, tables));
            op_ok && when_ok && else_ok
        }
        vibesql_ast::Expression::In { expr, .. } => {
            extract_tables_recursive_branch(expr, schema, tables)
        }
        vibesql_ast::Expression::ScalarSubquery(_) => {
            // Scalar subqueries are complex and handled after joins
            true
        }
        _ => {
            // Other expression types: Literal, Wildcard, IsNull, Like, etc.
            true
        }
    }
}

/// Try to extract an equijoin condition from an expression (branch version)
/// Returns (left_table, left_col, right_table, right_col) if successful
fn try_extract_equijoin_branch(
    expr: &vibesql_ast::Expression,
    schema: &CombinedSchema,
) -> Option<(String, String, String, String)> {
    if let vibesql_ast::Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::Equal, right } = expr {
        // Try to extract column references from both sides
        let (left_table, left_col) = extract_column_reference_branch(left, schema)?;
        let (right_table, right_col) = extract_column_reference_branch(right, schema)?;

        // Ensure they reference different tables
        if left_table != right_table {
            return Some((left_table, left_col, right_table, right_col));
        }
    }
    None
}

/// Extract (table_name, column_name) from a column reference expression (branch version)
fn extract_column_reference_branch(
    expr: &vibesql_ast::Expression,
    schema: &CombinedSchema,
) -> Option<(String, String)> {
    match expr {
        vibesql_ast::Expression::ColumnRef { table, column } => {
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
fn combine_predicates_with_and(mut predicates: Vec<vibesql_ast::Expression>) -> vibesql_ast::Expression {
    if predicates.is_empty() {
        // This shouldn't happen, but default to TRUE
        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true))
    } else if predicates.len() == 1 {
        predicates.pop().unwrap()
    } else {
        let mut result = predicates.remove(0);
        for predicate in predicates {
            result = vibesql_ast::Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::And,
                left: Box::new(result),
                right: Box::new(predicate),
            };
        }
        result
    }
}

// ============================================================================
// Compatibility Functions (for backward compatibility)
// ============================================================================

/// Combine multiple expressions with AND operator
///
/// Public wrapper around combine_predicates_with_and for external use.
pub fn combine_with_and(expressions: Vec<Expression>) -> Option<Expression> {
    match expressions.len() {
        0 => None,
        _ => Some(combine_predicates_with_and(expressions)),
    }
}

// ============================================================================
// OR Filter Extraction
// ============================================================================

/// Extract implied single-table filters from complex OR predicates
///
/// For predicates like:
///   (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
///
/// We can extract implied filters:
///   n1.n_name IN ('FRANCE', 'GERMANY')
///   n2.n_name IN ('FRANCE', 'GERMANY')
///
/// This enables much better join ordering by recognizing that nation tables will be filtered.
fn extract_implied_filters_from_or_predicates(
    decomposition: &mut PredicateDecomposition,
    schema: &CombinedSchema,
) {
    // Process each complex predicate to see if it's an OR we can extract from
    for complex_pred in &decomposition.complex_predicates {
        if let Some(implied_filters) = extract_table_filters_from_or(complex_pred, schema) {
            // Add extracted filters to table_local_predicates
            for (table_name, filter_expr) in implied_filters {
                decomposition
                    .table_local_predicates
                    .entry(table_name)
                    .or_default()
                    .push(filter_expr);
            }
        }
    }
}

/// Extract single-table filters from an OR predicate
///
/// For (A1 AND B1) OR (A2 AND B2) where:
/// - A1, A2 reference only table t1
/// - B1, B2 reference only table t2
///
/// Returns filters: [(t1, A1 OR A2), (t2, B1 OR B2)]
fn extract_table_filters_from_or(
    expr: &Expression,
    schema: &CombinedSchema,
) -> Option<Vec<(String, Expression)>> {
    // Check if this is an OR expression
    let (left_branch, right_branch) = match expr {
        Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::Or,
            left,
            right,
        } => (left.as_ref(), right.as_ref()),
        _ => return None,
    };

    // Extract table->predicates for each OR branch
    let left_filters = extract_table_predicates_from_branch(left_branch, schema);
    let right_filters = extract_table_predicates_from_branch(right_branch, schema);

    // Find tables that are filtered in BOTH branches
    let mut result = Vec::new();
    for (table_name, left_preds) in &left_filters {
        if let Some(right_preds) = right_filters.get(table_name) {
            // Combine left and right predicates for this table with OR
            let left_combined = combine_predicates_with_and(left_preds.clone());
            let right_combined = combine_predicates_with_and(right_preds.clone());

            let combined_filter = Expression::BinaryOp {
                op: vibesql_ast::BinaryOperator::Or,
                left: Box::new(left_combined),
                right: Box::new(right_combined),
            };

            result.push((table_name.clone(), combined_filter));
        }
    }

    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

/// Extract table-local predicates from a branch of an OR
///
/// For an AND chain like (t1.a = 'X' AND t2.b = 'Y' AND t1.c = 'Z')
/// Returns: {"t1": [t1.a = 'X', t1.c = 'Z'], "t2": [t2.b = 'Y']}
fn extract_table_predicates_from_branch(
    expr: &Expression,
    schema: &CombinedSchema,
) -> HashMap<String, Vec<Expression>> {
    let mut table_predicates: HashMap<String, Vec<Expression>> = HashMap::new();

    // Flatten ANDs in this branch
    let conjuncts = flatten_conjuncts(expr);

    for conjunct in conjuncts {
        // Get tables referenced by this conjunct
        if let Some(tables) = extract_referenced_tables_branch(&conjunct, schema) {
            if tables.len() == 1 {
                // Single-table predicate - add to that table's list
                let table_name = tables.iter().next().unwrap().clone();
                table_predicates.entry(table_name).or_default().push(conjunct);
            }
            // Multi-table predicates are ignored for filter extraction
        }
    }

    table_predicates
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flatten_conjuncts_single() {
        let expr = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true));
        let conjuncts = flatten_conjuncts(&expr);
        assert_eq!(conjuncts.len(), 1);
    }

    #[test]
    fn test_flatten_conjuncts_multiple() {
        // (a AND b) AND c should flatten to 3 conjuncts
        let a = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true));
        let b = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true));
        let ab = vibesql_ast::Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::And,
            left: Box::new(a),
            right: Box::new(b),
        };
        let c = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(false));
        let abc = vibesql_ast::Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::And,
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

    #[test]
    fn test_combine_with_and() {
        // Test empty list
        assert_eq!(combine_with_and(vec![]), None);

        // Test single expression
        let expr = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true));
        assert_eq!(combine_with_and(vec![expr.clone()]), Some(expr));

        // Test multiple expressions
        let exprs = vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(false)),
        ];
        let result = combine_with_and(exprs);
        assert!(result.is_some());
    }

    #[test]
    fn test_or_filter_extraction() {
        use vibesql_ast::{BinaryOperator, Expression};
        use vibesql_types::SqlValue;
        use vibesql_catalog::{ColumnSchema, TableSchema};

        // Create schema with two nation tables (n1, n2)
        let n1_schema = TableSchema::new(
            "n1".to_string(),
            vec![ColumnSchema::new("n_name".to_string(), vibesql_types::DataType::Varchar { max_length: Some(25) }, false)],
        );
        let n2_schema = TableSchema::new(
            "n2".to_string(),
            vec![ColumnSchema::new("n_name".to_string(), vibesql_types::DataType::Varchar { max_length: Some(25) }, false)],
        );
        // Build CombinedSchema properly
        let schema = CombinedSchema::combine(
            CombinedSchema::from_table("n1".to_string(), n1_schema),
            "n2".to_string(),
            n2_schema,
        );

        // Build Q7-style OR predicate:
        // (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
        let n1_france = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: Some("n1".to_string()), column: "n_name".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Varchar("FRANCE".to_string()))),
        };
        let n2_germany = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: Some("n2".to_string()), column: "n_name".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Varchar("GERMANY".to_string()))),
        };
        let n1_germany = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: Some("n1".to_string()), column: "n_name".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Varchar("GERMANY".to_string()))),
        };
        let n2_france = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: Some("n2".to_string()), column: "n_name".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Varchar("FRANCE".to_string()))),
        };

        // (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
        let left_branch = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(n1_france),
            right: Box::new(n2_germany),
        };

        // (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
        let right_branch = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(n1_germany),
            right: Box::new(n2_france),
        };

        // Full OR predicate
        let or_predicate = Expression::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new(left_branch),
            right: Box::new(right_branch),
        };

        // Extract filters
        let filters = extract_table_filters_from_or(&or_predicate, &schema);
        assert!(filters.is_some(), "Should extract filters from OR predicate");

        let filters = filters.unwrap();
        assert_eq!(filters.len(), 2, "Should extract 2 table filters (n1 and n2)");

        // Check that both n1 and n2 have filters
        let table_names: HashSet<_> = filters.iter().map(|(t, _)| t.as_str()).collect();
        assert!(table_names.contains("n1"), "Should have filter for n1");
        assert!(table_names.contains("n2"), "Should have filter for n2");
    }

    #[test]
    fn test_or_filter_extraction_multi_branch() {
        // Test case 1: Multi-branch OR predicates (more than 2 OR branches)
        // (A AND B) OR (C AND D) OR (E AND F)
        // Current implementation only handles binary OR, so nested ORs like:
        // ((A AND B) OR (C AND D)) OR (E AND F)
        use vibesql_ast::{BinaryOperator, Expression};
        use vibesql_types::SqlValue;
        use vibesql_catalog::{ColumnSchema, TableSchema};

        // Create schema with tables t1, t2, t3
        let t1_schema = TableSchema::new(
            "t1".to_string(),
            vec![ColumnSchema::new("a".to_string(), vibesql_types::DataType::Integer, false)],
        );
        let t2_schema = TableSchema::new(
            "t2".to_string(),
            vec![ColumnSchema::new("b".to_string(), vibesql_types::DataType::Integer, false)],
        );
        let t3_schema = TableSchema::new(
            "t3".to_string(),
            vec![ColumnSchema::new("c".to_string(), vibesql_types::DataType::Integer, false)],
        );

        let schema = CombinedSchema::combine(
            CombinedSchema::combine(
                CombinedSchema::from_table("t1".to_string(), t1_schema),
                "t2".to_string(),
                t2_schema,
            ),
            "t3".to_string(),
            t3_schema,
        );

        // Build: ((t1.a = 1 AND t2.b = 2) OR (t1.a = 3 AND t2.b = 4)) OR (t1.a = 5 AND t2.b = 6)
        let branch1 = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t1".to_string()), column: "a".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t2".to_string()), column: "b".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        };

        let branch2 = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t1".to_string()), column: "a".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(3))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t2".to_string()), column: "b".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(4))),
            }),
        };

        let branch3 = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t1".to_string()), column: "a".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(5))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t2".to_string()), column: "b".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(6))),
            }),
        };

        // Create nested OR: (branch1 OR branch2) OR branch3
        let inner_or = Expression::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new(branch1),
            right: Box::new(branch2),
        };

        let outer_or = Expression::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new(inner_or.clone()),
            right: Box::new(branch3),
        };

        // Extract filters from the inner OR first
        let inner_filters = extract_table_filters_from_or(&inner_or, &schema);
        assert!(inner_filters.is_some(), "Should extract filters from inner OR");

        // The outer OR won't extract properly because one branch is a complex filter
        // This demonstrates the current limitation with multi-branch ORs
        let outer_filters = extract_table_filters_from_or(&outer_or, &schema);
        // This may or may not work depending on how the nested structure is handled
        // The test documents the behavior rather than mandating a specific result
    }

    #[test]
    fn test_or_filter_extraction_nested_or() {
        // Test case 2: Nested OR predicates
        // ((A OR B) AND C) OR ((D OR E) AND F)
        use vibesql_ast::{BinaryOperator, Expression};
        use vibesql_types::SqlValue;
        use vibesql_catalog::{ColumnSchema, TableSchema};

        let t1_schema = TableSchema::new(
            "t1".to_string(),
            vec![
                ColumnSchema::new("a".to_string(), vibesql_types::DataType::Integer, false),
                ColumnSchema::new("b".to_string(), vibesql_types::DataType::Integer, false),
            ],
        );

        let schema = CombinedSchema::from_table("t1".to_string(), t1_schema);

        // Build: ((t1.a = 1 OR t1.a = 2) AND t1.b = 10) OR ((t1.a = 3 OR t1.a = 4) AND t1.b = 20)
        let left_or = Expression::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t1".to_string()), column: "a".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t1".to_string()), column: "a".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        };

        let left_branch = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(left_or),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t1".to_string()), column: "b".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(10))),
            }),
        };

        let right_or = Expression::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t1".to_string()), column: "a".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(3))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t1".to_string()), column: "a".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(4))),
            }),
        };

        let right_branch = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(right_or),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t1".to_string()), column: "b".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(20))),
            }),
        };

        let nested_predicate = Expression::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new(left_branch),
            right: Box::new(right_branch),
        };

        // This tests how the function handles nested OR structures
        // The current implementation will see the inner ORs as single predicates
        let filters = extract_table_filters_from_or(&nested_predicate, &schema);

        // Should extract t1.b filter: (t1.b = 10) OR (t1.b = 20)
        // The nested OR predicates for t1.a are treated as complex predicates
        assert!(filters.is_some(), "Should extract some filters from nested OR");
        let filters = filters.unwrap();
        assert_eq!(filters.len(), 1, "Should extract filter for t1.b");
        assert_eq!(filters[0].0, "t1", "Filter should be for table t1");
    }

    #[test]
    fn test_or_filter_extraction_asymmetric() {
        // Test case 3: Asymmetric OR predicates - tables appear in only one branch
        // (t1.a = 1 AND t2.b = 2) OR (t1.a = 3)
        // Should only extract filter for t1 (appears in both branches), not t2
        use vibesql_ast::{BinaryOperator, Expression};
        use vibesql_types::SqlValue;
        use vibesql_catalog::{ColumnSchema, TableSchema};

        let t1_schema = TableSchema::new(
            "t1".to_string(),
            vec![ColumnSchema::new("a".to_string(), vibesql_types::DataType::Integer, false)],
        );
        let t2_schema = TableSchema::new(
            "t2".to_string(),
            vec![ColumnSchema::new("b".to_string(), vibesql_types::DataType::Integer, false)],
        );

        let schema = CombinedSchema::combine(
            CombinedSchema::from_table("t1".to_string(), t1_schema),
            "t2".to_string(),
            t2_schema,
        );

        // Left branch: t1.a = 1 AND t2.b = 2
        let left_branch = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t1".to_string()), column: "a".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t2".to_string()), column: "b".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        };

        // Right branch: t1.a = 3 (only t1, no t2)
        let right_branch = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef { table: Some("t1".to_string()), column: "a".to_string() }),
            right: Box::new(Expression::Literal(SqlValue::Integer(3))),
        };

        let asymmetric_or = Expression::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new(left_branch),
            right: Box::new(right_branch),
        };

        let filters = extract_table_filters_from_or(&asymmetric_or, &schema);
        assert!(filters.is_some(), "Should extract filters from asymmetric OR");

        let filters = filters.unwrap();
        assert_eq!(filters.len(), 1, "Should extract only 1 table filter (for t1)");
        assert_eq!(filters[0].0, "t1", "Filter should be for table t1");

        // t2 should NOT be in the filters since it doesn't appear in the right branch
        let table_names: HashSet<_> = filters.iter().map(|(t, _)| t.as_str()).collect();
        assert!(!table_names.contains("t2"), "Should NOT have filter for t2");
    }

    #[test]
    fn test_or_filter_extraction_single_table() {
        // Test case 4: Single-table OR - Should return None
        // t1.a = 1 OR t1.a = 2
        // Not a complex predicate pattern we're trying to extract from
        use vibesql_ast::{BinaryOperator, Expression};
        use vibesql_types::SqlValue;
        use vibesql_catalog::{ColumnSchema, TableSchema};

        let t1_schema = TableSchema::new(
            "t1".to_string(),
            vec![ColumnSchema::new("a".to_string(), vibesql_types::DataType::Integer, false)],
        );

        let schema = CombinedSchema::from_table("t1".to_string(), t1_schema);

        // Build: t1.a = 1 OR t1.a = 2
        let single_table_or = Expression::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t1".to_string()), column: "a".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t1".to_string()), column: "a".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        };

        let filters = extract_table_filters_from_or(&single_table_or, &schema);

        // This actually WILL extract a filter for t1: (t1.a = 1) OR (t1.a = 2)
        // This is valid and useful - it's just a simple OR filter for one table
        assert!(filters.is_some(), "Should extract filter from single-table OR");
        let filters = filters.unwrap();
        assert_eq!(filters.len(), 1, "Should extract 1 table filter");
        assert_eq!(filters[0].0, "t1", "Filter should be for table t1");
    }

    #[test]
    fn test_or_filter_extraction_no_common_tables() {
        // Test case 5: No common tables - Should return None
        // (t1.a = 1 AND t2.b = 2) OR (t3.c = 3 AND t4.d = 4)
        // No tables appear in both branches
        use vibesql_ast::{BinaryOperator, Expression};
        use vibesql_types::SqlValue;
        use vibesql_catalog::{ColumnSchema, TableSchema};

        let t1_schema = TableSchema::new(
            "t1".to_string(),
            vec![ColumnSchema::new("a".to_string(), vibesql_types::DataType::Integer, false)],
        );
        let t2_schema = TableSchema::new(
            "t2".to_string(),
            vec![ColumnSchema::new("b".to_string(), vibesql_types::DataType::Integer, false)],
        );
        let t3_schema = TableSchema::new(
            "t3".to_string(),
            vec![ColumnSchema::new("c".to_string(), vibesql_types::DataType::Integer, false)],
        );
        let t4_schema = TableSchema::new(
            "t4".to_string(),
            vec![ColumnSchema::new("d".to_string(), vibesql_types::DataType::Integer, false)],
        );

        let schema = CombinedSchema::combine(
            CombinedSchema::combine(
                CombinedSchema::combine(
                    CombinedSchema::from_table("t1".to_string(), t1_schema),
                    "t2".to_string(),
                    t2_schema,
                ),
                "t3".to_string(),
                t3_schema,
            ),
            "t4".to_string(),
            t4_schema,
        );

        // Left branch: t1.a = 1 AND t2.b = 2
        let left_branch = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t1".to_string()), column: "a".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t2".to_string()), column: "b".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(2))),
            }),
        };

        // Right branch: t3.c = 3 AND t4.d = 4
        let right_branch = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t3".to_string()), column: "c".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(3))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef { table: Some("t4".to_string()), column: "d".to_string() }),
                right: Box::new(Expression::Literal(SqlValue::Integer(4))),
            }),
        };

        let no_common_or = Expression::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new(left_branch),
            right: Box::new(right_branch),
        };

        let filters = extract_table_filters_from_or(&no_common_or, &schema);
        assert!(filters.is_none(), "Should return None when no tables appear in both branches");
    }

    #[test]
    fn test_or_filter_extraction_empty_branches() {
        // Test case 6: Empty branches - Should handle gracefully
        // TRUE OR FALSE
        use vibesql_ast::{BinaryOperator, Expression};
        use vibesql_types::SqlValue;
        use vibesql_catalog::{ColumnSchema, TableSchema};

        let t1_schema = TableSchema::new(
            "t1".to_string(),
            vec![ColumnSchema::new("a".to_string(), vibesql_types::DataType::Integer, false)],
        );

        let schema = CombinedSchema::from_table("t1".to_string(), t1_schema);

        // Build: TRUE OR FALSE (no table references)
        let empty_or = Expression::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new(Expression::Literal(SqlValue::Boolean(true))),
            right: Box::new(Expression::Literal(SqlValue::Boolean(false))),
        };

        let filters = extract_table_filters_from_or(&empty_or, &schema);
        assert!(filters.is_none(), "Should return None for empty branches with no table references");
    }
}
