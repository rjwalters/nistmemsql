//! Unified predicate classification and representation
//!
//! This module provides a type-safe representation of WHERE clause predicates
//! after classification, enabling consistent handling throughout the optimizer
//! and executor layers.

use std::collections::{HashMap, HashSet};

use vibesql_ast::Expression;

use crate::schema::CombinedSchema;

/// A classified predicate ready for optimization and execution
#[derive(Debug, Clone)]
pub enum Predicate {
    /// Predicate that references only one table
    ///
    /// These predicates can be pushed down to table scans for early filtering.
    ///
    /// Examples: `t1.a > 5`, `t2.name LIKE 'foo%'`
    TableLocal {
        table_id: TableId,
        column_refs: Vec<ColumnRef>,
        expression: Expression,
        estimated_selectivity: Option<f64>,
    },

    /// Equality join between two tables
    ///
    /// These predicates can be used as join conditions for hash joins or merge joins.
    ///
    /// Example: `t1.id = t2.user_id`
    EquiJoin {
        left_table: TableId,
        left_column: ColumnRef,
        right_table: TableId,
        right_column: ColumnRef,
        expression: Expression,
    },

    /// Complex predicate requiring multiple tables
    ///
    /// These predicates must be applied after joins complete.
    ///
    /// Examples: `t1.a + t2.b > 10`, `t1.x = t2.y OR t1.z = t3.w`
    Complex {
        table_refs: Vec<TableId>,
        column_refs: Vec<ColumnRef>,
        expression: Expression,
    },

    /// Constant predicate (no table references)
    ///
    /// These are expressions that don't reference any tables.
    ///
    /// Examples: `1 = 1`, `'foo' LIKE 'f%'`
    Constant { value: bool, expression: Expression },
}

/// Reference to a table (either by name or alias)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableId(pub String);

/// Reference to a column within a table
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRef {
    pub table_id: TableId,
    pub column_name: String,
}

impl Predicate {
    /// Classify an expression as a predicate
    ///
    /// This is the main entry point for converting raw WHERE clause expressions
    /// into typed predicates. It analyzes the expression to determine which
    /// tables and columns it references, then classifies it appropriately.
    pub fn from_expression(expr: &Expression, schema: &CombinedSchema) -> Result<Self, String> {
        // Extract column references from the expression
        let column_refs = extract_column_refs(expr, schema)?;

        // Group columns by table to determine how many tables are involved
        let tables: HashSet<TableId> = column_refs.iter().map(|cr| cr.table_id.clone()).collect();

        match tables.len() {
            0 => {
                // No table references - this is a constant expression
                // Try to evaluate it (simplified version - just return false for now)
                Ok(Predicate::Constant {
                    value: false,
                    expression: expr.clone(),
                })
            }
            1 => {
                // Single table - can be pushed to table scan
                Ok(Predicate::TableLocal {
                    table_id: tables.into_iter().next().unwrap(),
                    column_refs,
                    expression: expr.clone(),
                    estimated_selectivity: None, // Computed later by statistics module
                })
            }
            2 if is_equijoin(expr, &column_refs) => {
                // Two tables with simple equality - can use as join condition
                let (left, right) = extract_equijoin_columns(expr, &column_refs)?;
                Ok(Predicate::EquiJoin {
                    left_table: left.table_id.clone(),
                    left_column: left,
                    right_table: right.table_id.clone(),
                    right_column: right,
                    expression: expr.clone(),
                })
            }
            _ => {
                // Multiple tables or complex condition - apply after joins
                Ok(Predicate::Complex {
                    table_refs: tables.into_iter().collect(),
                    column_refs,
                    expression: expr.clone(),
                })
            }
        }
    }

    /// Get the tables referenced by this predicate
    pub fn table_refs(&self) -> Vec<&TableId> {
        match self {
            Predicate::TableLocal { table_id, .. } => vec![table_id],
            Predicate::EquiJoin {
                left_table,
                right_table,
                ..
            } => vec![left_table, right_table],
            Predicate::Complex { table_refs, .. } => table_refs.iter().collect(),
            Predicate::Constant { .. } => vec![],
        }
    }

    /// Check if this predicate can be pushed to a specific table
    ///
    /// Only table-local and constant predicates can be pushed down.
    pub fn can_push_to(&self, table_id: &TableId) -> bool {
        match self {
            Predicate::TableLocal { table_id: tid, .. } => tid == table_id,
            Predicate::Constant { .. } => true, // Constant predicates can be evaluated anywhere
            _ => false,
        }
    }

    /// Get the underlying expression
    pub fn expression(&self) -> &Expression {
        match self {
            Predicate::TableLocal { expression, .. } => expression,
            Predicate::EquiJoin { expression, .. } => expression,
            Predicate::Complex { expression, .. } => expression,
            Predicate::Constant { expression, .. } => expression,
        }
    }
}

/// Extract all column references from an expression
fn extract_column_refs(expr: &Expression, schema: &CombinedSchema) -> Result<Vec<ColumnRef>, String> {
    let mut column_refs = Vec::new();
    extract_column_refs_recursive(expr, schema, &mut column_refs)?;
    Ok(column_refs)
}

/// Recursive helper to extract column references
fn extract_column_refs_recursive(
    expr: &Expression,
    schema: &CombinedSchema,
    column_refs: &mut Vec<ColumnRef>,
) -> Result<(), String> {
    match expr {
        Expression::ColumnRef {
            table: Some(table_name),
            column,
        } => {
            let normalized_table = table_name.to_lowercase();
            if !schema.table_schemas.contains_key(&normalized_table) {
                return Err(format!("Unknown table: {}", table_name));
            }
            column_refs.push(ColumnRef {
                table_id: TableId(normalized_table),
                column_name: column.to_lowercase(),
            });
            Ok(())
        }
        Expression::ColumnRef {
            table: None,
            column,
        } => {
            // Unqualified column - resolve to table(s)
            let column_lower = column.to_lowercase();
            let mut found = false;
            for (table_name, (_start_idx, table_schema)) in &schema.table_schemas {
                if table_schema
                    .columns
                    .iter()
                    .any(|col| col.name.to_lowercase() == column_lower)
                {
                    column_refs.push(ColumnRef {
                        table_id: TableId(table_name.clone()),
                        column_name: column_lower.clone(),
                    });
                    found = true;
                }
            }
            if !found {
                return Err(format!("Unknown column: {}", column));
            }
            Ok(())
        }
        Expression::BinaryOp { left, right, .. } => {
            extract_column_refs_recursive(left, schema, column_refs)?;
            extract_column_refs_recursive(right, schema, column_refs)
        }
        Expression::UnaryOp { expr: inner, .. } => {
            extract_column_refs_recursive(inner, schema, column_refs)
        }
        Expression::Function { args, .. } => {
            for arg in args {
                extract_column_refs_recursive(arg, schema, column_refs)?;
            }
            Ok(())
        }
        Expression::Between { expr, low, high, .. } => {
            extract_column_refs_recursive(expr, schema, column_refs)?;
            extract_column_refs_recursive(low, schema, column_refs)?;
            extract_column_refs_recursive(high, schema, column_refs)
        }
        Expression::InList { expr, values, .. } => {
            extract_column_refs_recursive(expr, schema, column_refs)?;
            for val in values {
                extract_column_refs_recursive(val, schema, column_refs)?;
            }
            Ok(())
        }
        Expression::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            if let Some(op) = operand {
                extract_column_refs_recursive(op, schema, column_refs)?;
            }
            for when_clause in when_clauses {
                for condition in &when_clause.conditions {
                    extract_column_refs_recursive(condition, schema, column_refs)?;
                }
                extract_column_refs_recursive(&when_clause.result, schema, column_refs)?;
            }
            if let Some(else_res) = else_result {
                extract_column_refs_recursive(else_res, schema, column_refs)?;
            }
            Ok(())
        }
        Expression::In { expr, .. } => extract_column_refs_recursive(expr, schema, column_refs),
        _ => Ok(()), // Literals, wildcards, etc. - no column refs
    }
}

/// Check if an expression is a simple equijoin (t1.col = t2.col)
fn is_equijoin(expr: &Expression, column_refs: &[ColumnRef]) -> bool {
    if let Expression::BinaryOp {
        left,
        op: vibesql_ast::BinaryOperator::Equal,
        right,
    } = expr
    {
        // Both sides must be simple column references
        let left_is_col = matches!(left.as_ref(), Expression::ColumnRef { .. });
        let right_is_col = matches!(right.as_ref(), Expression::ColumnRef { .. });

        if left_is_col && right_is_col && column_refs.len() == 2 {
            // Ensure the two columns reference different tables
            return column_refs[0].table_id != column_refs[1].table_id;
        }
    }
    false
}

/// Extract the two column references from an equijoin expression
fn extract_equijoin_columns(
    expr: &Expression,
    column_refs: &[ColumnRef],
) -> Result<(ColumnRef, ColumnRef), String> {
    if column_refs.len() != 2 {
        return Err("Equijoin must reference exactly 2 columns".to_string());
    }

    if let Expression::BinaryOp {
        left,
        op: vibesql_ast::BinaryOperator::Equal,
        ..
    } = expr
    {
        // Determine which column appears first in the expression
        if let Expression::ColumnRef { table, column } = left.as_ref() {
            let left_table = table.as_ref().map(|t| t.to_lowercase());
            let left_col = column.to_lowercase();

            // Find which column ref matches the left side
            if column_refs[0].column_name == left_col
                && (left_table.is_none() || left_table.as_ref() == Some(&column_refs[0].table_id.0))
            {
                Ok((column_refs[0].clone(), column_refs[1].clone()))
            } else {
                Ok((column_refs[1].clone(), column_refs[0].clone()))
            }
        } else {
            Err("Invalid equijoin expression".to_string())
        }
    } else {
        Err("Not an equijoin expression".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::BinaryOperator;
    use vibesql_types::SqlValue;

    #[test]
    fn test_table_id_equality() {
        let id1 = TableId("users".to_string());
        let id2 = TableId("users".to_string());
        let id3 = TableId("posts".to_string());

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_column_ref_equality() {
        let col1 = ColumnRef {
            table_id: TableId("users".to_string()),
            column_name: "id".to_string(),
        };
        let col2 = ColumnRef {
            table_id: TableId("users".to_string()),
            column_name: "id".to_string(),
        };
        let col3 = ColumnRef {
            table_id: TableId("users".to_string()),
            column_name: "name".to_string(),
        };

        assert_eq!(col1, col2);
        assert_ne!(col1, col3);
    }

    #[test]
    fn test_predicate_table_refs() {
        let table_id = TableId("users".to_string());
        let expr = Expression::Literal(SqlValue::Boolean(true));

        let pred = Predicate::TableLocal {
            table_id: table_id.clone(),
            column_refs: vec![],
            expression: expr.clone(),
            estimated_selectivity: None,
        };

        let refs = pred.table_refs();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0], &table_id);
    }

    #[test]
    fn test_predicate_can_push_to() {
        let table_id = TableId("users".to_string());
        let other_id = TableId("posts".to_string());
        let expr = Expression::Literal(SqlValue::Boolean(true));

        let pred = Predicate::TableLocal {
            table_id: table_id.clone(),
            column_refs: vec![],
            expression: expr,
            estimated_selectivity: None,
        };

        assert!(pred.can_push_to(&table_id));
        assert!(!pred.can_push_to(&other_id));
    }

    #[test]
    fn test_constant_predicate_can_push_anywhere() {
        let expr = Expression::Literal(SqlValue::Boolean(true));
        let pred = Predicate::Constant {
            value: true,
            expression: expr,
        };

        let table_id = TableId("users".to_string());
        assert!(pred.can_push_to(&table_id));
    }

    #[test]
    fn test_is_equijoin_valid() {
        let col1 = ColumnRef {
            table_id: TableId("t1".to_string()),
            column_name: "a".to_string(),
        };
        let col2 = ColumnRef {
            table_id: TableId("t2".to_string()),
            column_name: "b".to_string(),
        };
        let column_refs = vec![col1, col2];

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "a".to_string(),
            }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "b".to_string(),
            }),
        };

        assert!(is_equijoin(&expr, &column_refs));
    }

    #[test]
    fn test_is_equijoin_same_table() {
        let col1 = ColumnRef {
            table_id: TableId("t1".to_string()),
            column_name: "a".to_string(),
        };
        let col2 = ColumnRef {
            table_id: TableId("t1".to_string()),
            column_name: "b".to_string(),
        };
        let column_refs = vec![col1, col2];

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "a".to_string(),
            }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "b".to_string(),
            }),
        };

        assert!(!is_equijoin(&expr, &column_refs));
    }
}
