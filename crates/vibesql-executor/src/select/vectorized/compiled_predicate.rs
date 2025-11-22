//! Compiled predicate evaluation for fast-path WHERE clauses
//!
//! This module provides specialized evaluation for simple predicates commonly found
//! in analytical queries (e.g., TPC-H Q6). Instead of walking the full expression tree
//! for each row, we "compile" predicates into a more efficient representation.
//!
//! Optimizations:
//! - Pre-compute column indices to avoid schema lookups
//! - Flatten AND chains into a vector of predicates
//! - Use type-specific comparison functions to skip type coercion
//! - Direct memory access to column values

use vibesql_ast::{BinaryOperator, Expression};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use crate::{errors::ExecutorError, schema::CombinedSchema};

/// A compiled simple predicate for fast evaluation
#[derive(Debug, Clone)]
enum CompiledPredicate {
    /// Column comparison: column_idx op literal_value
    ColumnLiteral { column_idx: usize, op: ComparisonOp, literal: SqlValue },
    /// BETWEEN: column_idx BETWEEN low AND high
    Between { column_idx: usize, low: SqlValue, high: SqlValue, negated: bool },
}

/// Supported comparison operators
#[derive(Debug, Clone, Copy)]
enum ComparisonOp {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

/// A compiled WHERE clause consisting of AND-combined simple predicates
pub struct CompiledWhereClause {
    predicates: Vec<CompiledPredicate>,
}

impl CompiledWhereClause {
    /// Try to compile a WHERE clause into the optimized form
    /// Returns None if the WHERE clause contains unsupported patterns
    pub fn try_compile(where_expr: &Expression, schema: &CombinedSchema) -> Option<Self> {
        let mut predicates = Vec::new();

        // Try to extract AND-combined predicates
        if !Self::extract_and_predicates(where_expr, schema, &mut predicates) {
            return None;
        }

        if predicates.is_empty() {
            return None;
        }

        Some(CompiledWhereClause { predicates })
    }

    /// Extract predicates from AND-combined expression
    fn extract_and_predicates(
        expr: &Expression,
        schema: &CombinedSchema,
        predicates: &mut Vec<CompiledPredicate>,
    ) -> bool {
        match expr {
            Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
                // Recursively extract from left and right
                Self::extract_and_predicates(left, schema, predicates)
                    && Self::extract_and_predicates(right, schema, predicates)
            }
            Expression::Between { expr: col_expr, low, high, negated, symmetric: _ } => {
                // Try to compile BETWEEN predicate
                Self::try_compile_between(col_expr, low, high, *negated, schema, predicates)
            }
            _ => {
                // Try to compile as simple binary comparison
                Self::try_compile_comparison(expr, schema, predicates)
            }
        }
    }

    /// Try to compile a BETWEEN predicate
    fn try_compile_between(
        col_expr: &Expression,
        low_expr: &Expression,
        high_expr: &Expression,
        negated: bool,
        schema: &CombinedSchema,
        predicates: &mut Vec<CompiledPredicate>,
    ) -> bool {
        // Extract column reference
        let column_idx = match col_expr {
            Expression::ColumnRef { table, column } => {
                schema.get_column_index(table.as_deref(), column)
            }
            _ => None,
        };

        let column_idx = match column_idx {
            Some(idx) => idx,
            None => return false,
        };

        // Extract literal values
        let low = match low_expr {
            Expression::Literal(val) => val.clone(),
            _ => return false,
        };

        let high = match high_expr {
            Expression::Literal(val) => val.clone(),
            _ => return false,
        };

        predicates.push(CompiledPredicate::Between { column_idx, low, high, negated });

        true
    }

    /// Try to compile a simple comparison predicate
    fn try_compile_comparison(
        expr: &Expression,
        schema: &CombinedSchema,
        predicates: &mut Vec<CompiledPredicate>,
    ) -> bool {
        // Must be a binary operation
        let (left, op, right) = match expr {
            Expression::BinaryOp { left, op, right } => (left, op, right),
            _ => return false,
        };

        // Convert operator
        let comp_op = match op {
            BinaryOperator::Equal => ComparisonOp::Equal,
            BinaryOperator::NotEqual => ComparisonOp::NotEqual,
            BinaryOperator::LessThan => ComparisonOp::LessThan,
            BinaryOperator::LessThanOrEqual => ComparisonOp::LessThanOrEqual,
            BinaryOperator::GreaterThan => ComparisonOp::GreaterThan,
            BinaryOperator::GreaterThanOrEqual => ComparisonOp::GreaterThanOrEqual,
            _ => return false, // Not a comparison operator
        };

        // Try column on left, literal on right
        if let (Some(column_idx), Some(literal)) =
            (Self::try_extract_column(left, schema), Self::try_extract_literal(right))
        {
            predicates.push(CompiledPredicate::ColumnLiteral { column_idx, op: comp_op, literal });
            return true;
        }

        // Try literal on left, column on right (flip operator)
        if let (Some(literal), Some(column_idx)) =
            (Self::try_extract_literal(left), Self::try_extract_column(right, schema))
        {
            let flipped_op = Self::flip_operator(comp_op);
            predicates.push(CompiledPredicate::ColumnLiteral {
                column_idx,
                op: flipped_op,
                literal,
            });
            return true;
        }

        false
    }

    /// Try to extract a column index from an expression
    fn try_extract_column(expr: &Expression, schema: &CombinedSchema) -> Option<usize> {
        match expr {
            Expression::ColumnRef { table, column } => {
                schema.get_column_index(table.as_deref(), column)
            }
            _ => None,
        }
    }

    /// Try to extract a literal value from an expression
    fn try_extract_literal(expr: &Expression) -> Option<SqlValue> {
        match expr {
            Expression::Literal(val) => Some(val.clone()),
            _ => None,
        }
    }

    /// Flip a comparison operator for when operands are swapped
    fn flip_operator(op: ComparisonOp) -> ComparisonOp {
        match op {
            ComparisonOp::Equal => ComparisonOp::Equal,
            ComparisonOp::NotEqual => ComparisonOp::NotEqual,
            ComparisonOp::LessThan => ComparisonOp::GreaterThan,
            ComparisonOp::LessThanOrEqual => ComparisonOp::GreaterThanOrEqual,
            ComparisonOp::GreaterThan => ComparisonOp::LessThan,
            ComparisonOp::GreaterThanOrEqual => ComparisonOp::LessThanOrEqual,
        }
    }

    /// Evaluate the compiled predicates on a row (returns true if row matches ALL predicates)
    #[inline(always)]
    pub fn evaluate(&self, row: &Row) -> Result<bool, ExecutorError> {
        for predicate in &self.predicates {
            if !self.evaluate_single(predicate, row)? {
                return Ok(false); // Short-circuit: AND semantics
            }
        }
        Ok(true)
    }

    /// Evaluate a single compiled predicate
    #[inline(always)]
    fn evaluate_single(
        &self,
        predicate: &CompiledPredicate,
        row: &Row,
    ) -> Result<bool, ExecutorError> {
        match predicate {
            CompiledPredicate::ColumnLiteral { column_idx, op, literal } => {
                let column_value = &row.values[*column_idx];
                self.compare_values(column_value, literal, *op)
            }
            CompiledPredicate::Between { column_idx, low, high, negated } => {
                let column_value = &row.values[*column_idx];
                let result = self.is_between(column_value, low, high)?;
                Ok(if *negated { !result } else { result })
            }
        }
    }

    /// Fast comparison of SQL values with type-specific logic
    #[inline(always)]
    fn compare_values(
        &self,
        left: &SqlValue,
        right: &SqlValue,
        op: ComparisonOp,
    ) -> Result<bool, ExecutorError> {
        use SqlValue::*;

        // Handle NULL - comparisons with NULL return NULL (false in WHERE context)
        if matches!(left, Null) || matches!(right, Null) {
            return Ok(false);
        }

        // Fast path for common types - avoid generic comparison overhead
        match (left, right) {
            // Integer comparisons
            (Integer(l), Integer(r)) => Ok(self.apply_op(*l, *r, op)),
            (Bigint(l), Bigint(r)) => Ok(self.apply_op(*l, *r, op)),
            (Smallint(l), Smallint(r)) => Ok(self.apply_op(*l, *r, op)),

            // Float comparisons - use PartialOrd since floats don't impl Ord
            (Double(l), Double(r)) => Ok(self.apply_op_float(*l, *r, op)),
            (Float(l), Float(r)) => Ok(self.apply_op_float(*l, *r, op)),
            (Real(l), Real(r)) => Ok(self.apply_op_float(*l, *r, op)),

            // String comparisons (dates are stored as strings in TPC-H)
            (Varchar(l), Varchar(r))
            | (Character(l), Character(r))
            | (Varchar(l), Character(r))
            | (Character(l), Varchar(r)) => Ok(self.apply_op(l.as_str(), r.as_str(), op)),

            // Cross-type comparisons - fall back to slower path
            _ => self.compare_values_generic(left, right, op),
        }
    }

    /// Generic comparison for cross-type cases
    fn compare_values_generic(
        &self,
        left: &SqlValue,
        right: &SqlValue,
        op: ComparisonOp,
    ) -> Result<bool, ExecutorError> {
        // Use the existing operator registry for correctness
        use vibesql_ast::BinaryOperator;

        use crate::evaluator::operators::OperatorRegistry;

        let ast_op = match op {
            ComparisonOp::Equal => BinaryOperator::Equal,
            ComparisonOp::NotEqual => BinaryOperator::NotEqual,
            ComparisonOp::LessThan => BinaryOperator::LessThan,
            ComparisonOp::LessThanOrEqual => BinaryOperator::LessThanOrEqual,
            ComparisonOp::GreaterThan => BinaryOperator::GreaterThan,
            ComparisonOp::GreaterThanOrEqual => BinaryOperator::GreaterThanOrEqual,
        };

        let result = OperatorRegistry::eval_binary_op(left, &ast_op, right, Default::default())?;

        match result {
            SqlValue::Boolean(b) => Ok(b),
            SqlValue::Null => Ok(false),
            _ => {
                Err(ExecutorError::InvalidWhereClause("Comparison must return boolean".to_string()))
            }
        }
    }

    /// Apply comparison operator to Ord types
    #[inline(always)]
    fn apply_op<T: Ord>(&self, left: T, right: T, op: ComparisonOp) -> bool {
        match op {
            ComparisonOp::Equal => left == right,
            ComparisonOp::NotEqual => left != right,
            ComparisonOp::LessThan => left < right,
            ComparisonOp::LessThanOrEqual => left <= right,
            ComparisonOp::GreaterThan => left > right,
            ComparisonOp::GreaterThanOrEqual => left >= right,
        }
    }

    /// Apply comparison operator to float types (PartialOrd)
    #[inline(always)]
    fn apply_op_float<T: PartialOrd>(&self, left: T, right: T, op: ComparisonOp) -> bool {
        match op {
            ComparisonOp::Equal => left == right,
            ComparisonOp::NotEqual => left != right,
            ComparisonOp::LessThan => left < right,
            ComparisonOp::LessThanOrEqual => left <= right,
            ComparisonOp::GreaterThan => left > right,
            ComparisonOp::GreaterThanOrEqual => left >= right,
        }
    }

    /// Check if value is between low and high (inclusive)
    #[inline(always)]
    fn is_between(
        &self,
        value: &SqlValue,
        low: &SqlValue,
        high: &SqlValue,
    ) -> Result<bool, ExecutorError> {
        // BETWEEN is inclusive on both ends: value >= low AND value <= high
        let ge_low = self.compare_values(value, low, ComparisonOp::GreaterThanOrEqual)?;
        let le_high = self.compare_values(value, high, ComparisonOp::LessThanOrEqual)?;
        Ok(ge_low && le_high)
    }
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    use super::*;

    fn make_test_schema() -> CombinedSchema {
        let schema = TableSchema::new(
            "lineitem".to_string(),
            vec![
                ColumnSchema {
                    name: "l_quantity".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "l_discount".to_string(),
                    data_type: DataType::DoublePrecision,
                    nullable: false,
                    default_value: None,
                },
                ColumnSchema {
                    name: "l_shipdate".to_string(),
                    data_type: DataType::Varchar { max_length: None },
                    nullable: false,
                    default_value: None,
                },
            ],
        );
        CombinedSchema::from_table("lineitem".to_string(), schema)
    }

    #[test]
    fn test_compile_simple_comparison() {
        let schema = make_test_schema();

        // l_quantity < 24
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "l_quantity".to_string() }),
            op: BinaryOperator::LessThan,
            right: Box::new(Expression::Literal(SqlValue::Integer(24))),
        };

        let compiled = CompiledWhereClause::try_compile(&expr, &schema);
        assert!(compiled.is_some());

        let compiled = compiled.unwrap();
        assert_eq!(compiled.predicates.len(), 1);
    }

    #[test]
    fn test_compile_and_chain() {
        let schema = make_test_schema();

        // l_quantity < 24 AND l_discount >= 0.05
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "l_quantity".to_string(),
                }),
                op: BinaryOperator::LessThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(24))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "l_discount".to_string(),
                }),
                op: BinaryOperator::GreaterThanOrEqual,
                right: Box::new(Expression::Literal(SqlValue::Double(0.05))),
            }),
        };

        let compiled = CompiledWhereClause::try_compile(&expr, &schema);
        assert!(compiled.is_some());

        let compiled = compiled.unwrap();
        assert_eq!(compiled.predicates.len(), 2);
    }

    #[test]
    fn test_evaluate_predicate() {
        let schema = make_test_schema();

        // l_quantity < 24
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "l_quantity".to_string() }),
            op: BinaryOperator::LessThan,
            right: Box::new(Expression::Literal(SqlValue::Integer(24))),
        };

        let compiled = CompiledWhereClause::try_compile(&expr, &schema).unwrap();

        // Test row that matches
        let row = Row {
            values: vec![
                SqlValue::Integer(20),
                SqlValue::Double(0.05),
                SqlValue::Varchar("1994-01-01".to_string()),
            ],
        };
        assert!(compiled.evaluate(&row).unwrap());

        // Test row that doesn't match
        let row2 = Row {
            values: vec![
                SqlValue::Integer(30),
                SqlValue::Double(0.05),
                SqlValue::Varchar("1994-01-01".to_string()),
            ],
        };
        assert!(!compiled.evaluate(&row2).unwrap());
    }
}
