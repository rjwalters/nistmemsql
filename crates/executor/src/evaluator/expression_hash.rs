//! Expression hashing for common sub-expression elimination (CSE)
//!
//! This module provides structural hashing of expression trees to enable
//! caching and reuse of identical sub-expressions during evaluation.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Computes structural hashes for expression trees
pub struct ExpressionHasher;

impl ExpressionHasher {
    /// Compute structural hash for an expression tree
    ///
    /// Two structurally identical expressions will hash to the same value,
    /// regardless of memory addresses or pointer values.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let expr1 = Expression::BinaryOp { /* a + b */ };
    /// let expr2 = Expression::BinaryOp { /* a + b */ };
    /// assert_eq!(ExpressionHasher::hash(&expr1), ExpressionHasher::hash(&expr2));
    /// ```
    pub fn hash(expr: &ast::Expression) -> u64 {
        let mut hasher = DefaultHasher::new();
        Self::hash_expression(expr, &mut hasher);
        hasher.finish()
    }

    /// Check if an expression is deterministic (safe to cache)
    ///
    /// Non-deterministic expressions like RAND() or CURRENT_TIMESTAMP should
    /// not be cached as they may produce different values on each evaluation.
    pub fn is_deterministic(expr: &ast::Expression) -> bool {
        match expr {
            // Non-deterministic datetime functions
            ast::Expression::CurrentDate
            | ast::Expression::CurrentTime { .. }
            | ast::Expression::CurrentTimestamp { .. } => false,

            // Non-deterministic functions and aggregate functions used as regular functions
            ast::Expression::Function { name, args, .. } => {
                let name_upper = name.to_uppercase();
                // List of known non-deterministic functions and aggregate functions
                // (aggregate functions should not be cached at row level)
                if matches!(
                    name_upper.as_str(),
                    "RAND" | "RANDOM" | "NOW" | "CURRENT_DATE" | "CURRENT_TIME"
                        | "CURRENT_TIMESTAMP"
                        | "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
                ) {
                    return false;
                }
                // Recursively check arguments
                args.iter().all(Self::is_deterministic)
            }

            // Sequence functions are non-deterministic
            ast::Expression::NextValue { .. } => false,

            // Subqueries may be non-deterministic (conservative approach)
            ast::Expression::ScalarSubquery(_)
            | ast::Expression::In { .. }
            | ast::Expression::Exists { .. }
            | ast::Expression::QuantifiedComparison { .. } => false,

            // Recursively check sub-expressions
            ast::Expression::BinaryOp { left, right, .. } => {
                Self::is_deterministic(left) && Self::is_deterministic(right)
            }

            ast::Expression::UnaryOp { expr, .. } => Self::is_deterministic(expr),

            ast::Expression::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                // Check operand
                if let Some(op) = operand {
                    if !Self::is_deterministic(op) {
                        return false;
                    }
                }
                // Check all when conditions and results
                for clause in when_clauses {
                    // Check all conditions (there can be multiple per clause)
                    for condition in &clause.conditions {
                        if !Self::is_deterministic(condition) {
                            return false;
                        }
                    }
                    if !Self::is_deterministic(&clause.result) {
                        return false;
                    }
                }
                // Check else result
                if let Some(else_expr) = else_result {
                    if !Self::is_deterministic(else_expr) {
                        return false;
                    }
                }
                true
            }

            ast::Expression::Between { expr, low, high, .. } => {
                Self::is_deterministic(expr)
                    && Self::is_deterministic(low)
                    && Self::is_deterministic(high)
            }

            ast::Expression::Cast { expr, .. } => Self::is_deterministic(expr),

            ast::Expression::InList { expr, values, .. } => {
                Self::is_deterministic(expr) && values.iter().all(Self::is_deterministic)
            }

            ast::Expression::Like { expr, pattern, .. } => {
                Self::is_deterministic(expr) && Self::is_deterministic(pattern)
            }

            ast::Expression::IsNull { expr, .. } => Self::is_deterministic(expr),

            ast::Expression::Position { substring, string, .. } => {
                Self::is_deterministic(substring) && Self::is_deterministic(string)
            }

            ast::Expression::Trim { string, removal_char, .. } => {
                Self::is_deterministic(string)
                    && removal_char.as_ref().map_or(true, |c| Self::is_deterministic(c))
            }

            // Literals are deterministic
            ast::Expression::Literal(_) => true,

            // Column references should NOT be cached - they vary by row
            // Caching them causes bugs when the same evaluator is reused for multiple rows
            ast::Expression::ColumnRef { .. } => false,

            // Window and aggregate functions should not be cached at this level
            ast::Expression::WindowFunction { .. }
            | ast::Expression::AggregateFunction { .. } => false,

            // Wildcards and DEFAULT are special cases
            ast::Expression::Wildcard | ast::Expression::Default => true,
        }
    }

    /// Internal recursive hash function
    fn hash_expression(expr: &ast::Expression, hasher: &mut DefaultHasher) {
        // Hash the discriminant (which variant of the enum)
        std::mem::discriminant(expr).hash(hasher);

        // Hash the contents based on variant
        match expr {
            ast::Expression::Literal(val) => {
                // Hash the SQL value
                Self::hash_sql_value(val, hasher);
            }

            ast::Expression::ColumnRef { table, column } => {
                table.hash(hasher);
                column.hash(hasher);
            }

            ast::Expression::BinaryOp { left, op, right } => {
                Self::hash_expression(left, hasher);
                std::mem::discriminant(op).hash(hasher);
                Self::hash_expression(right, hasher);
            }

            ast::Expression::UnaryOp { op, expr } => {
                std::mem::discriminant(op).hash(hasher);
                Self::hash_expression(expr, hasher);
            }

            ast::Expression::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                if let Some(op) = operand {
                    Self::hash_expression(op, hasher);
                }
                when_clauses.len().hash(hasher);
                for clause in when_clauses {
                    clause.conditions.len().hash(hasher);
                    for condition in &clause.conditions {
                        Self::hash_expression(condition, hasher);
                    }
                    Self::hash_expression(&clause.result, hasher);
                }
                if let Some(else_expr) = else_result {
                    Self::hash_expression(else_expr, hasher);
                }
            }

            ast::Expression::Between {
                expr,
                low,
                high,
                negated,
                symmetric,
            } => {
                Self::hash_expression(expr, hasher);
                Self::hash_expression(low, hasher);
                Self::hash_expression(high, hasher);
                negated.hash(hasher);
                symmetric.hash(hasher);
            }

            ast::Expression::Cast { expr, data_type } => {
                Self::hash_expression(expr, hasher);
                Self::hash_data_type(data_type, hasher);
            }

            ast::Expression::InList { expr, values, negated } => {
                Self::hash_expression(expr, hasher);
                values.len().hash(hasher);
                for val in values {
                    Self::hash_expression(val, hasher);
                }
                negated.hash(hasher);
            }

            ast::Expression::Like { expr, pattern, negated } => {
                Self::hash_expression(expr, hasher);
                Self::hash_expression(pattern, hasher);
                negated.hash(hasher);
            }

            ast::Expression::IsNull { expr, negated } => {
                Self::hash_expression(expr, hasher);
                negated.hash(hasher);
            }

            ast::Expression::Function {
                name,
                args,
                character_unit,
            } => {
                name.hash(hasher);
                args.len().hash(hasher);
                for arg in args {
                    Self::hash_expression(arg, hasher);
                }
                if let Some(unit) = character_unit {
                    std::mem::discriminant(unit).hash(hasher);
                }
            }

            ast::Expression::Position {
                substring,
                string,
                character_unit,
            } => {
                Self::hash_expression(substring, hasher);
                Self::hash_expression(string, hasher);
                if let Some(unit) = character_unit {
                    std::mem::discriminant(unit).hash(hasher);
                }
            }

            ast::Expression::Trim {
                position,
                removal_char,
                string,
            } => {
                if let Some(pos) = position {
                    std::mem::discriminant(pos).hash(hasher);
                }
                if let Some(ch) = removal_char {
                    Self::hash_expression(ch, hasher);
                }
                Self::hash_expression(string, hasher);
            }

            ast::Expression::CurrentDate => {
                "CURRENT_DATE".hash(hasher);
            }

            ast::Expression::CurrentTime { precision } => {
                "CURRENT_TIME".hash(hasher);
                precision.hash(hasher);
            }

            ast::Expression::CurrentTimestamp { precision } => {
                "CURRENT_TIMESTAMP".hash(hasher);
                precision.hash(hasher);
            }

            // For subqueries and complex expressions, we still hash them
            // but is_deterministic() will prevent caching
            ast::Expression::In { expr, subquery, negated } => {
                Self::hash_expression(expr, hasher);
                format!("{:?}", subquery).hash(hasher); // Simple hash for now
                negated.hash(hasher);
            }

            ast::Expression::ScalarSubquery(subquery) => {
                format!("{:?}", subquery).hash(hasher);
            }

            ast::Expression::Exists { subquery, negated } => {
                format!("{:?}", subquery).hash(hasher);
                negated.hash(hasher);
            }

            ast::Expression::QuantifiedComparison { expr, op, quantifier, subquery } => {
                Self::hash_expression(expr, hasher);
                std::mem::discriminant(op).hash(hasher);
                std::mem::discriminant(quantifier).hash(hasher);
                format!("{:?}", subquery).hash(hasher);
            }

            ast::Expression::WindowFunction { .. } => {
                // Hash the debug representation for now
                format!("{:?}", expr).hash(hasher);
            }

            ast::Expression::AggregateFunction { .. } => {
                // Hash the debug representation for now
                format!("{:?}", expr).hash(hasher);
            }

            ast::Expression::Wildcard => {
                "*".hash(hasher);
            }

            ast::Expression::Default => {
                "DEFAULT".hash(hasher);
            }

            ast::Expression::NextValue { sequence_name } => {
                sequence_name.hash(hasher);
            }
        }
    }

    /// Hash a SQL value
    fn hash_sql_value(val: &types::SqlValue, hasher: &mut DefaultHasher) {
        std::mem::discriminant(val).hash(hasher);
        match val {
            types::SqlValue::Null => {}
            types::SqlValue::Integer(i) => i.hash(hasher),
            types::SqlValue::Smallint(s) => s.hash(hasher),
            types::SqlValue::Bigint(b) => b.hash(hasher),
            types::SqlValue::Unsigned(u) => u.hash(hasher),
            types::SqlValue::Numeric(n) => n.to_bits().hash(hasher), // Hash float bits
            types::SqlValue::Float(f) => f.to_bits().hash(hasher),
            types::SqlValue::Real(r) => r.to_bits().hash(hasher),
            types::SqlValue::Double(d) => d.to_bits().hash(hasher),
            types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => s.hash(hasher),
            types::SqlValue::Boolean(b) => b.hash(hasher),
            types::SqlValue::Date(d) => d.hash(hasher),
            types::SqlValue::Time(t) => t.hash(hasher),
            types::SqlValue::Timestamp(ts) => ts.hash(hasher),
            types::SqlValue::Interval(i) => i.hash(hasher),
        }
    }

    /// Hash a data type
    fn hash_data_type(data_type: &types::DataType, hasher: &mut DefaultHasher) {
        std::mem::discriminant(data_type).hash(hasher);
        match data_type {
            types::DataType::Integer => {}
            types::DataType::Smallint => {}
            types::DataType::Bigint => {}
            types::DataType::Unsigned => {}
            types::DataType::Numeric { precision, scale } | types::DataType::Decimal { precision, scale } => {
                precision.hash(hasher);
                scale.hash(hasher);
            }
            types::DataType::Float { precision } => precision.hash(hasher),
            types::DataType::Real => {}
            types::DataType::DoublePrecision => {}
            types::DataType::Varchar { max_length } => max_length.hash(hasher),
            types::DataType::Character { length } => length.hash(hasher),
            types::DataType::CharacterLargeObject => {}
            types::DataType::Name => {}
            types::DataType::Boolean => {}
            types::DataType::Date => {}
            types::DataType::Time { with_timezone } => with_timezone.hash(hasher),
            types::DataType::Timestamp { with_timezone } => with_timezone.hash(hasher),
            types::DataType::Interval { start_field, end_field } => {
                std::mem::discriminant(start_field).hash(hasher);
                if let Some(ef) = end_field {
                    std::mem::discriminant(ef).hash(hasher);
                }
            }
            types::DataType::BinaryLargeObject => {}
            types::DataType::UserDefined { type_name } => type_name.hash(hasher),
            types::DataType::Null => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identical_literals_hash_equal() {
        let expr1 = ast::Expression::Literal(types::SqlValue::Integer(42));
        let expr2 = ast::Expression::Literal(types::SqlValue::Integer(42));
        assert_eq!(ExpressionHasher::hash(&expr1), ExpressionHasher::hash(&expr2));
    }

    #[test]
    fn test_different_literals_hash_different() {
        let expr1 = ast::Expression::Literal(types::SqlValue::Integer(42));
        let expr2 = ast::Expression::Literal(types::SqlValue::Integer(43));
        assert_ne!(ExpressionHasher::hash(&expr1), ExpressionHasher::hash(&expr2));
    }

    #[test]
    fn test_identical_binary_ops_hash_equal() {
        let left = Box::new(ast::Expression::Literal(types::SqlValue::Integer(1)));
        let right = Box::new(ast::Expression::Literal(types::SqlValue::Integer(2)));

        let expr1 = ast::Expression::BinaryOp {
            left: left.clone(),
            op: ast::BinaryOperator::Plus,
            right: right.clone(),
        };

        let expr2 = ast::Expression::BinaryOp {
            left,
            op: ast::BinaryOperator::Plus,
            right,
        };

        assert_eq!(ExpressionHasher::hash(&expr1), ExpressionHasher::hash(&expr2));
    }

    #[test]
    fn test_literals_are_deterministic() {
        let expr = ast::Expression::Literal(types::SqlValue::Integer(42));
        assert!(ExpressionHasher::is_deterministic(&expr));
    }

    #[test]
    fn test_column_refs_are_not_deterministic() {
        let expr = ast::Expression::ColumnRef {
            table: None,
            column: "col1".to_string(),
        };
        // Column refs should not be cached - they vary by row
        assert!(!ExpressionHasher::is_deterministic(&expr));
    }

    #[test]
    fn test_current_timestamp_is_not_deterministic() {
        let expr = ast::Expression::CurrentTimestamp { precision: None };
        assert!(!ExpressionHasher::is_deterministic(&expr));
    }

    #[test]
    fn test_rand_function_is_not_deterministic() {
        let expr = ast::Expression::Function {
            name: "RAND".to_string(),
            args: vec![],
            character_unit: None,
        };
        assert!(!ExpressionHasher::is_deterministic(&expr));
    }

    #[test]
    fn test_deterministic_function_is_deterministic() {
        let arg = ast::Expression::Literal(types::SqlValue::Integer(42));
        let expr = ast::Expression::Function {
            name: "ABS".to_string(),
            args: vec![arg],
            character_unit: None,
        };
        assert!(ExpressionHasher::is_deterministic(&expr));
    }
}
