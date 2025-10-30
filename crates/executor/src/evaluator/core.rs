use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use super::casting::{is_exact_numeric, is_approximate_numeric, to_i64, to_f64};

/// Evaluates expressions in the context of a row
pub struct ExpressionEvaluator<'a> {
    pub(super) schema: &'a catalog::TableSchema,
    pub(super) outer_row: Option<&'a storage::Row>,
    pub(super) outer_schema: Option<&'a catalog::TableSchema>,
    pub(super) database: Option<&'a storage::Database>,
}

/// Evaluates expressions with combined schema (for JOINs)
pub struct CombinedExpressionEvaluator<'a> {
    pub(super) schema: &'a CombinedSchema,
    pub(super) database: Option<&'a storage::Database>,
    pub(super) outer_row: Option<&'a storage::Row>,
    pub(super) outer_schema: Option<&'a CombinedSchema>,
}

impl<'a> ExpressionEvaluator<'a> {
    /// Create a new expression evaluator for a given schema
    pub fn new(schema: &'a catalog::TableSchema) -> Self {
        ExpressionEvaluator { schema, outer_row: None, outer_schema: None, database: None }
    }

    /// Create a new expression evaluator with outer query context for correlated subqueries
    pub fn with_outer_context(
        schema: &'a catalog::TableSchema,
        outer_row: &'a storage::Row,
        outer_schema: &'a catalog::TableSchema,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
            database: None,
        }
    }

    /// Create a new expression evaluator with database reference for subqueries
    pub fn with_database(
        schema: &'a catalog::TableSchema,
        database: &'a storage::Database,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: None,
            outer_schema: None,
            database: Some(database),
        }
    }

    /// Create a new expression evaluator with database and outer context (for correlated subqueries)
    pub fn with_database_and_outer_context(
        schema: &'a catalog::TableSchema,
        database: &'a storage::Database,
        outer_row: &'a storage::Row,
        outer_schema: &'a catalog::TableSchema,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
            database: Some(database),
        }
    }

    /// Evaluate a binary operation
    pub(crate) fn eval_binary_op(
        &self,
        left: &types::SqlValue,
        op: &ast::BinaryOperator,
        right: &types::SqlValue,
    ) -> Result<types::SqlValue, ExecutorError> {
        Self::eval_binary_op_static(left, op, right)
    }

    /// Static version of eval_binary_op for shared logic
    pub(crate) fn eval_binary_op_static(
        left: &types::SqlValue,
        op: &ast::BinaryOperator,
        right: &types::SqlValue,
    ) -> Result<types::SqlValue, ExecutorError> {
        use ast::BinaryOperator::*;
        use types::SqlValue::*;

        match (left, op, right) {
            // Integer arithmetic
            (Integer(a), Plus, Integer(b)) => Ok(Integer(a + b)),
            (Integer(a), Minus, Integer(b)) => Ok(Integer(a - b)),
            (Integer(a), Multiply, Integer(b)) => Ok(Integer(a * b)),
            (Integer(a), Divide, Integer(b)) => {
                if *b == 0 {
                    Err(ExecutorError::DivisionByZero)
                } else {
                    Ok(Integer(a / b))
                }
            }

            // Integer comparisons
            (Integer(a), Equal, Integer(b)) => Ok(Boolean(a == b)),
            (Integer(a), NotEqual, Integer(b)) => Ok(Boolean(a != b)),
            (Integer(a), LessThan, Integer(b)) => Ok(Boolean(a < b)),
            (Integer(a), LessThanOrEqual, Integer(b)) => Ok(Boolean(a <= b)),
            (Integer(a), GreaterThan, Integer(b)) => Ok(Boolean(a > b)),
            (Integer(a), GreaterThanOrEqual, Integer(b)) => Ok(Boolean(a >= b)),

            // String comparisons (VARCHAR and CHAR are compatible)
            (Varchar(a), Equal, Varchar(b)) => Ok(Boolean(a == b)),
            (Varchar(a), NotEqual, Varchar(b)) => Ok(Boolean(a != b)),
            (Character(a), Equal, Character(b)) => Ok(Boolean(a == b)),
            (Character(a), NotEqual, Character(b)) => Ok(Boolean(a != b)),

            // Cross-type string comparisons (CHAR vs VARCHAR)
            (Character(a), Equal, Varchar(b)) | (Varchar(b), Equal, Character(a)) => Ok(Boolean(a == b)),
            (Character(a), NotEqual, Varchar(b)) | (Varchar(b), NotEqual, Character(a)) => Ok(Boolean(a != b)),

            // String concatenation (||)
            (Varchar(a), Concat, Varchar(b)) => Ok(Varchar(format!("{}{}", a, b))),
            (Varchar(a), Concat, Character(b)) => Ok(Varchar(format!("{}{}", a, b))),
            (Character(a), Concat, Varchar(b)) => Ok(Varchar(format!("{}{}", a, b))),
            (Character(a), Concat, Character(b)) => Ok(Varchar(format!("{}{}", a, b))),

            // Boolean comparisons
            (Boolean(a), Equal, Boolean(b)) => Ok(Boolean(a == b)),
            (Boolean(a), NotEqual, Boolean(b)) => Ok(Boolean(a != b)),

            // Boolean logic
            (Boolean(a), And, Boolean(b)) => Ok(Boolean(*a && *b)),
            (Boolean(a), Or, Boolean(b)) => Ok(Boolean(*a || *b)),

            // NULL comparisons - NULL compared to anything is NULL (three-valued logic)
            (Null, _, _) | (_, _, Null) => Ok(Null),

            // Cross-type numeric comparisons - promote to common type
            // Compare exact integer types (SMALLINT, INTEGER, BIGINT) by promoting to i64
            (left_val, op @ (Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual), right_val)
                if is_exact_numeric(left_val) && is_exact_numeric(right_val) =>
            {
                let left_i64 = to_i64(left_val)?;
                let right_i64 = to_i64(right_val)?;
                match op {
                    Equal => Ok(Boolean(left_i64 == right_i64)),
                    NotEqual => Ok(Boolean(left_i64 != right_i64)),
                    LessThan => Ok(Boolean(left_i64 < right_i64)),
                    LessThanOrEqual => Ok(Boolean(left_i64 <= right_i64)),
                    GreaterThan => Ok(Boolean(left_i64 > right_i64)),
                    GreaterThanOrEqual => Ok(Boolean(left_i64 >= right_i64)),
                    _ => unreachable!(),
                }
            }

            // Compare approximate numeric types (FLOAT, REAL, DOUBLE) by promoting to f64
            (left_val, op @ (Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual), right_val)
                if is_approximate_numeric(left_val) && is_approximate_numeric(right_val) =>
            {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                match op {
                    Equal => Ok(Boolean(left_f64 == right_f64)),
                    NotEqual => Ok(Boolean(left_f64 != right_f64)),
                    LessThan => Ok(Boolean(left_f64 < right_f64)),
                    LessThanOrEqual => Ok(Boolean(left_f64 <= right_f64)),
                    GreaterThan => Ok(Boolean(left_f64 > right_f64)),
                    GreaterThanOrEqual => Ok(Boolean(left_f64 >= right_f64)),
                    _ => unreachable!(),
                }
            }

            // Mixed Float/Integer arithmetic - promote Integer to Float for all operations
            (left_val @ (Float(_) | Real(_) | Double(_)), op @ (Plus | Minus | Multiply | Divide), right_val @ (Integer(_) | Smallint(_) | Bigint(_))) => {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                match op {
                    Plus => Ok(Float((left_f64 + right_f64) as f32)),
                    Minus => Ok(Float((left_f64 - right_f64) as f32)),
                    Multiply => Ok(Float((left_f64 * right_f64) as f32)),
                    Divide => {
                        if right_f64 == 0.0 {
                            Err(ExecutorError::DivisionByZero)
                        } else {
                            Ok(Float((left_f64 / right_f64) as f32))
                        }
                    }
                    _ => unreachable!(),
                }
            }
            (left_val @ (Integer(_) | Smallint(_) | Bigint(_)), op @ (Plus | Minus | Multiply | Divide), right_val @ (Float(_) | Real(_) | Double(_))) => {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                match op {
                    Plus => Ok(Float((left_f64 + right_f64) as f32)),
                    Minus => Ok(Float((left_f64 - right_f64) as f32)),
                    Multiply => Ok(Float((left_f64 * right_f64) as f32)),
                    Divide => {
                        if right_f64 == 0.0 {
                            Err(ExecutorError::DivisionByZero)
                        } else {
                            Ok(Float((left_f64 / right_f64) as f32))
                        }
                    }
                    _ => unreachable!(),
                }
            }

            // Mixed Float/Integer comparisons - promote Integer to Float for comparison
            (left_val @ (Float(_) | Real(_) | Double(_)), op @ (Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual), right_val @ (Integer(_) | Smallint(_) | Bigint(_))) => {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                match op {
                    Equal => Ok(Boolean(left_f64 == right_f64)),
                    NotEqual => Ok(Boolean(left_f64 != right_f64)),
                    LessThan => Ok(Boolean(left_f64 < right_f64)),
                    LessThanOrEqual => Ok(Boolean(left_f64 <= right_f64)),
                    GreaterThan => Ok(Boolean(left_f64 > right_f64)),
                    GreaterThanOrEqual => Ok(Boolean(left_f64 >= right_f64)),
                    _ => unreachable!(),
                }
            }
            (left_val @ (Integer(_) | Smallint(_) | Bigint(_)), op @ (Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual), right_val @ (Float(_) | Real(_) | Double(_))) => {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                match op {
                    Equal => Ok(Boolean(left_f64 == right_f64)),
                    NotEqual => Ok(Boolean(left_f64 != right_f64)),
                    LessThan => Ok(Boolean(left_f64 < right_f64)),
                    LessThanOrEqual => Ok(Boolean(left_f64 <= right_f64)),
                    GreaterThan => Ok(Boolean(left_f64 > right_f64)),
                    GreaterThanOrEqual => Ok(Boolean(left_f64 >= right_f64)),
                    _ => unreachable!(),
                }
            }

            // NUMERIC type coercion - treat NUMERIC as approximate numeric for arithmetic
            // NUMERIC with any other numeric type
            (left_val @ Numeric(_), op @ (Plus | Minus | Multiply | Divide), right_val) if matches!(right_val, Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)) => {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                match op {
                    Plus => Ok(Float((left_f64 + right_f64) as f32)),
                    Minus => Ok(Float((left_f64 - right_f64) as f32)),
                    Multiply => Ok(Float((left_f64 * right_f64) as f32)),
                    Divide => {
                        if right_f64 == 0.0 {
                            Err(ExecutorError::DivisionByZero)
                        } else {
                            Ok(Float((left_f64 / right_f64) as f32))
                        }
                    }
                    _ => unreachable!(),
                }
            }
            (left_val, op @ (Plus | Minus | Multiply | Divide), right_val @ Numeric(_)) if matches!(left_val, Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_)) => {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                match op {
                    Plus => Ok(Float((left_f64 + right_f64) as f32)),
                    Minus => Ok(Float((left_f64 - right_f64) as f32)),
                    Multiply => Ok(Float((left_f64 * right_f64) as f32)),
                    Divide => {
                        if right_f64 == 0.0 {
                            Err(ExecutorError::DivisionByZero)
                        } else {
                            Ok(Float((left_f64 / right_f64) as f32))
                        }
                    }
                    _ => unreachable!(),
                }
            }

            // NUMERIC comparisons
            (left_val @ Numeric(_), op @ (Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual), right_val) if matches!(right_val, Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_) | Numeric(_)) => {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                match op {
                    Equal => Ok(Boolean(left_f64 == right_f64)),
                    NotEqual => Ok(Boolean(left_f64 != right_f64)),
                    LessThan => Ok(Boolean(left_f64 < right_f64)),
                    LessThanOrEqual => Ok(Boolean(left_f64 <= right_f64)),
                    GreaterThan => Ok(Boolean(left_f64 > right_f64)),
                    GreaterThanOrEqual => Ok(Boolean(left_f64 >= right_f64)),
                    _ => unreachable!(),
                }
            }
            (left_val, op @ (Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual), right_val @ Numeric(_)) if matches!(left_val, Integer(_) | Smallint(_) | Bigint(_) | Float(_) | Real(_) | Double(_)) => {
                let left_f64 = to_f64(left_val)?;
                let right_f64 = to_f64(right_val)?;
                match op {
                    Equal => Ok(Boolean(left_f64 == right_f64)),
                    NotEqual => Ok(Boolean(left_f64 != right_f64)),
                    LessThan => Ok(Boolean(left_f64 < right_f64)),
                    LessThanOrEqual => Ok(Boolean(left_f64 <= right_f64)),
                    GreaterThan => Ok(Boolean(left_f64 > right_f64)),
                    GreaterThanOrEqual => Ok(Boolean(left_f64 >= right_f64)),
                    _ => unreachable!(),
                }
            }

            // Type mismatch
            _ => Err(ExecutorError::TypeMismatch {
                left: left.clone(),
                op: format!("{:?}", op),
                right: right.clone(),
            }),
        }
    }

    /// Compare two SQL values for equality (NULL-safe for simple CASE)
    /// Uses IS NOT DISTINCT FROM semantics where NULL = NULL is TRUE
    pub(crate) fn values_are_equal(left: &types::SqlValue, right: &types::SqlValue) -> bool {
        use types::SqlValue::*;

        // SQL:1999 semantics for CASE equality:
        // - NULL = NULL is TRUE (different from WHERE clause behavior!)
        // - This is "IS NOT DISTINCT FROM" semantics
        match (left, right) {
            (Null, Null) => true,
            (Null, _) | (_, Null) => false,
            (Integer(a), Integer(b)) => a == b,
            (Varchar(a), Varchar(b)) => a == b,
            (Character(a), Character(b)) => a == b,
            (Character(a), Varchar(b)) | (Varchar(a), Character(b)) => a == b,
            (Boolean(a), Boolean(b)) => a == b,
            _ => false, // Type mismatch = not equal
        }
    }
}

impl<'a> CombinedExpressionEvaluator<'a> {
    /// Create a new combined expression evaluator
    /// Note: Currently unused as all callers use with_database(), but kept for API completeness
    #[allow(dead_code)]
    pub(crate) fn new(schema: &'a CombinedSchema) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: None,
            outer_row: None,
            outer_schema: None,
        }
    }

    /// Create a new combined expression evaluator with database reference
    pub(crate) fn with_database(
        schema: &'a CombinedSchema,
        database: &'a storage::Database,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: None,
            outer_schema: None,
        }
    }

    /// Create a new combined expression evaluator with database and outer context for correlated subqueries
    pub(crate) fn with_database_and_outer_context(
        schema: &'a CombinedSchema,
        database: &'a storage::Database,
        outer_row: &'a storage::Row,
        outer_schema: &'a CombinedSchema,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
        }
    }
}
