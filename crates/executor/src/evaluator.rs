use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;

/// Evaluates expressions in the context of a row
pub struct ExpressionEvaluator<'a> {
    schema: &'a catalog::TableSchema,
    outer_row: Option<&'a storage::Row>,
    outer_schema: Option<&'a catalog::TableSchema>,
    database: Option<&'a storage::Database>,
}

/// Evaluates expressions with combined schema (for JOINs)
pub(crate) struct CombinedExpressionEvaluator<'a> {
    schema: &'a CombinedSchema,
    database: Option<&'a storage::Database>,
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

    /// Evaluate an expression in the context of a row
    pub fn eval(
        &self,
        expr: &ast::Expression,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        match expr {
            // Literals - just return the value
            ast::Expression::Literal(val) => Ok(val.clone()),

            // Column reference - look up column index and get value from row
            ast::Expression::ColumnRef { table: _, column } => {
                // Try to resolve in inner schema first
                if let Some(col_index) = self.schema.get_column_index(column) {
                    return row
                        .get(col_index)
                        .cloned()
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                }

                // If not found in inner schema and outer context exists, try outer schema
                if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
                    if let Some(col_index) = outer_schema.get_column_index(column) {
                        return outer_row
                            .get(col_index)
                            .cloned()
                            .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                    }
                }

                // Column not found in either schema
                Err(ExecutorError::ColumnNotFound(column.clone()))
            }

            // Binary operations
            ast::Expression::BinaryOp { left, op, right } => {
                let left_val = self.eval(left, row)?;
                let right_val = self.eval(right, row)?;
                self.eval_binary_op(&left_val, op, &right_val)
            }

            // CASE expression
            ast::Expression::Case { operand, when_clauses, else_result } => {
                self.eval_case(operand, when_clauses, else_result, row)
            }

            // IN operator with subquery
            ast::Expression::In { expr, subquery: _, negated: _ } => {
                // TODO: Full implementation requires database access to execute subquery
                // This requires refactoring ExpressionEvaluator to have database reference
                // For now, evaluate the left expression to ensure it's valid
                let _left_val = self.eval(expr, row)?;
                Err(ExecutorError::UnsupportedFeature(
                    "IN with subquery requires database access - implementation pending"
                        .to_string(),
                ))
            }

            // Scalar subquery - must return exactly one row and one column
            ast::Expression::ScalarSubquery(subquery) => {
                let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
                    "Subquery execution requires database reference".to_string(),
                ))?;

                // Execute the subquery using SelectExecutor
                // Pass current row and schema as outer context for correlated subqueries
                let select_executor = crate::select::SelectExecutor::new_with_outer_context(
                    database,
                    row,
                    self.schema,
                );
                let rows = select_executor.execute(subquery)?;

                // SQL:1999 Section 7.9: Scalar subquery must return exactly 1 row
                if rows.len() > 1 {
                    return Err(ExecutorError::SubqueryReturnedMultipleRows {
                        expected: 1,
                        actual: rows.len(),
                    });
                }

                // SQL:1999 Section 7.9: Scalar subquery must return exactly 1 column
                // Check column count from SELECT list
                if subquery.select_list.len() != 1 {
                    return Err(ExecutorError::SubqueryColumnCountMismatch {
                        expected: 1,
                        actual: subquery.select_list.len(),
                    });
                }

                // Return the single value, or NULL if no rows
                if rows.is_empty() {
                    Ok(types::SqlValue::Null)
                } else {
                    rows[0]
                        .get(0)
                        .cloned()
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })
                }
            }

            // BETWEEN predicate: expr BETWEEN low AND high
            // Equivalent to: expr >= low AND expr <= high
            // If negated: expr < low OR expr > high
            ast::Expression::Between { expr, low, high, negated } => {
                let expr_val = self.eval(expr, row)?;
                let low_val = self.eval(low, row)?;
                let high_val = self.eval(high, row)?;

                // Check if expr >= low
                let ge_low = self.eval_binary_op(
                    &expr_val,
                    &ast::BinaryOperator::GreaterThanOrEqual,
                    &low_val,
                )?;

                // Check if expr <= high
                let le_high = self.eval_binary_op(
                    &expr_val,
                    &ast::BinaryOperator::LessThanOrEqual,
                    &high_val,
                )?;

                // Combine with AND/OR depending on negated
                if *negated {
                    // NOT BETWEEN: expr < low OR expr > high
                    let lt_low = self.eval_binary_op(
                        &expr_val,
                        &ast::BinaryOperator::LessThan,
                        &low_val,
                    )?;
                    let gt_high = self.eval_binary_op(
                        &expr_val,
                        &ast::BinaryOperator::GreaterThan,
                        &high_val,
                    )?;
                    self.eval_binary_op(&lt_low, &ast::BinaryOperator::Or, &gt_high)
                } else {
                    // BETWEEN: expr >= low AND expr <= high
                    self.eval_binary_op(&ge_low, &ast::BinaryOperator::And, &le_high)
                }
            }

            // CAST expression: CAST(expr AS data_type)
            // Explicit type conversion
            ast::Expression::Cast { expr, data_type } => {
                let value = self.eval(expr, row)?;
                cast_value(&value, data_type)
            }

            // LIKE pattern matching: expr LIKE pattern
            // Supports wildcards: % (any chars), _ (single char)
            ast::Expression::Like { expr, pattern, negated } => {
                let expr_val = self.eval(expr, row)?;
                let pattern_val = self.eval(pattern, row)?;

                // Extract string values
                let text = match expr_val {
                    types::SqlValue::Varchar(ref s) | types::SqlValue::Character(ref s) => s.clone(),
                    types::SqlValue::Null => return Ok(types::SqlValue::Null),
                    _ => {
                        return Err(ExecutorError::TypeMismatch {
                            left: expr_val,
                            op: "LIKE".to_string(),
                            right: pattern_val,
                        })
                    }
                };

                let pattern_str = match pattern_val {
                    types::SqlValue::Varchar(ref s) | types::SqlValue::Character(ref s) => s.clone(),
                    types::SqlValue::Null => return Ok(types::SqlValue::Null),
                    _ => {
                        return Err(ExecutorError::TypeMismatch {
                            left: expr_val,
                            op: "LIKE".to_string(),
                            right: pattern_val,
                        })
                    }
                };

                // Perform pattern matching
                let matches = like_match(&text, &pattern_str);

                // Apply negation if needed
                let result = if *negated { !matches } else { matches };

                Ok(types::SqlValue::Boolean(result))
            }

            // IN operator with value list: expr IN (val1, val2, ...)
            // SQL:1999 Section 8.4: IN predicate
            // Returns TRUE if expr equals any value in the list
            // Returns FALSE if no match and no NULLs
            // Returns NULL if no match and list contains NULL
            ast::Expression::InList { expr, values, negated } => {
                let expr_val = self.eval(expr, row)?;

                // If left expression is NULL, result is NULL
                if matches!(expr_val, types::SqlValue::Null) {
                    return Ok(types::SqlValue::Null);
                }

                let mut found_null = false;

                // Check each value in the list
                for value_expr in values {
                    let value = self.eval(value_expr, row)?;

                    // Track if we encounter NULL
                    if matches!(value, types::SqlValue::Null) {
                        found_null = true;
                        continue;
                    }

                    // Compare using equality
                    let eq_result = self.eval_binary_op(&expr_val, &ast::BinaryOperator::Equal, &value)?;

                    // If we found a match, return TRUE (or FALSE if negated)
                    if matches!(eq_result, types::SqlValue::Boolean(true)) {
                        return Ok(types::SqlValue::Boolean(!negated));
                    }
                }

                // No match found
                // If we encountered NULL, return NULL (per SQL three-valued logic)
                // Otherwise return FALSE (or TRUE if negated)
                if found_null {
                    Ok(types::SqlValue::Null)
                } else {
                    Ok(types::SqlValue::Boolean(*negated))
                }
            }

            // EXISTS predicate: EXISTS (SELECT ...)
            // SQL:1999 Section 8.7: EXISTS predicate
            // Returns TRUE if subquery returns at least one row
            // Returns FALSE if subquery returns zero rows
            // Never returns NULL (unlike most predicates)
            ast::Expression::Exists { subquery, negated } => {
                let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
                    "EXISTS requires database reference".to_string(),
                ))?;

                // Execute the subquery using SelectExecutor
                // Pass current row and schema as outer context for correlated subqueries
                let select_executor = crate::select::SelectExecutor::new_with_outer_context(
                    database,
                    row,
                    self.schema,
                );
                let rows = select_executor.execute(subquery)?;

                // Check if subquery returned any rows
                let has_rows = !rows.is_empty();

                // Apply negation if needed
                let result = if *negated { !has_rows } else { has_rows };

                Ok(types::SqlValue::Boolean(result))
            }

            // TODO: Implement other expression types
            _ => Err(ExecutorError::UnsupportedExpression(format!("{:?}", expr))),
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

            // Type mismatch
            _ => Err(ExecutorError::TypeMismatch {
                left: left.clone(),
                op: format!("{:?}", op),
                right: right.clone(),
            }),
        }
    }

    /// Evaluate CASE expression
    fn eval_case(
        &self,
        operand: &Option<Box<ast::Expression>>,
        when_clauses: &[(ast::Expression, ast::Expression)],
        else_result: &Option<Box<ast::Expression>>,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        match operand {
            // Simple CASE: CASE operand WHEN value THEN result ...
            Some(operand_expr) => {
                let operand_value = self.eval(operand_expr, row)?;

                // Iterate through WHEN clauses
                for (when_value_expr, then_result_expr) in when_clauses {
                    let when_value = self.eval(when_value_expr, row)?;

                    // Compare operand to when_value using SQL equality semantics
                    // IMPORTANT: Use IS NOT DISTINCT FROM for NULL-safe comparison
                    if Self::values_are_equal(&operand_value, &when_value) {
                        return self.eval(then_result_expr, row);
                    }
                }
            }

            // Searched CASE: CASE WHEN condition THEN result ...
            None => {
                // Iterate through WHEN clauses
                for (when_condition_expr, then_result_expr) in when_clauses {
                    let condition_result = self.eval(when_condition_expr, row)?;

                    // Check if condition is TRUE (not just truthy)
                    if matches!(condition_result, types::SqlValue::Boolean(true)) {
                        return self.eval(then_result_expr, row);
                    }
                    // Note: NULL and FALSE both skip this branch
                }
            }
        }

        // No WHEN matched, evaluate ELSE or return NULL
        match else_result {
            Some(else_expr) => self.eval(else_expr, row),
            None => Ok(types::SqlValue::Null),
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
        CombinedExpressionEvaluator { schema, database: None }
    }

    /// Create a new combined expression evaluator with database reference
    pub(crate) fn with_database(
        schema: &'a CombinedSchema,
        database: &'a storage::Database,
    ) -> Self {
        CombinedExpressionEvaluator { schema, database: Some(database) }
    }

    /// Evaluate an expression in the context of a combined row
    pub(crate) fn eval(
        &self,
        expr: &ast::Expression,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        match expr {
            // Literals - just return the value
            ast::Expression::Literal(val) => Ok(val.clone()),

            // Column reference - look up column index (with optional table qualifier)
            ast::Expression::ColumnRef { table, column } => {
                let col_index = self
                    .schema
                    .get_column_index(table.as_deref(), column)
                    .ok_or_else(|| ExecutorError::ColumnNotFound(column.clone()))?;
                row.get(col_index)
                    .cloned()
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index })
            }

            // Binary operations
            ast::Expression::BinaryOp { left, op, right } => {
                let left_val = self.eval(left, row)?;
                let right_val = self.eval(right, row)?;
                ExpressionEvaluator::eval_binary_op_static(&left_val, op, &right_val)
            }

            // CASE expression
            ast::Expression::Case { operand, when_clauses, else_result } => {
                self.eval_case(operand, when_clauses, else_result, row)
            }

            // IN operator with subquery
            ast::Expression::In { expr, subquery: _, negated: _ } => {
                // TODO: Full implementation requires database access to execute subquery
                // This requires refactoring CombinedExpressionEvaluator to have database reference
                // For now, evaluate the left expression to ensure it's valid
                let _left_val = self.eval(expr, row)?;
                Err(ExecutorError::UnsupportedFeature(
                    "IN with subquery requires database access - implementation pending"
                        .to_string(),
                ))
            }

            // Scalar subquery - must return exactly one row and one column
            ast::Expression::ScalarSubquery(subquery) => {
                let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
                    "Subquery execution requires database reference".to_string(),
                ))?;

                // Execute the subquery using SelectExecutor
                let select_executor = crate::select::SelectExecutor::new(database);
                let rows = select_executor.execute(subquery)?;

                // SQL:1999 Section 7.9: Scalar subquery must return exactly 1 row
                if rows.len() > 1 {
                    return Err(ExecutorError::SubqueryReturnedMultipleRows {
                        expected: 1,
                        actual: rows.len(),
                    });
                }

                // SQL:1999 Section 7.9: Scalar subquery must return exactly 1 column
                // Check column count from SELECT list
                if subquery.select_list.len() != 1 {
                    return Err(ExecutorError::SubqueryColumnCountMismatch {
                        expected: 1,
                        actual: subquery.select_list.len(),
                    });
                }

                // Return the single value, or NULL if no rows
                if rows.is_empty() {
                    Ok(types::SqlValue::Null)
                } else {
                    rows[0]
                        .get(0)
                        .cloned()
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })
                }
            }

            // BETWEEN predicate: expr BETWEEN low AND high
            // Equivalent to: expr >= low AND expr <= high
            // If negated: expr < low OR expr > high
            ast::Expression::Between { expr, low, high, negated } => {
                let expr_val = self.eval(expr, row)?;
                let low_val = self.eval(low, row)?;
                let high_val = self.eval(high, row)?;

                // Check if expr >= low
                let ge_low = ExpressionEvaluator::eval_binary_op_static(
                    &expr_val,
                    &ast::BinaryOperator::GreaterThanOrEqual,
                    &low_val,
                )?;

                // Check if expr <= high
                let le_high = ExpressionEvaluator::eval_binary_op_static(
                    &expr_val,
                    &ast::BinaryOperator::LessThanOrEqual,
                    &high_val,
                )?;

                // Combine with AND/OR depending on negated
                if *negated {
                    // NOT BETWEEN: expr < low OR expr > high
                    let lt_low = ExpressionEvaluator::eval_binary_op_static(
                        &expr_val,
                        &ast::BinaryOperator::LessThan,
                        &low_val,
                    )?;
                    let gt_high = ExpressionEvaluator::eval_binary_op_static(
                        &expr_val,
                        &ast::BinaryOperator::GreaterThan,
                        &high_val,
                    )?;
                    ExpressionEvaluator::eval_binary_op_static(&lt_low, &ast::BinaryOperator::Or, &gt_high)
                } else {
                    // BETWEEN: expr >= low AND expr <= high
                    ExpressionEvaluator::eval_binary_op_static(&ge_low, &ast::BinaryOperator::And, &le_high)
                }
            }

            // CAST expression: CAST(expr AS data_type)
            // Explicit type conversion
            ast::Expression::Cast { expr, data_type } => {
                let value = self.eval(expr, row)?;
                cast_value(&value, data_type)
            }

            // LIKE pattern matching: expr LIKE pattern
            // Supports wildcards: % (any chars), _ (single char)
            ast::Expression::Like { expr, pattern, negated } => {
                let expr_val = self.eval(expr, row)?;
                let pattern_val = self.eval(pattern, row)?;

                // Extract string values
                let text = match expr_val {
                    types::SqlValue::Varchar(ref s) | types::SqlValue::Character(ref s) => s.clone(),
                    types::SqlValue::Null => return Ok(types::SqlValue::Null),
                    _ => {
                        return Err(ExecutorError::TypeMismatch {
                            left: expr_val,
                            op: "LIKE".to_string(),
                            right: pattern_val,
                        })
                    }
                };

                let pattern_str = match pattern_val {
                    types::SqlValue::Varchar(ref s) | types::SqlValue::Character(ref s) => s.clone(),
                    types::SqlValue::Null => return Ok(types::SqlValue::Null),
                    _ => {
                        return Err(ExecutorError::TypeMismatch {
                            left: expr_val,
                            op: "LIKE".to_string(),
                            right: pattern_val,
                        })
                    }
                };

                // Perform pattern matching
                let matches = like_match(&text, &pattern_str);

                // Apply negation if needed
                let result = if *negated { !matches } else { matches };

                Ok(types::SqlValue::Boolean(result))
            }

            // IN operator with value list: expr IN (val1, val2, ...)
            // SQL:1999 Section 8.4: IN predicate
            // Returns TRUE if expr equals any value in the list
            // Returns FALSE if no match and no NULLs
            // Returns NULL if no match and list contains NULL
            ast::Expression::InList { expr, values, negated } => {
                let expr_val = self.eval(expr, row)?;

                // If left expression is NULL, result is NULL
                if matches!(expr_val, types::SqlValue::Null) {
                    return Ok(types::SqlValue::Null);
                }

                let mut found_null = false;

                // Check each value in the list
                for value_expr in values {
                    let value = self.eval(value_expr, row)?;

                    // Track if we encounter NULL
                    if matches!(value, types::SqlValue::Null) {
                        found_null = true;
                        continue;
                    }

                    // Compare using equality
                    let eq_result = ExpressionEvaluator::eval_binary_op_static(&expr_val, &ast::BinaryOperator::Equal, &value)?;

                    // If we found a match, return TRUE (or FALSE if negated)
                    if matches!(eq_result, types::SqlValue::Boolean(true)) {
                        return Ok(types::SqlValue::Boolean(!negated));
                    }
                }

                // No match found
                // If we encountered NULL, return NULL (per SQL three-valued logic)
                // Otherwise return FALSE (or TRUE if negated)
                if found_null {
                    Ok(types::SqlValue::Null)
                } else {
                    Ok(types::SqlValue::Boolean(*negated))
                }
            }

            // EXISTS predicate: EXISTS (SELECT ...)
            // SQL:1999 Section 8.7: EXISTS predicate
            // Returns TRUE if subquery returns at least one row
            // Returns FALSE if subquery returns zero rows
            // Never returns NULL (unlike most predicates)
            ast::Expression::Exists { subquery, negated } => {
                let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
                    "EXISTS requires database reference".to_string(),
                ))?;

                // Execute the subquery using SelectExecutor
                let select_executor = crate::select::SelectExecutor::new(database);
                let rows = select_executor.execute(subquery)?;

                // Check if subquery returned any rows
                let has_rows = !rows.is_empty();

                // Apply negation if needed
                let result = if *negated { !has_rows } else { has_rows };

                Ok(types::SqlValue::Boolean(result))
            }

            // TODO: Implement other expression types
            _ => Err(ExecutorError::UnsupportedExpression(format!("{:?}", expr))),
        }
    }

    /// Evaluate CASE expression
    fn eval_case(
        &self,
        operand: &Option<Box<ast::Expression>>,
        when_clauses: &[(ast::Expression, ast::Expression)],
        else_result: &Option<Box<ast::Expression>>,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        match operand {
            // Simple CASE: CASE operand WHEN value THEN result ...
            Some(operand_expr) => {
                let operand_value = self.eval(operand_expr, row)?;

                // Iterate through WHEN clauses
                for (when_value_expr, then_result_expr) in when_clauses {
                    let when_value = self.eval(when_value_expr, row)?;

                    // Compare operand to when_value using SQL equality semantics
                    if ExpressionEvaluator::values_are_equal(&operand_value, &when_value) {
                        return self.eval(then_result_expr, row);
                    }
                }
            }

            // Searched CASE: CASE WHEN condition THEN result ...
            None => {
                // Iterate through WHEN clauses
                for (when_condition_expr, then_result_expr) in when_clauses {
                    let condition_result = self.eval(when_condition_expr, row)?;

                    // Check if condition is TRUE (not just truthy)
                    if matches!(condition_result, types::SqlValue::Boolean(true)) {
                        return self.eval(then_result_expr, row);
                    }
                }
            }
        }

        // No WHEN matched, evaluate ELSE or return NULL
        match else_result {
            Some(else_expr) => self.eval(else_expr, row),
            None => Ok(types::SqlValue::Null),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::{ColumnSchema, TableSchema};
    use types::{DataType, SqlValue};

    #[test]
    fn test_evaluator_with_outer_context_resolves_inner_column() {
        // Create inner schema with "inner_col"
        let inner_schema = TableSchema::new(
            "inner".to_string(),
            vec![ColumnSchema::new("inner_col".to_string(), DataType::Integer, false)],
        );

        // Create outer schema with "outer_col"
        let outer_schema = TableSchema::new(
            "outer".to_string(),
            vec![ColumnSchema::new("outer_col".to_string(), DataType::Integer, false)],
        );

        let outer_row = storage::Row::new(vec![SqlValue::Integer(100)]);
        let inner_row = storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Should resolve inner_col from inner row
        let expr = ast::Expression::ColumnRef { table: None, column: "inner_col".to_string() };

        let result = evaluator.eval(&expr, &inner_row).unwrap();
        assert_eq!(result, SqlValue::Integer(42));
    }

    #[test]
    fn test_evaluator_with_outer_context_resolves_outer_column() {
        // Create inner schema with "inner_col"
        let inner_schema = TableSchema::new(
            "inner".to_string(),
            vec![ColumnSchema::new("inner_col".to_string(), DataType::Integer, false)],
        );

        // Create outer schema with "outer_col"
        let outer_schema = TableSchema::new(
            "outer".to_string(),
            vec![ColumnSchema::new("outer_col".to_string(), DataType::Integer, false)],
        );

        let outer_row = storage::Row::new(vec![SqlValue::Integer(100)]);
        let inner_row = storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Should resolve outer_col from outer row (not in inner schema)
        let expr = ast::Expression::ColumnRef { table: None, column: "outer_col".to_string() };

        let result = evaluator.eval(&expr, &inner_row).unwrap();
        assert_eq!(result, SqlValue::Integer(100));
    }

    #[test]
    fn test_evaluator_with_outer_context_inner_shadows_outer() {
        // Both schemas have "col" - inner should win
        let inner_schema = TableSchema::new(
            "inner".to_string(),
            vec![ColumnSchema::new("col".to_string(), DataType::Integer, false)],
        );

        let outer_schema = TableSchema::new(
            "outer".to_string(),
            vec![ColumnSchema::new("col".to_string(), DataType::Integer, false)],
        );

        let outer_row = storage::Row::new(vec![SqlValue::Integer(999)]);
        let inner_row = storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        let expr = ast::Expression::ColumnRef { table: None, column: "col".to_string() };

        let result = evaluator.eval(&expr, &inner_row).unwrap();
        // Should get inner value (42), not outer (999)
        assert_eq!(result, SqlValue::Integer(42));
    }

    #[test]
    fn test_evaluator_with_outer_context_column_not_found() {
        let inner_schema = TableSchema::new(
            "inner".to_string(),
            vec![ColumnSchema::new("inner_col".to_string(), DataType::Integer, false)],
        );

        let outer_schema = TableSchema::new(
            "outer".to_string(),
            vec![ColumnSchema::new("outer_col".to_string(), DataType::Integer, false)],
        );

        let outer_row = storage::Row::new(vec![SqlValue::Integer(100)]);
        let inner_row = storage::Row::new(vec![SqlValue::Integer(42)]);

        let evaluator =
            ExpressionEvaluator::with_outer_context(&inner_schema, &outer_row, &outer_schema);

        // Try to resolve non-existent column
        let expr = ast::Expression::ColumnRef { table: None, column: "nonexistent".to_string() };

        let result = evaluator.eval(&expr, &inner_row);
        assert!(matches!(result, Err(ExecutorError::ColumnNotFound(_))));
    }

    #[test]
    fn test_evaluator_without_outer_context() {
        // Normal evaluator without outer context
        let schema = TableSchema::new(
            "table".to_string(),
            vec![ColumnSchema::new("col".to_string(), DataType::Integer, false)],
        );

        let evaluator = ExpressionEvaluator::new(&schema);
        let row = storage::Row::new(vec![SqlValue::Integer(42)]);

        let expr = ast::Expression::ColumnRef { table: None, column: "col".to_string() };

        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Integer(42));
    }
}

// ========================================================================
// Type Coercion Helper Functions
// ========================================================================

/// Check if a value is an exact numeric type (SMALLINT, INTEGER, BIGINT)
fn is_exact_numeric(value: &types::SqlValue) -> bool {
    matches!(
        value,
        types::SqlValue::Smallint(_) | types::SqlValue::Integer(_) | types::SqlValue::Bigint(_)
    )
}

/// Check if a value is an approximate numeric type (FLOAT, REAL, DOUBLE)
fn is_approximate_numeric(value: &types::SqlValue) -> bool {
    matches!(
        value,
        types::SqlValue::Float(_) | types::SqlValue::Real(_) | types::SqlValue::Double(_)
    )
}

/// Convert exact numeric types to i64 for comparison
fn to_i64(value: &types::SqlValue) -> Result<i64, ExecutorError> {
    match value {
        types::SqlValue::Smallint(n) => Ok(*n as i64),
        types::SqlValue::Integer(n) => Ok(*n),
        types::SqlValue::Bigint(n) => Ok(*n),
        _ => Err(ExecutorError::TypeMismatch {
            left: value.clone(),
            op: "numeric_conversion".to_string(),
            right: types::SqlValue::Null,
        }),
    }
}

/// Convert approximate numeric types to f64 for comparison
fn to_f64(value: &types::SqlValue) -> Result<f64, ExecutorError> {
    match value {
        types::SqlValue::Float(n) => Ok(*n as f64),
        types::SqlValue::Real(n) => Ok(*n as f64),
        types::SqlValue::Double(n) => Ok(*n),
        _ => Err(ExecutorError::TypeMismatch {
            left: value.clone(),
            op: "numeric_conversion".to_string(),
            right: types::SqlValue::Null,
        }),
    }
}

/// Cast a value to the target data type
/// Implements SQL:1999 CAST semantics for explicit type conversion
fn cast_value(
    value: &types::SqlValue,
    target_type: &types::DataType,
) -> Result<types::SqlValue, ExecutorError> {
    use types::DataType::*;
    use types::SqlValue;

    // NULL can be cast to any type and remains NULL
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    match target_type {
        // Cast to INTEGER
        Integer => match value {
            SqlValue::Integer(n) => Ok(SqlValue::Integer(*n)),
            SqlValue::Smallint(n) => Ok(SqlValue::Integer(*n as i64)),
            SqlValue::Bigint(n) => Ok(SqlValue::Integer(*n)),
            SqlValue::Varchar(s) => s.parse::<i64>().map(SqlValue::Integer).map_err(|_| {
                ExecutorError::CastError {
                    from_type: format!("{:?}", value),
                    to_type: "INTEGER".to_string(),
                }
            }),
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "INTEGER".to_string(),
            }),
        },

        // Cast to SMALLINT
        Smallint => match value {
            SqlValue::Smallint(n) => Ok(SqlValue::Smallint(*n)),
            SqlValue::Integer(n) => Ok(SqlValue::Smallint(*n as i16)),
            SqlValue::Bigint(n) => Ok(SqlValue::Smallint(*n as i16)),
            SqlValue::Varchar(s) => s.parse::<i16>().map(SqlValue::Smallint).map_err(|_| {
                ExecutorError::CastError {
                    from_type: format!("{:?}", value),
                    to_type: "SMALLINT".to_string(),
                }
            }),
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "SMALLINT".to_string(),
            }),
        },

        // Cast to BIGINT
        Bigint => match value {
            SqlValue::Bigint(n) => Ok(SqlValue::Bigint(*n)),
            SqlValue::Integer(n) => Ok(SqlValue::Bigint(*n)),
            SqlValue::Smallint(n) => Ok(SqlValue::Bigint(*n as i64)),
            SqlValue::Varchar(s) => s.parse::<i64>().map(SqlValue::Bigint).map_err(|_| {
                ExecutorError::CastError {
                    from_type: format!("{:?}", value),
                    to_type: "BIGINT".to_string(),
                }
            }),
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "BIGINT".to_string(),
            }),
        },

        // Cast to FLOAT
        Float => match value {
            SqlValue::Float(n) => Ok(SqlValue::Float(*n)),
            SqlValue::Real(n) => Ok(SqlValue::Float(*n)),
            SqlValue::Double(n) => Ok(SqlValue::Float(*n as f32)),
            SqlValue::Integer(n) => Ok(SqlValue::Float(*n as f32)),
            SqlValue::Smallint(n) => Ok(SqlValue::Float(*n as f32)),
            SqlValue::Bigint(n) => Ok(SqlValue::Float(*n as f32)),
            SqlValue::Varchar(s) => s.parse::<f32>().map(SqlValue::Float).map_err(|_| {
                ExecutorError::CastError {
                    from_type: format!("{:?}", value),
                    to_type: "FLOAT".to_string(),
                }
            }),
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "FLOAT".to_string(),
            }),
        },

        // Cast to DOUBLE PRECISION
        DoublePrecision => match value {
            SqlValue::Double(n) => Ok(SqlValue::Double(*n)),
            SqlValue::Float(n) => Ok(SqlValue::Double(*n as f64)),
            SqlValue::Real(n) => Ok(SqlValue::Double(*n as f64)),
            SqlValue::Integer(n) => Ok(SqlValue::Double(*n as f64)),
            SqlValue::Smallint(n) => Ok(SqlValue::Double(*n as f64)),
            SqlValue::Bigint(n) => Ok(SqlValue::Double(*n as f64)),
            SqlValue::Varchar(s) => s.parse::<f64>().map(SqlValue::Double).map_err(|_| {
                ExecutorError::CastError {
                    from_type: format!("{:?}", value),
                    to_type: "DOUBLE PRECISION".to_string(),
                }
            }),
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "DOUBLE PRECISION".to_string(),
            }),
        },

        // Cast to VARCHAR
        Varchar { max_length } => {
            let string_val = match value {
                SqlValue::Varchar(s) => s.clone(),
                SqlValue::Integer(n) => n.to_string(),
                SqlValue::Smallint(n) => n.to_string(),
                SqlValue::Bigint(n) => n.to_string(),
                SqlValue::Float(n) => n.to_string(),
                SqlValue::Real(n) => n.to_string(),
                SqlValue::Double(n) => n.to_string(),
                SqlValue::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
                SqlValue::Date(s) => s.clone(),
                SqlValue::Time(s) => s.clone(),
                SqlValue::Timestamp(s) => s.clone(),
                _ => {
                    return Err(ExecutorError::CastError {
                        from_type: format!("{:?}", value),
                        to_type: format!("VARCHAR({})", max_length),
                    })
                }
            };

            // Truncate if exceeds max_length
            if string_val.len() > *max_length {
                Ok(SqlValue::Varchar(string_val[..*max_length].to_string()))
            } else {
                Ok(SqlValue::Varchar(string_val))
            }
        }

        // Cast to DATE
        Date => match value {
            SqlValue::Date(s) => Ok(SqlValue::Date(s.clone())),
            SqlValue::Timestamp(s) => {
                // Extract date part from timestamp (YYYY-MM-DD)
                if let Some(date_part) = s.split_whitespace().next() {
                    Ok(SqlValue::Date(date_part.to_string()))
                } else {
                    Ok(SqlValue::Date(s.clone()))
                }
            }
            SqlValue::Varchar(s) => Ok(SqlValue::Date(s.clone())),
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "DATE".to_string(),
            }),
        },

        // Cast to TIME
        Time { .. } => match value {
            SqlValue::Time(s) => Ok(SqlValue::Time(s.clone())),
            SqlValue::Timestamp(s) => {
                // Extract time part from timestamp (HH:MM:SS)
                if let Some(time_part) = s.split_whitespace().nth(1) {
                    Ok(SqlValue::Time(time_part.to_string()))
                } else {
                    Ok(SqlValue::Time(s.clone()))
                }
            }
            SqlValue::Varchar(s) => Ok(SqlValue::Time(s.clone())),
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "TIME".to_string(),
            }),
        },

        // Cast to TIMESTAMP
        Timestamp { .. } => match value {
            SqlValue::Timestamp(s) => Ok(SqlValue::Timestamp(s.clone())),
            SqlValue::Date(s) => Ok(SqlValue::Timestamp(format!("{} 00:00:00", s))),
            SqlValue::Varchar(s) => Ok(SqlValue::Timestamp(s.clone())),
            _ => Err(ExecutorError::CastError {
                from_type: format!("{:?}", value),
                to_type: "TIMESTAMP".to_string(),
            }),
        },

        // Unsupported target types
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "CAST to {:?} not yet implemented",
            target_type
        ))),
    }
}

/// SQL LIKE pattern matching
/// Supports wildcards:
/// - % matches any sequence of characters (including empty)
/// - _ matches exactly one character
///
/// This is a case-sensitive match following SQL:1999 semantics
fn like_match(text: &str, pattern: &str) -> bool {
    like_match_recursive(text.as_bytes(), pattern.as_bytes(), 0, 0)
}

/// Recursive helper for LIKE pattern matching
fn like_match_recursive(text: &[u8], pattern: &[u8], text_pos: usize, pattern_pos: usize) -> bool {
    // If we've consumed the entire pattern
    if pattern_pos >= pattern.len() {
        // Match succeeds if we've also consumed all of text
        return text_pos >= text.len();
    }

    let pattern_char = pattern[pattern_pos];

    match pattern_char {
        b'%' => {
            // % matches zero or more characters
            // Try matching with % consuming 0 chars, 1 char, 2 chars, etc.
            for skip in 0..=(text.len() - text_pos) {
                if like_match_recursive(text, pattern, text_pos + skip, pattern_pos + 1) {
                    return true;
                }
            }
            false
        }
        b'_' => {
            // _ matches exactly one character
            if text_pos >= text.len() {
                // No character left to match
                return false;
            }
            // Skip one character in text and one in pattern
            like_match_recursive(text, pattern, text_pos + 1, pattern_pos + 1)
        }
        _ => {
            // Regular character must match exactly
            if text_pos >= text.len() {
                // No character left in text
                return false;
            }
            if text[text_pos] != pattern_char {
                // Characters don't match
                return false;
            }
            // Characters match, continue
            like_match_recursive(text, pattern, text_pos + 1, pattern_pos + 1)
        }
    }
}
