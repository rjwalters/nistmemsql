use crate::errors::ExecutorError;
use super::core::ExpressionEvaluator;
use super::casting::cast_value;
use super::functions::eval_scalar_function;
use super::pattern::like_match;

impl<'a> ExpressionEvaluator<'a> {
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
            // SQL:1999 Section 8.4: IN predicate
            // expr IN (SELECT ...) returns:
            // - TRUE if expr equals any value from subquery
            // - FALSE if subquery returns no rows or no match found
            // - NULL if expr is NULL or subquery contains NULL and no match
            ast::Expression::In { expr, subquery, negated } => {
                let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
                    "IN with subquery requires database reference".to_string(),
                ))?;

                // Evaluate the left expression
                let expr_val = self.eval(expr, row)?;

                // If left expression is NULL, result is NULL
                if matches!(expr_val, types::SqlValue::Null) {
                    return Ok(types::SqlValue::Null);
                }

                // IN subquery must return exactly one column
                if subquery.select_list.len() != 1 {
                    return Err(ExecutorError::SubqueryColumnCountMismatch {
                        expected: 1,
                        actual: subquery.select_list.len(),
                    });
                }

                // Execute the subquery using SelectExecutor
                // Pass current row and schema as outer context for correlated subqueries
                let select_executor = crate::select::SelectExecutor::new_with_outer_context(
                    database,
                    row,
                    self.schema,
                );
                let rows = select_executor.execute(subquery)?;

                // Check if expr_val matches any value from subquery result
                let mut found_null = false;
                for subquery_row in &rows {
                    // Get the first (and only) column value
                    let subquery_val = subquery_row
                        .get(0)
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })?;

                    // Track if we encounter NULL
                    if matches!(subquery_val, types::SqlValue::Null) {
                        found_null = true;
                        continue;
                    }

                    // Check for equality
                    if expr_val == *subquery_val {
                        // Found a match
                        return Ok(types::SqlValue::Boolean(!*negated));
                    }
                }

                // No match found
                if found_null {
                    // Subquery contained NULL, result is NULL
                    Ok(types::SqlValue::Null)
                } else {
                    // No NULLs, result is FALSE (or TRUE if negated)
                    Ok(types::SqlValue::Boolean(*negated))
                }
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

            // Quantified comparison: expr op ALL/ANY/SOME (SELECT ...)
            // SQL:1999 Section 8.8: Quantified comparison predicate
            // ALL: comparison must be TRUE for all rows
            // ANY/SOME: comparison must be TRUE for at least one row
            ast::Expression::QuantifiedComparison { expr, op, quantifier, subquery } => {
                let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
                    "Quantified comparison requires database reference".to_string(),
                ))?;

                // Evaluate the left-hand expression
                let left_val = self.eval(expr, row)?;

                // Execute the subquery using SelectExecutor
                let select_executor = crate::select::SelectExecutor::new_with_outer_context(
                    database,
                    row,
                    self.schema,
                );
                let rows = select_executor.execute(subquery)?;

                // Empty subquery special cases:
                // - ALL: returns TRUE (vacuously true - all zero rows satisfy the condition)
                // - ANY/SOME: returns FALSE (no rows to satisfy the condition)
                if rows.is_empty() {
                    return Ok(types::SqlValue::Boolean(matches!(quantifier, ast::Quantifier::All)));
                }

                // If left value is NULL, result depends on quantifier and subquery results
                if matches!(left_val, types::SqlValue::Null) {
                    return Ok(types::SqlValue::Null);
                }

                match quantifier {
                    ast::Quantifier::All => {
                        // ALL: comparison must be TRUE for all rows
                        // If any comparison is FALSE, return FALSE
                        // If any comparison is NULL (and none FALSE), return NULL
                        let mut has_null = false;

                        for subquery_row in &rows {
                            if subquery_row.values.len() != 1 {
                                return Err(ExecutorError::SubqueryColumnCountMismatch {
                                    expected: 1,
                                    actual: subquery_row.values.len(),
                                });
                            }

                            let right_val = &subquery_row.values[0];

                            // Handle NULL in subquery result
                            if matches!(right_val, types::SqlValue::Null) {
                                has_null = true;
                                continue;
                            }

                            // Evaluate comparison
                            let cmp_result = self.eval_binary_op(&left_val, op, right_val)?;

                            match cmp_result {
                                types::SqlValue::Boolean(false) => return Ok(types::SqlValue::Boolean(false)),
                                types::SqlValue::Null => has_null = true,
                                _ => {} // TRUE, continue checking
                            }
                        }

                        // If we saw any NULLs (and no FALSEs), return NULL
                        // Otherwise return TRUE (all comparisons were TRUE)
                        if has_null {
                            Ok(types::SqlValue::Null)
                        } else {
                            Ok(types::SqlValue::Boolean(true))
                        }
                    }

                    ast::Quantifier::Any | ast::Quantifier::Some => {
                        // ANY/SOME: comparison must be TRUE for at least one row
                        // If any comparison is TRUE, return TRUE
                        // If all comparisons are FALSE, return FALSE
                        // If any comparison is NULL (and none TRUE), return NULL
                        let mut has_null = false;

                        for subquery_row in &rows {
                            if subquery_row.values.len() != 1 {
                                return Err(ExecutorError::SubqueryColumnCountMismatch {
                                    expected: 1,
                                    actual: subquery_row.values.len(),
                                });
                            }

                            let right_val = &subquery_row.values[0];

                            // Handle NULL in subquery result
                            if matches!(right_val, types::SqlValue::Null) {
                                has_null = true;
                                continue;
                            }

                            // Evaluate comparison
                            let cmp_result = self.eval_binary_op(&left_val, op, right_val)?;

                            match cmp_result {
                                types::SqlValue::Boolean(true) => return Ok(types::SqlValue::Boolean(true)),
                                types::SqlValue::Null => has_null = true,
                                _ => {} // FALSE, continue checking
                            }
                        }

                        // If we saw any NULLs (and no TRUEs), return NULL
                        // Otherwise return FALSE (no comparisons were TRUE)
                        if has_null {
                            Ok(types::SqlValue::Null)
                        } else {
                            Ok(types::SqlValue::Boolean(false))
                        }
                    }
                }
            }

            // Function expressions - handle scalar functions (not aggregates)
            // Aggregates (COUNT, SUM, etc.) are handled in SelectExecutor
            ast::Expression::Function { name, args } => {
                // Evaluate all arguments
                let mut arg_values = Vec::new();
                for arg in args {
                    arg_values.push(self.eval(arg, row)?);
                }

                // Call shared scalar function evaluator
                eval_scalar_function(name, &arg_values)
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
}
