use crate::errors::ExecutorError;
use super::core::{CombinedExpressionEvaluator, ExpressionEvaluator};
use super::casting::cast_value;
use super::functions::eval_scalar_function;
use super::pattern::like_match;

impl<'a> CombinedExpressionEvaluator<'a> {
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
                let select_executor = crate::select::SelectExecutor::new(database);
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
                            let cmp_result = ExpressionEvaluator::eval_binary_op_static(&left_val, op, right_val)?;

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
                            let cmp_result = ExpressionEvaluator::eval_binary_op_static(&left_val, op, right_val)?;

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

            // IS NULL / IS NOT NULL
            ast::Expression::IsNull { expr, negated } => {
                let value = self.eval(expr, row)?;
                let is_null = matches!(value, types::SqlValue::Null);
                let result = if *negated { !is_null } else { is_null };
                Ok(types::SqlValue::Boolean(result))
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
