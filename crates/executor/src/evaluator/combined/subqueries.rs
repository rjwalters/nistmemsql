///! Subquery evaluation for combined expressions

use crate::errors::ExecutorError;
use super::super::core::{CombinedExpressionEvaluator, ExpressionEvaluator};

impl<'a> CombinedExpressionEvaluator<'a> {
    /// Evaluate scalar subquery - must return exactly one row and one column
    pub(super) fn eval_scalar_subquery(
        &self,
        subquery: &ast::SelectStmt,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "Subquery execution requires database reference".to_string(),
        ))?;

        // Execute the subquery with outer context for correlated subqueries
        // Pass the entire CombinedSchema to preserve alias information
        let select_executor = if self.schema.table_schemas.len() >= 1 {
            eprintln!("DEBUG eval_scalar_subquery: Passing outer context with tables {:?}",
                     self.schema.table_schemas.keys().collect::<Vec<_>>());
            crate::select::SelectExecutor::new_with_outer_context(database, row, self.schema)
        } else {
            eprintln!("DEBUG eval_scalar_subquery: No outer context (no tables)");
            crate::select::SelectExecutor::new(database)
        };
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

    /// Evaluate EXISTS predicate: EXISTS (SELECT ...)
    /// SQL:1999 Section 8.7: EXISTS predicate
    /// Returns TRUE if subquery returns at least one row
    /// Returns FALSE if subquery returns zero rows
    /// Never returns NULL (unlike most predicates)
    pub(super) fn eval_exists(
        &self,
        subquery: &ast::SelectStmt,
        negated: bool,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "EXISTS requires database reference".to_string(),
        ))?;

        // Execute the subquery with outer context
        let select_executor = if self.schema.table_schemas.len() >= 1 {
            crate::select::SelectExecutor::new_with_outer_context(database, row, self.schema)
        } else {
            crate::select::SelectExecutor::new(database)
        };
        let rows = select_executor.execute(subquery)?;

        // Check if subquery returned any rows
        let has_rows = !rows.is_empty();

        // Apply negation if needed
        let result = if negated { !has_rows } else { has_rows };

        Ok(types::SqlValue::Boolean(result))
    }

    /// Evaluate quantified comparison: expr op ALL/ANY/SOME (SELECT ...)
    /// SQL:1999 Section 8.8: Quantified comparison predicate
    /// ALL: comparison must be TRUE for all rows
    /// ANY/SOME: comparison must be TRUE for at least one row
    pub(super) fn eval_quantified(
        &self,
        expr: &ast::Expression,
        op: &ast::BinaryOperator,
        quantifier: &ast::Quantifier,
        subquery: &ast::SelectStmt,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "Quantified comparison requires database reference".to_string(),
        ))?;

        // Evaluate the left-hand expression
        let left_val = self.eval(expr, row)?;

        // Execute the subquery with outer context
        let select_executor = if self.schema.table_schemas.len() >= 1 {
            crate::select::SelectExecutor::new_with_outer_context(database, row, self.schema)
        } else {
            crate::select::SelectExecutor::new(database)
        };
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

    /// Evaluate IN operator with subquery
    /// SQL:1999 Section 8.4: IN predicate with subquery
    pub(super) fn eval_in_subquery(
        &self,
        expr: &ast::Expression,
        subquery: &ast::SelectStmt,
        negated: bool,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "IN with subquery requires database reference".to_string(),
        ))?;

        // Evaluate the left-hand expression
        let expr_val = self.eval(expr, row)?;

        // If left expression is NULL, result is NULL
        if matches!(expr_val, types::SqlValue::Null) {
            return Ok(types::SqlValue::Null);
        }

        // Subquery must return exactly one column
        if subquery.select_list.len() != 1 {
            return Err(ExecutorError::SubqueryColumnCountMismatch {
                expected: 1,
                actual: subquery.select_list.len(),
            });
        }

        // Execute the subquery with outer context
        let select_executor = if self.schema.table_schemas.len() >= 1 {
            crate::select::SelectExecutor::new_with_outer_context(database, row, self.schema)
        } else {
            crate::select::SelectExecutor::new(database)
        };
        let rows = select_executor.execute(subquery)?;

        let mut found_null = false;

        // Check each row from subquery
        for subquery_row in &rows {
            let subquery_val = subquery_row
                .get(0)
                .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })?;

            // Track if we encounter NULL
            if matches!(subquery_val, types::SqlValue::Null) {
                found_null = true;
                continue;
            }

            // Compare using equality
            let eq_result = ExpressionEvaluator::eval_binary_op_static(
                &expr_val,
                &ast::BinaryOperator::Equal,
                subquery_val,
            )?;

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
            Ok(types::SqlValue::Boolean(negated))
        }
    }
}
