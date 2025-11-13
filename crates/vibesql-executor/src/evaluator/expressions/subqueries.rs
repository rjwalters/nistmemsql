//! Subquery evaluation methods

use super::super::core::ExpressionEvaluator;
use crate::errors::ExecutorError;

impl ExpressionEvaluator<'_> {
    /// Evaluate IN subquery predicate
    pub(super) fn eval_in_subquery(
        &self,
        expr: &vibesql_ast::Expression,
        subquery: &vibesql_ast::SelectStmt,
        negated: bool,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Check depth limit to prevent stack overflow
        if self.depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "IN with subquery requires database reference".to_string(),
        ))?;

        let expr_val = self.eval(expr, row)?;

        if matches!(expr_val, vibesql_types::SqlValue::Null) {
            return Ok(vibesql_types::SqlValue::Null);
        }

        if subquery.select_list.len() != 1 {
            return Err(ExecutorError::SubqueryColumnCountMismatch {
                expected: 1,
                actual: subquery.select_list.len(),
            });
        }

        // Convert TableSchema to CombinedSchema for outer context
        let outer_combined = crate::schema::CombinedSchema::from_table(
            self.schema.name.clone(),
            self.schema.clone(),
        );
        let select_executor = crate::select::SelectExecutor::new_with_outer_context_and_depth(
            database,
            row,
            &outer_combined,
            self.depth,
        );
        let rows = select_executor.execute(subquery)?;

        let mut found_null = false;
        for subquery_row in &rows {
            let subquery_val =
                subquery_row.get(0).ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })?;

            if matches!(subquery_val, vibesql_types::SqlValue::Null) {
                found_null = true;
                continue;
            }

            if expr_val == *subquery_val {
                return Ok(vibesql_types::SqlValue::Boolean(!negated));
            }
        }

        if found_null {
            Ok(vibesql_types::SqlValue::Null)
        } else {
            Ok(vibesql_types::SqlValue::Boolean(negated))
        }
    }

    /// Evaluate scalar subquery
    pub(super) fn eval_scalar_subquery(
        &self,
        subquery: &vibesql_ast::SelectStmt,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Check depth limit to prevent stack overflow
        if self.depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "Subquery execution requires database reference".to_string(),
        ))?;

        // Convert TableSchema to CombinedSchema for outer context
        let outer_combined = crate::schema::CombinedSchema::from_table(
            self.schema.name.clone(),
            self.schema.clone(),
        );

        Self::eval_scalar_subquery_static(database, &outer_combined, row, subquery, self.depth)
    }

    /// Static helper for scalar subquery evaluation - shared between both evaluators
    /// SQL:1999 Section 7.9: Scalar subquery must return exactly 1 row and 1 column
    pub(crate) fn eval_scalar_subquery_static(
        database: &vibesql_storage::Database,
        outer_schema: &crate::schema::CombinedSchema,
        row: &vibesql_storage::Row,
        subquery: &vibesql_ast::SelectStmt,
        depth: usize,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // SQL:1999 Section 7.9: Scalar subquery must return exactly 1 column
        // Check column count from SELECT list
        if subquery.select_list.len() != 1 {
            return Err(ExecutorError::SubqueryColumnCountMismatch {
                expected: 1,
                actual: subquery.select_list.len(),
            });
        }

        // Execute the subquery with outer context for correlated subqueries
        // Pass the CombinedSchema to preserve alias information and propagate depth
        let select_executor = if !outer_schema.table_schemas.is_empty() {
            crate::select::SelectExecutor::new_with_outer_context_and_depth(
                database,
                row,
                outer_schema,
                depth,
            )
        } else {
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

        // Return the single value, or NULL if no rows
        if rows.is_empty() {
            Ok(vibesql_types::SqlValue::Null)
        } else {
            rows[0].get(0).cloned().ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })
        }
    }

    /// Evaluate EXISTS predicate
    pub(super) fn eval_exists(
        &self,
        subquery: &vibesql_ast::SelectStmt,
        negated: bool,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Check depth limit to prevent stack overflow
        if self.depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "EXISTS requires database reference".to_string(),
        ))?;

        // Convert TableSchema to CombinedSchema for outer context
        let outer_combined = crate::schema::CombinedSchema::from_table(
            self.schema.name.clone(),
            self.schema.clone(),
        );

        Self::eval_exists_static(database, &outer_combined, row, subquery, negated, self.depth)
    }

    /// Static helper for EXISTS predicate evaluation - shared between both evaluators
    /// SQL:1999 Section 8.7: EXISTS predicate
    /// Returns TRUE if subquery returns at least one row
    /// Returns FALSE if subquery returns zero rows
    /// Never returns NULL (unlike most predicates)
    pub(crate) fn eval_exists_static(
        database: &vibesql_storage::Database,
        outer_schema: &crate::schema::CombinedSchema,
        row: &vibesql_storage::Row,
        subquery: &vibesql_ast::SelectStmt,
        negated: bool,
        depth: usize,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Execute the subquery with outer context and propagate depth
        let select_executor = if !outer_schema.table_schemas.is_empty() {
            crate::select::SelectExecutor::new_with_outer_context_and_depth(
                database,
                row,
                outer_schema,
                depth,
            )
        } else {
            crate::select::SelectExecutor::new(database)
        };
        let rows = select_executor.execute(subquery)?;

        // Check if subquery returned any rows
        let has_rows = !rows.is_empty();

        // Apply negation if needed
        let result = if negated { !has_rows } else { has_rows };

        Ok(vibesql_types::SqlValue::Boolean(result))
    }

    /// Evaluate quantified comparison (ALL/ANY/SOME)
    pub(super) fn eval_quantified(
        &self,
        expr: &vibesql_ast::Expression,
        op: &vibesql_ast::BinaryOperator,
        quantifier: &vibesql_ast::Quantifier,
        subquery: &vibesql_ast::SelectStmt,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Check depth limit to prevent stack overflow
        if self.depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "Quantified comparison requires database reference".to_string(),
        ))?;

        // Evaluate the left-hand expression
        let left_val = self.eval(expr, row)?;

        // Convert TableSchema to CombinedSchema for outer context
        let outer_combined = crate::schema::CombinedSchema::from_table(
            self.schema.name.clone(),
            self.schema.clone(),
        );

        Self::eval_quantified_static(database, &outer_combined, row, &left_val, op, quantifier, subquery, self.depth)
    }

    /// Static helper for quantified comparison evaluation - shared between both evaluators
    /// SQL:1999 Section 8.8: Quantified comparison predicate
    /// ALL: comparison must be TRUE for all rows
    /// ANY/SOME: comparison must be TRUE for at least one row
    pub(crate) fn eval_quantified_static(
        database: &vibesql_storage::Database,
        outer_schema: &crate::schema::CombinedSchema,
        row: &vibesql_storage::Row,
        left_val: &vibesql_types::SqlValue,
        op: &vibesql_ast::BinaryOperator,
        quantifier: &vibesql_ast::Quantifier,
        subquery: &vibesql_ast::SelectStmt,
        depth: usize,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Execute the subquery with outer context and propagate depth
        let select_executor = if !outer_schema.table_schemas.is_empty() {
            crate::select::SelectExecutor::new_with_outer_context_and_depth(
                database,
                row,
                outer_schema,
                depth,
            )
        } else {
            crate::select::SelectExecutor::new(database)
        };
        let rows = select_executor.execute(subquery)?;

        // Empty subquery special cases:
        // - ALL: returns TRUE (vacuously true - all zero rows satisfy the condition)
        // - ANY/SOME: returns FALSE (no rows to satisfy the condition)
        if rows.is_empty() {
            return Ok(vibesql_types::SqlValue::Boolean(matches!(quantifier, vibesql_ast::Quantifier::All)));
        }

        // If left value is NULL, result depends on quantifier and subquery results
        if matches!(left_val, vibesql_types::SqlValue::Null) {
            return Ok(vibesql_types::SqlValue::Null);
        }

        match quantifier {
            vibesql_ast::Quantifier::All => {
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
                    if matches!(right_val, vibesql_types::SqlValue::Null) {
                        has_null = true;
                        continue;
                    }

                    // Evaluate comparison
                    let cmp_result = Self::eval_binary_op_static(left_val, op, right_val)?;

                    match cmp_result {
                        vibesql_types::SqlValue::Boolean(false) => {
                            return Ok(vibesql_types::SqlValue::Boolean(false))
                        }
                        vibesql_types::SqlValue::Null => has_null = true,
                        _ => {} // TRUE, continue checking
                    }
                }

                // If we saw any NULLs (and no FALSEs), return NULL
                // Otherwise return TRUE (all comparisons were TRUE)
                if has_null {
                    Ok(vibesql_types::SqlValue::Null)
                } else {
                    Ok(vibesql_types::SqlValue::Boolean(true))
                }
            }

            vibesql_ast::Quantifier::Any | vibesql_ast::Quantifier::Some => {
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
                    if matches!(right_val, vibesql_types::SqlValue::Null) {
                        has_null = true;
                        continue;
                    }

                    // Evaluate comparison
                    let cmp_result = Self::eval_binary_op_static(left_val, op, right_val)?;

                    match cmp_result {
                        vibesql_types::SqlValue::Boolean(true) => {
                            return Ok(vibesql_types::SqlValue::Boolean(true))
                        }
                        vibesql_types::SqlValue::Null => has_null = true,
                        _ => {} // FALSE, continue checking
                    }
                }

                // If we saw any NULLs (and no TRUEs), return NULL
                // Otherwise return FALSE (no comparisons were TRUE)
                if has_null {
                    Ok(vibesql_types::SqlValue::Null)
                } else {
                    Ok(vibesql_types::SqlValue::Boolean(false))
                }
            }
        }
    }
}
