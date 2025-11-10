//! Subquery evaluation methods

use super::super::core::ExpressionEvaluator;
use crate::errors::ExecutorError;

impl ExpressionEvaluator<'_> {
    /// Evaluate IN subquery predicate
    pub(super) fn eval_in_subquery(
        &self,
        expr: &ast::Expression,
        subquery: &ast::SelectStmt,
        negated: bool,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
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

        if matches!(expr_val, types::SqlValue::Null) {
            return Ok(types::SqlValue::Null);
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

            if matches!(subquery_val, types::SqlValue::Null) {
                found_null = true;
                continue;
            }

            if expr_val == *subquery_val {
                return Ok(types::SqlValue::Boolean(!negated));
            }
        }

        if found_null {
            Ok(types::SqlValue::Null)
        } else {
            Ok(types::SqlValue::Boolean(negated))
        }
    }

    /// Evaluate scalar subquery
    pub(super) fn eval_scalar_subquery(
        &self,
        subquery: &ast::SelectStmt,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "Subquery execution requires database reference".to_string(),
        ))?;

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

        if rows.len() > 1 {
            return Err(ExecutorError::SubqueryReturnedMultipleRows {
                expected: 1,
                actual: rows.len(),
            });
        }

        if subquery.select_list.len() != 1 {
            return Err(ExecutorError::SubqueryColumnCountMismatch {
                expected: 1,
                actual: subquery.select_list.len(),
            });
        }

        if rows.is_empty() {
            Ok(types::SqlValue::Null)
        } else {
            rows[0].get(0).cloned().ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })
        }
    }

    /// Evaluate EXISTS predicate
    pub(super) fn eval_exists(
        &self,
        subquery: &ast::SelectStmt,
        negated: bool,
        row: &storage::Row,
    ) -> Result<types::SqlValue, ExecutorError> {
        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "EXISTS requires database reference".to_string(),
        ))?;

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

        let has_rows = !rows.is_empty();
        let result = if negated { !has_rows } else { has_rows };

        Ok(types::SqlValue::Boolean(result))
    }

    /// Evaluate quantified comparison (ALL/ANY/SOME)
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

        let left_val = self.eval(expr, row)?;

        if matches!(left_val, types::SqlValue::Null) {
            return Ok(types::SqlValue::Null);
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

        if rows.is_empty() {
            return Ok(types::SqlValue::Boolean(matches!(quantifier, ast::Quantifier::All)));
        }

        let mut has_null = false;

        match quantifier {
            ast::Quantifier::All => {
                for subquery_row in &rows {
                    if subquery_row.values.len() != 1 {
                        return Err(ExecutorError::SubqueryColumnCountMismatch {
                            expected: 1,
                            actual: subquery_row.values.len(),
                        });
                    }

                    let right_val = &subquery_row.values[0];

                    if matches!(right_val, types::SqlValue::Null) {
                        has_null = true;
                        continue;
                    }

                    let cmp_result = self.eval_binary_op(&left_val, op, right_val)?;

                    match cmp_result {
                        types::SqlValue::Boolean(false) => {
                            return Ok(types::SqlValue::Boolean(false))
                        }
                        types::SqlValue::Null => has_null = true,
                        _ => {}
                    }
                }

                if has_null {
                    Ok(types::SqlValue::Null)
                } else {
                    Ok(types::SqlValue::Boolean(true))
                }
            }

            ast::Quantifier::Any | ast::Quantifier::Some => {
                for subquery_row in &rows {
                    if subquery_row.values.len() != 1 {
                        return Err(ExecutorError::SubqueryColumnCountMismatch {
                            expected: 1,
                            actual: subquery_row.values.len(),
                        });
                    }

                    let right_val = &subquery_row.values[0];

                    if matches!(right_val, types::SqlValue::Null) {
                        has_null = true;
                        continue;
                    }

                    let cmp_result = self.eval_binary_op(&left_val, op, right_val)?;

                    match cmp_result {
                        types::SqlValue::Boolean(true) => {
                            return Ok(types::SqlValue::Boolean(true))
                        }
                        types::SqlValue::Null => has_null = true,
                        _ => {}
                    }
                }

                if has_null {
                    Ok(types::SqlValue::Null)
                } else {
                    Ok(types::SqlValue::Boolean(false))
                }
            }
        }
    }
}
