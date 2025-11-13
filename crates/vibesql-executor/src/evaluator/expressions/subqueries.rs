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

        // SQL standard behavior for NULL IN (subquery):
        // - NULL IN (empty set) → FALSE (special case per R-52275-55503)
        // - NULL IN (non-empty set without NULL) → NULL (three-valued logic)
        // - NULL IN (set containing NULL) → NULL
        if matches!(expr_val, vibesql_types::SqlValue::Null) {
            // Special case: empty set always returns FALSE for IN, TRUE for NOT IN
            // This overrides the usual NULL behavior (R-52275-55503)
            if rows.is_empty() {
                return Ok(vibesql_types::SqlValue::Boolean(negated));
            }

            // For non-empty sets, check if subquery contains NULL
            for subquery_row in &rows {
                let subquery_val =
                    subquery_row.get(0).ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })?;

                if matches!(subquery_val, vibesql_types::SqlValue::Null) {
                    // NULL IN (set with NULL) → NULL
                    return Ok(vibesql_types::SqlValue::Null);
                }
            }

            // NULL IN (non-empty set without NULL) → NULL (not FALSE!)
            // This follows three-valued logic: NULL compared to any value is NULL
            return Ok(vibesql_types::SqlValue::Null);
        }

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

        // Execute the subquery with outer context for correlated subqueries
        let select_executor = if !outer_combined.table_schemas.is_empty() {
            crate::select::SelectExecutor::new_with_outer_context_and_depth(
                database,
                row,
                &outer_combined,
                self.depth,
            )
        } else {
            crate::select::SelectExecutor::new(database)
        };
        let rows = select_executor.execute(subquery)?;

        // Delegate to shared logic
        super::super::subqueries_shared::eval_scalar_subquery_core(&rows, subquery.select_list.len())
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

        // Execute the subquery with outer context and propagate depth
        let select_executor = if !outer_combined.table_schemas.is_empty() {
            crate::select::SelectExecutor::new_with_outer_context_and_depth(
                database,
                row,
                &outer_combined,
                self.depth,
            )
        } else {
            crate::select::SelectExecutor::new(database)
        };
        let rows = select_executor.execute(subquery)?;

        // Delegate to shared logic
        Ok(super::super::subqueries_shared::eval_exists_core(!rows.is_empty(), negated))
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

        // Execute the subquery with outer context and propagate depth
        let select_executor = if !outer_combined.table_schemas.is_empty() {
            crate::select::SelectExecutor::new_with_outer_context_and_depth(
                database,
                row,
                &outer_combined,
                self.depth,
            )
        } else {
            crate::select::SelectExecutor::new(database)
        };
        let rows = select_executor.execute(subquery)?;

        // Delegate to shared logic
        super::super::subqueries_shared::eval_quantified_core(
            &left_val,
            &rows,
            op,
            quantifier,
            Self::eval_binary_op_static,
        )
    }
}
