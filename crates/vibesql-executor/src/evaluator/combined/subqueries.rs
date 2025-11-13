//! Subquery evaluation for combined expressions

use super::super::core::{CombinedExpressionEvaluator, ExpressionEvaluator};
use crate::errors::ExecutorError;

impl CombinedExpressionEvaluator<'_> {
    /// Evaluate scalar subquery - must return exactly one row and one column
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

        // Delegate to shared static implementation
        ExpressionEvaluator::eval_scalar_subquery_static(database, self.schema, row, subquery, self.depth)
    }

    /// Evaluate EXISTS predicate: EXISTS (SELECT ...)
    /// SQL:1999 Section 8.7: EXISTS predicate
    /// Returns TRUE if subquery returns at least one row
    /// Returns FALSE if subquery returns zero rows
    /// Never returns NULL (unlike most predicates)
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

        // Delegate to shared static implementation
        ExpressionEvaluator::eval_exists_static(database, self.schema, row, subquery, negated, self.depth)
    }

    /// Evaluate quantified comparison: expr op ALL/ANY/SOME (SELECT ...)
    /// SQL:1999 Section 8.8: Quantified comparison predicate
    /// ALL: comparison must be TRUE for all rows
    /// ANY/SOME: comparison must be TRUE for at least one row
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

        // Delegate to shared static implementation
        ExpressionEvaluator::eval_quantified_static(database, self.schema, row, &left_val, op, quantifier, subquery, self.depth)
    }

    /// Evaluate IN operator with subquery
    /// SQL:1999 Section 8.4: IN predicate with subquery
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

        // Subquery must return exactly one column
        if subquery.select_list.len() != 1 {
            return Err(ExecutorError::SubqueryColumnCountMismatch {
                expected: 1,
                actual: subquery.select_list.len(),
            });
        }

        // Execute the subquery with outer context and propagate depth
        let select_executor = if !self.schema.table_schemas.is_empty() {
            crate::select::SelectExecutor::new_with_outer_context_and_depth(
                database,
                row,
                self.schema,
                self.depth,
            )
        } else {
            crate::select::SelectExecutor::new(database)
        };
        let rows = select_executor.execute(subquery)?;

        // Empty set optimization (SQLite behavior):
        // If the subquery returns no rows, return FALSE for IN, TRUE for NOT IN
        // This is true regardless of whether the left expression is NULL
        // Rationale: No value can match an empty set
        if rows.is_empty() {
            return Ok(vibesql_types::SqlValue::Boolean(negated));
        }

        // Evaluate the left-hand expression
        let expr_val = self.eval(expr, row)?;

        // If left expression is NULL, result is NULL (per SQL three-valued logic)
        if matches!(expr_val, vibesql_types::SqlValue::Null) {
            return Ok(vibesql_types::SqlValue::Null);
        }

        let mut found_null = false;

        // Check each row from subquery
        for subquery_row in &rows {
            let subquery_val =
                subquery_row.get(0).ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })?;

            // Track if we encounter NULL
            if matches!(subquery_val, vibesql_types::SqlValue::Null) {
                found_null = true;
                continue;
            }

            // Compare using equality
            let eq_result = ExpressionEvaluator::eval_binary_op_static(
                &expr_val,
                &vibesql_ast::BinaryOperator::Equal,
                subquery_val,
            )?;

            // If we found a match, return TRUE (or FALSE if negated)
            if matches!(eq_result, vibesql_types::SqlValue::Boolean(true)) {
                return Ok(vibesql_types::SqlValue::Boolean(!negated));
            }
        }

        // No match found
        // If we encountered NULL, return NULL (per SQL three-valued logic)
        // Otherwise return FALSE (or TRUE if negated)
        if found_null {
            Ok(vibesql_types::SqlValue::Null)
        } else {
            Ok(vibesql_types::SqlValue::Boolean(negated))
        }
    }
}
