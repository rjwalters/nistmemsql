//! Subquery evaluation for combined expressions

use super::super::core::{CombinedExpressionEvaluator, ExpressionEvaluator};
use crate::errors::ExecutorError;

/// Compute the number of columns in a SELECT statement's result
/// Handles wildcards by expanding them using table schemas from the database
fn compute_select_list_column_count(
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
) -> Result<usize, ExecutorError> {
    let mut count = 0;

    for item in &stmt.select_list {
        match item {
            vibesql_ast::SelectItem::Wildcard { .. } => {
                // Expand * to count all columns from all tables in FROM clause
                if let Some(from) = &stmt.from {
                    count += count_columns_in_from_clause(from, database)?;
                } else {
                    // SELECT * without FROM is an error (should be caught earlier)
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * requires FROM clause".to_string(),
                    ));
                }
            }
            vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                // Expand table.* to count columns from that specific table
                let tbl = database
                    .get_table(qualifier)
                    .ok_or_else(|| ExecutorError::TableNotFound(qualifier.clone()))?;
                count += tbl.schema.columns.len();
            }
            vibesql_ast::SelectItem::Expression { .. } => {
                // Each expression contributes one column
                count += 1;
            }
        }
    }

    Ok(count)
}

/// Count total columns in a FROM clause (handles joins and multiple tables)
fn count_columns_in_from_clause(
    from: &vibesql_ast::FromClause,
    database: &vibesql_storage::Database,
) -> Result<usize, ExecutorError> {
    match from {
        vibesql_ast::FromClause::Table { name, .. } => {
            let table = database
                .get_table(name)
                .ok_or_else(|| ExecutorError::TableNotFound(name.clone()))?;
            Ok(table.schema.columns.len())
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            let left_count = count_columns_in_from_clause(left, database)?;
            let right_count = count_columns_in_from_clause(right, database)?;
            Ok(left_count + right_count)
        }
        vibesql_ast::FromClause::Subquery { .. } => {
            // For subqueries in FROM, we'd need to execute them to know column count
            // This is complex, so for now we'll return an error
            // In practice, this case is rare in IN subqueries
            Err(ExecutorError::UnsupportedFeature(
                "Subqueries in FROM clause within IN predicates are not yet supported for schema validation".to_string(),
            ))
        }
    }
}

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

        // Execute the subquery with outer context for correlated subqueries
        // Pass the entire CombinedSchema to preserve alias information and propagate depth
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

        // Delegate to shared logic
        super::super::subqueries_shared::eval_scalar_subquery_core(&rows, subquery.select_list.len())
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

        // Delegate to shared logic
        Ok(super::super::subqueries_shared::eval_exists_core(!rows.is_empty(), negated))
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

        // Delegate to shared logic
        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or(vibesql_types::SqlMode::default());
        super::super::subqueries_shared::eval_quantified_core(
            &left_val,
            &rows,
            op,
            quantifier,
            |left, op, right| ExpressionEvaluator::eval_binary_op_static(left, op, right, sql_mode.clone()),
        )
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
        let sql_mode = database.sql_mode();

        // Evaluate the left-hand expression
        let expr_val = self.eval(expr, row)?;

        // Check if this subquery is non-correlated and can be cached
        // A subquery is non-correlated if we're not passing outer context to it
        let is_non_correlated = self.schema.table_schemas.is_empty();

        // For non-correlated subqueries, check cache first
        let rows = if is_non_correlated {
            // Compute hash of subquery for cache key
            let subquery_hash = crate::evaluator::expression_hash::ExpressionHasher::hash_select_stmt(subquery);

            // Check if result is already in cache
            if let Some(cached_rows) = self.subquery_cache.borrow().get(&subquery_hash) {
                // Cache hit! Reuse the cached result
                cached_rows.clone()
            } else {
                // Cache miss - execute the subquery
                let select_executor = crate::select::SelectExecutor::new(database);
                let result_rows = select_executor.execute(subquery)?;

                // Store in cache for future use
                self.subquery_cache.borrow_mut().insert(subquery_hash, result_rows.clone());

                result_rows
            }
        } else {
            // Correlated subquery - cannot cache, execute normally
            // Pass shared cache to child executor for nested subqueries
            let select_executor = crate::select::SelectExecutor::new_with_shared_cache(
                database,
                row,
                self.schema,
                self.depth,
                self.subquery_cache.clone(),
            );
            select_executor.execute(subquery)?
        };

        // SQL standard (R-35033-20570): The subquery must be a scalar subquery
        // (single column) when the left expression is not a row value expression.
        // We must validate this AFTER execution because wildcards like SELECT *
        // expand to multiple columns at runtime.
        //
        // Validation must occur even for empty result sets to catch schema errors.
        let column_count = if !rows.is_empty() {
            // Get column count from first row
            rows[0].values.len()
        } else {
            // For empty result sets, compute column count from SELECT list
            compute_select_list_column_count(subquery, database)?
        };

        if column_count != 1 {
            return Err(ExecutorError::SubqueryColumnCountMismatch {
                expected: 1,
                actual: column_count,
            });
        }

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
                sql_mode.clone(),
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
