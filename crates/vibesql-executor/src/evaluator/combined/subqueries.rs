//! Subquery evaluation for combined expressions

use super::super::core::{CombinedExpressionEvaluator, ExpressionEvaluator};
use crate::errors::ExecutorError;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Compute a hash for a subquery to use as a cache key
///
/// # Implementation Note
///
/// Currently uses Debug format for hashing, which has trade-offs:
///
/// **Pros:**
/// - Simple and works with existing AST types
/// - Sufficient for typical queries in practice
/// - Hash collisions are rare
///
/// **Cons:**
/// - Fragile: Debug format could change with Rust versions
/// - Less efficient: Allocates string for each hash
/// - Not cryptographically secure (uses DefaultHasher)
///
/// **Future Improvement:**
/// Ideally, SelectStmt and child types should derive Hash for:
/// - Better performance (direct AST traversal)
/// - Stability (Hash trait is stable)
/// - Type safety (compiler-enforced consistency)
///
/// This requires adding Hash to ~15-20 AST types, which should be
/// done in a dedicated refactoring PR to minimize risk.
///
/// See: https://github.com/rjwalters/vibesql/issues/2137#hash-improvement
fn compute_subquery_hash(subquery: &vibesql_ast::SelectStmt) -> u64 {
    let mut hasher = DefaultHasher::new();
    // Use the debug format as a stable representation
    // This works because SelectStmt derives Debug and PartialEq
    format!("{:?}", subquery).hash(&mut hasher);
    hasher.finish()
}

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
        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
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
    ///
    /// # Implementation
    ///
    /// **Uncorrelated subqueries:** Uses HashSet caching for O(1) membership testing.
    /// The subquery is executed once, results cached as a HashSet, then reused across
    /// all row evaluations. For same-type comparisons, uses O(1) HashSet.contains().
    /// For cross-type comparisons (e.g., Integer vs Float), falls back to SQL equality
    /// semantics with type coercion.
    ///
    /// **Correlated subqueries:** Executes with outer context for each row (no caching).
    ///
    /// NULL handling follows SQL standard: NULL IN (empty) = FALSE, NULL IN (values) = NULL
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

        // Phase 3 optimization: Try index-aware execution for simple uncorrelated subqueries
        // Only applies to simple SELECT column FROM table WHERE ... queries
        // This is tried first because it's the fastest when applicable
        if can_use_index_for_in_subquery(subquery, database) {
            if let Some(index_result) = try_index_optimized_in_subquery(
                &expr_val,
                subquery,
                negated,
                database,
                sql_mode.clone(),
            )? {
                return Ok(index_result);
            }
            // If index optimization fails, fall through to caching/regular execution
        }

        // Check if this is a non-correlated subquery that can use HashSet cache
        let is_correlated = crate::correlation::is_correlated(subquery, self.schema);

        if !is_correlated {
            // Non-correlated subquery - use HashSet cache for O(1) lookups
            let cache_key = compute_subquery_hash(subquery);

            // Check if we have a cached value set
            let cached_values = self.in_value_cache.borrow().peek(&cache_key).cloned();

            let (value_set, has_null) = if let Some(cached) = cached_values {
                // Cache hit - use cached value set
                cached
            } else {
                // Cache miss - execute subquery and build value set
                let select_executor = crate::select::SelectExecutor::new_with_depth(database, self.depth);
                let rows = select_executor.execute(subquery)?;

                // Validate single column
                let column_count = if !rows.is_empty() {
                    rows[0].values.len()
                } else {
                    compute_select_list_column_count(subquery, database)?
                };

                if column_count != 1 {
                    return Err(ExecutorError::SubqueryColumnCountMismatch {
                        expected: 1,
                        actual: column_count,
                    });
                }

                // Build HashSet of values and track NULL presence
                let mut values = std::collections::HashSet::new();
                let mut found_null = false;

                for subquery_row in &rows {
                    let val = subquery_row.get(0).ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })?;
                    if matches!(val, vibesql_types::SqlValue::Null) {
                        found_null = true;
                    } else {
                        values.insert(val.clone());
                    }
                }

                // Cache the value set
                self.in_value_cache.borrow_mut().put(cache_key, (values.clone(), found_null));

                (values, found_null)
            };

            // Now use O(1) HashSet lookup for membership testing
            // Handle NULL input specially
            if matches!(expr_val, vibesql_types::SqlValue::Null) {
                if value_set.is_empty() && !has_null {
                    return Ok(vibesql_types::SqlValue::Boolean(negated));
                }
                return Ok(vibesql_types::SqlValue::Null);
            }

            // Try exact HashSet match first (O(1))
            if value_set.contains(&expr_val) {
                return Ok(vibesql_types::SqlValue::Boolean(!negated));
            }

            // For type coercion cases, fall back to linear search if types differ
            // This handles Integer(5) == Float(5.0) SQL semantics
            for subquery_val in &value_set {
                // Skip if same type (already checked by HashSet)
                if std::mem::discriminant(&expr_val) == std::mem::discriminant(subquery_val) {
                    continue;
                }

                // Check with type coercion
                let eq_result = ExpressionEvaluator::eval_binary_op_static(
                    &expr_val,
                    &vibesql_ast::BinaryOperator::Equal,
                    subquery_val,
                    sql_mode.clone(),
                )?;

                if matches!(eq_result, vibesql_types::SqlValue::Boolean(true)) {
                    return Ok(vibesql_types::SqlValue::Boolean(!negated));
                }
            }

            // No match found
            if has_null {
                Ok(vibesql_types::SqlValue::Null)
            } else {
                Ok(vibesql_types::SqlValue::Boolean(negated))
            }
        } else {
            // Correlated subquery - execute with outer context (can't cache)
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

            // Validate single column
            let column_count = if !rows.is_empty() {
                rows[0].values.len()
            } else {
                compute_select_list_column_count(subquery, database)?
            };

            if column_count != 1 {
                return Err(ExecutorError::SubqueryColumnCountMismatch {
                    expected: 1,
                    actual: column_count,
                });
            }

            // SQL standard behavior for NULL IN (subquery)
            if matches!(expr_val, vibesql_types::SqlValue::Null) {
                if rows.is_empty() {
                    return Ok(vibesql_types::SqlValue::Boolean(negated));
                }

                for subquery_row in &rows {
                    let subquery_val =
                        subquery_row.get(0).ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })?;

                    if matches!(subquery_val, vibesql_types::SqlValue::Null) {
                        return Ok(vibesql_types::SqlValue::Null);
                    }
                }

                return Ok(vibesql_types::SqlValue::Null);
            }

            // Linear search for correlated subqueries
            let mut found_null = false;

            for subquery_row in &rows {
                let subquery_val =
                    subquery_row.get(0).ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })?;

                if matches!(subquery_val, vibesql_types::SqlValue::Null) {
                    found_null = true;
                    continue;
                }

                let eq_result = ExpressionEvaluator::eval_binary_op_static(
                    &expr_val,
                    &vibesql_ast::BinaryOperator::Equal,
                    subquery_val,
                    sql_mode.clone(),
                )?;

                if matches!(eq_result, vibesql_types::SqlValue::Boolean(true)) {
                    return Ok(vibesql_types::SqlValue::Boolean(!negated));
                }
            }

            if found_null {
                Ok(vibesql_types::SqlValue::Null)
            } else {
                Ok(vibesql_types::SqlValue::Boolean(negated))
            }
        }
    }
}

/// Check if a subquery can use index optimization for IN evaluation
///
/// Returns true if the subquery is:
/// - Simple SELECT column FROM table WHERE ... (no joins, no aggregates, no subqueries)
/// - Single table access
/// - Projected column exists and is indexed
fn can_use_index_for_in_subquery(
    subquery: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
) -> bool {
    // Must have a FROM clause with single table
    let table_name = match &subquery.from {
        Some(vibesql_ast::FromClause::Table { name, .. }) => name,
        _ => return false, // Joins, subqueries, or no FROM clause
    };

    // Must not have GROUP BY, HAVING, LIMIT, OFFSET, or DISTINCT
    if subquery.group_by.is_some()
        || subquery.having.is_some()
        || subquery.limit.is_some()
        || subquery.offset.is_some()
        || subquery.distinct
    {
        return false;
    }

    // Must project exactly one column (not wildcard, not multiple columns)
    if subquery.select_list.len() != 1 {
        return false;
    }

    // Get the projected column name
    #[allow(clippy::collapsible_match)]
    let column_name = match &subquery.select_list[0] {
        vibesql_ast::SelectItem::Expression { expr, .. } => {
            match expr {
                vibesql_ast::Expression::ColumnRef { column, .. } => column,
                _ => return false, // Expressions, functions, etc.
            }
        }
        _ => return false, // Wildcards
    };

    // Check if an index exists that covers this column
    let indexes = database.list_indexes_for_table(table_name);
    for index_name in &indexes {
        if let Some(index_metadata) = database.get_index(index_name) {
            // Check if first indexed column matches our projected column
            if let Some(first_col) = index_metadata.columns.first() {
                if &first_col.column_name == column_name {
                    return true;
                }
            }
        }
    }

    false
}

/// Try to evaluate IN subquery using index optimization
///
/// Returns Some(result) if index optimization succeeds, None to fall back to regular execution
fn try_index_optimized_in_subquery(
    expr_val: &vibesql_types::SqlValue,
    subquery: &vibesql_ast::SelectStmt,
    negated: bool,
    database: &vibesql_storage::Database,
    sql_mode: vibesql_types::SqlMode,
) -> Result<Option<vibesql_types::SqlValue>, ExecutorError> {
    // Extract table and column names (already validated by can_use_index_for_in_subquery)
    let table_name = match &subquery.from {
        Some(vibesql_ast::FromClause::Table { name, .. }) => name,
        _ => return Ok(None),
    };

    #[allow(clippy::collapsible_match)]
    let column_name = match &subquery.select_list[0] {
        vibesql_ast::SelectItem::Expression { expr, .. } => {
            match expr {
                vibesql_ast::Expression::ColumnRef { column, .. } => column,
                _ => return Ok(None),
            }
        }
        _ => return Ok(None),
    };

    // Find the best index for this column
    let indexes = database.list_indexes_for_table(table_name);
    let mut selected_index: Option<String> = None;

    for index_name in &indexes {
        if let Some(index_metadata) = database.get_index(index_name) {
            if let Some(first_col) = index_metadata.columns.first() {
                if &first_col.column_name == column_name {
                    selected_index = Some(index_name.clone());
                    break;
                }
            }
        }
    }

    let index_name = match selected_index {
        Some(name) => name,
        None => return Ok(None),
    };

    // Get index data
    let index_data = match database.get_index_data(&index_name) {
        Some(data) => data,
        None => return Ok(None),
    };

    let table = match database.get_table(table_name) {
        Some(t) => t,
        None => return Ok(None),
    };

    // Use index to get all values efficiently
    // Two strategies based on presence of WHERE clause:
    // 1. With WHERE: Extract predicate, use index scan with filtering
    // 2. Without WHERE: Scan all values from index

    let values_set = if let Some(where_expr) = &subquery.where_clause {
        // Strategy 1: Use index scan with predicate pushdown
        use crate::select::scan::index_scan::predicate::extract_index_predicate;

        // Try to extract index predicate (range or IN)
        if let Some(index_pred) = extract_index_predicate(where_expr, column_name) {
            // Get row indices using index predicate
            let row_indices: Vec<usize> = match index_pred {
                crate::select::scan::index_scan::predicate::IndexPredicate::Range(range) => {
                    index_data.range_scan(
                        range.start.as_ref(),
                        range.end.as_ref(),
                        range.inclusive_start,
                        range.inclusive_end,
                    )
                }
                crate::select::scan::index_scan::predicate::IndexPredicate::In(vals) => {
                    index_data.multi_lookup(&vals)
                }
            };

            // Fetch actual column values from matched rows
            let all_rows = table.scan();
            let column_index = table
                .schema
                .columns
                .iter()
                .position(|col| col.name == *column_name)
                .ok_or_else(|| ExecutorError::ColumnNotFound {
                    column_name: column_name.clone(),
                    table_name: table_name.clone(),
                    searched_tables: vec![table_name.clone()],
                    available_columns: table.schema.columns.iter().map(|c| c.name.clone()).collect(),
                })?;

            let mut values = std::collections::HashSet::new();
            for row_idx in row_indices {
                if let Some(row) = all_rows.get(row_idx) {
                    if let Some(value) = row.values.get(column_index) {
                        values.insert(value.clone());
                    }
                }
            }
            values
        } else {
            // WHERE clause exists but can't use index - fall back
            return Ok(None);
        }
    } else {
        // Strategy 2: No WHERE clause - scan all indexed values
        // This is still faster than full subquery execution if we can read from index
        let all_rows = table.scan();
        let column_index = table
            .schema
            .columns
            .iter()
            .position(|col| col.name == *column_name)
            .ok_or_else(|| ExecutorError::ColumnNotFound {
                column_name: column_name.clone(),
                table_name: table_name.clone(),
                searched_tables: vec![table_name.clone()],
                available_columns: table.schema.columns.iter().map(|c| c.name.clone()).collect(),
            })?;

        // Collect all distinct values from the column
        let mut values = std::collections::HashSet::new();
        for row in all_rows {
            if let Some(value) = row.values.get(column_index) {
                values.insert(value.clone());
            }
        }
        values
    };

    // Now check if expr_val is in the set (same logic as original implementation)
    // Handle NULL cases per SQL standard
    if matches!(expr_val, vibesql_types::SqlValue::Null) {
        if values_set.is_empty() {
            return Ok(Some(vibesql_types::SqlValue::Boolean(negated)));
        }

        if values_set.contains(&vibesql_types::SqlValue::Null) {
            return Ok(Some(vibesql_types::SqlValue::Null));
        }

        return Ok(Some(vibesql_types::SqlValue::Null));
    }

    let mut found_null = false;
    for value in &values_set {
        if matches!(value, vibesql_types::SqlValue::Null) {
            found_null = true;
            continue;
        }

        // Compare using equality
        let eq_result = ExpressionEvaluator::eval_binary_op_static(
            expr_val,
            &vibesql_ast::BinaryOperator::Equal,
            value,
            sql_mode.clone(),
        )?;

        if matches!(eq_result, vibesql_types::SqlValue::Boolean(true)) {
            return Ok(Some(vibesql_types::SqlValue::Boolean(!negated)));
        }
    }

    // No match found
    if found_null {
        Ok(Some(vibesql_types::SqlValue::Null))
    } else {
        Ok(Some(vibesql_types::SqlValue::Boolean(negated)))
    }
}
