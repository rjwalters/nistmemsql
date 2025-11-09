//! Non-aggregation execution methods for SelectExecutor

use std::collections::HashMap;

use storage::database::IndexData;
use types::SqlValue;

use super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator},
    optimizer::{decompose_where_clause, optimize_where_clause},
    schema::CombinedSchema,
    select::{
        filter::apply_where_filter_combined,
        grouping::compare_sql_values,
        helpers::{apply_distinct, apply_limit_offset},
        join::FromResult,
        order::{apply_order_by, RowWithSortKeys},
        projection::project_row_combined,
        window::{
            collect_order_by_window_functions, evaluate_order_by_window_functions,
            evaluate_window_functions, expression_has_window_function, has_window_functions,
        },
    },
};

impl SelectExecutor<'_> {
    /// Execute SELECT without aggregation
    pub(super) fn execute_without_aggregation(
        &self,
        stmt: &ast::SelectStmt,
        from_result: FromResult,
    ) -> Result<Vec<storage::Row>, ExecutorError> {
        let FromResult { schema, rows } = from_result;

        // Track memory used by FROM clause results (JOINs, table scans, etc.)
        let from_memory_bytes = std::mem::size_of::<storage::Row>() * rows.len()
            + rows.iter().map(|r| std::mem::size_of_val(r.values.as_slice())).sum::<usize>();
        self.track_memory_allocation(from_memory_bytes)?;

        // Decompose WHERE clause for predicate pushdown optimization
        // This analyzes the WHERE clause to identify:
        // - Table-local predicates (can be pushed to table scans)
        // - Equijoin conditions (can be optimized in join operators)
        // - Complex predicates (deferred to post-join filtering)
        let _predicate_decomposition = if let Some(where_expr) = &stmt.where_clause {
            match decompose_where_clause(Some(where_expr), &schema) {
                Ok(decomp) => Some(decomp),
                Err(_) => {
                    // If decomposition fails, continue with standard WHERE processing
                    None
                }
            }
        } else {
            None
        };

        // Create evaluator with outer context if available (outer schema is already a
        // CombinedSchema)
        let evaluator =
            if let (Some(outer_row), Some(outer_schema)) = (self._outer_row, self._outer_schema) {
                CombinedExpressionEvaluator::with_database_and_outer_context(
                    &schema,
                    self.database,
                    outer_row,
                    outer_schema,
                )
            } else {
                CombinedExpressionEvaluator::with_database(&schema, self.database)
            };

        // Try index-based WHERE optimization first
        let mut filtered_rows = if let Some(index_filtered) =
            self.try_index_based_where_filtering(stmt.where_clause.as_ref(), &rows, &schema)?
        {
            index_filtered
        } else {
            // Fall back to full WHERE clause evaluation
            // Note: Table-local predicates have already been pushed down and applied during table
            // scan. However, we still apply the full WHERE clause here for correctness
            // with JOINs and complex predicates that can't be pushed down.
            // The table-local predicates being applied twice is safe (same result) but not optimal.
            // TODO: In Phase 3, extract and remove table-local predicates here to avoid double
            // filtering
            let where_optimization = optimize_where_clause(stmt.where_clause.as_ref(), &evaluator)?;

            match where_optimization {
                crate::optimizer::WhereOptimization::AlwaysTrue => {
                    // WHERE TRUE - no filtering needed
                    rows
                }
                crate::optimizer::WhereOptimization::AlwaysFalse => {
                    // WHERE FALSE - return empty result
                    Vec::new()
                }
                crate::optimizer::WhereOptimization::Optimized(ref expr) => {
                    // Apply optimized WHERE clause
                    apply_where_filter_combined(rows, Some(expr), &evaluator, self)?
                }
                crate::optimizer::WhereOptimization::Unchanged(where_expr) => {
                    // Apply original WHERE clause
                    apply_where_filter_combined(rows, where_expr.as_ref(), &evaluator, self)?
                }
            }
        };

        // Post-FROM predicate optimization: apply table-local predicates
        // This filters rows right after FROM execution but before other operations
        // Note: Ideally these would be applied during table scan for better performance,
        // but applying them here is simpler and still provides memory benefit for joins
        if let Some(pred_decomp) = &_predicate_decomposition {
            if !pred_decomp.table_local_predicates.is_empty() {
                // Apply each table's local predicates
                // TODO: Implement filtering using table_local_predicates
                // For now, this is a placeholder for the structure
            }
        }

        // Check if SELECT list has window functions
        let has_select_windows = has_window_functions(&stmt.select_list);

        // Check if ORDER BY has window functions
        let has_order_by_windows = stmt
            .order_by
            .as_ref()
            .map(|order_by| order_by.iter().any(|item| expression_has_window_function(&item.expr)))
            .unwrap_or(false);

        // If there are window functions, evaluate them first
        // Window functions operate on the filtered result set
        let mut window_mapping = if has_select_windows {
            let (rows_with_windows, mapping) =
                evaluate_window_functions(filtered_rows, &stmt.select_list, &evaluator)?;
            filtered_rows = rows_with_windows;
            Some(mapping)
        } else {
            None
        };

        // If ORDER BY has window functions, evaluate those too
        if has_order_by_windows {
            let order_by_window_functions =
                collect_order_by_window_functions(stmt.order_by.as_ref().unwrap());
            if !order_by_window_functions.is_empty() {
                let (rows_with_order_by_windows, order_by_mapping) =
                    evaluate_order_by_window_functions(
                        filtered_rows,
                        order_by_window_functions,
                        &evaluator,
                        window_mapping.as_ref(),
                    )?;
                filtered_rows = rows_with_order_by_windows;

                // Merge mappings
                if let Some(ref mut existing_mapping) = window_mapping {
                    existing_mapping.extend(order_by_mapping);
                } else {
                    window_mapping = Some(order_by_mapping);
                }
            }
        }

        // Convert to RowWithSortKeys format
        let mut result_rows: Vec<RowWithSortKeys> =
            filtered_rows.into_iter().map(|row| (row, None)).collect();

        // Apply ORDER BY sorting if present
        if let Some(order_by) = &stmt.order_by {
            // Try to use index for ordering first
            if let Some(ordered_rows) =
                self.try_index_based_ordering(&result_rows, order_by, &schema, &stmt.from)?
            {
                result_rows = ordered_rows;
            } else {
                // Fall back to sorting
                // Create evaluator with window mapping for ORDER BY (if window functions are
                // present)
                let order_by_evaluator = if let Some(ref mapping) = window_mapping {
                    CombinedExpressionEvaluator::with_database_and_windows(
                        &schema,
                        self.database,
                        mapping,
                    )
                } else {
                    CombinedExpressionEvaluator::with_database(&schema, self.database)
                };
                result_rows =
                    apply_order_by(result_rows, order_by, &order_by_evaluator, &stmt.select_list)?;
            }
        }

        // Project columns from the sorted rows
        let mut final_rows = Vec::new();
        for (row, _) in result_rows {
            // Check timeout during projection
            self.check_timeout()?;

            // Clear CSE cache before projecting each row to prevent column values
            // from being incorrectly cached across different rows
            evaluator.clear_cse_cache();

            let projected_row = project_row_combined(
                &row,
                &stmt.select_list,
                &evaluator,
                &schema,
                &window_mapping,
            )?;

            // Track memory for each projected row
            let row_memory = std::mem::size_of::<storage::Row>()
                + std::mem::size_of_val(projected_row.values.as_slice());
            self.track_memory_allocation(row_memory)?;

            final_rows.push(projected_row);
        }

        // Apply DISTINCT if specified
        let final_rows = if stmt.distinct { apply_distinct(final_rows) } else { final_rows };

        // Don't apply LIMIT/OFFSET if we have a set operation - it will be applied later
        if stmt.set_operation.is_some() {
            Ok(final_rows)
        } else {
            Ok(apply_limit_offset(final_rows, stmt.limit, stmt.offset))
        }
    }

    /// Execute SELECT without FROM clause
    ///
    /// Evaluates expressions in the SELECT list without any table context.
    /// Returns a single row with the evaluated expressions.
    pub(super) fn execute_select_without_from(
        &self,
        stmt: &ast::SelectStmt,
    ) -> Result<Vec<storage::Row>, ExecutorError> {
        // Create an empty schema (no table context)
        let empty_schema = catalog::TableSchema::new("".to_string(), vec![]);
        let evaluator = ExpressionEvaluator::new(&empty_schema);

        // Create an empty row (no data to reference)
        let empty_row = storage::Row::new(vec![]);

        // Evaluate each item in the SELECT list
        let mut values = Vec::new();
        for item in &stmt.select_list {
            match item {
                ast::SelectItem::Wildcard { .. } | ast::SelectItem::QualifiedWildcard { .. } => {
                    return Err(ExecutorError::UnsupportedFeature(
                        "SELECT * and qualified wildcards require FROM clause".to_string(),
                    ));
                }
                ast::SelectItem::Expression { expr, .. } => {
                    // Check if expression references a column
                    if self.expression_references_column(expr) {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Column reference requires FROM clause".to_string(),
                        ));
                    }

                    // Evaluate the expression
                    let value = evaluator.eval(expr, &empty_row)?;
                    values.push(value);
                }
            }
        }

        // Return a single row with the evaluated values
        Ok(vec![storage::Row::new(values)])
    }

    /// Try to use an index for ORDER BY optimization
    /// Returns ordered rows if an index can be used, None otherwise
    fn try_index_based_ordering(
        &self,
        rows: &[RowWithSortKeys],
        order_by: &[ast::OrderByItem],
        schema: &CombinedSchema,
        _from_clause: &Option<ast::FromClause>,
    ) -> Result<Option<Vec<RowWithSortKeys>>, ExecutorError> {
        // For now, only support single-column ORDER BY
        if order_by.len() != 1 {
            return Ok(None);
        }

        let order_item = &order_by[0];

        // Check if ORDER BY is on a simple column reference
        let column_name = match &order_item.expr {
            ast::Expression::ColumnRef { table: None, column } => column,
            _ => return Ok(None), // Complex expressions can't use index
        };

        // For now, assume single table and try to find any table that has this column
        let mut found_table = None;
        for (table_name, (_start_idx, table_schema)) in &schema.table_schemas {
            if table_schema.get_column_index(column_name).is_some() {
                found_table = Some(table_name.clone());
                break;
            }
        }

        let table_name = match found_table {
            Some(name) => name,
            None => return Ok(None),
        };

        // Find an index on this table and column
        let index_name =
            self.find_index_for_ordering(&table_name, column_name, order_item.direction.clone())?;
        if index_name.is_none() {
            return Ok(None);
        }
        let index_name = index_name.unwrap();

        // Get the index data
        let index_data = if index_name.starts_with("__pk_") {
            // Primary key index
            let table_name = &index_name[5..]; // Remove "__pk_" prefix
            let qualified_table_name = format!("public.{}", table_name);
            if let Some(table) = self.database.get_table(&qualified_table_name) {
                if let Some(pk_index) = table.primary_key_index() {
                    // Convert to IndexData format (HashMap)
                    let data: HashMap<Vec<SqlValue>, Vec<usize>> = pk_index
                        .iter()
                        .map(|(key, &row_idx)| (key.clone(), vec![row_idx]))
                        .collect();
                    IndexData { data }
                } else {
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        } else {
            match self.database.get_index_data(&index_name) {
                Some(data) => data.clone(),
                None => return Ok(None),
            }
        };

        // For this proof of concept, only use index when we have all rows from the table
        // Check by getting the table and comparing row counts
        let table_row_count = match self.database.get_table(&table_name) {
            Some(table) => table.row_count(),
            None => return Ok(None),
        };

        if rows.len() != table_row_count {
            // WHERE filtering was applied, can't use index easily
            return Ok(None);
        }

        // All rows are included, we can use the index directly
        // Convert HashMap to Vec and sort for consistent ordering
        let mut data_vec: Vec<(Vec<SqlValue>, Vec<usize>)> =
            index_data.data.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

        // Sort by key
        data_vec.sort_by(|(a, _), (b, _)| {
            for (val_a, val_b) in a.iter().zip(b.iter()) {
                match compare_sql_values(val_a, val_b) {
                    std::cmp::Ordering::Equal => continue,
                    other => return other,
                }
            }
            std::cmp::Ordering::Equal
        });

        // Reverse if DESC
        if order_item.direction == ast::OrderDirection::Desc {
            data_vec.reverse();
        }

        // Build ordered rows
        let mut ordered_rows = Vec::new();
        for (_, indices) in data_vec {
            for &row_idx in &indices {
                if row_idx < rows.len() {
                    ordered_rows.push(rows[row_idx].clone());
                }
            }
        }

        Ok(Some(ordered_rows))
    }

    /// Find an index that can be used for ordering by the given column
    fn find_index_for_ordering(
        &self,
        table_name: &str,
        column_name: &str,
        direction: ast::OrderDirection,
    ) -> Result<Option<String>, ExecutorError> {
        // For now, look through all indexes (this is inefficient but works for the proof of
        // concept) In a real implementation, we'd have better index lookup
        let all_indexes = self.database.list_indexes();
        for index_name in all_indexes {
            if let Some(metadata) = self.database.get_index(&index_name) {
                if metadata.table_name == table_name
                    && metadata.columns.len() == 1
                    && metadata.columns[0].column_name == column_name
                    && metadata.columns[0].direction == direction
                {
                    return Ok(Some(index_name));
                }
            }
        }

        // Check if this is a primary key column (implicit ASC index)
        if direction == ast::OrderDirection::Asc {
            if let Some(table) = self.database.get_table(&format!("public.{}", table_name)) {
                if let Some(pk_columns) = &table.schema.primary_key {
                    if pk_columns.len() == 1 && pk_columns[0] == column_name {
                        // Return a special name for primary key index
                        return Ok(Some(format!("__pk_{}", table_name)));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Try to use indexes for WHERE clause filtering
    /// Returns Some(rows) if index optimization was applied, None if not applicable
    fn try_index_based_where_filtering(
        &self,
        where_expr: Option<&ast::Expression>,
        all_rows: &[storage::Row],
        schema: &CombinedSchema,
    ) -> Result<Option<Vec<storage::Row>>, ExecutorError> {
        let where_expr = match where_expr {
            Some(expr) => expr,
            None => return Ok(None), // No WHERE clause
        };

        // Try to match different predicate patterns
        match where_expr {
            // AND expressions (for BETWEEN pattern) - check first before binary op
            ast::Expression::BinaryOp { op: ast::BinaryOperator::And, left, right } => {
                self.try_index_for_and_expr(left, right, all_rows, schema)
            }
            // Simple binary operations: column OP value
            ast::Expression::BinaryOp { left, op, right } => {
                self.try_index_for_binary_op(left, op, right, all_rows, schema)
            }
            // IN expressions: column IN (val1, val2, ...)
            ast::Expression::InList { expr, values, negated: false } => {
                self.try_index_for_in_expr(expr, values, all_rows, schema)
            }
            // Other expressions not supported for index optimization
            _ => Ok(None),
        }
    }

    /// Try to use index for binary operation predicates (=, <, >, <=, >=)
    fn try_index_for_binary_op(
        &self,
        left: &ast::Expression,
        op: &ast::BinaryOperator,
        right: &ast::Expression,
        all_rows: &[storage::Row],
        schema: &CombinedSchema,
    ) -> Result<Option<Vec<storage::Row>>, ExecutorError> {
        // Extract column and value
        let (table_name, column_name, value) = match (left, right) {
            (ast::Expression::ColumnRef { table: None, column }, ast::Expression::Literal(val)) => {
                // Find which table this column belongs to
                let mut found_table = None;
                for (table, (_start_idx, _table_schema)) in &schema.table_schemas {
                    if _table_schema.get_column_index(column).is_some() {
                        found_table = Some(table.clone());
                        break;
                    }
                }
                match found_table {
                    Some(table) => (table, column.clone(), val.clone()),
                    None => return Ok(None), // Column not found
                }
            }
            _ => return Ok(None), // Not a simple column OP literal
        };

        // Find an index on this table and column
        let index_name = self.find_index_for_where(&table_name, &column_name)?;
        if index_name.is_none() {
            return Ok(None);
        }
        let index_name = index_name.unwrap();

        // Get the index data
        let index_data = match self.database.get_index_data(&index_name) {
            Some(data) => data,
            None => return Ok(None),
        };

        // Get matching row indices based on operator
        let matching_row_indices = match op {
            ast::BinaryOperator::Equal => {
                // Equality: exact lookup
                let search_key = vec![value];
                index_data.data.get(&search_key).cloned().unwrap_or_else(Vec::new)
            }
            ast::BinaryOperator::GreaterThan => {
                // col > value: use range_scan(Some(value), None, false, false)
                index_data.range_scan(Some(&value), None, false, false)
            }
            ast::BinaryOperator::LessThan => {
                // col < value: use range_scan(None, Some(value), false, false)
                index_data.range_scan(None, Some(&value), false, false)
            }
            ast::BinaryOperator::GreaterThanOrEqual => {
                // col >= value: use range_scan(Some(value), None, true, false)
                index_data.range_scan(Some(&value), None, true, false)
            }
            ast::BinaryOperator::LessThanOrEqual => {
                // col <= value: use range_scan(None, Some(value), false, true)
                index_data.range_scan(None, Some(&value), false, true)
            }
            _ => return Ok(None), // Operator not supported for index optimization
        };

        // Convert row indices to actual rows
        let result_rows = matching_row_indices
            .iter()
            .filter_map(|&row_idx| {
                if row_idx < all_rows.len() {
                    Some(all_rows[row_idx].clone())
                } else {
                    None
                }
            })
            .collect();

        Ok(Some(result_rows))
    }

    /// Try to use index for AND expressions (detecting BETWEEN pattern)
    fn try_index_for_and_expr(
        &self,
        left: &ast::Expression,
        right: &ast::Expression,
        all_rows: &[storage::Row],
        schema: &CombinedSchema,
    ) -> Result<Option<Vec<storage::Row>>, ExecutorError> {
        // Try to detect BETWEEN pattern: (col >= start) AND (col <= end)
        // or variations like (col > start) AND (col < end)

        let (col_name, start_val, start_inclusive, end_val, end_inclusive) = match (left, right) {
            (
                ast::Expression::BinaryOp { left: left_col, op: left_op, right: left_val },
                ast::Expression::BinaryOp { left: right_col, op: right_op, right: right_val },
            ) => {
                // Both sides are binary operations
                // Check if both refer to the same column
                let (left_col_name, _right_col_name) = match (left_col.as_ref(), right_col.as_ref())
                {
                    (
                        ast::Expression::ColumnRef { table: None, column: lc },
                        ast::Expression::ColumnRef { table: None, column: rc },
                    ) if lc == rc => (lc, rc),
                    _ => return Ok(None), // Not the same column
                };

                // Extract values
                let (left_lit, right_lit) = match (left_val.as_ref(), right_val.as_ref()) {
                    (ast::Expression::Literal(lv), ast::Expression::Literal(rv)) => (lv, rv),
                    _ => return Ok(None), // Not literals
                };

                // Determine the bounds based on operators
                // left is lower bound operation (>= or >)
                // right is upper bound operation (<= or <)
                match (left_op, right_op) {
                    (
                        ast::BinaryOperator::GreaterThanOrEqual,
                        ast::BinaryOperator::LessThanOrEqual,
                    ) => (left_col_name.clone(), left_lit.clone(), true, right_lit.clone(), true),
                    (ast::BinaryOperator::GreaterThanOrEqual, ast::BinaryOperator::LessThan) => {
                        (left_col_name.clone(), left_lit.clone(), true, right_lit.clone(), false)
                    }
                    (ast::BinaryOperator::GreaterThan, ast::BinaryOperator::LessThanOrEqual) => {
                        (left_col_name.clone(), left_lit.clone(), false, right_lit.clone(), true)
                    }
                    (ast::BinaryOperator::GreaterThan, ast::BinaryOperator::LessThan) => {
                        (left_col_name.clone(), left_lit.clone(), false, right_lit.clone(), false)
                    }
                    _ => return Ok(None), // Not a BETWEEN-like pattern
                }
            }
            _ => return Ok(None), // Not a BETWEEN-like pattern
        };

        // Find which table this column belongs to
        let mut found_table = None;
        for (table, (_start_idx, _table_schema)) in &schema.table_schemas {
            if _table_schema.get_column_index(&col_name).is_some() {
                found_table = Some(table.clone());
                break;
            }
        }
        let table_name = match found_table {
            Some(table) => table,
            None => return Ok(None), // Column not found
        };

        // Find an index on this table and column
        let index_name = self.find_index_for_where(&table_name, &col_name)?;
        if index_name.is_none() {
            return Ok(None);
        }
        let index_name = index_name.unwrap();

        // Get the index data
        let index_data = match self.database.get_index_data(&index_name) {
            Some(data) => data,
            None => return Ok(None),
        };

        // Use range_scan with both bounds
        let matching_row_indices =
            index_data.range_scan(Some(&start_val), Some(&end_val), start_inclusive, end_inclusive);

        // Convert row indices to actual rows
        let result_rows = matching_row_indices
            .iter()
            .filter_map(|&row_idx| {
                if row_idx < all_rows.len() {
                    Some(all_rows[row_idx].clone())
                } else {
                    None
                }
            })
            .collect();

        Ok(Some(result_rows))
    }

    /// Try to use index for IN expressions
    fn try_index_for_in_expr(
        &self,
        expr: &ast::Expression,
        values: &[ast::Expression],
        all_rows: &[storage::Row],
        schema: &CombinedSchema,
    ) -> Result<Option<Vec<storage::Row>>, ExecutorError> {
        // Extract column name
        let column_name = match expr {
            ast::Expression::ColumnRef { table: None, column } => column,
            _ => return Ok(None), // Not a simple column reference
        };

        // Find which table this column belongs to
        let mut found_table = None;
        for (table, (_start_idx, _table_schema)) in &schema.table_schemas {
            if _table_schema.get_column_index(column_name).is_some() {
                found_table = Some(table.clone());
                break;
            }
        }
        let table_name = match found_table {
            Some(table) => table,
            None => return Ok(None), // Column not found
        };

        // Extract literal values
        let mut literal_values = Vec::new();
        for val_expr in values {
            if let ast::Expression::Literal(val) = val_expr {
                literal_values.push(val.clone());
            } else {
                return Ok(None); // Not all values are literals
            }
        }

        // Find an index on this table and column
        let index_name = self.find_index_for_where(&table_name, column_name)?;
        if index_name.is_none() {
            return Ok(None);
        }
        let index_name = index_name.unwrap();

        // Get the index data
        let index_data = match self.database.get_index_data(&index_name) {
            Some(data) => data,
            None => return Ok(None),
        };

        // Use multi_lookup for IN predicate
        let matching_row_indices = index_data.multi_lookup(&literal_values);

        // Convert row indices to actual rows
        let result_rows = matching_row_indices
            .iter()
            .filter_map(|&row_idx| {
                if row_idx < all_rows.len() {
                    Some(all_rows[row_idx].clone())
                } else {
                    None
                }
            })
            .collect();

        Ok(Some(result_rows))
    }

    /// Find an index that can be used for WHERE clause filtering
    fn find_index_for_where(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Result<Option<String>, ExecutorError> {
        // Look through all indexes for one on this table and column
        let all_indexes = self.database.list_indexes();
        for index_name in all_indexes {
            if let Some(metadata) = self.database.get_index(&index_name) {
                if metadata.table_name == table_name
                    && metadata.columns.len() == 1
                    && metadata.columns[0].column_name == column_name
                {
                    return Ok(Some(index_name));
                }
            }
        }
        Ok(None)
    }
}
