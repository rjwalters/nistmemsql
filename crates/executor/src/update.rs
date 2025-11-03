//! UPDATE statement execution

use ast::{BinaryOperator, Expression, UpdateStmt};
use storage::Database;

use crate::errors::ExecutorError;
use crate::evaluator::ExpressionEvaluator;
use crate::privilege_checker::PrivilegeChecker;

/// Executor for UPDATE statements
pub struct UpdateExecutor;

impl UpdateExecutor {
    /// Analyze WHERE expression to see if it can use primary key index for fast lookup
    ///
    /// Returns the primary key value if the expression is a simple equality on the primary key,
    /// otherwise returns None.
    fn extract_primary_key_lookup(
        where_expr: &Expression,
        schema: &catalog::TableSchema,
    ) -> Option<Vec<types::SqlValue>> {
        // Only handle simple binary equality operations
        match where_expr {
            Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
                // Check if left side is a column reference and right side is a literal
                if let (Expression::ColumnRef { column, .. }, Expression::Literal(value)) =
                    (left.as_ref(), right.as_ref())
                {
                    // Check if this column is the primary key
                    if let Some(pk_indices) = schema.get_primary_key_indices() {
                        if let Some(col_index) = schema.get_column_index(column) {
                            // Only handle single-column primary keys for now
                            if pk_indices.len() == 1 && pk_indices[0] == col_index {
                                return Some(vec![value.clone()]);
                            }
                        }
                    }
                }

                // Also check the reverse: literal = column
                if let (Expression::Literal(value), Expression::ColumnRef { column, .. }) =
                    (left.as_ref(), right.as_ref())
                {
                    if let Some(pk_indices) = schema.get_primary_key_indices() {
                        if let Some(col_index) = schema.get_column_index(column) {
                            if pk_indices.len() == 1 && pk_indices[0] == col_index {
                                return Some(vec![value.clone()]);
                            }
                        }
                    }
                }
            }
            _ => {}
        }

        None
    }

    /// Execute an UPDATE statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The UPDATE statement AST node
    /// * `database` - The database to update
    ///
    /// # Returns
    ///
    /// Number of rows updated or error
    ///
    /// # Examples
    ///
    /// ```
    /// use ast::{UpdateStmt, Assignment, Expression};
    /// use types::SqlValue;
    /// use storage::Database;
    /// use catalog::{TableSchema, ColumnSchema};
    /// use types::DataType;
    /// use executor::UpdateExecutor;
    ///
    /// let mut db = Database::new();
    ///
    /// // Create table
    /// let schema = TableSchema::new(
    ///     "employees".to_string(),
    ///     vec![
    ///         ColumnSchema::new("id".to_string(), DataType::Integer, false),
    ///         ColumnSchema::new("salary".to_string(), DataType::Integer, false),
    ///     ],
    /// );
    /// db.create_table(schema).unwrap();
    ///
    /// // Insert a row
    /// db.insert_row("employees", storage::Row::new(vec![
    ///     SqlValue::Integer(1),
    ///     SqlValue::Integer(50000),
    /// ])).unwrap();
    ///
    /// // Update salary
    /// let stmt = UpdateStmt {
    ///     table_name: "employees".to_string(),
    ///     assignments: vec![
    ///         Assignment {
    ///             column: "salary".to_string(),
    ///             value: Expression::Literal(SqlValue::Integer(60000)),
    ///         },
    ///     ],
    ///     where_clause: None,
    ///};
    ///
    /// let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    /// assert_eq!(count, 1);
    /// ```
    pub fn execute(stmt: &UpdateStmt, database: &mut Database) -> Result<usize, ExecutorError> {
        Self::execute_with_schema(stmt, database, None)
    }

    /// Execute an UPDATE statement with optional pre-fetched schema
    ///
    /// This method allows cursor-level schema caching to reduce redundant catalog lookups.
    /// If schema is provided, skips the catalog lookup step.
    ///
    /// # Arguments
    ///
    /// * `stmt` - The UPDATE statement AST node
    /// * `database` - The database to update
    /// * `schema` - Optional pre-fetched schema (from cursor cache)
    ///
    /// # Returns
    ///
    /// Number of rows updated or error
    pub fn execute_with_schema(
        stmt: &UpdateStmt,
        database: &mut Database,
        schema: Option<&catalog::TableSchema>,
    ) -> Result<usize, ExecutorError> {
        // Check UPDATE privilege on the table
        PrivilegeChecker::check_update(database, &stmt.table_name)?;

        // Step 1: Get table schema - use provided schema or fetch from catalog
        let schema = if let Some(s) = schema {
            s
        } else {
            database
                .catalog
                .get_table(&stmt.table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?
        };

        // Step 2: Get table from storage (for reading rows)
        let table = database
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Step 3: Create expression evaluator with database reference for subquery support
        let evaluator = ExpressionEvaluator::with_database(schema, database);

        // Step 4: Build list of updates (two-phase execution for SQL semantics)
        // Each update consists of: (row_index, new_row, changed_columns)
        let mut updates: Vec<(usize, storage::Row, std::collections::HashSet<usize>)> = Vec::new();

        // Try to use primary key index for fast lookup
        let candidate_rows: Vec<(usize, storage::Row)> =
            if let Some(ast::WhereClause::Condition(where_expr)) = &stmt.where_clause {
                if let Some(pk_values) = Self::extract_primary_key_lookup(where_expr, schema) {
                    // Use primary key index for O(1) lookup
                    if let Some(pk_index) = table.primary_key_index() {
                        if let Some(&row_index) = pk_index.get(&pk_values) {
                            // Found the row via index - single row to update
                            vec![(row_index, table.scan()[row_index].clone())]
                        } else {
                            // Primary key not found - no rows to update
                            vec![]
                        }
                    } else {
                        // No primary key index available, fall back to table scan
                        Self::collect_candidate_rows(table, &stmt.where_clause, &evaluator)?
                    }
                } else {
                    // WHERE clause exists but can't use index, fall back to table scan
                    Self::collect_candidate_rows(table, &stmt.where_clause, &evaluator)?
                }
            } else {
                // No WHERE clause or unsupported WHERE type - scan all rows
                Self::collect_candidate_rows(table, &stmt.where_clause, &evaluator)?
            };

        for (row_index, row) in candidate_rows {
            // If the primary key is being updated, we need to check for child references
            if let Some(pk_indices) = schema.get_primary_key_indices() {
                let updates_pk = stmt.assignments.iter().any(|a| {
                    let col_index = schema.get_column_index(&a.column).unwrap();
                    pk_indices.contains(&col_index)
                });

                if updates_pk {
                    check_no_child_references(database, &stmt.table_name, &row)?;
                }
            }

            // Build updated row by cloning original and applying assignments
            let mut new_row = row.clone();

            // Track which columns are being updated for selective index maintenance
            let mut changed_columns = std::collections::HashSet::new();

            // Apply each assignment
            for assignment in &stmt.assignments {
                // Find column index
                let col_index = schema.get_column_index(&assignment.column).ok_or_else(|| {
                    ExecutorError::ColumnNotFound {
                        column_name: assignment.column.clone(),
                        table_name: stmt.table_name.clone(),
                    }
                })?;

                // Evaluate new value expression
                // Handle DEFAULT specially before evaluating other expressions
                let new_value = match &assignment.value {
                    ast::Expression::Default => {
                        // Use column's default value, or NULL if no default is defined
                        let column = &schema.columns[col_index];
                        if let Some(default_expr) = &column.default_value {
                            // Evaluate the default expression (currently only supports literals)
                            match default_expr {
                                ast::Expression::Literal(lit) => lit.clone(),
                                _ => return Err(ExecutorError::UnsupportedExpression(format!(
                                    "Complex default expressions not yet supported for column '{}'",
                                    column.name
                                ))),
                            }
                        } else {
                            // No default value defined, use NULL
                            types::SqlValue::Null
                        }
                    }
                    _ => {
                        // Evaluate other expressions against ORIGINAL row
                        evaluator.eval(&assignment.value, &row)?
                    }
                };

                // Update column in new row
                new_row
                    .set(col_index, new_value)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                // Track that this column changed
                changed_columns.insert(col_index);
            }

            // Enforce NOT NULL constraints on the updated row
            for (col_idx, col) in schema.columns.iter().enumerate() {
                let value = new_row
                    .get(col_idx)
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_idx })?;

                if !col.nullable && *value == types::SqlValue::Null {
                    return Err(ExecutorError::ConstraintViolation(format!(
                        "NOT NULL constraint violation: column '{}' in table '{}' cannot be NULL",
                        col.name, stmt.table_name
                    )));
                }
            }

            // Enforce PRIMARY KEY constraint (uniqueness)
            if let Some(pk_indices) = schema.get_primary_key_indices() {
                // Extract primary key values from the updated row
                let new_pk_values: Vec<types::SqlValue> =
                    pk_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

                // Use hash index for O(1) lookup instead of O(n) scan
                if let Some(pk_index) = table.primary_key_index() {
                    if pk_index.contains_key(&new_pk_values) {
                        // Check if it's not the same row (hash index doesn't store row index for easy exclusion)
                        // We need to verify it's actually a different row by checking if this is an update to the same PK
                        let original_pk_values: Vec<types::SqlValue> =
                            pk_indices.iter().map(|&idx| row.values[idx].clone()).collect();

                        if new_pk_values != original_pk_values {
                            let pk_col_names: Vec<String> =
                                schema.primary_key.as_ref().unwrap().clone();
                            return Err(ExecutorError::ConstraintViolation(format!(
                                "PRIMARY KEY constraint violated: duplicate key value for ({})",
                                pk_col_names.join(", ")
                            )));
                        }
                    }
                } else {
                    // Fallback to table scan if index not available (should not happen in normal operation)
                    for (other_idx, other_row) in table.scan().iter().enumerate() {
                        // Skip the row being updated
                        if other_idx == row_index {
                            continue;
                        }

                        let other_pk_values: Vec<&types::SqlValue> =
                            pk_indices.iter().filter_map(|&idx| other_row.get(idx)).collect();

                        if new_pk_values.iter().zip(other_pk_values).all(|(a, b)| *a == *b) {
                            let pk_col_names: Vec<String> =
                                schema.primary_key.as_ref().unwrap().clone();
                            return Err(ExecutorError::ConstraintViolation(format!(
                                "PRIMARY KEY constraint violated: duplicate key value for ({})",
                                pk_col_names.join(", ")
                            )));
                        }
                    }
                }
            }

            // Enforce UNIQUE constraints
            let unique_constraint_indices = schema.get_unique_constraint_indices();
            for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
                // Extract unique constraint values from the updated row
                let new_unique_values: Vec<types::SqlValue> =
                    unique_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

                // Skip if any value in the unique constraint is NULL
                // (NULL != NULL in SQL, so multiple NULLs are allowed)
                if new_unique_values.iter().any(|v| *v == types::SqlValue::Null) {
                    continue;
                }

                // Use hash index for O(1) lookup instead of O(n) scan
                let unique_indexes = table.unique_indexes();
                if constraint_idx < unique_indexes.len() {
                    let unique_index = &unique_indexes[constraint_idx];
                    if unique_index.contains_key(&new_unique_values) {
                        // Check if it's not the same row by comparing original values
                        let original_unique_values: Vec<types::SqlValue> =
                            unique_indices.iter().map(|&idx| row.values[idx].clone()).collect();

                        if new_unique_values != original_unique_values {
                            let unique_col_names: Vec<String> =
                                schema.unique_constraints[constraint_idx].clone();
                            return Err(ExecutorError::ConstraintViolation(format!(
                                "UNIQUE constraint violated: duplicate value for ({})",
                                unique_col_names.join(", ")
                            )));
                        }
                    }
                } else {
                    // Fallback to table scan if index not available (should not happen in normal operation)
                    for (other_idx, other_row) in table.scan().iter().enumerate() {
                        // Skip the row being updated
                        if other_idx == row_index {
                            continue;
                        }

                        let other_unique_values: Vec<&types::SqlValue> =
                            unique_indices.iter().filter_map(|&idx| other_row.get(idx)).collect();

                        // Skip if any existing value is NULL
                        if other_unique_values.iter().any(|v| **v == types::SqlValue::Null) {
                            continue;
                        }

                        if new_unique_values.iter().zip(other_unique_values).all(|(a, b)| *a == *b)
                        {
                            let unique_col_names: Vec<String> =
                                schema.unique_constraints[constraint_idx].clone();
                            return Err(ExecutorError::ConstraintViolation(format!(
                                "UNIQUE constraint violated: duplicate value for ({})",
                                unique_col_names.join(", ")
                            )));
                        }
                    }
                }
            }

            // Enforce CHECK constraints
            if !schema.check_constraints.is_empty() {
                let evaluator = crate::evaluator::ExpressionEvaluator::new(schema);

                for (constraint_name, check_expr) in &schema.check_constraints {
                    // Evaluate the CHECK expression against the updated row
                    let result = evaluator.eval(check_expr, &new_row)?;

                    // CHECK constraint passes if result is TRUE or NULL (UNKNOWN)
                    // CHECK constraint fails if result is FALSE
                    if result == types::SqlValue::Boolean(false) {
                        return Err(ExecutorError::ConstraintViolation(format!(
                            "CHECK constraint '{}' violated",
                            constraint_name
                        )));
                    }
                }
            }

            // Enforce FOREIGN KEY constraints (child table)
            if !schema.foreign_keys.is_empty() {
                validate_foreign_key_constraints(database, &stmt.table_name, &new_row.values)?;
            }

            updates.push((row_index, new_row, changed_columns));
        }

        // Step 5: Apply all updates (after evaluation phase completes)
        let update_count = updates.len();

        // Get mutable table reference
        let table_mut = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        for (index, new_row, changed_columns) in updates {
            table_mut
                .update_row_selective(index, new_row, &changed_columns)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
        }

        Ok(update_count)
    }

    /// Collect candidate rows that match the WHERE clause (fallback for non-indexed queries)
    fn collect_candidate_rows(
        table: &storage::Table,
        where_clause: &Option<ast::WhereClause>,
        evaluator: &ExpressionEvaluator,
    ) -> Result<Vec<(usize, storage::Row)>, ExecutorError> {
        let mut candidate_rows = Vec::new();

        for (row_index, row) in table.scan().iter().enumerate() {
            // Check WHERE clause
            let should_update = if let Some(ref where_clause) = where_clause {
                match where_clause {
                    ast::WhereClause::Condition(where_expr) => {
                        let result = evaluator.eval(where_expr, row)?;
                        // SQL semantics: only TRUE (not NULL) causes update
                        matches!(result, types::SqlValue::Boolean(true))
                    }
                    ast::WhereClause::CurrentOf(_cursor_name) => {
                        // TODO: Implement cursor support - for now return error
                        return Err(ExecutorError::UnsupportedFeature(
                            "WHERE CURRENT OF cursor is not yet implemented".to_string(),
                        ));
                    }
                }
            } else {
                true // No WHERE clause = update all rows
            };

            if should_update {
                candidate_rows.push((row_index, row.clone()));
            }
        }

        Ok(candidate_rows)
    }
}

/// Validate FOREIGN KEY constraints for a new row
fn validate_foreign_key_constraints(
    db: &storage::Database,
    table_name: &str,
    row_values: &[types::SqlValue],
) -> Result<(), ExecutorError> {
    let schema = db
        .catalog
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    for fk in &schema.foreign_keys {
        // Extract FK values from the new row
        let fk_values: Vec<types::SqlValue> =
            fk.column_indices.iter().map(|&idx| row_values[idx].clone()).collect();

        // If any part of the foreign key is NULL, the constraint is not violated.
        if fk_values.iter().any(|v| v.is_null()) {
            continue;
        }

        // Check if the referenced key exists in the parent table
        let parent_table = db
            .get_table(&fk.parent_table)
            .ok_or_else(|| ExecutorError::TableNotFound(fk.parent_table.clone()))?;

        let key_exists = parent_table.scan().iter().any(|parent_row| {
            fk.parent_column_indices
                .iter()
                .zip(&fk_values)
                .all(|(&parent_idx, fk_val)| parent_row.get(parent_idx) == Some(fk_val))
        });

        if !key_exists {
            return Err(ExecutorError::ConstraintViolation(format!(
                "FOREIGN KEY constraint \'{}\' violated: key ({}) not found in table \'{}\'",
                fk.name.as_deref().unwrap_or(""),
                fk.column_names.join(", "),
                fk.parent_table
            )));
        }
    }

    Ok(())
}

/// Check that no child tables reference a row that is about to be deleted or updated.
fn check_no_child_references(
    db: &storage::Database,
    parent_table_name: &str,
    parent_row: &storage::Row,
) -> Result<(), ExecutorError> {
    let parent_schema = db
        .catalog
        .get_table(parent_table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(parent_table_name.to_string()))?;

    // This check is only meaningful if the parent table has a primary key.
    let pk_indices = match parent_schema.get_primary_key_indices() {
        Some(indices) => indices,
        None => return Ok(()),
    };

    let parent_key_values: Vec<types::SqlValue> =
        pk_indices.iter().map(|&idx| parent_row.values[idx].clone()).collect();

    // Scan all tables in the database to find foreign keys that reference this table.
    for table_name in db.catalog.list_tables() {
        let child_schema = db.catalog.get_table(&table_name).unwrap();

        for fk in &child_schema.foreign_keys {
            if fk.parent_table != parent_table_name {
                continue;
            }

            // Check if any row in the child table references the parent row.
            let child_table = db.get_table(&table_name).unwrap();
            let has_references = child_table.scan().iter().any(|child_row| {
                let child_fk_values: Vec<types::SqlValue> =
                    fk.column_indices.iter().map(|&idx| child_row.values[idx].clone()).collect();
                child_fk_values == parent_key_values
            });

            if has_references {
                return Err(ExecutorError::ConstraintViolation(format!(
                    "FOREIGN KEY constraint violation: cannot delete or update a parent row when a foreign key constraint exists. The conflict occurred in table \'{}\', constraint \'{}\'.",
                    table_name,
                    fk.name.as_deref().unwrap_or(""),
                )));
            }
        }
    }

    Ok(())
}
