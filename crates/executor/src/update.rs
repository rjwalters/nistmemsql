//! UPDATE statement execution

use ast::UpdateStmt;
use storage::Database;

use crate::errors::ExecutorError;
use crate::evaluator::ExpressionEvaluator;
use crate::privilege_checker::PrivilegeChecker;

/// Executor for UPDATE statements
pub struct UpdateExecutor;

impl UpdateExecutor {
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
        // Check UPDATE privilege on the table
        PrivilegeChecker::check_update(database, &stmt.table_name)?;

        // Step 1: Get table schema from catalog
        let schema = database
            .catalog
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Step 2: Get table from storage (for reading rows)
        let table = database
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Step 3: Create expression evaluator with database reference for subquery support
        let evaluator = ExpressionEvaluator::with_database(schema, database);

        // Step 4: Build list of updates (two-phase execution for SQL semantics)
        let mut updates = Vec::new();

        for (row_index, row) in table.scan().iter().enumerate() {
            // Check WHERE clause
            let should_update = if let Some(ref where_expr) = stmt.where_clause {
                let result = evaluator.eval(where_expr, row)?;
                // SQL semantics: only TRUE (not NULL) causes update
                matches!(result, types::SqlValue::Boolean(true))
            } else {
                true // No WHERE clause = update all rows
            };

            if should_update {
                // If the primary key is being updated, we need to check for child references
                if let Some(pk_indices) = schema.get_primary_key_indices() {
                    let updates_pk = stmt.assignments.iter().any(|a| {
                        let col_index = schema.get_column_index(&a.column).unwrap();
                        pk_indices.contains(&col_index)
                    });

                    if updates_pk {
                        check_no_child_references(database, &stmt.table_name, row)?;
                    }
                }

                // Build updated row by cloning original and applying assignments
                let mut new_row = row.clone();

                // Apply each assignment
                for assignment in &stmt.assignments {
                    // Find column index
                    let col_index = schema
                        .get_column_index(&assignment.column)
                        .ok_or_else(|| ExecutorError::ColumnNotFound(assignment.column.clone()))?;

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
                                    _ => return Err(ExecutorError::UnsupportedExpression(
                                        format!("Complex default expressions not yet supported for column '{}'", column.name)
                                    ))
                                }
                            } else {
                                // No default value defined, use NULL
                                types::SqlValue::Null
                            }
                        }
                        _ => {
                            // Evaluate other expressions against ORIGINAL row
                            evaluator.eval(&assignment.value, row)?
                        }
                    };

                    // Update column in new row
                    new_row
                        .set(col_index, new_value)
                        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
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
                    let new_pk_values: Vec<&types::SqlValue> =
                        pk_indices.iter().filter_map(|&idx| new_row.get(idx)).collect();

                    // Check if any OTHER row has the same primary key
                    for (other_idx, other_row) in table.scan().iter().enumerate() {
                        // Skip the row being updated
                        if other_idx == row_index {
                            continue;
                        }

                        let other_pk_values: Vec<&types::SqlValue> =
                            pk_indices.iter().filter_map(|&idx| other_row.get(idx)).collect();

                        if new_pk_values == other_pk_values {
                            let pk_col_names: Vec<String> =
                                schema.primary_key.as_ref().unwrap().clone();
                            return Err(ExecutorError::ConstraintViolation(format!(
                                "PRIMARY KEY constraint violated: duplicate key value for ({})",
                                pk_col_names.join(", ")
                            )));
                        }
                    }
                }

                // Enforce UNIQUE constraints
                let unique_constraint_indices = schema.get_unique_constraint_indices();
                for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate()
                {
                    // Extract unique constraint values from the updated row
                    let new_unique_values: Vec<&types::SqlValue> =
                        unique_indices.iter().filter_map(|&idx| new_row.get(idx)).collect();

                    // Skip if any value in the unique constraint is NULL
                    // (NULL != NULL in SQL, so multiple NULLs are allowed)
                    if new_unique_values.iter().any(|v| **v == types::SqlValue::Null) {
                        continue;
                    }

                    // Check if any OTHER row has the same unique constraint values
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

                        if new_unique_values == other_unique_values {
                            let unique_col_names: Vec<String> =
                                schema.unique_constraints[constraint_idx].clone();
                            return Err(ExecutorError::ConstraintViolation(format!(
                                "UNIQUE constraint violated: duplicate value for ({})",
                                unique_col_names.join(", ")
                            )));
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

                updates.push((row_index, new_row));
            }
        }

        // Step 5: Apply all updates (after evaluation phase completes)
        let update_count = updates.len();

        // Get mutable table reference
        let table_mut = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        for (index, new_row) in updates {
            table_mut
                .update_row(index, new_row)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
        }

        Ok(update_count)
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
