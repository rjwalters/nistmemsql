//! UPDATE statement execution

use ast::UpdateStmt;
use storage::Database;

use crate::errors::ExecutorError;
use crate::evaluator::ExpressionEvaluator;

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
        // Step 1: Get table schema from catalog
        let schema = database
            .catalog
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Step 2: Get table from storage (for reading rows)
        let table = database
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Step 3: Create expression evaluator
        let evaluator = ExpressionEvaluator::new(schema);

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
                // Build updated row by cloning original and applying assignments
                let mut new_row = row.clone();

                // Apply each assignment
                for assignment in &stmt.assignments {
                    // Find column index
                    let col_index = schema
                        .get_column_index(&assignment.column)
                        .ok_or_else(|| ExecutorError::ColumnNotFound(assignment.column.clone()))?;

                    // Evaluate new value expression against ORIGINAL row
                    let new_value = evaluator.eval(&assignment.value, row)?;

                    // Update column in new row
                    new_row
                        .set(col_index, new_value)
                        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                }

                // Enforce NOT NULL constraints on the updated row
                for (col_idx, col) in schema.columns.iter().enumerate() {
                    let value = new_row.get(col_idx)
                        .ok_or_else(|| ExecutorError::ColumnIndexOutOfBounds { index: col_idx })?;

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
                    let new_pk_values: Vec<&types::SqlValue> = pk_indices
                        .iter()
                        .filter_map(|&idx| new_row.get(idx))
                        .collect();

                    // Check if any OTHER row has the same primary key
                    for (other_idx, other_row) in table.scan().iter().enumerate() {
                        // Skip the row being updated
                        if other_idx == row_index {
                            continue;
                        }

                        let other_pk_values: Vec<&types::SqlValue> = pk_indices
                            .iter()
                            .filter_map(|&idx| other_row.get(idx))
                            .collect();

                        if new_pk_values == other_pk_values {
                            let pk_col_names: Vec<String> = schema.primary_key
                                .as_ref()
                                .unwrap()
                                .clone();
                            return Err(ExecutorError::ConstraintViolation(format!(
                                "PRIMARY KEY constraint violated: duplicate key value for ({})",
                                pk_col_names.join(", ")
                            )));
                        }
                    }
                }

                // Enforce UNIQUE constraints
                let unique_constraint_indices = schema.get_unique_constraint_indices();
                for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
                    // Extract unique constraint values from the updated row
                    let new_unique_values: Vec<&types::SqlValue> = unique_indices
                        .iter()
                        .filter_map(|&idx| new_row.get(idx))
                        .collect();

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

                        let other_unique_values: Vec<&types::SqlValue> = unique_indices
                            .iter()
                            .filter_map(|&idx| other_row.get(idx))
                            .collect();

                        // Skip if any existing value is NULL
                        if other_unique_values.iter().any(|v| **v == types::SqlValue::Null) {
                            continue;
                        }

                        if new_unique_values == other_unique_values {
                            let unique_col_names: Vec<String> = schema.unique_constraints[constraint_idx].clone();
                            return Err(ExecutorError::ConstraintViolation(format!(
                                "UNIQUE constraint violated: duplicate value for ({})",
                                unique_col_names.join(", ")
                            )));
                        }
                    }
                }

                // Enforce CHECK constraints
                if !schema.check_constraints.is_empty() {
                    let evaluator = crate::evaluator::ExpressionEvaluator::new(&schema);

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

