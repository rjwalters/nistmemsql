use crate::errors::ExecutorError;

/// Executor for INSERT statements
pub struct InsertExecutor;

impl InsertExecutor {
    /// Execute an INSERT statement
    /// Returns number of rows inserted
    pub fn execute(
        db: &mut storage::Database,
        stmt: &ast::InsertStmt,
    ) -> Result<usize, ExecutorError> {
        // Get table schema from catalog (clone to avoid borrow issues)
        let schema = db
            .catalog
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?
            .clone();

        // Determine target column indices and types
        let target_column_info: Vec<(usize, types::DataType)> = if stmt.columns.is_empty() {
            // No columns specified: INSERT INTO t VALUES (...)
            // Use all columns in schema order
            schema
                .columns
                .iter()
                .enumerate()
                .map(|(idx, col)| (idx, col.data_type.clone()))
                .collect()
        } else {
            // Columns specified: INSERT INTO t (col1, col2) VALUES (...)
            // Validate and resolve columns
            stmt.columns
                .iter()
                .map(|col_name| {
                    schema
                        .get_column_index(col_name)
                        .map(|idx| {
                            let col = &schema.columns[idx];
                            (idx, col.data_type.clone())
                        })
                        .ok_or_else(|| ExecutorError::ColumnNotFound(col_name.clone()))
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        // Validate each row has correct number of values
        for (row_idx, value_exprs) in stmt.values.iter().enumerate() {
            if value_exprs.len() != target_column_info.len() {
                return Err(ExecutorError::UnsupportedExpression(format!(
                    "INSERT row {} column count mismatch: expected {}, got {}",
                    row_idx + 1,
                    target_column_info.len(),
                    value_exprs.len()
                )));
            }
        }

        let mut rows_inserted = 0;

        for value_exprs in &stmt.values {
            // Build a complete row with values for all columns
            // Start with NULL for all columns, then fill in provided values
            let mut full_row_values = vec![types::SqlValue::Null; schema.columns.len()];

            for (expr, (col_idx, data_type)) in value_exprs.iter().zip(target_column_info.iter()) {
                // Evaluate expression (only literals supported initially)
                let value = evaluate_literal_expression(expr)?;

                // Type check: ensure value matches column type
                validate_type(&value, data_type)?;

                full_row_values[*col_idx] = value;
            }

            // Enforce NOT NULL constraints
            for (col_idx, col) in schema.columns.iter().enumerate() {
                if !col.nullable && full_row_values[col_idx] == types::SqlValue::Null {
                    return Err(ExecutorError::ConstraintViolation(format!(
                        "NOT NULL constraint violated for column '{}'",
                        col.name
                    )));
                }
            }

            // Enforce PRIMARY KEY constraint (uniqueness)
            if let Some(pk_indices) = schema.get_primary_key_indices() {
                // Extract primary key values from the new row
                let new_pk_values: Vec<&types::SqlValue> = pk_indices
                    .iter()
                    .map(|&idx| &full_row_values[idx])
                    .collect();

                // Check if any existing row has the same primary key
                let table = db.get_table(&stmt.table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

                for existing_row in table.scan() {
                    let existing_pk_values: Vec<&types::SqlValue> = pk_indices
                        .iter()
                        .filter_map(|&idx| existing_row.get(idx))
                        .collect();

                    if new_pk_values == existing_pk_values {
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
                // Extract unique constraint values from the new row
                let new_unique_values: Vec<&types::SqlValue> = unique_indices
                    .iter()
                    .map(|&idx| &full_row_values[idx])
                    .collect();

                // Skip if any value in the unique constraint is NULL
                // (NULL != NULL in SQL, so multiple NULLs are allowed)
                if new_unique_values.iter().any(|v| **v == types::SqlValue::Null) {
                    continue;
                }

                // Check if any existing row has the same unique constraint values
                let table = db.get_table(&stmt.table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

                for existing_row in table.scan() {
                    let existing_unique_values: Vec<&types::SqlValue> = unique_indices
                        .iter()
                        .filter_map(|&idx| existing_row.get(idx))
                        .collect();

                    // Skip if any existing value is NULL
                    if existing_unique_values.iter().any(|v| **v == types::SqlValue::Null) {
                        continue;
                    }

                    if new_unique_values == existing_unique_values {
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
                // Create a row from the values to evaluate the expression
                let row = storage::Row::new(full_row_values.clone());
                let evaluator = crate::evaluator::ExpressionEvaluator::new(&schema);

                for (constraint_name, check_expr) in &schema.check_constraints {
                    // Evaluate the CHECK expression against the row
                    let result = evaluator.eval(check_expr, &row)?;

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

            // Insert the row
            let row = storage::Row::new(full_row_values);
            db.insert_row(&stmt.table_name, row).map_err(|e| {
                ExecutorError::UnsupportedExpression(format!("Storage error: {}", e))
            })?;
            rows_inserted += 1;
        }

        Ok(rows_inserted)
    }
}

/// Evaluate a literal expression to SqlValue
fn evaluate_literal_expression(expr: &ast::Expression) -> Result<types::SqlValue, ExecutorError> {
    match expr {
        ast::Expression::Literal(lit) => Ok(lit.clone()),
        _ => Err(ExecutorError::UnsupportedExpression(
            "INSERT only supports literal values".to_string(),
        )),
    }
}

/// Validate that a value matches the expected column type
fn validate_type(
    value: &types::SqlValue,
    expected_type: &types::DataType,
) -> Result<(), ExecutorError> {
    use types::{DataType, SqlValue};

    // NULL is valid for any type (NOT NULL constraint checked separately)
    if matches!(value, SqlValue::Null) {
        return Ok(());
    }

    // Check type compatibility
    match (value, expected_type) {
        (SqlValue::Integer(_), DataType::Integer) => Ok(()),
        (SqlValue::Varchar(_), DataType::Varchar { .. }) => Ok(()),
        (SqlValue::Boolean(_), DataType::Boolean) => Ok(()),
        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Type mismatch: expected {:?}, got {:?}",
            expected_type, value
        ))),
    }
}

