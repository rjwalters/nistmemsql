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

        // Get the rows to insert based on the source
        let rows_to_insert = match &stmt.source {
            ast::InsertSource::Values(values) => {
                // For VALUES, we already have the rows as expressions
                values.clone()
            }
            ast::InsertSource::Select(select_stmt) => {
                // For SELECT, execute the query and get the result rows
                let select_executor = crate::SelectExecutor::new(db);
                let select_result = select_executor.execute_with_columns(select_stmt)?;

                // Validate column count
                if select_result.columns.len() != target_column_info.len() {
                    return Err(ExecutorError::UnsupportedExpression(format!(
                        "INSERT column count mismatch: expected {}, got {} from SELECT",
                        target_column_info.len(),
                        select_result.columns.len()
                    )));
                }

                // Convert SelectResult to Vec<Vec<Expression>> format
                // Each row becomes a Vec<Expression> with literals
                select_result
                    .rows
                    .into_iter()
                    .map(|row| {
                        row.values
                            .into_iter()
                            .map(ast::Expression::Literal)
                            .collect()
                    })
                    .collect()
            }
        };

        // Validate each row has correct number of values
        for (row_idx, value_exprs) in rows_to_insert.iter().enumerate() {
            if value_exprs.len() != target_column_info.len() {
                return Err(ExecutorError::UnsupportedExpression(format!(
                    "INSERT row {} column count mismatch: expected {}, got {}",
                    row_idx + 1,
                    target_column_info.len(),
                    value_exprs.len()
                )));
            }
        }

        // For multi-row INSERT, validate all rows first, then insert all
        // This ensures atomicity: all rows succeed or all fail
        let mut validated_rows = Vec::new();
        let mut primary_key_values: Vec<Vec<types::SqlValue>> = Vec::new(); // Track PK values for duplicate checking within batch
        let mut unique_constraint_values = if schema.get_unique_constraint_indices().is_empty() {
            Vec::new()
        } else {
            vec![Vec::new(); schema.get_unique_constraint_indices().len()]
        }; // Track UNIQUE values for each constraint

        for value_exprs in &rows_to_insert {
            // Build a complete row with values for all columns
            // Start with NULL for all columns, then fill in provided values
            let mut full_row_values = vec![types::SqlValue::Null; schema.columns.len()];

            for (expr, (col_idx, data_type)) in value_exprs.iter().zip(target_column_info.iter()) {
                // Evaluate expression (literals and DEFAULT)
                let value = evaluate_insert_expression(expr, &schema.columns[*col_idx])?;

                // Type check and coerce: ensure value matches column type
                let coerced_value = coerce_value(value, data_type)?;

                full_row_values[*col_idx] = coerced_value;
            }

            // Enforce NOT NULL constraints
            for (col_idx, col) in schema.columns.iter().enumerate() {
                if !col.nullable && full_row_values[col_idx] == types::SqlValue::Null {
                    return Err(ExecutorError::ConstraintViolation(format!(
                        "NOT NULL constraint violation: column '{}' in table '{}' cannot be NULL",
                        col.name, stmt.table_name
                    )));
                }
            }

            // Enforce PRIMARY KEY constraint (uniqueness)
            if let Some(pk_indices) = schema.get_primary_key_indices() {
                // Extract primary key values from the new row
                let new_pk_values: Vec<types::SqlValue> = pk_indices
                    .iter()
                    .map(|&idx| full_row_values[idx].clone())
                    .collect();

                // Check for duplicates within the batch of rows being inserted
                if primary_key_values.contains(&new_pk_values) {
                    let pk_col_names: Vec<String> = schema.primary_key
                        .as_ref()
                        .unwrap()
                        .clone();
                    return Err(ExecutorError::ConstraintViolation(format!(
                        "PRIMARY KEY constraint violated: duplicate key value for ({})",
                        pk_col_names.join(", ")
                    )));
                }

                // Check if any existing row has the same primary key
                let table = db.get_table(&stmt.table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

                for existing_row in table.scan() {
                    let existing_pk_values: Vec<types::SqlValue> = pk_indices
                        .iter()
                        .filter_map(|&idx| existing_row.get(idx).cloned())
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

                // Add to the batch for future duplicate checking
                primary_key_values.push(new_pk_values);
            }

            // Enforce UNIQUE constraints
            let unique_constraint_indices = schema.get_unique_constraint_indices();
            for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
                // Extract unique constraint values from the new row
                let new_unique_values: Vec<types::SqlValue> = unique_indices
                    .iter()
                    .map(|&idx| full_row_values[idx].clone())
                    .collect();

                // Skip if any value in the unique constraint is NULL
                // (NULL != NULL in SQL, so multiple NULLs are allowed)
                if new_unique_values.iter().any(|v| *v == types::SqlValue::Null) {
                    continue;
                }

                // Check for duplicates within the batch of rows being inserted
                if unique_constraint_values[constraint_idx].contains(&new_unique_values) {
                    let unique_col_names: Vec<String> = schema.unique_constraints[constraint_idx].clone();
                    return Err(ExecutorError::ConstraintViolation(format!(
                        "UNIQUE constraint violated: duplicate value for ({})",
                        unique_col_names.join(", ")
                    )));
                }

                // Check if any existing row has the same unique constraint values
                let table = db.get_table(&stmt.table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

                for existing_row in table.scan() {
                    let existing_unique_values: Vec<types::SqlValue> = unique_indices
                        .iter()
                        .filter_map(|&idx| existing_row.get(idx).cloned())
                        .collect();

                    // Skip if any existing value is NULL
                    if existing_unique_values.iter().any(|v| *v == types::SqlValue::Null) {
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

                // Add to the batch for future duplicate checking
                unique_constraint_values[constraint_idx].push(new_unique_values);
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

            // Enforce FOREIGN KEY constraints (child table)
            if !schema.foreign_keys.is_empty() {
                validate_foreign_key_constraints(db, &stmt.table_name, &full_row_values)?;
            }

            // Store validated row for insertion
            validated_rows.push(full_row_values);
        }

        // All rows validated successfully, now insert them
        let mut rows_inserted = 0;
        for full_row_values in validated_rows {
            let row = storage::Row::new(full_row_values);
            db.insert_row(&stmt.table_name, row).map_err(|e| {
                ExecutorError::UnsupportedExpression(format!("Storage error: {}", e))
            })?;
            rows_inserted += 1;
        }

        Ok(rows_inserted)
    }
}

/// Evaluate an INSERT expression to SqlValue
/// Supports literals and DEFAULT keyword
fn evaluate_insert_expression(
    expr: &ast::Expression,
    column: &catalog::ColumnSchema,
) -> Result<types::SqlValue, ExecutorError> {
    match expr {
        ast::Expression::Literal(lit) => Ok(lit.clone()),
        ast::Expression::Default => {
            // Use column's default value, or NULL if no default is defined
            if let Some(default_expr) = &column.default_value {
                // Evaluate the default expression (currently only supports literals)
                match default_expr {
                    ast::Expression::Literal(lit) => Ok(lit.clone()),
                    _ => Err(ExecutorError::UnsupportedExpression(
                        format!("Complex default expressions not yet supported for column '{}'", column.name)
                    ))
                }
            } else {
                // No default value defined, use NULL
                Ok(types::SqlValue::Null)
            }
        }
        _ => Err(ExecutorError::UnsupportedExpression(
            "INSERT only supports literal values and DEFAULT".to_string(),
        )),
    }
}

/// Coerce a value to match the expected column type
/// Performs automatic type conversions where appropriate
fn coerce_value(
    value: types::SqlValue,
    expected_type: &types::DataType,
) -> Result<types::SqlValue, ExecutorError> {
    use types::{DataType, SqlValue};

    // NULL is valid for any type (NOT NULL constraint checked separately)
    if matches!(value, SqlValue::Null) {
        return Ok(value);
    }

    // Check type compatibility with automatic coercion
    match (&value, expected_type) {
        // Exact matches - no coercion needed
        (SqlValue::Integer(_), DataType::Integer) => Ok(value),
        (SqlValue::Varchar(_), DataType::Varchar { .. }) => Ok(value),
        (SqlValue::Character(_), DataType::Character { .. }) => Ok(value),
        (SqlValue::Boolean(_), DataType::Boolean) => Ok(value),
        (SqlValue::Float(_), DataType::Float { .. }) => Ok(value),
        (SqlValue::Real(_), DataType::Real) => Ok(value),
        (SqlValue::Double(_), DataType::DoublePrecision) => Ok(value),
        (SqlValue::Date(_), DataType::Date) => Ok(value),
        (SqlValue::Time(_), DataType::Time { .. }) => Ok(value),
        (SqlValue::Timestamp(_), DataType::Timestamp { .. }) => Ok(value),
        (SqlValue::Interval(_), DataType::Interval { .. }) => Ok(value),
        (SqlValue::Smallint(_), DataType::Smallint) => Ok(value),
        (SqlValue::Bigint(_), DataType::Bigint) => Ok(value),
        (SqlValue::Numeric(_), DataType::Numeric { .. }) => Ok(value),
        (SqlValue::Numeric(_), DataType::Decimal { .. }) => Ok(value),

        // Numeric literal → Float/Real/Double
        (SqlValue::Numeric(s), DataType::Float { .. }) => {
            s.parse::<f32>()
                .map(SqlValue::Float)
                .map_err(|_| ExecutorError::UnsupportedExpression(format!(
                    "Cannot convert numeric '{}' to Float", s
                )))
        }
        (SqlValue::Numeric(s), DataType::Real) => {
            s.parse::<f32>()
                .map(SqlValue::Real)
                .map_err(|_| ExecutorError::UnsupportedExpression(format!(
                    "Cannot convert numeric '{}' to Real", s
                )))
        }
        (SqlValue::Numeric(s), DataType::DoublePrecision) => {
            s.parse::<f64>()
                .map(SqlValue::Double)
                .map_err(|_| ExecutorError::UnsupportedExpression(format!(
                    "Cannot convert numeric '{}' to DoublePrecision", s
                )))
        }

        // Numeric literal → Integer types
        (SqlValue::Numeric(s), DataType::Integer) => {
            s.parse::<i64>()
                .map(SqlValue::Integer)
                .map_err(|_| ExecutorError::UnsupportedExpression(format!(
                    "Cannot convert numeric '{}' to Integer (must be whole number)", s
                )))
        }
        (SqlValue::Numeric(s), DataType::Smallint) => {
            s.parse::<i16>()
                .map(SqlValue::Smallint)
                .map_err(|_| ExecutorError::UnsupportedExpression(format!(
                    "Cannot convert numeric '{}' to Smallint (must be whole number)", s
                )))
        }
        (SqlValue::Numeric(s), DataType::Bigint) => {
            s.parse::<i64>()
                .map(SqlValue::Bigint)
                .map_err(|_| ExecutorError::UnsupportedExpression(format!(
                    "Cannot convert numeric '{}' to Bigint (must be whole number)", s
                )))
        }

        // Integer → Float types (safe widening conversion)
        (SqlValue::Integer(i), DataType::Float { .. }) => Ok(SqlValue::Float(*i as f32)),
        (SqlValue::Integer(i), DataType::Real) => Ok(SqlValue::Real(*i as f32)),
        (SqlValue::Integer(i), DataType::DoublePrecision) => Ok(SqlValue::Double(*i as f64)),
        (SqlValue::Smallint(i), DataType::Float { .. }) => Ok(SqlValue::Float(*i as f32)),
        (SqlValue::Smallint(i), DataType::Real) => Ok(SqlValue::Real(*i as f32)),
        (SqlValue::Smallint(i), DataType::DoublePrecision) => Ok(SqlValue::Double(*i as f64)),
        (SqlValue::Bigint(i), DataType::Float { .. }) => Ok(SqlValue::Float(*i as f32)),
        (SqlValue::Bigint(i), DataType::Real) => Ok(SqlValue::Real(*i as f32)),
        (SqlValue::Bigint(i), DataType::DoublePrecision) => Ok(SqlValue::Double(*i as f64)),

        // Integer widening conversions
        (SqlValue::Smallint(i), DataType::Integer) => Ok(SqlValue::Integer(*i as i64)),
        (SqlValue::Smallint(i), DataType::Bigint) => Ok(SqlValue::Bigint(*i as i64)),
        (SqlValue::Integer(i), DataType::Bigint) => Ok(SqlValue::Bigint(*i)),

        // Varchar ↔ Character conversions
        (SqlValue::Varchar(s), DataType::Character { length }) => {
            let s = if s.len() > *length {
                s[..*length].to_string()  // Truncate
            } else {
                format!("{:width$}", s, width = length)  // Pad with spaces
            };
            Ok(SqlValue::Character(s))
        }
        (SqlValue::Character(s), DataType::Varchar { .. }) => {
            Ok(SqlValue::Varchar(s.trim_end().to_string()))  // Remove trailing spaces
        }

        // Type mismatch
        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Type mismatch: expected {:?}, got {:?}",
            expected_type, value
        ))),
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
        let fk_values: Vec<types::SqlValue> = fk
            .column_indices
            .iter()
            .map(|&idx| row_values[idx].clone())
            .collect();

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
                .all(|(&parent_idx, fk_val)| {
                    parent_row.get(parent_idx) == Some(fk_val)
                })
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

