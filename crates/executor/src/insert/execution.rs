use crate::errors::ExecutorError;
use crate::privilege_checker::PrivilegeChecker;

/// Execute an INSERT statement
/// Returns number of rows inserted
pub fn execute_insert(
    db: &mut storage::Database,
    stmt: &ast::InsertStmt,
) -> Result<usize, ExecutorError> {
    // Check INSERT privilege on the table
    PrivilegeChecker::check_insert(db, &stmt.table_name)?;

    // Get table schema from catalog (clone to avoid borrow issues)
    let schema = db
        .catalog
        .get_table(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?
        .clone();

    // Determine target column indices and types
    let target_column_info = super::validation::resolve_target_columns(&schema, &stmt.columns)?;

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
                .map(|row| row.values.into_iter().map(ast::Expression::Literal).collect())
                .collect()
        }
    };

    // Validate each row has correct number of values
    super::validation::validate_row_column_counts(&rows_to_insert, target_column_info.len())?;

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
            let value = super::defaults::evaluate_insert_expression(expr, &schema.columns[*col_idx])?;

            // Type check and coerce: ensure value matches column type
            let coerced_value = super::validation::coerce_value(value, data_type)?;

            full_row_values[*col_idx] = coerced_value;
        }

        // Apply DEFAULT values for unspecified columns
        super::defaults::apply_default_values(&schema, &mut full_row_values)?;

        // Enforce NOT NULL constraints
        super::constraints::enforce_not_null_constraints(&schema, &stmt.table_name, &full_row_values)?;

        // Enforce PRIMARY KEY constraint (uniqueness)
        super::constraints::enforce_primary_key_constraint(
            db,
            &schema,
            &stmt.table_name,
            &full_row_values,
            &primary_key_values,
        )?;

        // Track PK values for batch duplicate checking
        if let Some(pk_indices) = schema.get_primary_key_indices() {
            let new_pk_values: Vec<types::SqlValue> =
                pk_indices.iter().map(|&idx| full_row_values[idx].clone()).collect();
            primary_key_values.push(new_pk_values);
        }

        // Enforce UNIQUE constraints
        super::constraints::enforce_unique_constraints(
            db,
            &schema,
            &stmt.table_name,
            &full_row_values,
            &unique_constraint_values,
        )?;

        // Track UNIQUE values for batch duplicate checking
        let unique_constraint_indices = schema.get_unique_constraint_indices();
        for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
            let new_unique_values: Vec<types::SqlValue> =
                unique_indices.iter().map(|&idx| full_row_values[idx].clone()).collect();

            // Skip if any value is NULL (multiple NULLs allowed in UNIQUE constraints)
            if !new_unique_values.iter().any(|v| *v == types::SqlValue::Null) {
                unique_constraint_values[constraint_idx].push(new_unique_values);
            }
        }

        // Enforce CHECK constraints
        super::constraints::enforce_check_constraints(&schema, &full_row_values)?;

        // Enforce FOREIGN KEY constraints (child table)
        if !schema.foreign_keys.is_empty() {
            super::foreign_keys::validate_foreign_key_constraints(db, &stmt.table_name, &full_row_values)?;
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
