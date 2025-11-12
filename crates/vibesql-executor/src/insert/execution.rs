use crate::{errors::ExecutorError, privilege_checker::PrivilegeChecker};

/// Execute an INSERT statement
/// Returns number of rows inserted
pub fn execute_insert(
    db: &mut vibesql_storage::Database,
    stmt: &vibesql_ast::InsertStmt,
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
    let target_column_info =
        super::validation::resolve_target_columns(&schema, &stmt.table_name, &stmt.columns)?;

    // Get the rows to insert based on the source
    let rows_to_insert = match &stmt.source {
        vibesql_ast::InsertSource::Values(values) => {
            // For VALUES, we already have the rows as expressions
            values.clone()
        }
        vibesql_ast::InsertSource::Select(select_stmt) => {
            // Try bulk transfer optimization first (Phase 1-3)
            // This provides 10-50x performance improvement for compatible schemas
            if stmt.columns.is_empty() {
                // Only attempt bulk transfer for INSERT INTO table SELECT (no column list)
                if let Some(count) =
                    super::bulk_transfer::try_bulk_transfer(db, &stmt.table_name, select_stmt)?
                {
                    // Fast path succeeded, return early
                    return Ok(count);
                }
            }

            // Fall back to normal path: execute SELECT and convert to expressions
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
                .map(|row| row.values.into_iter().map(vibesql_ast::Expression::Literal).collect())
                .collect()
        }
    };

    // Validate each row has correct number of values
    super::validation::validate_row_column_counts(&rows_to_insert, target_column_info.len())?;

    // For multi-row INSERT, validate all rows first, then insert all
    // This ensures atomicity: all rows succeed or all fail
    let mut validated_rows = Vec::new();
    let mut primary_key_values: Vec<Vec<vibesql_types::SqlValue>> = Vec::new(); // Track PK values for duplicate checking within batch
    let mut unique_constraint_values = if schema.get_unique_constraint_indices().is_empty() {
        Vec::new()
    } else {
        vec![Vec::new(); schema.get_unique_constraint_indices().len()]
    }; // Track UNIQUE values for each constraint

    for value_exprs in &rows_to_insert {
        // Build a complete row with values for all columns
        // Start with NULL for all columns, then fill in provided values
        let mut full_row_values = vec![vibesql_types::SqlValue::Null; schema.columns.len()];

        for (expr, (col_idx, data_type)) in value_exprs.iter().zip(target_column_info.iter()) {
            // Evaluate expression (literals and DEFAULT)
            let value =
                super::defaults::evaluate_insert_expression(expr, &schema.columns[*col_idx])?;

            // Type check and coerce: ensure value matches column type
            let coerced_value = super::validation::coerce_value(value, data_type)?;

            full_row_values[*col_idx] = coerced_value;
        }

        // Apply DEFAULT values for unspecified columns
        super::defaults::apply_default_values(&schema, &mut full_row_values, db)?;

        // Validate all constraints in a single pass and extract index keys
        // Skip PK/UNIQUE duplicate checks if using REPLACE conflict clause or ON DUPLICATE KEY UPDATE
        let skip_duplicate_checks =
            matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Replace))
                || stmt.on_duplicate_key_update.is_some();
        let validator = super::row_validator::RowValidator::new(
            db,
            &schema,
            &stmt.table_name,
            &primary_key_values,
            &unique_constraint_values,
            skip_duplicate_checks,
        );
        let validation_result = validator.validate(&full_row_values)?;

        // Track PK values for batch duplicate checking (using pre-extracted keys)
        if let Some(pk_values) = validation_result.primary_key {
            primary_key_values.push(pk_values);
        }

        // Track UNIQUE values for batch duplicate checking (using pre-extracted keys)
        for (constraint_idx, unique_values) in validation_result.unique_keys.into_iter().enumerate()
        {
            if let Some(values) = unique_values {
                unique_constraint_values[constraint_idx].push(values);
            }
        }

        // Store validated row for insertion
        validated_rows.push(full_row_values);
    }

    // All rows validated successfully, now insert them
    let mut rows_inserted = 0;
    for full_row_values in validated_rows {
        // Check if ON DUPLICATE KEY UPDATE is specified
        if let Some(ref assignments) = stmt.on_duplicate_key_update {
            // Try to update an existing row if there's a conflict
            let update_result = super::duplicate_key_update::handle_duplicate_key_update(
                db,
                &stmt.table_name,
                &schema,
                &full_row_values,
                assignments,
            )?;

            if update_result.is_some() {
                // Row was updated, count it
                rows_inserted += 1;
                continue;
            }
            // No conflict, fall through to insert
        } else if matches!(
            stmt.conflict_clause,
            Some(vibesql_ast::ConflictClause::Replace)
        ) {
            // If REPLACE conflict clause, delete conflicting rows first
            super::replace::handle_replace_conflicts(
                db,
                &stmt.table_name,
                &schema,
                &full_row_values,
            )?;
        }

        // Insert the row
        let row = vibesql_storage::Row::new(full_row_values);
        db.insert_row(&stmt.table_name, row)
            .map_err(|e| ExecutorError::UnsupportedExpression(format!("Storage error: {}", e)))?;
        rows_inserted += 1;
    }

    Ok(rows_inserted)
}
