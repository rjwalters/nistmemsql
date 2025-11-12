use crate::errors::ExecutorError;

/// Handle REPLACE logic: detect conflicts and delete conflicting rows
/// Returns Ok(()) if no conflict or conflict was resolved
pub fn handle_replace_conflicts(
    db: &mut vibesql_storage::Database,
    table_name: &str,
    schema: &vibesql_catalog::TableSchema,
    row_values: &[vibesql_types::SqlValue],
) -> Result<(), ExecutorError> {
    // Build list of row values to match for deletion
    let mut pk_match: Option<Vec<vibesql_types::SqlValue>> = None;
    let mut unique_matches: Vec<Option<Vec<vibesql_types::SqlValue>>> = Vec::new();

    // Check PRIMARY KEY conflict
    if let Some(pk_indices) = schema.get_primary_key_indices() {
        pk_match = Some(pk_indices.iter().map(|&idx| row_values[idx].clone()).collect());
    }

    // Check UNIQUE constraints conflicts
    let unique_constraint_indices = schema.get_unique_constraint_indices();
    for unique_indices in unique_constraint_indices {
        let unique_values: Vec<vibesql_types::SqlValue> =
            unique_indices.iter().map(|&idx| row_values[idx].clone()).collect();

        // Skip if contains NULL (NULLs don't cause conflicts in UNIQUE constraints)
        if unique_values.iter().any(|v| *v == vibesql_types::SqlValue::Null) {
            unique_matches.push(None);
        } else {
            unique_matches.push(Some(unique_values));
        }
    }

    // Delete conflicting rows using delete_where with a predicate
    let table_mut = db
        .get_table_mut(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let deleted = table_mut.delete_where(|row| {
        // Check if this row matches the PRIMARY KEY
        if let Some(ref pk_values) = pk_match {
            if let Some(pk_indices) = schema.get_primary_key_indices() {
                let row_pk_values: Vec<vibesql_types::SqlValue> =
                    pk_indices.iter().map(|&idx| row.values[idx].clone()).collect();
                if &row_pk_values == pk_values {
                    return true; // Delete this row
                }
            }
        }

        // Check if this row matches any UNIQUE constraint
        let unique_constraint_indices = schema.get_unique_constraint_indices();
        for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
            if let Some(unique_values) = unique_matches.get(constraint_idx).and_then(|v| v.as_ref()) {
                let row_unique_values: Vec<vibesql_types::SqlValue> =
                    unique_indices.iter().map(|&idx| row.values[idx].clone()).collect();
                if row_unique_values == *unique_values {
                    return true; // Delete this row
                }
            }
        }

        false // Don't delete this row
    });

    // Return success (deleted count not needed, conflicts resolved)
    drop(deleted); // Explicitly acknowledge we don't use the count
    Ok(())
}
