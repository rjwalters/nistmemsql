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

    // Get indices once before the closure (performance optimization)
    let pk_indices = schema.get_primary_key_indices();
    let unique_constraint_indices = schema.get_unique_constraint_indices();

    // Check PRIMARY KEY conflict
    if let Some(ref pk_idx) = pk_indices {
        pk_match = Some(pk_idx.iter().map(|&idx| row_values[idx].clone()).collect());
    }

    // Check UNIQUE constraints conflicts
    for unique_indices in unique_constraint_indices.iter() {
        let unique_values: Vec<vibesql_types::SqlValue> =
            unique_indices.iter().map(|&idx| row_values[idx].clone()).collect();

        // Skip if contains NULL (NULLs don't cause conflicts in UNIQUE constraints)
        if unique_values.contains(&vibesql_types::SqlValue::Null) {
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
            if let Some(ref pk_idx) = pk_indices {
                let row_pk_values: Vec<vibesql_types::SqlValue> =
                    pk_idx.iter().map(|&idx| row.values[idx].clone()).collect();
                if &row_pk_values == pk_values {
                    return true; // Delete this row (early return for performance)
                }
            }
        }

        // Check if this row matches any UNIQUE constraint
        for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
            if let Some(unique_values) = unique_matches.get(constraint_idx).and_then(|v| v.as_ref())
            {
                let row_unique_values: Vec<vibesql_types::SqlValue> =
                    unique_indices.iter().map(|&idx| row.values[idx].clone()).collect();
                if row_unique_values == *unique_values {
                    return true; // Delete this row (early return for performance)
                }
            }
        }

        false // Don't delete this row
    });

    // Return success (deleted count not needed, conflicts resolved)
    let _ = deleted; // Explicitly acknowledge we don't use the count
    Ok(())
}
