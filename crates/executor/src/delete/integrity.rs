//! Foreign key integrity checking for DELETE operations

use crate::errors::ExecutorError;

/// Check that no child tables reference a row that is about to be deleted or updated.
///
/// This function enforces referential integrity by ensuring that no child table
/// rows reference the parent row being deleted through foreign key constraints.
///
/// # Arguments
///
/// * `db` - The database to check
/// * `parent_table_name` - Name of the table containing the row to delete
/// * `parent_row` - The row being deleted
///
/// # Returns
///
/// * `Ok(())` if no references exist or parent table has no primary key
/// * `Err(ExecutorError::ConstraintViolation)` if child references exist
pub fn check_no_child_references(
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

    // Optimization: Check if any table in the database has foreign keys at all
    // If not, skip the expensive scan of all tables
    let has_any_fks = db.catalog.list_tables().iter().any(|table_name| {
        db.catalog
            .get_table(table_name)
            .map(|schema| !schema.foreign_keys.is_empty())
            .unwrap_or(false)
    });

    if !has_any_fks {
        return Ok(());
    }

    // Scan all tables in the database to find foreign keys that reference this table.
    for table_name in db.catalog.list_tables() {
        let child_schema = db.catalog.get_table(&table_name).unwrap();

        // Skip tables without foreign keys (optimization)
        if child_schema.foreign_keys.is_empty() {
            continue;
        }

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
