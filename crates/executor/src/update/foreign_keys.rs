//! Foreign key constraint validation for UPDATE operations

use crate::errors::ExecutorError;
use storage::Database;

/// Validator for foreign key constraints
pub struct ForeignKeyValidator;

impl ForeignKeyValidator {
    /// Validate FOREIGN KEY constraints for a new row
    ///
    /// Checks that all foreign key values in the row reference existing parent rows.
    /// NULL values in foreign keys are allowed (not considered violations).
    pub fn validate_constraints(
        db: &Database,
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
    ///
    /// This is called before updating a primary key to ensure referential integrity.
    pub fn check_no_child_references(
        db: &Database,
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
}
