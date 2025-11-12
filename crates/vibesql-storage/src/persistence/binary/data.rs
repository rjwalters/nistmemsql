// ============================================================================
// Table Data Serialization
// ============================================================================
//
// Handles reading and writing table row data.

use std::io::{Read, Write};
use crate::{Database, StorageError};
use super::io::*;
use super::value::{read_sql_value, write_sql_value};

pub fn write_data<W: Write>(writer: &mut W, db: &Database) -> Result<(), StorageError> {
    let table_names = db.catalog.list_tables();

    for table_name in table_names {
        if let Some(table) = db.get_table(&table_name) {
            // Write table name
            write_string(writer, &table_name)?;

            // Write row count
            write_u64(writer, table.row_count() as u64)?;

            // Write each row
            for row in table.scan() {
                for value in &row.values {
                    write_sql_value(writer, value)?;
                }
            }
        }
    }

    Ok(())
}

pub fn read_data<R: Read>(reader: &mut R, db: &mut Database) -> Result<(), StorageError> {
    let table_count = db.catalog.list_tables().len();

    for _ in 0..table_count {
        // Read table name from file (don't rely on list_tables() ordering)
        let table_name = read_string(reader)?;
        let row_count = read_u64(reader)?;

        // Get column count first
        let column_count = db
            .get_table(&table_name)
            .map(|t| t.schema.columns.len())
            .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

        // Now get mutable reference and insert rows
        if let Some(table) = db.get_table_mut(&table_name) {
            for _ in 0..row_count {
                let mut values = Vec::with_capacity(column_count);
                for _ in 0..column_count {
                    values.push(read_sql_value(reader)?);
                }

                let row = crate::Row { values };
                table.insert(row).map_err(|e| {
                    StorageError::NotImplemented(format!("Failed to insert row: {}", e))
                })?;
            }
        }
    }

    Ok(())
}
