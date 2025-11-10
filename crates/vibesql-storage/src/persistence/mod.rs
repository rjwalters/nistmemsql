// ============================================================================
// Database Persistence Module
// ============================================================================
//
// Provides two persistence formats:
//
// 1. SQL dump (human-readable, portable):
//    - `save`: SQL dump generation and serialization
//    - `load`: SQL dump parsing and deserialization utilities
//
// 2. Binary format (fast, efficient):
//    - `binary`: Binary serialization/deserialization
//
// The `save_sql_dump` and `save_binary` methods are implemented directly
// on the `Database` type.
//
// Load utilities are exported for use by the executor layer.

use std::{fs, io::Read, path::Path};

mod binary;
pub mod load;
mod save;

#[cfg(test)]
mod tests;

/// Persistence format detection and auto-loading
impl crate::Database {
    /// Load database from file with automatic format detection
    ///
    /// Detects format based on:
    /// 1. File extension (.vbsql for binary, .sql for SQL dump)
    /// 2. Magic number in file header (if extension is ambiguous)
    ///
    /// # Example
    /// ```no_run
    /// # use vibesql_storage::Database;
    /// // Auto-detects format from extension and content
    /// let db = Database::load("database.vbsql").unwrap();
    /// let db2 = Database::load("database.sql").unwrap();
    /// ```
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, crate::StorageError> {
        let path_ref = path.as_ref();
        let format = detect_format(path_ref)?;

        match format {
            PersistenceFormat::Binary => Self::load_binary(path),
            PersistenceFormat::Sql => {
                // SQL dump requires executor for parsing, so we return an error
                // directing users to use the executor layer's load_sql_dump function
                Err(crate::StorageError::NotImplemented(
                    "SQL dump loading requires the executor layer. \
                     Use vibesql_executor::load_sql_dump() instead, or use binary format (.vbsql)"
                        .to_string(),
                ))
            }
        }
    }
}

/// Persistence format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceFormat {
    /// SQL dump format (.sql) - human-readable text
    Sql,
    /// Binary format (.vbsql) - efficient binary
    Binary,
}

/// Detect persistence format from file
fn detect_format<P: AsRef<Path>>(path: P) -> Result<PersistenceFormat, crate::StorageError> {
    let path_ref = path.as_ref();

    // First try extension-based detection
    if let Some(ext) = path_ref.extension() {
        match ext.to_str() {
            Some("vbsql") => return Ok(PersistenceFormat::Binary),
            Some("sql") => return Ok(PersistenceFormat::Sql),
            _ => {}
        }
    }

    // If extension doesn't match, check magic number
    let mut file = fs::File::open(path_ref).map_err(|e| {
        crate::StorageError::NotImplemented(format!("Failed to open file {:?}: {}", path_ref, e))
    })?;

    let mut magic = [0u8; 5];
    if file.read_exact(&mut magic).is_ok() && &magic == b"VBSQL" {
        return Ok(PersistenceFormat::Binary);
    }

    // Default to SQL if we can't determine
    Ok(PersistenceFormat::Sql)
}
