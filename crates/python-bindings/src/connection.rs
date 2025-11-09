//! Database connection implementation
//!
//! This module provides the Database class for establishing connections
//! to the in-memory vibesql database following DB-API 2.0 conventions.

use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::prelude::*;

use crate::cursor::Cursor;

/// Database connection object
///
/// Represents a connection to an in-memory vibesql database.
/// Follows DB-API 2.0 conventions for database connections.
///
/// # Example
/// ```python
/// db = vibesql.connect()
/// cursor = db.cursor()
/// cursor.execute("SELECT 1")
/// ```
#[pyclass]
pub struct Database {
    pub(crate) db: Arc<Mutex<storage::Database>>,
}

#[pymethods]
impl Database {
    /// Create a new database connection
    ///
    /// # Returns
    /// A new Database instance with an empty in-memory database.
    #[new]
    pub fn new() -> Self {
        Database { db: Arc::new(Mutex::new(storage::Database::new())) }
    }

    /// Create a cursor for executing queries
    ///
    /// # Returns
    /// A new Cursor object for executing SQL statements and fetching results.
    fn cursor(&self) -> PyResult<Cursor> {
        Cursor::new(Arc::clone(&self.db))
    }

    /// Close the database connection
    ///
    /// For in-memory databases, this is a no-op but provided for DB-API 2.0 compatibility.
    fn close(&self) -> PyResult<()> {
        // In-memory database, no cleanup needed
        Ok(())
    }

    /// Get version string
    ///
    /// # Returns
    /// A string containing the version identifier.
    fn version(&self) -> String {
        "vibesql-py 0.1.0".to_string()
    }
}
