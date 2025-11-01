//! Python bindings for nistmemsql using PyO3
//!
//! This module provides Python bindings following DB-API 2.0 conventions
//! to expose the Rust database library to Python for benchmarking and usage.

// Suppress PyO3 macro warnings
#![allow(non_local_definitions)]

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use std::sync::{Arc, Mutex};

pyo3::create_exception!(nistmemsql, DatabaseError, PyException);
pyo3::create_exception!(nistmemsql, OperationalError, DatabaseError);
pyo3::create_exception!(nistmemsql, ProgrammingError, DatabaseError);

/// Converts a Rust SqlValue to a Python object
fn sqlvalue_to_py(py: Python, value: &types::SqlValue) -> PyResult<Py<PyAny>> {
    Ok(match value {
        types::SqlValue::Integer(i) => (*i).into_pyobject(py)?.into_any().unbind(),
        types::SqlValue::Smallint(i) => (*i).into_pyobject(py)?.into_any().unbind(),
        types::SqlValue::Bigint(i) => (*i).into_pyobject(py)?.into_any().unbind(),
        types::SqlValue::Float(f) => (*f as f64).into_pyobject(py)?.into_any().unbind(),
        types::SqlValue::Real(f) => (*f as f64).into_pyobject(py)?.into_any().unbind(),
        types::SqlValue::Double(f) => (*f).into_pyobject(py)?.into_any().unbind(),
        types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => {
            s.into_pyobject(py)?.into_any().unbind()
        }
        types::SqlValue::Boolean(b) => b.into_pyobject(py)?.to_owned().into_any().unbind(),
        types::SqlValue::Numeric(n) => n.into_pyobject(py)?.into_any().unbind(),
        types::SqlValue::Date(s)
        | types::SqlValue::Time(s)
        | types::SqlValue::Timestamp(s)
        | types::SqlValue::Interval(s) => s.into_pyobject(py)?.into_any().unbind(),
        types::SqlValue::Null => py.None(),
    })
}

/// Database connection object
///
/// Represents a connection to an in-memory nistmemsql database.
#[pyclass]
struct Database {
    db: Arc<Mutex<storage::Database>>,
}

#[pymethods]
impl Database {
    /// Create a new database connection
    #[new]
    fn new() -> Self {
        Database { db: Arc::new(Mutex::new(storage::Database::new())) }
    }

    /// Create a cursor for executing queries
    ///
    /// Returns:
    ///     Cursor: A new cursor object
    fn cursor(&self) -> PyResult<Cursor> {
        Ok(Cursor { db: Arc::clone(&self.db), last_result: None })
    }

    /// Close the database connection
    fn close(&self) -> PyResult<()> {
        // In-memory database, no cleanup needed
        Ok(())
    }

    /// Get version string
    fn version(&self) -> String {
        "nistmemsql-py 0.1.0".to_string()
    }
}

/// Query result storage
enum QueryResultData {
    /// SELECT query result with columns and rows
    Select {
        #[allow(dead_code)]
        columns: Vec<String>,
        rows: Vec<storage::Row>,
    },
    /// DML/DDL result with rows affected and message
    Execute {
        rows_affected: usize,
        #[allow(dead_code)]
        message: String,
    },
}

/// Cursor object for executing SQL statements
///
/// Follows DB-API 2.0 conventions for query execution and result fetching.
#[pyclass]
struct Cursor {
    db: Arc<Mutex<storage::Database>>,
    last_result: Option<QueryResultData>,
}

#[pymethods]
impl Cursor {
    /// Execute a SQL statement
    ///
    /// Args:
    ///     sql (str): The SQL statement to execute
    ///
    /// Returns:
    ///     Cursor: Returns self for method chaining
    fn execute(&mut self, sql: &str) -> PyResult<()> {
        // Parse the SQL
        let stmt = parser::Parser::parse_sql(sql)
            .map_err(|e| ProgrammingError::new_err(format!("Parse error: {:?}", e)))?;

        // Execute based on statement type
        match stmt {
            ast::Statement::Select(select_stmt) => {
                let db = self.db.lock().unwrap();
                let select_executor = executor::SelectExecutor::new(&db);
                let result = select_executor
                    .execute_with_columns(&select_stmt)
                    .map_err(|e| OperationalError::new_err(format!("Execution error: {:?}", e)))?;

                self.last_result =
                    Some(QueryResultData::Select { columns: result.columns, rows: result.rows });

                Ok(())
            }
            ast::Statement::CreateTable(create_stmt) => {
                let mut db = self.db.lock().unwrap();
                executor::CreateTableExecutor::execute(&create_stmt, &mut db)
                    .map_err(|e| OperationalError::new_err(format!("Execution error: {:?}", e)))?;

                self.last_result = Some(QueryResultData::Execute {
                    rows_affected: 0,
                    message: format!("Table '{}' created successfully", create_stmt.table_name),
                });

                Ok(())
            }
            ast::Statement::DropTable(drop_stmt) => {
                let mut db = self.db.lock().unwrap();
                let message = executor::DropTableExecutor::execute(&drop_stmt, &mut db)
                    .map_err(|e| OperationalError::new_err(format!("Execution error: {:?}", e)))?;

                self.last_result = Some(QueryResultData::Execute { rows_affected: 0, message });

                Ok(())
            }
            ast::Statement::Insert(insert_stmt) => {
                let mut db = self.db.lock().unwrap();
                let row_count = executor::InsertExecutor::execute(&mut db, &insert_stmt)
                    .map_err(|e| OperationalError::new_err(format!("Execution error: {:?}", e)))?;

                self.last_result = Some(QueryResultData::Execute {
                    rows_affected: row_count,
                    message: format!(
                        "{} row{} inserted into '{}'",
                        row_count,
                        if row_count == 1 { "" } else { "s" },
                        insert_stmt.table_name
                    ),
                });

                Ok(())
            }
            ast::Statement::Update(update_stmt) => {
                let mut db = self.db.lock().unwrap();
                let row_count = executor::UpdateExecutor::execute(&update_stmt, &mut db)
                    .map_err(|e| OperationalError::new_err(format!("Execution error: {:?}", e)))?;

                self.last_result = Some(QueryResultData::Execute {
                    rows_affected: row_count,
                    message: format!(
                        "{} row{} updated in '{}'",
                        row_count,
                        if row_count == 1 { "" } else { "s" },
                        update_stmt.table_name
                    ),
                });

                Ok(())
            }
            ast::Statement::Delete(delete_stmt) => {
                let mut db = self.db.lock().unwrap();
                let row_count = executor::DeleteExecutor::execute(&delete_stmt, &mut db)
                    .map_err(|e| OperationalError::new_err(format!("Execution error: {:?}", e)))?;

                self.last_result = Some(QueryResultData::Execute {
                    rows_affected: row_count,
                    message: format!(
                        "{} row{} deleted from '{}'",
                        row_count,
                        if row_count == 1 { "" } else { "s" },
                        delete_stmt.table_name
                    ),
                });

                Ok(())
            }
            _ => Err(ProgrammingError::new_err(format!(
                "Statement type not yet supported: {:?}",
                stmt
            ))),
        }
    }

    /// Fetch all rows from the last query result
    ///
    /// Returns:
    ///     list: List of tuples, each representing a row
    fn fetchall(&mut self, py: Python) -> PyResult<Py<PyAny>> {
        match &self.last_result {
            Some(QueryResultData::Select { rows, .. }) => {
                let result_list = PyList::empty(py);
                for row in rows {
                    let py_values: Vec<Py<PyAny>> =
                        row.values.iter().map(|v| sqlvalue_to_py(py, v).unwrap()).collect();
                    let py_row = PyTuple::new(py, py_values)?;
                    result_list.append(py_row)?;
                }
                Ok(result_list.into())
            }
            Some(QueryResultData::Execute { .. }) => {
                Err(ProgrammingError::new_err("No SELECT result to fetch"))
            }
            None => Err(ProgrammingError::new_err("No query has been executed")),
        }
    }

    /// Fetch one row from the last query result
    ///
    /// Returns:
    ///     tuple or None: A tuple representing the next row, or None if no more rows
    fn fetchone(&mut self, py: Python) -> PyResult<Py<PyAny>> {
        match &mut self.last_result {
            Some(QueryResultData::Select { rows, .. }) => {
                if rows.is_empty() {
                    Ok(py.None())
                } else {
                    let row = rows.remove(0);
                    let py_values: Vec<Py<PyAny>> =
                        row.values.iter().map(|v| sqlvalue_to_py(py, v).unwrap()).collect();
                    let py_row = PyTuple::new(py, py_values)?;
                    Ok(py_row.into())
                }
            }
            Some(QueryResultData::Execute { .. }) => {
                Err(ProgrammingError::new_err("No SELECT result to fetch"))
            }
            None => Err(ProgrammingError::new_err("No query has been executed")),
        }
    }

    /// Fetch multiple rows from the last query result
    ///
    /// Args:
    ///     size (int): Number of rows to fetch
    ///
    /// Returns:
    ///     list: List of tuples, each representing a row
    fn fetchmany(&mut self, py: Python, size: usize) -> PyResult<Py<PyAny>> {
        match &mut self.last_result {
            Some(QueryResultData::Select { rows, .. }) => {
                let fetch_count = size.min(rows.len());
                let result_list = PyList::empty(py);

                for _ in 0..fetch_count {
                    if rows.is_empty() {
                        break;
                    }
                    let row = rows.remove(0);
                    let py_values: Vec<Py<PyAny>> =
                        row.values.iter().map(|v| sqlvalue_to_py(py, v).unwrap()).collect();
                    let py_row = PyTuple::new(py, py_values)?;
                    result_list.append(py_row)?;
                }

                Ok(result_list.into())
            }
            Some(QueryResultData::Execute { .. }) => {
                Err(ProgrammingError::new_err("No SELECT result to fetch"))
            }
            None => Err(ProgrammingError::new_err("No query has been executed")),
        }
    }

    /// Get the number of rows affected by the last DML operation
    ///
    /// Returns:
    ///     int: Number of rows affected, or -1 if not applicable
    #[getter]
    fn rowcount(&self) -> PyResult<i64> {
        match &self.last_result {
            Some(QueryResultData::Execute { rows_affected, .. }) => Ok(*rows_affected as i64),
            Some(QueryResultData::Select { rows, .. }) => Ok(rows.len() as i64),
            None => Ok(-1),
        }
    }

    /// Close the cursor
    fn close(&self) -> PyResult<()> {
        // No cleanup needed
        Ok(())
    }
}

/// Factory function to create a database connection
///
/// Returns:
///     Database: A new database connection
#[pyfunction]
fn connect() -> PyResult<Database> {
    Ok(Database::new())
}

/// Python module initialization
#[pymodule]
fn nistmemsql(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_class::<Database>()?;
    m.add_class::<Cursor>()?;
    m.add("DatabaseError", m.py().get_type::<DatabaseError>())?;
    m.add("OperationalError", m.py().get_type::<OperationalError>())?;
    m.add("ProgrammingError", m.py().get_type::<ProgrammingError>())?;
    Ok(())
}
