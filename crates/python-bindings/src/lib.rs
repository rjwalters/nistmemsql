//! Python bindings for nistmemsql using PyO3
//!
//! This module provides Python bindings following DB-API 2.0 conventions
//! to expose the Rust database library to Python for benchmarking and usage.

// Suppress PyO3 macro warnings
#![allow(non_local_definitions)]

use lru::LruCache;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use std::num::NonZeroUsize;
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

/// Converts a Python object to a Rust SqlValue
///
/// This is the inverse of sqlvalue_to_py() and supports DB-API 2.0 type conversion.
fn py_to_sqlvalue(_py: Python, obj: &Bound<'_, PyAny>) -> PyResult<types::SqlValue> {
    // Check for None (SQL NULL)
    if obj.is_none() {
        return Ok(types::SqlValue::Null);
    }

    // Try to extract Python types in order of likelihood
    // 1. Integer types
    if let Ok(val) = obj.extract::<i64>() {
        // Check range to determine which integer type to use
        if val >= i16::MIN as i64 && val <= i16::MAX as i64 {
            return Ok(types::SqlValue::Smallint(val as i16));
        } else if val >= i32::MIN as i64 && val <= i32::MAX as i64 {
            return Ok(types::SqlValue::Integer(val));
        } else {
            return Ok(types::SqlValue::Bigint(val));
        }
    }

    // 2. Float types
    if let Ok(val) = obj.extract::<f64>() {
        return Ok(types::SqlValue::Double(val));
    }

    // 3. String types
    if let Ok(val) = obj.extract::<String>() {
        return Ok(types::SqlValue::Varchar(val));
    }

    // 4. Boolean types
    if let Ok(val) = obj.extract::<bool>() {
        return Ok(types::SqlValue::Boolean(val));
    }

    // If no type matched, return an error
    let type_name =
        obj.get_type().name().map(|s| s.to_string()).unwrap_or_else(|_| "unknown".to_string());
    Err(ProgrammingError::new_err(format!(
        "Cannot convert Python type '{}' to SQL value",
        type_name
    )))
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
        Ok(Cursor {
            db: Arc::clone(&self.db),
            last_result: None,
            stmt_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(1000).unwrap(),
            ))),
            schema_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(100).unwrap(),
            ))),
            cache_hits: Arc::new(Mutex::new(0)),
            cache_misses: Arc::new(Mutex::new(0)),
            schema_cache_hits: Arc::new(Mutex::new(0)),
            schema_cache_misses: Arc::new(Mutex::new(0)),
        })
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
    /// LRU cache for parsed SQL statements (max 1000 entries)
    /// Key: SQL string with ? placeholders, Value: parsed AST
    stmt_cache: Arc<Mutex<LruCache<String, ast::Statement>>>,
    /// Cache for table schemas (max 100 tables per cursor)
    /// Key: table name, Value: cached schema
    schema_cache: Arc<Mutex<LruCache<String, catalog::TableSchema>>>,
    /// Cache statistics for monitoring
    cache_hits: Arc<Mutex<usize>>,
    cache_misses: Arc<Mutex<usize>>,
    /// Schema cache statistics
    schema_cache_hits: Arc<Mutex<usize>>,
    schema_cache_misses: Arc<Mutex<usize>>,
}

#[pymethods]
impl Cursor {
    /// Execute a SQL statement with optional parameter binding
    ///
    /// Args:
    ///     sql (str): The SQL statement to execute (may contain ? placeholders)
    ///     params (tuple, optional): Parameter values to bind to ? placeholders
    ///
    /// Returns:
    ///     None
    #[pyo3(signature = (sql, params=None))]
    fn execute(
        &mut self,
        py: Python,
        sql: &str,
        params: Option<&Bound<'_, PyTuple>>,
    ) -> PyResult<()> {
        // Use the original SQL (with ? placeholders) as the cache key
        let cache_key = sql.to_string();

        // Check if we have a cached parsed AST for this SQL
        let mut cache = self.stmt_cache.lock().unwrap();
        let stmt = if let Some(cached_stmt) = cache.get(&cache_key) {
            // Cache hit! Clone the cached AST before releasing lock
            let cloned_stmt = cached_stmt.clone();
            drop(cache); // Release cache lock before updating stats
            *self.cache_hits.lock().unwrap() += 1;
            cloned_stmt
        } else {
            // Cache miss - need to parse
            drop(cache); // Release cache lock before parsing
            *self.cache_misses.lock().unwrap() += 1;

            // Process SQL with parameter substitution if params are provided
            let processed_sql = if let Some(params_tuple) = params {
            // Count placeholders in SQL
            let placeholder_count = sql.matches('?').count();
            let param_count = params_tuple.len();

            // Validate parameter count matches placeholder count
            if placeholder_count != param_count {
                return Err(ProgrammingError::new_err(format!(
                    "Parameter count mismatch: SQL has {} placeholders but {} parameters provided",
                    placeholder_count, param_count
                )));
            }

            // Convert Python parameters to SQL values
            let mut sql_values = Vec::new();
            for i in 0..param_count {
                let py_obj = params_tuple.get_item(i)?;
                let sql_value = py_to_sqlvalue(py, &py_obj).map_err(|e| {
                    ProgrammingError::new_err(format!(
                        "Parameter at position {} has invalid type: {}",
                        i, e
                    ))
                })?;
                sql_values.push(sql_value);
            }

            // Replace placeholders with SQL literal values
            let mut result = String::new();
            let mut param_idx = 0;
            let mut chars = sql.chars().peekable();

            while let Some(ch) = chars.next() {
                if ch == '?' {
                    // Replace ? with the corresponding parameter value as SQL literal
                    if param_idx < sql_values.len() {
                        let value_str = match &sql_values[param_idx] {
                            types::SqlValue::Integer(i) => i.to_string(),
                            types::SqlValue::Smallint(i) => i.to_string(),
                            types::SqlValue::Bigint(i) => i.to_string(),
                            types::SqlValue::Float(f) => f.to_string(),
                            types::SqlValue::Real(f) => f.to_string(),
                            types::SqlValue::Double(f) => f.to_string(),
                            types::SqlValue::Numeric(n) => n.to_string(),
                            types::SqlValue::Varchar(s) | types::SqlValue::Character(s) => {
                                // Escape single quotes by doubling them (SQL standard)
                                format!("'{}'", s.replace('\'', "''"))
                            }
                            types::SqlValue::Boolean(b) => {
                                if *b { "TRUE" } else { "FALSE" }.to_string()
                            }
                            types::SqlValue::Date(s) => format!("DATE '{}'", s),
                            types::SqlValue::Time(s) => format!("TIME '{}'", s),
                            types::SqlValue::Timestamp(s) => format!("TIMESTAMP '{}'", s),
                            types::SqlValue::Interval(s) => format!("INTERVAL '{}'", s),
                            types::SqlValue::Null => "NULL".to_string(),
                        };
                        result.push_str(&value_str);
                        param_idx += 1;
                    }
                } else {
                    result.push(ch);
                }
            }

                result
            } else {
                // No parameters provided, use SQL as-is
                sql.to_string()
            };

            // Parse the processed SQL
            let stmt = parser::Parser::parse_sql(&processed_sql)
                .map_err(|e| ProgrammingError::new_err(format!("Parse error: {:?}", e)))?;

            // Store the parsed AST in cache for future reuse
            let mut cache = self.stmt_cache.lock().unwrap();
            cache.put(cache_key.clone(), stmt.clone());
            drop(cache);

            stmt
        };

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

                // Clear both statement and schema caches on schema change
                let mut cache = self.stmt_cache.lock().unwrap();
                cache.clear();
                drop(cache);
                self.clear_schema_cache();

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

                // Clear both statement and schema caches on schema change
                let mut cache = self.stmt_cache.lock().unwrap();
                cache.clear();
                drop(cache);
                self.clear_schema_cache();

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
                // Use cached schema to reduce catalog lookups
                let cached_schema = self.get_cached_schema(&update_stmt.table_name)?;

                let mut db = self.db.lock().unwrap();
                let row_count = executor::UpdateExecutor::execute_with_schema(
                    &update_stmt,
                    &mut db,
                    Some(&cached_schema),
                )
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

    /// Get schema cache statistics
    ///
    /// Returns:
    ///     tuple: (schema_cache_hits, schema_cache_misses, schema_hit_rate)
    fn schema_cache_stats(&self) -> PyResult<(usize, usize, f64)> {
        let hits = *self.schema_cache_hits.lock().unwrap();
        let misses = *self.schema_cache_misses.lock().unwrap();
        let total = hits + misses;
        let hit_rate = if total > 0 {
            (hits as f64) / (total as f64)
        } else {
            0.0
        };
        Ok((hits, misses, hit_rate))
    }

    /// Get statement cache statistics
    ///
    /// Returns:
    ///     tuple: (cache_hits, cache_misses, hit_rate)
    fn cache_stats(&self) -> PyResult<(usize, usize, f64)> {
        let hits = *self.cache_hits.lock().unwrap();
        let misses = *self.cache_misses.lock().unwrap();
        let total = hits + misses;
        let hit_rate = if total > 0 {
            (hits as f64) / (total as f64)
        } else {
            0.0
        };
        Ok((hits, misses, hit_rate))
    }

    /// Clear both statement and schema caches
    ///
    /// Useful for testing or when schema changes occur outside this cursor
    fn clear_cache(&mut self) -> PyResult<()> {
        let mut cache = self.stmt_cache.lock().unwrap();
        cache.clear();
        drop(cache);

        self.clear_schema_cache();
        Ok(())
    }

    /// Close the cursor
    fn close(&self) -> PyResult<()> {
        // No cleanup needed
        Ok(())
    }
}

impl Cursor {
    /// Get table schema with caching
    ///
    /// First checks the schema cache, and only queries the database catalog on cache miss.
    /// This reduces redundant catalog lookups during repeated operations on the same table.
    fn get_cached_schema(&self, table_name: &str) -> PyResult<catalog::TableSchema> {
        // Check cache first
        let mut cache = self.schema_cache.lock().unwrap();
        if let Some(schema) = cache.get(table_name) {
            // Cache hit! Clone and return
            let schema = schema.clone();
            drop(cache);
            *self.schema_cache_hits.lock().unwrap() += 1;
            return Ok(schema);
        }
        drop(cache);

        // Cache miss - look up in database catalog
        *self.schema_cache_misses.lock().unwrap() += 1;
        let db = self.db.lock().unwrap();
        let schema = db
            .catalog
            .get_table(table_name)
            .ok_or_else(|| {
                OperationalError::new_err(format!("Table not found: {}", table_name))
            })?
            .clone();
        drop(db);

        // Store in cache for future use
        let mut cache = self.schema_cache.lock().unwrap();
        cache.put(table_name.to_string(), schema.clone());
        drop(cache);

        Ok(schema)
    }

    /// Invalidate schema cache for a specific table
    ///
    /// Call this after DDL operations that modify table schema.
    fn invalidate_schema_cache(&self, table_name: &str) {
        let mut cache = self.schema_cache.lock().unwrap();
        cache.pop(table_name);
    }

    /// Clear all schema caches
    ///
    /// Call this after any DDL operation that could affect multiple tables.
    fn clear_schema_cache(&self) {
        let mut cache = self.schema_cache.lock().unwrap();
        cache.clear();
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
