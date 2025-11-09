// ============================================================================
// Database Persistence - SQL Dump Format
// ============================================================================
//
// Provides SQL dump format (human-readable, portable) for database persistence.
// Generates SQL statements that recreate the database state including schemas,
// tables, data, indexes, roles, and privileges.

use crate::{Database, StorageError};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

impl Database {
    /// Save database state as SQL dump (human-readable, portable)
    ///
    /// Generates SQL statements that recreate the database state including:
    /// - Schemas
    /// - Tables with column definitions
    /// - Indexes
    /// - Data (INSERT statements)
    /// - Roles and privileges
    ///
    /// # Example
    /// ```no_run
    /// # use storage::Database;
    /// let db = Database::new();
    /// db.save_sql_dump("database.sql").unwrap();
    /// ```
    pub fn save_sql_dump<P: AsRef<Path>>(&self, path: P) -> Result<(), StorageError> {
        let file = File::create(path)
            .map_err(|e| StorageError::IoError(format!("Failed to create file: {}", e)))?;

        let mut writer = BufWriter::new(file);

        // Header
        writeln!(writer, "-- VibeSQL Database Dump")?;
        writeln!(writer, "-- Generated: {}", chrono::Utc::now())?;
        writeln!(writer, "--")?;
        writeln!(writer)?;

        // Export schemas (except default 'public' which always exists)
        writeln!(writer, "-- Schemas")?;
        for schema_name in self.catalog.list_schemas() {
            if schema_name != "public" {
                writeln!(writer, "CREATE SCHEMA {};", schema_name)?;
            }
        }
        writeln!(writer)?;

        // Export roles
        writeln!(writer, "-- Roles")?;
        for role_name in self.catalog.list_roles() {
            writeln!(writer, "CREATE ROLE {};", role_name)?;
        }
        writeln!(writer)?;

        // Export tables and data
        writeln!(writer, "-- Tables and Data")?;
        for (table_name, table) in &self.tables {
            // CREATE TABLE statement
            let schema = table.schema();
            write!(writer, "CREATE TABLE {} (", table_name)?;

            for (i, col) in schema.columns.iter().enumerate() {
                if i > 0 {
                    write!(writer, ", ")?;
                }
                write!(writer, "{} {}", col.name, format_data_type(&col.data_type))?;
                if !col.nullable {
                    write!(writer, " NOT NULL")?;
                }
            }

            writeln!(writer, ");")?;

            // INSERT statements for data
            if table.row_count() > 0 {
                writeln!(writer)?;
                for row in table.scan() {
                    write!(writer, "INSERT INTO {} VALUES (", table_name)?;
                    for (i, value) in row.values.iter().enumerate() {
                        if i > 0 {
                            write!(writer, ", ")?;
                        }
                        write!(writer, "{}", sql_value_to_literal(value))?;
                    }
                    writeln!(writer, ");")?;
                }
            }

            writeln!(writer)?;
        }

        // Export indexes
        writeln!(writer, "-- Indexes")?;
        for (index_name, metadata) in &self.indexes {
            write!(writer, "CREATE")?;
            if metadata.unique {
                write!(writer, " UNIQUE")?;
            }
            write!(writer, " INDEX {} ON {} (", index_name, metadata.table_name)?;

            for (i, col) in metadata.columns.iter().enumerate() {
                if i > 0 {
                    write!(writer, ", ")?;
                }
                write!(writer, "{}", col.name)?;
                if let Some(order) = &col.order {
                    write!(writer, " {}", order)?;
                }
            }

            writeln!(writer, ");")?;
        }
        writeln!(writer)?;

        writeln!(writer, "-- End of dump")?;

        Ok(())
    }
}

/// Format a DataType for SQL CREATE TABLE statement
fn format_data_type(data_type: &types::DataType) -> String {
    use types::DataType;

    match data_type {
        DataType::Integer => "INTEGER".to_string(),
        DataType::SmallInt => "SMALLINT".to_string(),
        DataType::BigInt => "BIGINT".to_string(),
        DataType::Float => "FLOAT".to_string(),
        DataType::Real => "REAL".to_string(),
        DataType::DoublePrecision => "DOUBLE PRECISION".to_string(),
        DataType::Varchar { max_length } => {
            if let Some(len) = max_length {
                format!("VARCHAR({})", len)
            } else {
                "VARCHAR".to_string()
            }
        }
        DataType::Char { length } => format!("CHAR({})", length),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Date => "DATE".to_string(),
        DataType::Time => "TIME".to_string(),
        DataType::Timestamp => "TIMESTAMP".to_string(),
        DataType::Interval => "INTERVAL".to_string(),
        DataType::Numeric { precision, scale } => {
            if let Some(scale_val) = scale {
                format!("NUMERIC({}, {})", precision, scale_val)
            } else {
                format!("NUMERIC({})", precision)
            }
        }
        DataType::Decimal { precision, scale } => {
            if let Some(scale_val) = scale {
                format!("DECIMAL({}, {})", precision, scale_val)
            } else {
                format!("DECIMAL({})", precision)
            }
        }
    }
}

/// Convert a SqlValue to its SQL literal representation
fn sql_value_to_literal(value: &types::SqlValue) -> String {
    use types::SqlValue;

    match value {
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Integer(n) => n.to_string(),
        SqlValue::Float(f) => {
            if f.is_nan() {
                "'NaN'".to_string()
            } else if f.is_infinite() {
                if f.is_sign_positive() {
                    "'Infinity'".to_string()
                } else {
                    "'-Infinity'".to_string()
                }
            } else {
                f.to_string()
            }
        }
        SqlValue::Text(s) | SqlValue::Varchar(s) => format!("'{}'", s.replace('\'', "''")),
        SqlValue::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        SqlValue::Date(d) => format!("DATE '{}'", d),
        SqlValue::Time(t) => format!("TIME '{}'", t),
        SqlValue::Timestamp(ts) => format!("TIMESTAMP '{}'", ts),
        SqlValue::Interval(i) => format!("INTERVAL '{}'", i),
    }
}

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        StorageError::IoError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::{ColumnSchema, TableSchema};
    use types::{DataType, SqlValue};

    #[test]
    fn test_save_sql_dump() {
        let mut db = Database::new();

        // Create test schema
        let schema = TableSchema {
            name: "test".to_string(),
            columns: vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, false),
            ],
            primary_key: None,
            unique_constraints: vec![],
        };

        db.create_table(schema).unwrap();

        // Insert test data
        let table = db.get_table_mut("test").unwrap();
        table.insert(crate::Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())])).unwrap();
        table.insert(crate::Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())])).unwrap();

        // Save SQL dump
        let path = "/tmp/test_db.sql";
        db.save_sql_dump(path).unwrap();

        // Verify file exists and contains expected content
        let content = std::fs::read_to_string(path).unwrap();
        assert!(content.contains("CREATE TABLE"));
        assert!(content.contains("INSERT INTO"));
        assert!(content.contains("Alice"));
        assert!(content.contains("Bob"));

        // Cleanup
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_sql_value_to_literal() {
        assert_eq!(sql_value_to_literal(&SqlValue::Null), "NULL");
        assert_eq!(sql_value_to_literal(&SqlValue::Integer(42)), "42");
        assert_eq!(sql_value_to_literal(&SqlValue::Varchar("test".to_string())), "'test'");
        assert_eq!(sql_value_to_literal(&SqlValue::Varchar("test's".to_string())), "'test''s'");
        assert_eq!(sql_value_to_literal(&SqlValue::Boolean(true)), "TRUE");
        assert_eq!(sql_value_to_literal(&SqlValue::Boolean(false)), "FALSE");
    }
}
