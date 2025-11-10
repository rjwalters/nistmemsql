// ============================================================================
// Binary Persistence Format
// ============================================================================
//
// Efficient binary serialization for vibesql databases.
//
// File Format:
// - Header (16 bytes): Magic number, version, flags
// - Catalog section: Schemas, tables, indexes, roles
// - Data section: Table data with type tags
//
// Uses little-endian byte order for cross-platform compatibility.

use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

use crate::{Database, StorageError};
use vibesql_types::{SqlValue, Date, Time, Timestamp, Interval};

/// Magic number for vibesql binary format: "VBSQL" in ASCII
const MAGIC: &[u8; 5] = b"VBSQL";

/// Current format version
const VERSION: u8 = 1;

/// Type tags for binary serialization
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypeTag {
    Null = 0x00,
    Smallint = 0x01,
    Integer = 0x02,
    Bigint = 0x03,
    Unsigned = 0x04,
    Numeric = 0x05,
    Float = 0x06,
    Real = 0x07,
    Double = 0x08,
    Character = 0x10,
    Varchar = 0x11,
    Boolean = 0x20,
    Date = 0x30,
    Time = 0x31,
    Timestamp = 0x32,
    Interval = 0x33,
}

impl TypeTag {
    fn from_u8(tag: u8) -> Result<Self, StorageError> {
        match tag {
            0x00 => Ok(TypeTag::Null),
            0x01 => Ok(TypeTag::Smallint),
            0x02 => Ok(TypeTag::Integer),
            0x03 => Ok(TypeTag::Bigint),
            0x04 => Ok(TypeTag::Unsigned),
            0x05 => Ok(TypeTag::Numeric),
            0x06 => Ok(TypeTag::Float),
            0x07 => Ok(TypeTag::Real),
            0x08 => Ok(TypeTag::Double),
            0x10 => Ok(TypeTag::Character),
            0x11 => Ok(TypeTag::Varchar),
            0x20 => Ok(TypeTag::Boolean),
            0x30 => Ok(TypeTag::Date),
            0x31 => Ok(TypeTag::Time),
            0x32 => Ok(TypeTag::Timestamp),
            0x33 => Ok(TypeTag::Interval),
            _ => Err(StorageError::NotImplemented(format!(
                "Unknown type tag: 0x{:02X}",
                tag
            ))),
        }
    }
}

impl Database {
    /// Save database in efficient binary format
    ///
    /// Binary format is faster and more compact than SQL dumps.
    /// Use `.vbsql` extension to indicate binary format.
    ///
    /// # Example
    /// ```no_run
    /// # use vibesql_storage::Database;
    /// let db = Database::new();
    /// db.save_binary("database.vbsql").unwrap();
    /// ```
    pub fn save_binary<P: AsRef<Path>>(&self, path: P) -> Result<(), StorageError> {
        let file = File::create(path)
            .map_err(|e| StorageError::NotImplemented(format!("Failed to create file: {}", e)))?;

        let mut writer = BufWriter::new(file);

        // Write header
        write_header(&mut writer)?;

        // Write catalog section
        write_catalog(&mut writer, self)?;

        // Write data section
        write_data(&mut writer, self)?;

        writer
            .flush()
            .map_err(|e| StorageError::NotImplemented(format!("Failed to flush: {}", e)))?;

        Ok(())
    }

    /// Load database from binary format
    ///
    /// Reads a binary `.vbsql` file and reconstructs the database.
    ///
    /// # Example
    /// ```no_run
    /// # use vibesql_storage::Database;
    /// let db = Database::load_binary("database.vbsql").unwrap();
    /// ```
    pub fn load_binary<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let file = File::open(path.as_ref()).map_err(|e| {
            StorageError::NotImplemented(format!(
                "Failed to open file {:?}: {}",
                path.as_ref(),
                e
            ))
        })?;

        let mut reader = BufReader::new(file);

        // Read and validate header
        read_header(&mut reader)?;

        // Read catalog section
        let mut db = read_catalog(&mut reader)?;

        // Read data section
        read_data(&mut reader, &mut db)?;

        Ok(db)
    }
}

// ============================================================================
// Header Functions
// ============================================================================

fn write_header<W: Write>(writer: &mut W) -> Result<(), StorageError> {
    // Magic number (5 bytes)
    writer
        .write_all(MAGIC)
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    // Version (1 byte)
    writer
        .write_all(&[VERSION])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    // Flags (1 byte) - reserved for future use (compression, encryption, etc.)
    writer
        .write_all(&[0])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    // Reserved (9 bytes)
    writer
        .write_all(&[0; 9])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    Ok(())
}

fn read_header<R: Read>(reader: &mut R) -> Result<(), StorageError> {
    // Read and validate magic number
    let mut magic = [0u8; 5];
    reader
        .read_exact(&mut magic)
        .map_err(|e| StorageError::NotImplemented(format!("Failed to read header: {}", e)))?;

    if &magic != MAGIC {
        return Err(StorageError::NotImplemented(format!(
            "Invalid file format: expected VBSQL magic number, got {:?}",
            String::from_utf8_lossy(&magic)
        )));
    }

    // Read and validate version
    let mut version = [0u8; 1];
    reader
        .read_exact(&mut version)
        .map_err(|e| StorageError::NotImplemented(format!("Failed to read version: {}", e)))?;

    if version[0] > VERSION {
        return Err(StorageError::NotImplemented(format!(
            "Unsupported format version: {} (current version: {})",
            version[0], VERSION
        )));
    }

    // Read flags (reserved for future use)
    let mut flags = [0u8; 1];
    reader
        .read_exact(&mut flags)
        .map_err(|e| StorageError::NotImplemented(format!("Failed to read flags: {}", e)))?;

    // Skip reserved bytes
    let mut reserved = [0u8; 9];
    reader
        .read_exact(&mut reserved)
        .map_err(|e| StorageError::NotImplemented(format!("Failed to read reserved bytes: {}", e)))?;

    Ok(())
}

// ============================================================================
// Catalog Functions
// ============================================================================

fn write_catalog<W: Write>(writer: &mut W, db: &Database) -> Result<(), StorageError> {
    // Write schemas
    let schemas: Vec<_> = db.catalog.list_schemas()
        .into_iter()
        .filter(|s| s != "public") // Skip default public schema
        .collect();

    write_u32(writer, schemas.len() as u32)?;
    for schema_name in schemas {
        write_string(writer, &schema_name)?;
    }

    // Write roles
    let roles = db.catalog.list_roles();
    write_u32(writer, roles.len() as u32)?;
    for role_name in roles {
        write_string(writer, &role_name)?;
    }

    // Write table schemas
    let table_names = db.catalog.list_tables();
    write_u32(writer, table_names.len() as u32)?;

    for table_name in &table_names {
        if let Some(table) = db.get_table(table_name) {
            write_string(writer, table_name)?;

            // Write column count
            write_u32(writer, table.schema.columns.len() as u32)?;

            // Write each column definition
            for col in &table.schema.columns {
                write_string(writer, &col.name)?;
                write_string(writer, &super::save::format_data_type(&col.data_type))?;
                write_bool(writer, col.nullable)?;
            }
        }
    }

    // Write indexes
    let index_names = db.list_indexes();
    write_u32(writer, index_names.len() as u32)?;

    for index_name in index_names {
        if let Some(metadata) = db.get_index(&index_name) {
            write_string(writer, &index_name)?;
            write_string(writer, &metadata.table_name)?;
            write_bool(writer, metadata.unique)?;

            // Write indexed columns
            write_u32(writer, metadata.columns.len() as u32)?;
            for col in &metadata.columns {
                write_string(writer, &col.column_name)?;
                // Write direction as u8 (0 = Asc, 1 = Desc)
                let direction = match col.direction {
                    vibesql_ast::OrderDirection::Asc => 0u8,
                    vibesql_ast::OrderDirection::Desc => 1u8,
                };
                writer
                    .write_all(&[direction])
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
        }
    }

    Ok(())
}

fn read_catalog<R: Read>(reader: &mut R) -> Result<Database, StorageError> {
    let mut db = Database::new();

    // Read schemas
    let schema_count = read_u32(reader)?;
    for _ in 0..schema_count {
        let schema_name = read_string(reader)?;
        // Create schema directly on catalog
        db.catalog.create_schema(schema_name)
            .map_err(|e| StorageError::NotImplemented(format!("Failed to create schema: {}", e)))?;
    }

    // Read roles
    let role_count = read_u32(reader)?;
    for _ in 0..role_count {
        let role_name = read_string(reader)?;
        db.catalog.create_role(role_name)
            .map_err(|e| StorageError::NotImplemented(format!("Failed to create role: {}", e)))?;
    }

    // Read table schemas (will create tables later when we read data)
    let table_count = read_u32(reader)?;
    let mut table_schemas = Vec::new();

    for _ in 0..table_count {
        let table_name = read_string(reader)?;
        let column_count = read_u32(reader)?;

        let mut columns = Vec::new();
        for _ in 0..column_count {
            let col_name = read_string(reader)?;
            let col_type_str = read_string(reader)?;
            let nullable = read_bool(reader)?;

            // Parse data type from string (reuse existing logic)
            let data_type = parse_data_type(&col_type_str)?;

            columns.push(vibesql_catalog::ColumnSchema {
                name: col_name,
                data_type,
                nullable,
                default_value: None,
            });
        }

        table_schemas.push((table_name, columns));
    }

    // Create tables
    for (table_name, columns) in table_schemas {
        let schema = vibesql_catalog::TableSchema::new(table_name, columns);

        db.create_table(schema)
            .map_err(|e| StorageError::NotImplemented(format!("Failed to create table: {}", e)))?;
    }

    // Read indexes
    let index_count = read_u32(reader)?;
    let mut index_specs = Vec::new();

    for _ in 0..index_count {
        let index_name = read_string(reader)?;
        let table_name = read_string(reader)?;
        let unique = read_bool(reader)?;

        let column_count = read_u32(reader)?;
        let mut columns = Vec::new();

        for _ in 0..column_count {
            let column_name = read_string(reader)?;
            let direction_byte = read_u8(reader)?;
            let direction = match direction_byte {
                0 => vibesql_ast::OrderDirection::Asc,
                1 => vibesql_ast::OrderDirection::Desc,
                _ => return Err(StorageError::NotImplemented(format!(
                    "Invalid sort direction: {}",
                    direction_byte
                ))),
            };

            columns.push(vibesql_ast::IndexColumn {
                column_name,
                direction,
            });
        }

        index_specs.push((index_name, table_name, unique, columns));
    }

    // Create indexes
    for (index_name, table_name, unique, columns) in index_specs {
        db.create_index(index_name, table_name, unique, columns)
            .map_err(|e| StorageError::NotImplemented(format!("Failed to create index: {}", e)))?;
    }

    Ok(db)
}

// ============================================================================
// Data Functions
// ============================================================================

fn write_data<W: Write>(writer: &mut W, db: &Database) -> Result<(), StorageError> {
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

fn read_data<R: Read>(reader: &mut R, db: &mut Database) -> Result<(), StorageError> {
    let table_names = db.catalog.list_tables();

    for table_name in table_names {
        let _table_name_read = read_string(reader)?;
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
                table.insert(row)
                    .map_err(|e| StorageError::NotImplemented(format!("Failed to insert row: {}", e)))?;
            }
        }
    }

    Ok(())
}

// ============================================================================
// Value Serialization
// ============================================================================

fn write_sql_value<W: Write>(writer: &mut W, value: &SqlValue) -> Result<(), StorageError> {
    match value {
        SqlValue::Null => {
            writer
                .write_all(&[TypeTag::Null as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        SqlValue::Smallint(n) => {
            writer
                .write_all(&[TypeTag::Smallint as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_i16(writer, *n)?;
        }
        SqlValue::Integer(n) => {
            writer
                .write_all(&[TypeTag::Integer as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_i64(writer, *n)?;
        }
        SqlValue::Bigint(n) => {
            writer
                .write_all(&[TypeTag::Bigint as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_i64(writer, *n)?;
        }
        SqlValue::Unsigned(n) => {
            writer
                .write_all(&[TypeTag::Unsigned as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_u64(writer, *n)?;
        }
        SqlValue::Numeric(f) => {
            writer
                .write_all(&[TypeTag::Numeric as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_f64(writer, *f)?;
        }
        SqlValue::Float(f) => {
            writer
                .write_all(&[TypeTag::Float as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_f32(writer, *f)?;
        }
        SqlValue::Real(f) => {
            writer
                .write_all(&[TypeTag::Real as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_f32(writer, *f)?;
        }
        SqlValue::Double(f) => {
            writer
                .write_all(&[TypeTag::Double as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_f64(writer, *f)?;
        }
        SqlValue::Character(s) | SqlValue::Varchar(s) => {
            let tag = if matches!(value, SqlValue::Character(_)) {
                TypeTag::Character
            } else {
                TypeTag::Varchar
            };
            writer
                .write_all(&[tag as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, s)?;
        }
        SqlValue::Boolean(b) => {
            writer
                .write_all(&[TypeTag::Boolean as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_bool(writer, *b)?;
        }
        SqlValue::Date(d) => {
            writer
                .write_all(&[TypeTag::Date as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, &d.to_string())?;
        }
        SqlValue::Time(t) => {
            writer
                .write_all(&[TypeTag::Time as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, &t.to_string())?;
        }
        SqlValue::Timestamp(ts) => {
            writer
                .write_all(&[TypeTag::Timestamp as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, &ts.to_string())?;
        }
        SqlValue::Interval(i) => {
            writer
                .write_all(&[TypeTag::Interval as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, &i.to_string())?;
        }
    }
    Ok(())
}

fn read_sql_value<R: Read>(reader: &mut R) -> Result<SqlValue, StorageError> {
    let tag = read_u8(reader)?;
    let type_tag = TypeTag::from_u8(tag)?;

    match type_tag {
        TypeTag::Null => Ok(SqlValue::Null),
        TypeTag::Smallint => Ok(SqlValue::Smallint(read_i16(reader)?)),
        TypeTag::Integer => Ok(SqlValue::Integer(read_i64(reader)?)),
        TypeTag::Bigint => Ok(SqlValue::Bigint(read_i64(reader)?)),
        TypeTag::Unsigned => Ok(SqlValue::Unsigned(read_u64(reader)?)),
        TypeTag::Numeric => Ok(SqlValue::Numeric(read_f64(reader)?)),
        TypeTag::Float => Ok(SqlValue::Float(read_f32(reader)?)),
        TypeTag::Real => Ok(SqlValue::Real(read_f32(reader)?)),
        TypeTag::Double => Ok(SqlValue::Double(read_f64(reader)?)),
        TypeTag::Character => Ok(SqlValue::Character(read_string(reader)?)),
        TypeTag::Varchar => Ok(SqlValue::Varchar(read_string(reader)?)),
        TypeTag::Boolean => Ok(SqlValue::Boolean(read_bool(reader)?)),
        TypeTag::Date => {
            let s = read_string(reader)?;
            let date = s
                .parse::<Date>()
                .map_err(|e| StorageError::NotImplemented(format!("Invalid date: {}", e)))?;
            Ok(SqlValue::Date(date))
        }
        TypeTag::Time => {
            let s = read_string(reader)?;
            let time = s
                .parse::<Time>()
                .map_err(|e| StorageError::NotImplemented(format!("Invalid time: {}", e)))?;
            Ok(SqlValue::Time(time))
        }
        TypeTag::Timestamp => {
            let s = read_string(reader)?;
            let timestamp = s
                .parse::<Timestamp>()
                .map_err(|e| StorageError::NotImplemented(format!("Invalid timestamp: {}", e)))?;
            Ok(SqlValue::Timestamp(timestamp))
        }
        TypeTag::Interval => {
            let s = read_string(reader)?;
            let interval = s
                .parse::<Interval>()
                .map_err(|e| StorageError::NotImplemented(format!("Invalid interval: {}", e)))?;
            Ok(SqlValue::Interval(interval))
        }
    }
}

// ============================================================================
// Low-Level I/O Primitives
// ============================================================================

fn write_u8<W: Write>(writer: &mut W, value: u8) -> Result<(), StorageError> {
    writer
        .write_all(&[value])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

fn read_u8<R: Read>(reader: &mut R) -> Result<u8, StorageError> {
    let mut buf = [0u8; 1];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    Ok(buf[0])
}

fn write_u32<W: Write>(writer: &mut W, value: u32) -> Result<(), StorageError> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

fn read_u32<R: Read>(reader: &mut R) -> Result<u32, StorageError> {
    let mut buf = [0u8; 4];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    Ok(u32::from_le_bytes(buf))
}

fn write_u64<W: Write>(writer: &mut W, value: u64) -> Result<(), StorageError> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

fn read_u64<R: Read>(reader: &mut R) -> Result<u64, StorageError> {
    let mut buf = [0u8; 8];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    Ok(u64::from_le_bytes(buf))
}

fn write_i16<W: Write>(writer: &mut W, value: i16) -> Result<(), StorageError> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

fn read_i16<R: Read>(reader: &mut R) -> Result<i16, StorageError> {
    let mut buf = [0u8; 2];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    Ok(i16::from_le_bytes(buf))
}

fn write_i64<W: Write>(writer: &mut W, value: i64) -> Result<(), StorageError> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

fn read_i64<R: Read>(reader: &mut R) -> Result<i64, StorageError> {
    let mut buf = [0u8; 8];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    Ok(i64::from_le_bytes(buf))
}

fn write_f32<W: Write>(writer: &mut W, value: f32) -> Result<(), StorageError> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

fn read_f32<R: Read>(reader: &mut R) -> Result<f32, StorageError> {
    let mut buf = [0u8; 4];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    Ok(f32::from_le_bytes(buf))
}

fn write_f64<W: Write>(writer: &mut W, value: f64) -> Result<(), StorageError> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

fn read_f64<R: Read>(reader: &mut R) -> Result<f64, StorageError> {
    let mut buf = [0u8; 8];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    Ok(f64::from_le_bytes(buf))
}

fn write_bool<W: Write>(writer: &mut W, value: bool) -> Result<(), StorageError> {
    writer
        .write_all(&[if value { 1 } else { 0 }])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

fn read_bool<R: Read>(reader: &mut R) -> Result<bool, StorageError> {
    let mut buf = [0u8; 1];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    Ok(buf[0] != 0)
}

fn write_string<W: Write>(writer: &mut W, s: &str) -> Result<(), StorageError> {
    let bytes = s.as_bytes();
    write_u32(writer, bytes.len() as u32)?;
    writer
        .write_all(bytes)
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))
}

fn read_string<R: Read>(reader: &mut R) -> Result<String, StorageError> {
    let len = read_u32(reader)?;
    let mut buf = vec![0u8; len as usize];
    reader
        .read_exact(&mut buf)
        .map_err(|e| StorageError::NotImplemented(format!("Read error: {}", e)))?;
    String::from_utf8(buf)
        .map_err(|e| StorageError::NotImplemented(format!("Invalid UTF-8: {}", e)))
}

// ============================================================================
// Helper Functions
// ============================================================================

fn parse_data_type(type_str: &str) -> Result<vibesql_types::DataType, StorageError> {
    // Simple parser for data type strings (matches format_data_type output)
    use vibesql_types::DataType;

    let upper = type_str.to_uppercase();

    match upper.as_str() {
        "INTEGER" => Ok(DataType::Integer),
        "SMALLINT" => Ok(DataType::Smallint),
        "BIGINT" => Ok(DataType::Bigint),
        "BIGINT UNSIGNED" => Ok(DataType::Unsigned),
        "REAL" => Ok(DataType::Real),
        "DOUBLE PRECISION" => Ok(DataType::DoublePrecision),
        "BOOLEAN" => Ok(DataType::Boolean),
        "DATE" => Ok(DataType::Date),
        "TIME" => Ok(DataType::Time { with_timezone: false }),
        "TIMESTAMP" => Ok(DataType::Timestamp { with_timezone: false }),
        "TIMESTAMP WITH TIME ZONE" => Ok(DataType::Timestamp { with_timezone: true }),
        s if s.starts_with("VARCHAR(") => {
            let len_str = s.trim_start_matches("VARCHAR(").trim_end_matches(')');
            let max_length = len_str.parse().ok();
            Ok(DataType::Varchar { max_length })
        }
        s if s.starts_with("VARCHAR") => {
            Ok(DataType::Varchar { max_length: None })
        }
        s if s.starts_with("CHAR(") => {
            let len_str = s.trim_start_matches("CHAR(").trim_end_matches(')');
            let length = len_str.parse().unwrap_or(1);
            Ok(DataType::Character { length })
        }
        s if s.starts_with("FLOAT(") => {
            let prec_str = s.trim_start_matches("FLOAT(").trim_end_matches(')');
            let precision = prec_str.parse().unwrap_or(53);
            Ok(DataType::Float { precision })
        }
        s if s.starts_with("NUMERIC(") => {
            let params = s.trim_start_matches("NUMERIC(").trim_end_matches(')');
            let parts: Vec<&str> = params.split(',').map(|p| p.trim()).collect();
            let precision = parts.get(0).and_then(|p| p.parse().ok()).unwrap_or(38);
            let scale = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
            Ok(DataType::Numeric { precision, scale })
        }
        s if s.starts_with("DECIMAL(") => {
            let params = s.trim_start_matches("DECIMAL(").trim_end_matches(')');
            let parts: Vec<&str> = params.split(',').map(|p| p.trim()).collect();
            let precision = parts.get(0).and_then(|p| p.parse().ok()).unwrap_or(38);
            let scale = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
            Ok(DataType::Decimal { precision, scale })
        }
        _ => Err(StorageError::NotImplemented(format!(
            "Unsupported data type: {}",
            type_str
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let mut buf = Vec::new();
        write_header(&mut buf).unwrap();

        assert_eq!(buf.len(), 16);
        assert_eq!(&buf[0..5], MAGIC);
        assert_eq!(buf[5], VERSION);

        let mut reader = &buf[..];
        read_header(&mut reader).unwrap();
    }

    #[test]
    fn test_primitives() {
        let mut buf = Vec::new();

        write_u32(&mut buf, 12345).unwrap();
        write_i64(&mut buf, -9876543210).unwrap();
        write_f64(&mut buf, 3.14159).unwrap();
        write_bool(&mut buf, true).unwrap();
        write_string(&mut buf, "Hello, VBSQL!").unwrap();

        let mut reader = &buf[..];
        assert_eq!(read_u32(&mut reader).unwrap(), 12345);
        assert_eq!(read_i64(&mut reader).unwrap(), -9876543210);
        assert!((read_f64(&mut reader).unwrap() - 3.14159).abs() < 1e-10);
        assert_eq!(read_bool(&mut reader).unwrap(), true);
        assert_eq!(read_string(&mut reader).unwrap(), "Hello, VBSQL!");
    }

    #[test]
    fn test_sql_value_roundtrip() {
        let test_values = vec![
            SqlValue::Null,
            SqlValue::Integer(42),
            SqlValue::Varchar("test".to_string()),
            SqlValue::Boolean(true),
            SqlValue::Float(3.14),
        ];

        let mut buf = Vec::new();
        for val in &test_values {
            write_sql_value(&mut buf, val).unwrap();
        }

        let mut reader = &buf[..];
        for expected in test_values {
            let actual = read_sql_value(&mut reader).unwrap();
            assert_eq!(actual, expected);
        }
    }
}
