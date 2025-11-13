// ============================================================================
// Catalog Serialization
// ============================================================================
//
// Handles serialization of schemas, tables, indexes, and roles.

use std::io::{Read, Write};

use super::io::*;
use crate::{persistence::save, Database, StorageError};

pub fn write_catalog<W: Write>(writer: &mut W, db: &Database) -> Result<(), StorageError> {
    // Write schemas
    let schemas: Vec<String> = db.catalog.list_schemas()
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
                write_string(writer, &save::format_data_type(&col.data_type))?;
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

    // Write triggers
    let trigger_names = db.catalog.list_triggers();
    write_u32(writer, trigger_names.len() as u32)?;

    for trigger_name in trigger_names {
        if let Some(trigger) = db.catalog.get_trigger(&trigger_name) {
            write_string(writer, &trigger.name)?;
            write_string(writer, &trigger.table_name)?;

            // Write timing as u8 (0 = Before, 1 = After, 2 = InsteadOf)
            let timing = match trigger.timing {
                vibesql_ast::TriggerTiming::Before => 0u8,
                vibesql_ast::TriggerTiming::After => 1u8,
                vibesql_ast::TriggerTiming::InsteadOf => 2u8,
            };
            writer
                .write_all(&[timing])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

            // Write event as u8 (0 = Insert, 1 = Update, 2 = Delete)
            // For Update with columns, write 3 followed by column list
            match &trigger.event {
                vibesql_ast::TriggerEvent::Insert => {
                    writer
                        .write_all(&[0u8])
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }
                vibesql_ast::TriggerEvent::Update(None) => {
                    writer
                        .write_all(&[1u8])
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }
                vibesql_ast::TriggerEvent::Update(Some(cols)) => {
                    writer
                        .write_all(&[3u8])
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                    write_u32(writer, cols.len() as u32)?;
                    for col in cols {
                        write_string(writer, &col)?;
                    }
                }
                vibesql_ast::TriggerEvent::Delete => {
                    writer
                        .write_all(&[2u8])
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }
            }

            // Write granularity as u8 (0 = Row, 1 = Statement)
            let granularity = match trigger.granularity {
                vibesql_ast::TriggerGranularity::Row => 0u8,
                vibesql_ast::TriggerGranularity::Statement => 1u8,
            };
            writer
                .write_all(&[granularity])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

            // Write when_condition (optional)
            match &trigger.when_condition {
                Some(_expr) => {
                    write_bool(writer, true)?;
                    // For now, we'll skip serializing the expression tree
                    // This can be implemented later with proper expression serialization
                    write_string(writer, "TODO: expression serialization")?;
                }
                None => {
                    write_bool(writer, false)?;
                }
            }

            // Write triggered_action
            match &trigger.triggered_action {
                vibesql_ast::TriggerAction::RawSql(sql) => {
                    writer
                        .write_all(&[0u8])
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                    write_string(writer, &sql)?;
                }
            }
        }
    }

    Ok(())
}

pub fn read_catalog<R: Read>(reader: &mut R) -> Result<Database, StorageError> {
    let mut db = Database::new();

    // Read schemas
    let schema_count = read_u32(reader)?;
    for _ in 0..schema_count {
        let schema_name = read_string(reader)?;
        // Create schema directly on catalog
        &mut db.catalog
            .create_schema(schema_name)
            .map_err(|e| StorageError::NotImplemented(format!("Failed to create schema: {}", e)))?;
    }

    // Read roles
    let role_count = read_u32(reader)?;
    for _ in 0..role_count {
        let role_name = read_string(reader)?;
        &mut db.catalog
            .create_role(role_name)
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
                _ => {
                    return Err(StorageError::NotImplemented(format!(
                        "Invalid sort direction: {}",
                        direction_byte
                    )))
                }
            };

            columns.push(vibesql_ast::IndexColumn { column_name, direction });
        }

        index_specs.push((index_name, table_name, unique, columns));
    }

    // Create indexes
    for (index_name, table_name, unique, columns) in index_specs {
        db.create_index(index_name, table_name, unique, columns)
            .map_err(|e| StorageError::NotImplemented(format!("Failed to create index: {}", e)))?;
    }

    // Read triggers
    let trigger_count = read_u32(reader)?;

    for _ in 0..trigger_count {
        let name = read_string(reader)?;
        let table_name = read_string(reader)?;

        // Read timing
        let timing_byte = read_u8(reader)?;
        let timing = match timing_byte {
            0 => vibesql_ast::TriggerTiming::Before,
            1 => vibesql_ast::TriggerTiming::After,
            2 => vibesql_ast::TriggerTiming::InsteadOf,
            _ => {
                return Err(StorageError::NotImplemented(format!(
                    "Invalid trigger timing: {}",
                    timing_byte
                )))
            }
        };

        // Read event
        let event_byte = read_u8(reader)?;
        let event = match event_byte {
            0 => vibesql_ast::TriggerEvent::Insert,
            1 => vibesql_ast::TriggerEvent::Update(None),
            2 => vibesql_ast::TriggerEvent::Delete,
            3 => {
                // Update with column list
                let col_count = read_u32(reader)?;
                let mut cols = Vec::new();
                for _ in 0..col_count {
                    cols.push(read_string(reader)?);
                }
                vibesql_ast::TriggerEvent::Update(Some(cols))
            }
            _ => {
                return Err(StorageError::NotImplemented(format!(
                    "Invalid trigger event: {}",
                    event_byte
                )))
            }
        };

        // Read granularity
        let granularity_byte = read_u8(reader)?;
        let granularity = match granularity_byte {
            0 => vibesql_ast::TriggerGranularity::Row,
            1 => vibesql_ast::TriggerGranularity::Statement,
            _ => {
                return Err(StorageError::NotImplemented(format!(
                    "Invalid trigger granularity: {}",
                    granularity_byte
                )))
            }
        };

        // Read when_condition
        let has_when = read_bool(reader)?;
        let when_condition = if has_when {
            let _expr_str = read_string(reader)?;
            // For now, we can't deserialize the expression
            // This will be implemented when proper expression serialization is added
            None
        } else {
            None
        };

        // Read triggered_action
        let action_type = read_u8(reader)?;
        let triggered_action = match action_type {
            0 => {
                let sql = read_string(reader)?;
                vibesql_ast::TriggerAction::RawSql(sql)
            }
            _ => {
                return Err(StorageError::NotImplemented(format!(
                    "Invalid trigger action type: {}",
                    action_type
                )))
            }
        };

        // Create trigger definition
        let trigger = vibesql_catalog::TriggerDefinition::new(
            name,
            timing,
            event,
            table_name,
            granularity,
            when_condition,
            triggered_action,
        );

        // Add to catalog
        &mut db.catalog.create_trigger(trigger).map_err(|e| {
            StorageError::NotImplemented(format!("Failed to create trigger: {}", e))
        })?;
    }

    Ok(db)
}

/// Parse data type string back to DataType enum
fn parse_data_type(type_str: &str) -> Result<vibesql_types::DataType, StorageError> {
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
        s if s.starts_with("VARCHAR") => Ok(DataType::Varchar { max_length: None }),
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
            let precision = parts.first().and_then(|p| p.parse().ok()).unwrap_or(38);
            let scale = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
            Ok(DataType::Numeric { precision, scale })
        }
        s if s.starts_with("DECIMAL(") => {
            let params = s.trim_start_matches("DECIMAL(").trim_end_matches(')');
            let parts: Vec<&str> = params.split(',').map(|p| p.trim()).collect();
            let precision = parts.first().and_then(|p| p.parse().ok()).unwrap_or(38);
            let scale = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
            Ok(DataType::Decimal { precision, scale })
        }
        _ => Err(StorageError::NotImplemented(format!("Unsupported data type: {}", type_str))),
    }
}
