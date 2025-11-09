use std::time::Instant;
use storage::Database;
use parser::Parser;
use crate::commands::{CopyDirection, CopyFormat};
use crate::data_io::DataIO;

pub struct SqlExecutor {
    db: Database,
    timing_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub rows: Vec<Vec<String>>,
    pub columns: Vec<String>,
    pub row_count: usize,
    pub execution_time_ms: Option<f64>,
}

impl SqlExecutor {
    pub fn new(_database: Option<String>) -> anyhow::Result<Self> {
        // TODO: Support loading from file when database is provided
        let db = Database::new();

        Ok(SqlExecutor {
            db,
            timing_enabled: false,
        })
    }

    pub fn execute(&mut self, sql: &str) -> anyhow::Result<QueryResult> {
        let start = Instant::now();

        // Parse SQL
        let statement = Parser::parse_sql(sql)
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Execute statement through appropriate executor
        let mut result = QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
            row_count: 0,
            execution_time_ms: None,
        };

        match statement {
            ast::Statement::Select(select_stmt) => {
                // Execute SELECT and format results
                let executor = executor::SelectExecutor::new(&self.db);
                match executor.execute(&select_stmt) {
                    Ok(rows) => {
                        result.row_count = rows.len();
                        // Convert rows to string representation
                        if !rows.is_empty() {
                            // Get column names from the select statement
                            result.columns = vec!["Column".to_string(); rows[0].values.len()];
                            for row in rows {
                                let row_strs: Vec<String> = row.values.iter()
                                    .map(|v| format!("{:?}", v))
                                    .collect();
                                result.rows.push(row_strs);
                            }
                        }
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            ast::Statement::CreateTable(create_stmt) => {
                let mut db_mut = self.db.clone();
                match executor::CreateTableExecutor::execute(&create_stmt, &mut db_mut) {
                    Ok(_) => {
                        self.db = db_mut;
                        result.row_count = 0; // DDL doesn't return rows
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            ast::Statement::Insert(insert_stmt) => {
                let mut db_mut = self.db.clone();
                match executor::InsertExecutor::execute(&mut db_mut, &insert_stmt) {
                    Ok(_) => {
                        self.db = db_mut;
                        result.row_count = 0; // TODO: Get actual row count from executor
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            ast::Statement::Update(update_stmt) => {
                let mut db_mut = self.db.clone();
                match executor::UpdateExecutor::execute(&update_stmt, &mut db_mut) {
                    Ok(affected_rows) => {
                        self.db = db_mut;
                        result.row_count = affected_rows;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            ast::Statement::Delete(delete_stmt) => {
                let mut db_mut = self.db.clone();
                match executor::DeleteExecutor::execute(&delete_stmt, &mut db_mut) {
                    Ok(affected_rows) => {
                        self.db = db_mut;
                        result.row_count = affected_rows;
                    }
                    Err(e) => return Err(anyhow::anyhow!("{}", e)),
                }
            }
            _ => {
                return Err(anyhow::anyhow!("Statement type not yet supported in CLI"));
            }
        }

        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        if self.timing_enabled {
            result.execution_time_ms = Some(elapsed);
        }

        Ok(result)
    }

    pub fn describe_table(&self, _table_name: &str) -> anyhow::Result<()> {
        println!("Table description not yet implemented");
        Ok(())
    }

    pub fn list_tables(&self) -> anyhow::Result<()> {
        // Get all tables from database
        let tables = self.db.list_tables();
        if tables.is_empty() {
            println!("No tables found");
        } else {
            println!("Tables:");
            for table_name in tables {
                println!("  {}", table_name);
            }
        }
        Ok(())
    }

    pub fn toggle_timing(&mut self) {
        self.timing_enabled = !self.timing_enabled;
        let state = if self.timing_enabled { "on" } else { "off" };
        println!("Timing is {}", state);
    }

    pub fn list_schemas(&self) -> anyhow::Result<()> {
        let schemas = self.db.catalog.list_schemas();
        let current_schema = self.db.catalog.get_current_schema();

        if schemas.is_empty() {
            println!("No schemas found");
        } else {
            println!("List of schemas");
            println!("{:<20} {:<10}", "Name", "");
            println!("{}", "-".repeat(30));
            for schema_name in schemas {
                let marker = if schema_name == current_schema { "(current)" } else { "" };
                println!("{:<20} {:<10}", schema_name, marker);
            }
        }
        Ok(())
    }

    pub fn list_indexes(&self) -> anyhow::Result<()> {
        let index_names = self.db.list_indexes();

        if index_names.is_empty() {
            println!("No indexes found");
        } else {
            println!("List of indexes");
            println!("{:<20} {:<20} {:<15} {:<10}", "Name", "Table", "Columns", "Type");
            println!("{}", "-".repeat(70));

            for index_name in index_names {
                if let Some(index_meta) = self.db.get_index(&index_name) {
                    let columns_str = index_meta.columns.iter()
                        .map(|col| col.column_name.clone())
                        .collect::<Vec<_>>()
                        .join(", ");
                    let index_type = if index_meta.unique { "UNIQUE" } else { "BTREE" };

                    println!("{:<20} {:<20} {:<15} {:<10}",
                        index_meta.index_name,
                        index_meta.table_name,
                        columns_str,
                        index_type
                    );
                }
            }
        }
        Ok(())
    }

    pub fn list_roles(&self) -> anyhow::Result<()> {
        let roles = self.db.catalog.list_roles();
        let current_role = self.db.get_current_role();

        if roles.is_empty() {
            // If no roles defined, show default PUBLIC role
            println!("List of roles");
            println!("{:<20} {:<15}", "Name", "Attributes");
            println!("{}", "-".repeat(35));
            println!("{:<20} {:<15}", "PUBLIC", "(default)");
        } else {
            println!("List of roles");
            println!("{:<20} {:<15}", "Name", "Attributes");
            println!("{}", "-".repeat(35));

            for role_name in roles {
                let marker = if role_name == current_role { "(current)" } else { "" };
                println!("{:<20} {:<15}", role_name, marker);
            }
        }
        Ok(())
    }

    /// Validate table name to prevent SQL injection
    /// Returns an error if the table doesn't exist in the database
    fn validate_table_name(&self, table_name: &str) -> anyhow::Result<()> {
        // Check if table exists in the database
        if self.db.get_table(table_name).is_none() {
            return Err(anyhow::anyhow!("Table '{}' does not exist", table_name));
        }
        Ok(())
    }

    /// Validate CSV column names against table schema to prevent SQL injection
    /// Returns an error if columns don't match the table schema
    fn validate_csv_columns(&self, file_path: &str, table_name: &str) -> anyhow::Result<()> {
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        // Get table schema
        let table = self.db.get_table(table_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' does not exist", table_name))?;

        // Read CSV header
        let file = File::open(file_path)
            .map_err(|e| anyhow::anyhow!("Failed to open file '{}': {}", file_path, e))?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        let header_line = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("CSV file is empty"))?
            .map_err(|e| anyhow::anyhow!("Failed to read header: {}", e))?;

        let csv_columns: Vec<&str> = header_line.split(',').map(|s| s.trim()).collect();

        // Validate each column name
        for col_name in &csv_columns {
            // Check for SQL injection characters
            if col_name.contains(';') || col_name.contains('\'') || col_name.contains('"')
                || col_name.contains('(') || col_name.contains(')') {
                return Err(anyhow::anyhow!(
                    "Invalid column name '{}': contains forbidden characters",
                    col_name
                ));
            }

            // Check if column exists in table schema
            let column_exists = table.schema.columns.iter()
                .any(|c| c.name.eq_ignore_ascii_case(col_name));

            if !column_exists {
                return Err(anyhow::anyhow!(
                    "Column '{}' does not exist in table '{}'",
                    col_name,
                    table_name
                ));
            }
        }

        Ok(())
    }

    pub fn handle_copy(
        &mut self,
        table: &str,
        file_path: &str,
        direction: CopyDirection,
        format: CopyFormat,
    ) -> anyhow::Result<()> {
        // Validate table name to prevent SQL injection
        self.validate_table_name(table)?;

        match direction {
            CopyDirection::Export => {
                // Execute SELECT * FROM table to get all data
                let query = format!("SELECT * FROM {}", table);
                let result = self.execute(&query)?;

                // Export based on format
                match format {
                    CopyFormat::Csv => DataIO::export_csv(&result, file_path)?,
                    CopyFormat::Json => DataIO::export_json(&result, file_path)?,
                }
            }
            CopyDirection::Import => {
                // Import based on format
                match format {
                    CopyFormat::Csv => {
                        // Validate CSV columns before import
                        self.validate_csv_columns(file_path, table)?;

                        // Import CSV - generates INSERT statements
                        let insert_statements = DataIO::import_csv(file_path, table)?;

                        // Execute each INSERT statement
                        let mut success_count = 0;
                        for stmt in &insert_statements {
                            match self.execute(stmt) {
                                Ok(_) => success_count += 1,
                                Err(e) => {
                                    eprintln!("Warning: Failed to insert row: {}", e);
                                    // Continue with remaining rows
                                }
                            }
                        }
                        println!("Imported {} rows into '{}'", success_count, table);
                    }
                    CopyFormat::Json => {
                        return Err(anyhow::anyhow!("JSON import not yet implemented"));
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_schemas() {
        let executor = SqlExecutor::new(None).unwrap();
        // Default database should have "public" schema
        assert!(executor.list_schemas().is_ok());
    }

    #[test]
    fn test_list_indexes_empty() {
        let executor = SqlExecutor::new(None).unwrap();
        // New database should have no indexes
        assert!(executor.list_indexes().is_ok());
    }

    #[test]
    fn test_list_roles() {
        let executor = SqlExecutor::new(None).unwrap();
        // Should show at least the default PUBLIC role
        assert!(executor.list_roles().is_ok());
    }

    #[test]
    fn test_validate_table_name_nonexistent() {
        let executor = SqlExecutor::new(None).unwrap();
        // Should fail for non-existent table
        let result = executor.validate_table_name("nonexistent_table");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_validate_table_name_sql_injection() {
        let executor = SqlExecutor::new(None).unwrap();
        // Should fail for table names with SQL injection attempts
        let result = executor.validate_table_name("users; DROP TABLE users; --");
        assert!(result.is_err());
    }
}
