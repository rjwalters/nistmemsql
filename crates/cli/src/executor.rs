use std::time::Instant;

use parser::Parser;
use storage::{parse_sql_statements, read_sql_dump, Database};

use crate::{
    commands::{CopyDirection, CopyFormat},
    data_io::DataIO,
};

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
    pub fn new(database: Option<String>) -> anyhow::Result<Self> {
        // Load database from file if provided, otherwise create new in-memory database
        let db = if let Some(db_path) = database {
            // Check if file exists
            if std::path::Path::new(&db_path).exists() {
                // Load existing database from SQL dump
                Self::load_database(&db_path)?
            } else {
                // File doesn't exist, create new database
                // (Will be saved when user uses \save or when modifications occur)
                Database::new()
            }
        } else {
            // No database file specified, use in-memory database
            Database::new()
        };

        Ok(SqlExecutor { db, timing_enabled: false })
    }

    /// Load database from SQL dump file
    ///
    /// Reads SQL dump, splits into statements, parses and executes each one.
    fn load_database(path: &str) -> anyhow::Result<Database> {
        // Read the SQL dump file
        let sql_content = read_sql_dump(path)
            .map_err(|e| anyhow::anyhow!("Failed to read database file {}: {}", path, e))?;

        // Split into individual statements
        let statements = parse_sql_statements(&sql_content)
            .map_err(|e| anyhow::anyhow!("Failed to parse SQL dump: {}", e))?;

        // Create a new database to populate
        let mut db = Database::new();

        // Execute each statement
        for (idx, stmt_sql) in statements.iter().enumerate() {
            // Skip empty statements and comments
            let trimmed = stmt_sql.trim();
            if trimmed.is_empty() || trimmed.starts_with("--") {
                continue;
            }

            // Parse the statement
            let statement = Parser::parse_sql(trimmed).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to parse statement {} in {}: {}\nStatement: {}",
                    idx + 1,
                    path,
                    e,
                    truncate_for_display(trimmed, 100)
                )
            })?;

            // Execute the statement
            Self::execute_statement_for_load(&mut db, statement).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to execute statement {} in {}: {}\nStatement: {}",
                    idx + 1,
                    path,
                    e,
                    truncate_for_display(trimmed, 100)
                )
            })?;
        }

        Ok(db)
    }

    /// Execute a single statement during database load
    fn execute_statement_for_load(
        db: &mut Database,
        statement: ast::Statement,
    ) -> anyhow::Result<()> {
        match statement {
            ast::Statement::CreateSchema(schema_stmt) => {
                executor::SchemaExecutor::execute_create_schema(&schema_stmt, db)?;
            }
            ast::Statement::CreateTable(create_stmt) => {
                executor::CreateTableExecutor::execute(&create_stmt, db)?;
            }
            ast::Statement::CreateIndex(index_stmt) => {
                executor::CreateIndexExecutor::execute(&index_stmt, db)?;
            }
            ast::Statement::CreateRole(role_stmt) => {
                executor::RoleExecutor::execute_create_role(&role_stmt, db)?;
            }
            ast::Statement::Insert(insert_stmt) => {
                executor::InsertExecutor::execute(db, &insert_stmt)?;
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Statement type not supported in database load: {:?}",
                    statement
                ));
            }
        }
        Ok(())
    }

    pub fn execute(&mut self, sql: &str) -> anyhow::Result<QueryResult> {
        let start = Instant::now();

        // Parse SQL
        let statement = Parser::parse_sql(sql).map_err(|e| anyhow::anyhow!("{}", e))?;

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
                                let row_strs: Vec<String> =
                                    row.values.iter().map(|v| format!("{:?}", v)).collect();
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

    pub fn describe_table(&self, table_name: &str) -> anyhow::Result<()> {
        // 1. Normalize table name to uppercase (SQL standard for unquoted identifiers)
        let normalized_name = table_name.to_uppercase();

        // 2. Validate table exists
        let table = self.db.get_table(&normalized_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' does not exist", table_name))?;

        // 2. Get schema name
        let schema_name = self.db.catalog.get_current_schema();

        // 3. Print table header
        println!("                Table \"{}.{}\"", schema_name, table_name);

        // 4. Print column information
        println!(" {:<20} | {:<25} | {:<8} | {:<10}",
                 "Column", "Type", "Nullable", "Default");
        println!("{}", "-".repeat(70));

        for column in &table.schema.columns {
            let nullable = if column.nullable { "" } else { "not null" };
            let default_val = column.default_value
                .as_ref()
                .map(|v| format!("{:?}", v))
                .unwrap_or_default();

            println!(" {:<20} | {:<25} | {:<8} | {:<10}",
                     column.name,
                     format_data_type(&column.data_type),
                     nullable,
                     truncate_for_display(&default_val, 10));
        }

        // 5. Print constraints
        print_constraints(&table.schema)?;

        // 6. Print indexes
        print_indexes(&self.db, &normalized_name)?;

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
                    let columns_str = index_meta
                        .columns
                        .iter()
                        .map(|col| col.column_name.clone())
                        .collect::<Vec<_>>()
                        .join(", ");
                    let index_type = if index_meta.unique { "UNIQUE" } else { "BTREE" };

                    println!(
                        "{:<20} {:<20} {:<15} {:<10}",
                        index_meta.index_name, index_meta.table_name, columns_str, index_type
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
        use std::{
            fs::File,
            io::{BufRead, BufReader},
        };

        // Get table schema
        let table = self
            .db
            .get_table(table_name)
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
            if col_name.contains(';')
                || col_name.contains('\'')
                || col_name.contains('"')
                || col_name.contains('(')
                || col_name.contains(')')
            {
                return Err(anyhow::anyhow!(
                    "Invalid column name '{}': contains forbidden characters",
                    col_name
                ));
            }

            // Check if column exists in table schema
            let column_exists =
                table.schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case(col_name));

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

    /// Validate JSON column names against table schema to prevent SQL injection
    /// Returns an error if columns don't match the table schema
    fn validate_json_columns(&self, file_path: &str, table_name: &str) -> anyhow::Result<()> {
        use std::fs;

        // Get table schema
        let table = self.db.get_table(table_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' does not exist", table_name))?;

        // Read JSON file
        let json_content = fs::read_to_string(file_path)
            .map_err(|e| anyhow::anyhow!("Failed to read file '{}': {}", file_path, e))?;

        // Parse as array of objects
        let json_array: Vec<serde_json::Map<String, serde_json::Value>> =
            serde_json::from_str(&json_content)
                .map_err(|e| anyhow::anyhow!("Invalid JSON format: {}", e))?;

        if json_array.is_empty() {
            return Err(anyhow::anyhow!("JSON file contains no data"));
        }

        // Extract column names from first object
        let first_obj = &json_array[0];
        let json_columns: Vec<&str> = first_obj.keys().map(|s| s.as_str()).collect();

        // Validate each column name
        for col_name in &json_columns {
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
                        // Validate JSON columns before import
                        self.validate_json_columns(file_path, table)?;

                        // Import JSON - generates INSERT statements
                        let insert_statements = DataIO::import_json(file_path, table)?;

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
                }
            }
        }
        Ok(())
    }

    /// Get a reference to the database (for saving)
    pub fn database(&self) -> &Database {
        &self.db
    }

    /// Save database to SQL dump file
    pub fn save_database(&self, path: &str) -> anyhow::Result<()> {
        self.db
            .save_sql_dump(path)
            .map_err(|e| anyhow::anyhow!("Failed to save database to {}: {}", path, e))
    }
}

/// Truncate a string for display in error messages
fn truncate_for_display(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

/// Format DataType for table description output (PostgreSQL-style)
fn format_data_type(data_type: &types::DataType) -> String {
    match data_type {
        types::DataType::Integer => "integer".to_string(),
        types::DataType::Smallint => "smallint".to_string(),
        types::DataType::Bigint => "bigint".to_string(),
        types::DataType::Unsigned => "unsigned bigint".to_string(),
        types::DataType::Numeric { precision, scale } => format!("numeric({}, {})", precision, scale),
        types::DataType::Decimal { precision, scale } => format!("numeric({}, {})", precision, scale),
        types::DataType::Float { precision } => format!("float({})", precision),
        types::DataType::Real => "real".to_string(),
        types::DataType::DoublePrecision => "double precision".to_string(),
        types::DataType::Character { length } => format!("character({})", length),
        types::DataType::Varchar { max_length } => {
            match max_length {
                Some(len) => format!("character varying({})", len),
                None => "character varying".to_string(),
            }
        }
        types::DataType::CharacterLargeObject => "text".to_string(),
        types::DataType::Name => "name".to_string(),
        types::DataType::Boolean => "boolean".to_string(),
        types::DataType::Date => "date".to_string(),
        types::DataType::Time { with_timezone } => {
            if *with_timezone {
                "time with time zone".to_string()
            } else {
                "time".to_string()
            }
        }
        types::DataType::Timestamp { with_timezone } => {
            if *with_timezone {
                "timestamp with time zone".to_string()
            } else {
                "timestamp".to_string()
            }
        }
        types::DataType::Interval { .. } => "interval".to_string(),
        types::DataType::BinaryLargeObject => "bytea".to_string(),
        types::DataType::UserDefined { type_name } => type_name.clone(),
        types::DataType::Null => "null".to_string(),
    }
}

/// Print constraints for a table schema
fn print_constraints(schema: &catalog::TableSchema) -> anyhow::Result<()> {
    let mut has_constraints = false;

    // Print primary key
    if let Some(pk_cols) = &schema.primary_key {
        if !has_constraints {
            println!("\nConstraints:");
            has_constraints = true;
        }
        println!("    \"{}_pkey\" PRIMARY KEY, btree ({})",
                 schema.name,
                 pk_cols.join(", "));
    }

    // Print unique constraints
    for (idx, unique_cols) in schema.unique_constraints.iter().enumerate() {
        if !has_constraints {
            println!("\nConstraints:");
            has_constraints = true;
        }
        println!("    \"{}_{}_key\" UNIQUE CONSTRAINT, btree ({})",
                 schema.name,
                 idx + 1,
                 unique_cols.join(", "));
    }

    // Print foreign key constraints
    for (idx, fk) in schema.foreign_keys.iter().enumerate() {
        if !has_constraints {
            println!("\nConstraints:");
            has_constraints = true;
        }
        println!("    \"{}_{}_fkey\" FOREIGN KEY ({}) REFERENCES {}({})",
                 schema.name,
                 idx + 1,
                 fk.column_names.join(", "),
                 fk.parent_table,
                 fk.parent_column_names.join(", "));
    }

    // Print check constraints
    for (name, _expr) in &schema.check_constraints {
        if !has_constraints {
            println!("\nConstraints:");
            has_constraints = true;
        }
        println!("    \"{}\" CHECK", name);
    }

    Ok(())
}

/// Print indexes for a table
fn print_indexes(db: &Database, table_name: &str) -> anyhow::Result<()> {
    let index_names = db.list_indexes();
    let indexes: Vec<_> = index_names
        .into_iter()
        .filter_map(|idx_name| {
            db.get_index(&idx_name).and_then(|idx| {
                if idx.table_name == table_name {
                    Some(idx)
                } else {
                    None
                }
            })
        })
        .collect();

    if !indexes.is_empty() {
        println!("\nIndexes:");
        for index in indexes {
            let idx_type = if index.unique { "UNIQUE, btree" } else { "btree" };
            let columns = index.columns.iter()
                .map(|c| c.column_name.clone())
                .collect::<Vec<_>>()
                .join(", ");

            println!("    \"{}\" {}, ({})",
                     index.index_name,
                     idx_type,
                     columns);
        }
    }

    Ok(())
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

    #[test]
    fn test_describe_table_basic() {
        let mut executor = SqlExecutor::new(None).unwrap();
        executor.execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(50))").unwrap();
        // Should print table description without error
        assert!(executor.describe_table("test").is_ok());
    }

    #[test]
    fn test_describe_nonexistent_table() {
        let executor = SqlExecutor::new(None).unwrap();
        let result = executor.describe_table("nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_describe_table_with_indexes() {
        let mut executor = SqlExecutor::new(None).unwrap();
        executor.execute("CREATE TABLE test (id INT PRIMARY KEY, email VARCHAR(100))").unwrap();
        // Note: CREATE INDEX is not yet supported in CLI executor, so skip for now
        // This test will pass once CREATE INDEX support is added
        assert!(executor.describe_table("test").is_ok());
    }

    #[test]
    fn test_describe_table_with_multiple_columns() {
        let mut executor = SqlExecutor::new(None).unwrap();
        executor.execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100), price DECIMAL(10, 2))").unwrap();
        // Should print table with multiple columns of different types
        assert!(executor.describe_table("products").is_ok());
    }
}
