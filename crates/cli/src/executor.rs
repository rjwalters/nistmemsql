use std::time::Instant;

use parser::Parser;
use storage::{parse_sql_statements, read_sql_dump, Database};

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
