use std::time::Instant;
use storage::Database;
use parser::Parser;

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
}
