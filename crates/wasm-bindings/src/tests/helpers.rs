//! Helper functions for WASM API tests

/// Helper to execute SQL (handles multi-statement SQL like loading database schemas)
pub fn execute_sql(db: &mut storage::Database, sql: &str) -> Result<(), String> {
    // Split by semicolons to handle multiple statements
    for statement_text in sql.split(';') {
        // Remove comment lines and trim
        let cleaned: String = statement_text
            .lines()
            .filter(|line| !line.trim().is_empty() && !line.trim().starts_with("--"))
            .collect::<Vec<&str>>()
            .join("\n");

        let trimmed = cleaned.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Parse the SQL
        let stmt = parser::Parser::parse_sql(trimmed).map_err(|e| {
            // Show first 100 chars of SQL for debugging
            let sql_preview = if trimmed.len() > 100 {
                format!("{}...", &trimmed[..100])
            } else {
                trimmed.to_string()
            };
            format!("Parse error in '{}': {:?}", sql_preview, e)
        })?;

        // Execute based on statement type
        match stmt {
            ast::Statement::CreateTable(create_stmt) => {
                executor::CreateTableExecutor::execute(&create_stmt, db)
                    .map_err(|e| format!("CreateTable error: {:?}", e))?;
            }
            ast::Statement::DropTable(drop_stmt) => {
                executor::DropTableExecutor::execute(&drop_stmt, db)
                    .map_err(|e| format!("DropTable error: {:?}", e))?;
            }
            ast::Statement::Insert(insert_stmt) => {
                executor::InsertExecutor::execute(db, &insert_stmt)
                    .map_err(|e| format!("Insert error: {:?}", e))?;
            }
            ast::Statement::Select(select_stmt) => {
                let select_executor = executor::SelectExecutor::new(db);
                select_executor
                    .execute(&select_stmt)
                    .map_err(|e| format!("Select error: {:?}", e))?;
            }
            ast::Statement::Update(update_stmt) => {
                executor::UpdateExecutor::execute(&update_stmt, db)
                    .map_err(|e| format!("Update error: {:?}", e))?;
            }
            ast::Statement::Delete(delete_stmt) => {
                executor::DeleteExecutor::execute(&delete_stmt, db)
                    .map_err(|e| format!("Delete error: {:?}", e))?;
            }
            ast::Statement::BeginTransaction(begin_stmt) => {
                executor::BeginTransactionExecutor::execute(&begin_stmt, db)
                    .map_err(|e| format!("Begin transaction error: {:?}", e))?;
            }
            ast::Statement::Commit(commit_stmt) => {
                executor::CommitExecutor::execute(&commit_stmt, db)
                    .map_err(|e| format!("Commit error: {:?}", e))?;
            }
            ast::Statement::Rollback(rollback_stmt) => {
                executor::RollbackExecutor::execute(&rollback_stmt, db)
                    .map_err(|e| format!("Rollback error: {:?}", e))?;
            }
            _ => return Err(format!("Unsupported statement type: {:?}", stmt)),
        }
    }
    Ok(())
}

/// Helper to create the appropriate database based on name
/// Returns (Database, Option<load_error>)
pub fn setup_example_database(db_name: &str) -> (storage::Database, Option<String>) {
    match db_name {
        "northwind" => {
            let mut db = storage::Database::new();
            let northwind_sql = include_str!("../../../../web-demo/examples/northwind.sql");
            match execute_sql(&mut db, northwind_sql) {
                Ok(_) => (db, None),
                Err(e) => {
                    (storage::Database::new(), Some(format!("northwind DB load failed: {}", e)))
                }
            }
        }
        "employees" => {
            let mut db = storage::Database::new();
            let employees_sql = include_str!("../../../../web-demo/examples/employees.sql");
            match execute_sql(&mut db, employees_sql) {
                Ok(_) => (db, None),
                Err(e) => {
                    (storage::Database::new(), Some(format!("employees DB load failed: {}", e)))
                }
            }
        }
        // All other databases (empty, company, university, etc.) start as empty
        // Examples using these databases create their own tables
        _ => (storage::Database::new(), None),
    }
}

/// Parse examples.ts to extract SQL queries for testing
/// Returns: Vec<(id, database, sql)>
pub fn parse_examples() -> Vec<(String, String, String)> {
    let examples_ts = include_str!("../../../../web-demo/src/data/examples.ts");

    let mut examples = Vec::new();
    let lines: Vec<&str> = examples_ts.lines().collect();

    let mut i = 0;
    while i < lines.len() {
        // Look for example id
        if let Some(id_line) = lines[i].strip_prefix("        id: '") {
            if let Some(id_end) = id_line.find("'") {
                let id = &id_line[..id_end];

                // Look for database (within next 5 lines)
                let mut database = String::new();
                for line in lines.iter().skip(i + 1).take(5) {
                    if let Some(db_line) = line.strip_prefix("        database: '") {
                        if let Some(db_end) = db_line.find("'") {
                            database = db_line[..db_end].to_string();
                            break;
                        }
                    }
                }

                // Look for SQL (within next 15 lines)
                for j in (i + 1)..(i + 15).min(lines.len()) {
                    // Check for single-quote single-line SQL
                    if let Some(sql_start) = lines[j].strip_prefix("        sql: '") {
                        // Single-line SQL in single quotes
                        if let Some(sql_end) = sql_start.rfind("',") {
                            let sql = &sql_start[..sql_end];
                            if !database.is_empty() && !sql.is_empty() {
                                examples.push((id.to_string(), database.clone(), sql.to_string()));
                            }
                            break;
                        }
                    }
                    // Check for backtick multi-line SQL
                    else if let Some(first_line_start) = lines[j].strip_prefix("        sql: `") {
                        // Multi-line SQL in backticks
                        let mut sql_lines = Vec::new();

                        // Check if SQL starts on the same line
                        if !first_line_start.is_empty()
                            && !first_line_start.ends_with("`,")
                            && !first_line_start.ends_with("`")
                        {
                            // SQL starts on same line as `sql: ``, add it
                            sql_lines.push(first_line_start);
                        }

                        // Collect remaining lines
                        let mut k = j + 1;
                        while k < lines.len() {
                            let line = lines[k];
                            if line.ends_with("`,") || line.ends_with("`") {
                                // Check if there's SQL on the closing line before the backtick
                                let line_trimmed =
                                    line.trim_end_matches("`,").trim_end_matches("`").trim_end();
                                if !line_trimmed.is_empty() {
                                    sql_lines.push(line_trimmed);
                                }
                                break;
                            }
                            // Remove leading whitespace but preserve SQL formatting
                            sql_lines.push(line.trim_start());
                            k += 1;
                        }

                        if !database.is_empty() && !sql_lines.is_empty() {
                            let sql = sql_lines.join("\n");
                            examples.push((id.to_string(), database.clone(), sql));
                        }
                        break;
                    }
                }
            }
        }
        i += 1;
    }

    examples
}

/// Check if an example uses known unsupported SQL features
pub fn uses_unsupported_features(sql: &str, example_id: &str) -> Option<&'static str> {
    let sql_upper = sql.to_uppercase();

    // DDL examples test CREATE TABLE with constraints (see issue #214)
    if example_id.starts_with("ddl-") {
        return Some("CREATE TABLE with constraints (PRIMARY KEY, UNIQUE, CHECK, etc.)");
    }

    // Check for known unsupported features
    if sql_upper.contains("WITH ") && (sql_upper.contains(" AS (") || sql_upper.contains(" AS(")) {
        return Some("Common Table Expressions (CTEs)");
    }
    if sql_upper.contains(" UNION ")
        || sql_upper.contains(" INTERSECT ")
        || sql_upper.contains(" EXCEPT ")
    {
        return Some("SET operations (UNION/INTERSECT/EXCEPT)");
    }
    if sql_upper.contains("ALTER TABLE") || sql_upper.contains("DROP INDEX") {
        return Some("DDL operations (ALTER TABLE/DROP INDEX)");
    }
    if sql.contains('|') && !sql.contains("'|'") {
        return Some("pipe character (possibly unsupported syntax)");
    }
    // Aggregate functions with GROUP BY or multi-column aggregates
    if (sql_upper.contains("COUNT(")
        || sql_upper.contains("AVG(")
        || sql_upper.contains("SUM(")
        || sql_upper.contains("MIN(")
        || sql_upper.contains("MAX("))
        && (sql_upper.contains("GROUP BY")
            || sql_upper.matches("COUNT(").count() > 1
            || sql_upper.contains("AVG(")
            || sql_upper.contains("SUM("))
    {
        return Some("Aggregate functions with GROUP BY or multiple aggregates");
    }
    // Subqueries
    if sql_upper.contains("SELECT") && sql_upper.matches("SELECT").count() > 1 {
        return Some("Subqueries");
    }
    // Recursive CTEs
    if sql_upper.contains("RECURSIVE") {
        return Some("Recursive CTEs");
    }
    if sql_upper.contains("EXTRACT(")
        || sql_upper.contains("DATE_PART(")
        || sql_upper.contains("YEAR(")
    {
        return Some("Date/time extraction functions");
    }

    None
}
