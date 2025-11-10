use parser::Parser;
use storage::{parse_sql_statements, read_sql_dump, Database};

use super::display::truncate_for_display;

/// Load database from SQL dump file
///
/// Reads SQL dump, splits into statements, parses and executes each one.
pub fn load_database(path: &str) -> anyhow::Result<Database> {
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
        execute_statement_for_load(&mut db, statement).map_err(|e| {
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
pub fn execute_statement_for_load(
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
