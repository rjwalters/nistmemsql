//! TRUNCATE TABLE statement execution

use std::collections::HashSet;
use vibesql_ast::{TruncateCascadeOption, TruncateTableStmt};
use vibesql_storage::Database;

use crate::{errors::ExecutorError, privilege_checker::PrivilegeChecker};

/// Executor for TRUNCATE TABLE statements
pub struct TruncateTableExecutor;

impl TruncateTableExecutor {
    /// Execute a TRUNCATE TABLE statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The TRUNCATE TABLE statement AST node
    /// * `database` - The database to truncate the table in
    ///
    /// # Returns
    ///
    /// Number of rows deleted or error
    ///
    /// # Examples
    ///
    /// ```
    /// use vibesql_ast::{ColumnDef, CreateTableStmt, TruncateTableStmt};
    /// use vibesql_executor::{CreateTableExecutor, TruncateTableExecutor};
    /// use vibesql_storage::{Database, Row};
    /// use vibesql_types::{DataType, SqlValue};
    ///
    /// let mut db = Database::new();
    /// let create_stmt = CreateTableStmt {
    ///     table_name: "users".to_string(),
    ///     columns: vec![ColumnDef {
    ///         name: "id".to_string(),
    ///         data_type: DataType::Integer,
    ///         nullable: false,
    ///         constraints: vec![],
    ///         default_value: None,
    ///         comment: None,
    ///     }],
    ///     table_constraints: vec![],
    ///     table_options: vec![],
    /// };
    /// CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
    ///
    /// // Insert some rows
    /// db.insert_row("users", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    /// db.insert_row("users", Row::new(vec![SqlValue::Integer(2)])).unwrap();
    ///
    /// let stmt = TruncateTableStmt { table_name: "users".to_string(), if_exists: false, cascade: None };
    ///
    /// let result = TruncateTableExecutor::execute(&stmt, &mut db);
    /// assert_eq!(result.unwrap(), 2); // 2 rows deleted
    /// assert_eq!(db.get_table("users").unwrap().row_count(), 0);
    /// ```
    pub fn execute(
        stmt: &TruncateTableStmt,
        database: &mut Database,
    ) -> Result<usize, ExecutorError> {
        // Check if table exists
        if !database.catalog.table_exists(&stmt.table_name) {
            if stmt.if_exists {
                // IF EXISTS specified and table doesn't exist - silently succeed
                return Ok(0);
            } else {
                return Err(ExecutorError::TableNotFound(stmt.table_name.clone()));
            }
        }

        // Determine CASCADE behavior (default to RESTRICT)
        let use_cascade = matches!(stmt.cascade, Some(TruncateCascadeOption::Cascade));

        if use_cascade {
            // CASCADE mode: recursively truncate dependent tables
            execute_truncate_cascade(database, &stmt.table_name)
        } else {
            // RESTRICT mode (default): fail if referenced by foreign keys
            // Check DELETE privilege on the table (TRUNCATE requires DELETE privilege)
            PrivilegeChecker::check_delete(database, &stmt.table_name)?;

            // Check if TRUNCATE is allowed (no DELETE triggers, no FK references)
            if !can_use_truncate(database, &stmt.table_name)? {
                return Err(ExecutorError::Other(format!(
                    "Cannot TRUNCATE table '{}': table has DELETE triggers or is referenced by foreign keys",
                    stmt.table_name
                )));
            }

            // Execute the truncate
            execute_truncate(database, &stmt.table_name)
        }
    }
}

/// Check if TRUNCATE can be used for the table
///
/// TRUNCATE cannot be used if:
/// - Table has DELETE triggers (BEFORE/AFTER DELETE)
/// - Table is referenced by foreign keys from other tables
///
/// # Returns
/// - `Ok(true)` if TRUNCATE can be safely used
/// - `Ok(false)` if TRUNCATE cannot be used
/// - `Err` if table doesn't exist
fn can_use_truncate(database: &Database, table_name: &str) -> Result<bool, ExecutorError> {
    // Check for DELETE triggers on this table
    if has_delete_triggers(database, table_name) {
        return Ok(false);
    }

    // Check if this table is referenced by foreign keys from other tables
    if is_fk_referenced(database, table_name)? {
        return Ok(false);
    }

    Ok(true)
}

/// Check if a table has any DELETE triggers
fn has_delete_triggers(database: &Database, table_name: &str) -> bool {
    database
        .catalog
        .get_triggers_for_table(table_name, Some(vibesql_ast::TriggerEvent::Delete))
        .next()
        .is_some()
}

/// Check if a table is referenced by foreign keys from other tables
///
/// Returns true if any other table has a foreign key constraint referencing this table.
fn is_fk_referenced(database: &Database, parent_table_name: &str) -> Result<bool, ExecutorError> {
    // Scan all tables to find foreign keys that reference this table
    for table_name in database.catalog.list_tables() {
        let child_schema = database
            .catalog
            .get_table(&table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

        for fk in &child_schema.foreign_keys {
            if fk.parent_table == parent_table_name {
                return Ok(true); // Found a reference
            }
        }
    }

    Ok(false) // No references found
}

/// Execute TRUNCATE operation
///
/// Clears all rows and indexes in a single operation.
/// Provides significant performance improvement over row-by-row deletion.
fn execute_truncate(database: &mut Database, table_name: &str) -> Result<usize, ExecutorError> {
    let table = database
        .get_table_mut(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let row_count = table.row_count();

    // Clear all data at once (O(1) operation)
    table.clear();

    Ok(row_count)
}

/// Execute TRUNCATE CASCADE operation
///
/// Recursively truncates all tables that reference the target table via foreign keys.
/// Uses topological sort to determine truncation order (children first).
///
/// # Arguments
///
/// * `database` - The database containing the tables
/// * `table_name` - The root table to truncate
///
/// # Returns
///
/// Total number of rows deleted across all tables
///
/// # Errors
///
/// Returns error if:
/// - Any table in the dependency chain has DELETE triggers
/// - User lacks DELETE privilege on any affected table
/// - Circular foreign key dependencies are detected
fn execute_truncate_cascade(
    database: &mut Database,
    table_name: &str,
) -> Result<usize, ExecutorError> {
    // Phase 1: Collect all dependent tables in topological order
    let truncate_order = collect_fk_dependencies(database, table_name)?;

    // Phase 2: Validate all tables can be truncated (no DELETE triggers)
    // and check DELETE privilege on all tables
    for tbl in &truncate_order {
        if has_delete_triggers(database, tbl) {
            return Err(ExecutorError::Other(format!(
                "Cannot TRUNCATE CASCADE table '{}': dependent table '{}' has DELETE triggers",
                table_name, tbl
            )));
        }
        PrivilegeChecker::check_delete(database, tbl)?;
    }

    // Phase 3: Execute truncates in order (children first, then parents)
    let mut total_rows_deleted = 0;
    for tbl in truncate_order {
        let rows_deleted = execute_truncate(database, &tbl)?;
        total_rows_deleted += rows_deleted;
    }

    Ok(total_rows_deleted)
}

/// Collect all tables that need to be truncated via CASCADE
///
/// Uses topological sort to determine the correct truncation order.
/// Returns tables in the order they should be truncated (children before parents).
///
/// # Arguments
///
/// * `database` - The database containing the tables
/// * `root_table` - The root table to start from
///
/// # Returns
///
/// Vector of table names in truncation order (children first)
///
/// # Errors
///
/// Returns error if circular foreign key dependencies are detected
fn collect_fk_dependencies(
    database: &Database,
    root_table: &str,
) -> Result<Vec<String>, ExecutorError> {
    let mut visited = HashSet::new();
    let mut order = Vec::new();
    let mut recursion_stack = HashSet::new();

    // Recursive DFS to collect dependencies
    fn visit(
        database: &Database,
        table_name: &str,
        visited: &mut HashSet<String>,
        order: &mut Vec<String>,
        recursion_stack: &mut HashSet<String>,
    ) -> Result<(), ExecutorError> {
        // Detect cycles
        if recursion_stack.contains(table_name) {
            return Err(ExecutorError::Other(format!(
                "Circular foreign key dependency detected involving table '{}'",
                table_name
            )));
        }

        if visited.contains(table_name) {
            return Ok(());
        }

        recursion_stack.insert(table_name.to_string());
        visited.insert(table_name.to_string());

        // Find all tables that reference this table (children)
        let children = get_fk_children(database, table_name)?;
        for child in children {
            visit(database, &child, visited, order, recursion_stack)?;
        }

        recursion_stack.remove(table_name);
        order.push(table_name.to_string());

        Ok(())
    }

    visit(
        database,
        root_table,
        &mut visited,
        &mut order,
        &mut recursion_stack,
    )?;

    Ok(order)
}

/// Get all tables that have foreign keys referencing the given table
///
/// Returns a list of table names that directly reference the parent table.
///
/// # Arguments
///
/// * `database` - The database containing the tables
/// * `parent_table` - The table being referenced
///
/// # Returns
///
/// Vector of table names that reference the parent table
fn get_fk_children(database: &Database, parent_table: &str) -> Result<Vec<String>, ExecutorError> {
    let mut children = Vec::new();

    // Scan all tables to find foreign keys that reference this table
    for table_name in database.catalog.list_tables() {
        let child_schema = database
            .catalog
            .get_table(&table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

        for fk in &child_schema.foreign_keys {
            if fk.parent_table == parent_table && table_name != parent_table {
                children.push(table_name.clone());
                break; // Only add each child once
            }
        }
    }

    Ok(children)
}
