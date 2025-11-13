//! TRUNCATE TABLE statement execution

use std::collections::HashSet;
use vibesql_ast::{TruncateCascadeOption, TruncateTableStmt};
use vibesql_storage::Database;

use crate::{
    errors::ExecutorError,
    privilege_checker::PrivilegeChecker,
    truncate_validation::{can_use_truncate, has_delete_triggers},
};

/// Executor for TRUNCATE TABLE statements
pub struct TruncateTableExecutor;

impl TruncateTableExecutor {
    /// Execute a TRUNCATE TABLE statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The TRUNCATE TABLE statement AST node
    /// * `database` - The database to truncate the table(s) in
    ///
    /// # Returns
    ///
    /// Total number of rows deleted from all tables or error
    ///
    /// # Behavior
    ///
    /// Supports truncating multiple tables in a single statement with all-or-nothing semantics:
    /// - Validates all tables first (existence, privileges, constraints)
    /// - Only truncates if all validations pass
    /// - IF EXISTS: skips non-existent tables, continues with existing ones
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
    /// let stmt = TruncateTableStmt { table_names: vec!["users".to_string()], if_exists: false, cascade: None };
    ///
    /// let result = TruncateTableExecutor::execute(&stmt, &mut db);
    /// assert_eq!(result.unwrap(), 2); // 2 rows deleted
    /// assert_eq!(db.get_table("users").unwrap().row_count(), 0);
    /// ```
    pub fn execute(
        stmt: &TruncateTableStmt,
        database: &mut Database,
    ) -> Result<usize, ExecutorError> {
        // Phase 1: Validation - Check all tables before truncating any
        // Collect tables that exist and need to be truncated
        let mut tables_to_truncate = Vec::new();

        for table_name in &stmt.table_names {
            // Check if table exists
            if !database.catalog.table_exists(table_name) {
                if stmt.if_exists {
                    // IF EXISTS specified and table doesn't exist - skip this table
                    continue;
                } else {
                    return Err(ExecutorError::TableNotFound(table_name.clone()));
                }
            }

            tables_to_truncate.push(table_name.as_str());
        }

        // If no tables to truncate (all were non-existent with IF EXISTS), return 0
        if tables_to_truncate.is_empty() {
            return Ok(0);
        }

        // Check DELETE privilege on all tables
        for table_name in &tables_to_truncate {
            PrivilegeChecker::check_delete(database, table_name)?;
        }

        // Determine CASCADE behavior (default to RESTRICT)
        let use_cascade = matches!(stmt.cascade, Some(TruncateCascadeOption::Cascade));

        if use_cascade {
            // CASCADE mode: recursively truncate dependent tables for each requested table
            let mut total_rows = 0;
            for table_name in &tables_to_truncate {
                total_rows += execute_truncate_cascade(database, table_name)?;
            }
            Ok(total_rows)
        } else {
            // RESTRICT mode (default): fail if referenced by foreign keys
            // Check if TRUNCATE is allowed on all tables (no DELETE triggers, no FK references)
            for table_name in &tables_to_truncate {
                if !can_use_truncate(database, table_name)? {
                    return Err(ExecutorError::Other(format!(
                        "Cannot TRUNCATE table '{}': table has DELETE triggers or is referenced by foreign keys",
                        table_name
                    )));
                }
            }

            // Phase 2: Execution - All validations passed, now truncate all tables
            let mut total_rows = 0;
            for table_name in &tables_to_truncate {
                total_rows += execute_truncate(database, table_name)?;
            }

            Ok(total_rows)
        }
    }
}


/// Reset AUTO_INCREMENT sequences for a table
///
/// Finds all AUTO_INCREMENT columns in the table and resets their associated sequences
/// to the initial value (1).
fn reset_auto_increment_sequences(database: &mut Database, table_name: &str) -> Result<(), ExecutorError> {
    // Get table schema to find AUTO_INCREMENT columns
    let table_schema = database
        .catalog
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    // Collect sequence names first to avoid borrow checker issues
    // (we can't hold an immutable reference to table_schema while mutating sequences)
    let mut sequence_names = Vec::new();
    for column in &table_schema.columns {
        if let Some(vibesql_ast::Expression::NextValue { sequence_name }) = &column.default_value {
            sequence_names.push(sequence_name.clone());
        }
    }

    // Now reset the sequences
    for sequence_name in sequence_names {
        if let Ok(sequence) = database.catalog.get_sequence_mut(&sequence_name) {
            // Reset to start value (None means use the original start_with value)
            sequence.restart(None);
        }
        // Note: If sequence doesn't exist, we silently continue.
        // This shouldn't happen in normal operation but makes the function more robust.
    }

    Ok(())
}

/// Execute TRUNCATE operation
///
/// Clears all rows and indexes in a single operation.
/// Provides significant performance improvement over row-by-row deletion.
/// Also resets any AUTO_INCREMENT sequences to their initial values.
fn execute_truncate(database: &mut Database, table_name: &str) -> Result<usize, ExecutorError> {
    let table = database
        .get_table_mut(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let row_count = table.row_count();

    // Clear all data at once (O(1) operation)
    table.clear();

    // Reset AUTO_INCREMENT sequences
    reset_auto_increment_sequences(database, table_name)?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::{
        ColumnDef, CreateTableStmt, TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming,
    };
    use vibesql_catalog::{
        ColumnSchema, ForeignKeyConstraint, ReferentialAction, TableSchema, TriggerDefinition,
    };
    use vibesql_storage::Row;
    use vibesql_types::{DataType, SqlValue};

    use crate::CreateTableExecutor;

    // Helper function to create a simple table
    fn create_test_table(db: &mut Database, table_name: &str) {
        let create_stmt = CreateTableStmt {
            table_name: table_name.to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
                ColumnDef {
                    name: "data".to_string(),
                    data_type: DataType::Varchar { max_length: Some(100) },
                    nullable: false,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
            ],
            table_constraints: vec![],
            table_options: vec![],
        };
        CreateTableExecutor::execute(&create_stmt, db).unwrap();
    }

    // ============================================================================
    // Basic Functionality Tests
    // ============================================================================

    #[test]
    fn test_truncate_basic() {
        let mut db = Database::new();
        create_test_table(&mut db, "test_table");

        // Insert test data
        db.insert_row(
            "test_table",
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("row1".to_string())]),
        )
        .unwrap();
        db.insert_row(
            "test_table",
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("row2".to_string())]),
        )
        .unwrap();
        db.insert_row(
            "test_table",
            Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar("row3".to_string())]),
        )
        .unwrap();

        assert_eq!(db.get_table("test_table").unwrap().row_count(), 3);

        // Execute truncate
        let stmt = TruncateTableStmt { table_names: vec!["test_table".to_string()], if_exists: false, cascade: None };
        let result = TruncateTableExecutor::execute(&stmt, &mut db);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3); // 3 rows deleted
        assert_eq!(db.get_table("test_table").unwrap().row_count(), 0);
    }

    #[test]
    fn test_truncate_empty_table() {
        let mut db = Database::new();
        create_test_table(&mut db, "empty_table");

        // Don't insert any data
        assert_eq!(db.get_table("empty_table").unwrap().row_count(), 0);

        // Execute truncate on empty table
        let stmt = TruncateTableStmt { table_names: vec!["empty_table".to_string()], if_exists: false, cascade: None };
        let result = TruncateTableExecutor::execute(&stmt, &mut db);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0); // 0 rows deleted
        assert_eq!(db.get_table("empty_table").unwrap().row_count(), 0);
    }

    #[test]
    fn test_truncate_if_exists_nonexistent() {
        let mut db = Database::new();

        // Truncate nonexistent table with IF EXISTS
        let stmt =
            TruncateTableStmt { table_names: vec!["nonexistent".to_string()], if_exists: true, cascade: None };
        let result = TruncateTableExecutor::execute(&stmt, &mut db);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0); // Silently succeeds with 0 rows
    }

    #[test]
    fn test_truncate_if_exists_existing() {
        let mut db = Database::new();
        create_test_table(&mut db, "existing_table");

        // Insert data
        db.insert_row(
            "existing_table",
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("data".to_string())]),
        )
        .unwrap();

        // Truncate with IF EXISTS
        let stmt =
            TruncateTableStmt { table_names: vec!["existing_table".to_string()], if_exists: true, cascade: None };
        let result = TruncateTableExecutor::execute(&stmt, &mut db);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1); // 1 row deleted
        assert_eq!(db.get_table("existing_table").unwrap().row_count(), 0);
    }

    #[test]
    fn test_truncate_missing_table() {
        let mut db = Database::new();

        // Truncate nonexistent table WITHOUT IF EXISTS
        let stmt =
            TruncateTableStmt { table_names: vec!["missing_table".to_string()], if_exists: false, cascade: None };
        let result = TruncateTableExecutor::execute(&stmt, &mut db);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutorError::TableNotFound(_)));
    }

    // ============================================================================
    // Safety Check Tests - DELETE Triggers
    // ============================================================================

    #[test]
    fn test_truncate_blocked_by_delete_triggers() {
        let mut db = Database::new();

        // Create table via catalog API (to add triggers)
        let schema = TableSchema::new(
            "triggered_table".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "data".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // Create BEFORE DELETE trigger
        let trigger = TriggerDefinition::new(
            "before_delete_trigger".to_string(),
            TriggerTiming::Before,
            TriggerEvent::Delete,
            "triggered_table".to_string(),
            TriggerGranularity::Row,
            None,
            TriggerAction::RawSql("-- audit logic".to_string()),
        );
        db.catalog.create_trigger(trigger).unwrap();

        // Insert rows
        db.insert_row(
            "triggered_table",
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("data1".to_string())]),
        )
        .unwrap();

        // Attempt truncate - should be blocked by DELETE trigger
        let stmt =
            TruncateTableStmt { table_names: vec!["triggered_table".to_string()], if_exists: false, cascade: None };
        let result = TruncateTableExecutor::execute(&stmt, &mut db);

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("DELETE triggers") || error_msg.contains("foreign keys"));

        // Verify data still exists
        assert_eq!(db.get_table("triggered_table").unwrap().row_count(), 1);
    }

    #[test]
    fn test_truncate_blocked_by_after_delete_trigger() {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "audit_table".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "value".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // Create AFTER DELETE trigger
        let trigger = TriggerDefinition::new(
            "after_delete_audit".to_string(),
            TriggerTiming::After,
            TriggerEvent::Delete,
            "audit_table".to_string(),
            TriggerGranularity::Row,
            None,
            TriggerAction::RawSql("INSERT INTO audit_log ...".to_string()),
        );
        db.catalog.create_trigger(trigger).unwrap();

        db.insert_row(
            "audit_table",
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("test".to_string())]),
        )
        .unwrap();

        // Attempt truncate - should be blocked
        let stmt = TruncateTableStmt { table_names: vec!["audit_table".to_string()], if_exists: false, cascade: None };
        let result = TruncateTableExecutor::execute(&stmt, &mut db);

        assert!(result.is_err());
        assert_eq!(db.get_table("audit_table").unwrap().row_count(), 1);
    }

    #[test]
    fn test_truncate_allowed_with_insert_trigger() {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "insert_triggered".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "data".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // Create INSERT trigger (not DELETE) - should NOT block TRUNCATE
        let trigger = TriggerDefinition::new(
            "insert_trigger".to_string(),
            TriggerTiming::After,
            TriggerEvent::Insert,
            "insert_triggered".to_string(),
            TriggerGranularity::Row,
            None,
            TriggerAction::RawSql("-- insert audit".to_string()),
        );
        db.catalog.create_trigger(trigger).unwrap();

        // Insert rows
        for i in 0..10 {
            db.insert_row(
                "insert_triggered",
                Row::new(vec![SqlValue::Integer(i), SqlValue::Varchar(format!("data{}", i))]),
            )
            .unwrap();
        }

        // Truncate should succeed because only INSERT trigger exists
        let stmt =
            TruncateTableStmt { table_names: vec!["insert_triggered".to_string()], if_exists: false, cascade: None };
        let result = TruncateTableExecutor::execute(&stmt, &mut db);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 10);
        assert_eq!(db.get_table("insert_triggered").unwrap().row_count(), 0);
    }

    #[test]
    fn test_truncate_allowed_with_update_trigger() {
        let mut db = Database::new();

        let schema = TableSchema::new(
            "update_triggered".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "data".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // Create UPDATE trigger - should NOT block TRUNCATE
        let trigger = TriggerDefinition::new(
            "update_trigger".to_string(),
            TriggerTiming::Before,
            TriggerEvent::Update(None), // No column list
            "update_triggered".to_string(),
            TriggerGranularity::Row,
            None,
            TriggerAction::RawSql("-- update validation".to_string()),
        );
        db.catalog.create_trigger(trigger).unwrap();

        db.insert_row(
            "update_triggered",
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("data".to_string())]),
        )
        .unwrap();

        // Truncate should succeed
        let stmt =
            TruncateTableStmt { table_names: vec!["update_triggered".to_string()], if_exists: false, cascade: None };
        let result = TruncateTableExecutor::execute(&stmt, &mut db);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
        assert_eq!(db.get_table("update_triggered").unwrap().row_count(), 0);
    }

    // ============================================================================
    // Safety Check Tests - Foreign Keys
    // ============================================================================

    #[test]
    fn test_truncate_blocked_by_fk_references() {
        let mut db = Database::new();

        // Create parent table with primary key
        let parent_schema = TableSchema::with_primary_key(
            "parent".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
            ],
            vec!["id".to_string()],
        );
        db.create_table(parent_schema).unwrap();

        // Create child table with foreign key referencing parent
        let child_schema = TableSchema::with_foreign_keys(
            "child".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("parent_id".to_string(), DataType::Integer, false),
            ],
            vec![ForeignKeyConstraint {
                name: Some("fk_child_parent".to_string()),
                column_names: vec!["parent_id".to_string()],
                column_indices: vec![1],
                parent_table: "parent".to_string(),
                parent_column_names: vec!["id".to_string()],
                parent_column_indices: vec![0],
                on_delete: ReferentialAction::NoAction,
                on_update: ReferentialAction::NoAction,
            }],
        );
        db.create_table(child_schema).unwrap();

        // Insert parent rows
        db.insert_row(
            "parent",
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Parent1".to_string())]),
        )
        .unwrap();

        // Attempt to truncate parent - should be blocked because child table references it
        let stmt = TruncateTableStmt { table_names: vec!["parent".to_string()], if_exists: false, cascade: None };
        let result = TruncateTableExecutor::execute(&stmt, &mut db);

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("DELETE triggers") || error_msg.contains("foreign keys"));

        // Verify parent data still exists
        assert_eq!(db.get_table("parent").unwrap().row_count(), 1);
    }

    #[test]
    fn test_truncate_allowed_no_fk_references() {
        let mut db = Database::new();

        // Create standalone table with no FK references
        let schema = TableSchema::new(
            "standalone".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "data".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // Insert data
        for i in 0..100 {
            db.insert_row(
                "standalone",
                Row::new(vec![
                    SqlValue::Integer(i),
                    SqlValue::Varchar(format!("data{}", i)),
                ]),
            )
            .unwrap();
        }

        // Truncate should succeed
        let stmt = TruncateTableStmt { table_names: vec!["standalone".to_string()], if_exists: false, cascade: None };
        let result = TruncateTableExecutor::execute(&stmt, &mut db);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 100);
        assert_eq!(db.get_table("standalone").unwrap().row_count(), 0);
    }

    // ============================================================================
    // Integration Tests
    // ============================================================================

    #[test]
    fn test_truncate_preserves_table_structure() {
        let mut db = Database::new();

        // Create table with specific structure
        let create_stmt = CreateTableStmt {
            table_name: "structured_table".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
                ColumnDef {
                    name: "email".to_string(),
                    data_type: DataType::Varchar { max_length: Some(255) },
                    nullable: false,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
                ColumnDef {
                    name: "age".to_string(),
                    data_type: DataType::Integer,
                    nullable: true,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
            ],
            table_constraints: vec![],
            table_options: vec![],
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Insert data
        db.insert_row(
            "structured_table",
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar("test@example.com".to_string()),
                SqlValue::Integer(25),
            ]),
        )
        .unwrap();

        // Get original table schema
        let original_schema = db.catalog.get_table("structured_table").unwrap();
        let original_columns = original_schema.columns.clone();

        // Truncate
        let stmt =
            TruncateTableStmt { table_names: vec!["structured_table".to_string()], if_exists: false, cascade: None };
        TruncateTableExecutor::execute(&stmt, &mut db).unwrap();

        // Verify table structure is intact
        let table_after = db.catalog.get_table("structured_table").unwrap();
        assert_eq!(table_after.columns.len(), 3);
        assert_eq!(table_after.columns, original_columns);

        // Verify we can still insert with same structure
        db.insert_row(
            "structured_table",
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Varchar("new@example.com".to_string()),
                SqlValue::Integer(30),
            ]),
        )
        .unwrap();

        assert_eq!(db.get_table("structured_table").unwrap().row_count(), 1);
    }

    #[test]
    fn test_truncate_clears_all_data() {
        let mut db = Database::new();
        create_test_table(&mut db, "large_table");

        // Insert many rows
        for i in 0..1000 {
            db.insert_row(
                "large_table",
                Row::new(vec![
                    SqlValue::Integer(i),
                    SqlValue::Varchar(format!("data_{}", i)),
                ]),
            )
            .unwrap();
        }

        assert_eq!(db.get_table("large_table").unwrap().row_count(), 1000);

        // Truncate
        let stmt = TruncateTableStmt { table_names: vec!["large_table".to_string()], if_exists: false, cascade: None };
        let deleted = TruncateTableExecutor::execute(&stmt, &mut db).unwrap();

        assert_eq!(deleted, 1000);
        assert_eq!(db.get_table("large_table").unwrap().row_count(), 0);

        // Verify we can insert new data
        db.insert_row(
            "large_table",
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("new_data".to_string())]),
        )
        .unwrap();
        assert_eq!(db.get_table("large_table").unwrap().row_count(), 1);
    }

    #[test]
    fn test_truncate_returns_correct_row_count() {
        let mut db = Database::new();
        create_test_table(&mut db, "count_test");

        // Test various row counts
        let test_counts = vec![0, 1, 5, 100, 1000];

        for count in test_counts {
            // Clear and insert specific number of rows
            db.get_table_mut("count_test").unwrap().clear();

            for i in 0..count {
                db.insert_row(
                    "count_test",
                    Row::new(vec![
                        SqlValue::Integer(i),
                        SqlValue::Varchar(format!("row_{}", i)),
                    ]),
                )
                .unwrap();
            }

            // Truncate and verify count
            let stmt =
                TruncateTableStmt { table_names: vec!["count_test".to_string()], if_exists: false, cascade: None };
            let deleted = TruncateTableExecutor::execute(&stmt, &mut db).unwrap();

            assert_eq!(deleted, count as usize, "Failed for count {}", count);
            assert_eq!(db.get_table("count_test").unwrap().row_count(), 0);
        }
    }
}
