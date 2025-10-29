//! DROP TABLE statement execution

use ast::DropTableStmt;
use storage::Database;

use crate::errors::ExecutorError;

/// Executor for DROP TABLE statements
pub struct DropTableExecutor;

impl DropTableExecutor {
    /// Execute a DROP TABLE statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The DROP TABLE statement AST node
    /// * `database` - The database to drop the table from
    ///
    /// # Returns
    ///
    /// Success message or error
    ///
    /// # Examples
    ///
    /// ```
    /// use ast::{CreateTableStmt, ColumnDef, DropTableStmt};
    /// use types::DataType;
    /// use storage::Database;
    /// use executor::{CreateTableExecutor, DropTableExecutor};
    ///
    /// let mut db = Database::new();
    /// let create_stmt = CreateTableStmt {
    ///     table_name: "users".to_string(),
    ///     columns: vec![
    ///         ColumnDef {
    ///             name: "id".to_string(),
    ///             data_type: DataType::Integer,
    ///             nullable: false,
    ///             constraints: vec![],
    ///         },
    ///     ],
    ///     table_constraints: vec![],
    /// };
    /// CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
    ///
    /// let stmt = DropTableStmt {
    ///     table_name: "users".to_string(),
    ///     if_exists: false,
    /// };
    ///
    /// let result = DropTableExecutor::execute(&stmt, &mut db);
    /// assert!(result.is_ok());
    /// ```
    pub fn execute(
        stmt: &DropTableStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Check if table exists
        let table_exists = database.catalog.table_exists(&stmt.table_name);

        // If IF EXISTS is specified and table doesn't exist, succeed silently
        if stmt.if_exists && !table_exists {
            return Ok(format!(
                "Table '{}' does not exist (IF EXISTS specified)",
                stmt.table_name
            ));
        }

        // If table doesn't exist and IF EXISTS is not specified, return error
        if !table_exists {
            return Err(ExecutorError::TableNotFound(stmt.table_name.clone()));
        }

        // Drop the table from storage (this also removes from catalog)
        database
            .drop_table(&stmt.table_name)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        // Return success message
        Ok(format!("Table '{}' dropped successfully", stmt.table_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::{ColumnDef, CreateTableStmt};
    use crate::CreateTableExecutor;
    use types::DataType;

    #[test]
    fn test_drop_existing_table() {
        let mut db = Database::new();

        // Create a table first
        let create_stmt = CreateTableStmt {
            table_name: "users".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
            }],
            table_constraints: vec![],
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
        assert!(db.catalog.table_exists("users"));

        // Now drop it
        let drop_stmt = DropTableStmt { table_name: "users".to_string(), if_exists: false };

        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Table 'users' dropped successfully");

        // Verify table no longer exists
        assert!(!db.catalog.table_exists("users"));
        assert!(db.get_table("users").is_none());
    }

    #[test]
    fn test_drop_nonexistent_table_without_if_exists() {
        let mut db = Database::new();

        let drop_stmt =
            DropTableStmt { table_name: "nonexistent".to_string(), if_exists: false };

        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
    }

    #[test]
    fn test_drop_nonexistent_table_with_if_exists() {
        let mut db = Database::new();

        let drop_stmt = DropTableStmt { table_name: "nonexistent".to_string(), if_exists: true };

        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            "Table 'nonexistent' does not exist (IF EXISTS specified)"
        );
    }

    #[test]
    fn test_drop_existing_table_with_if_exists() {
        let mut db = Database::new();

        // Create a table first
        let create_stmt = CreateTableStmt {
            table_name: "products".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
            }],
            table_constraints: vec![],
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Drop it with IF EXISTS
        let drop_stmt = DropTableStmt { table_name: "products".to_string(), if_exists: true };

        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Table 'products' dropped successfully");

        // Verify table no longer exists
        assert!(!db.catalog.table_exists("products"));
    }

    #[test]
    fn test_drop_table_with_data() {
        let mut db = Database::new();

        // Create a table with data
        let create_stmt = CreateTableStmt {
            table_name: "customers".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    constraints: vec![],
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Varchar { max_length: 100 },
                    nullable: false,
                    constraints: vec![],
                },
            ],
            table_constraints: vec![],
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Insert some data
        use storage::Row;
        use types::SqlValue;
        let row = Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]);
        db.insert_row("customers", row).unwrap();

        // Verify data exists
        assert_eq!(db.get_table("customers").unwrap().row_count(), 1);

        // Drop the table
        let drop_stmt = DropTableStmt { table_name: "customers".to_string(), if_exists: false };

        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());

        // Verify table and data are gone
        assert!(!db.catalog.table_exists("customers"));
        assert!(db.get_table("customers").is_none());
    }

    #[test]
    fn test_drop_and_recreate_table() {
        let mut db = Database::new();

        // Create table
        let create_stmt = CreateTableStmt {
            table_name: "temp".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
            }],
            table_constraints: vec![],
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Drop it
        let drop_stmt = DropTableStmt { table_name: "temp".to_string(), if_exists: false };
        DropTableExecutor::execute(&drop_stmt, &mut db).unwrap();

        // Recreate it
        let result = CreateTableExecutor::execute(&create_stmt, &mut db);
        assert!(result.is_ok());
        assert!(db.catalog.table_exists("temp"));
    }

    #[test]
    fn test_drop_multiple_tables() {
        let mut db = Database::new();

        // Create multiple tables
        for name in &["table1", "table2", "table3"] {
            let create_stmt = CreateTableStmt {
                table_name: name.to_string(),
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    constraints: vec![],
                }],
                table_constraints: vec![],
            };
            CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
        }

        assert_eq!(db.list_tables().len(), 3);

        // Drop them one by one
        for name in &["table1", "table2", "table3"] {
            let drop_stmt = DropTableStmt { table_name: name.to_string(), if_exists: false };
            let result = DropTableExecutor::execute(&drop_stmt, &mut db);
            assert!(result.is_ok());
        }

        assert_eq!(db.list_tables().len(), 0);
    }

    #[test]
    fn test_drop_table_case_sensitivity() {
        let mut db = Database::new();

        // Create table with specific case
        let create_stmt = CreateTableStmt {
            table_name: "MyTable".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
            }],
            table_constraints: vec![],
        };
        CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

        // Try to drop with exact case - should succeed
        let drop_stmt = DropTableStmt { table_name: "MyTable".to_string(), if_exists: false };
        let result = DropTableExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
    }
}
