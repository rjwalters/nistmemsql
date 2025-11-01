//! CREATE INDEX and DROP INDEX statement execution

use ast::{CreateIndexStmt, DropIndexStmt};
use storage::Database;

use crate::errors::ExecutorError;
use crate::privilege_checker::PrivilegeChecker;

/// Executor for CREATE INDEX statements
pub struct IndexExecutor;

impl IndexExecutor {
    /// Execute a CREATE INDEX statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The CREATE INDEX statement AST node
    /// * `database` - The database to create the index in
    ///
    /// # Returns
    ///
    /// Success message or error
    ///
    /// # Examples
    ///
    /// ```
    /// use ast::{CreateIndexStmt, IndexColumn};
    /// use ast::select::OrderDirection;
    /// use storage::Database;
    /// use executor::IndexExecutor;
    ///
    /// let mut db = Database::new();
    /// // First create a table
    /// // ... (table creation code) ...
    ///
    /// let stmt = CreateIndexStmt {
    ///     index_name: "idx_users_email".to_string(),
    ///     table_name: "users".to_string(),
    ///     unique: false,
    ///     columns: vec![
    ///         IndexColumn {
    ///             column_name: "email".to_string(),
    ///             direction: OrderDirection::Asc,
    ///         },
    ///     ],
    /// };
    ///
    /// let result = IndexExecutor::execute(&stmt, &mut db);
    /// // assert!(result.is_ok());
    /// ```
    pub fn execute(
        stmt: &CreateIndexStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Parse qualified table name (schema.table or just table)
        let (schema_name, table_name) =
            if let Some((schema_part, table_part)) = stmt.table_name.split_once('.') {
                (schema_part.to_string(), table_part.to_string())
            } else {
                (database.catalog.get_current_schema().to_string(), stmt.table_name.clone())
            };

        // Check CREATE privilege on the schema
        PrivilegeChecker::check_create(database, &schema_name)?;

        // Check if table exists in the target schema
        let qualified_table_name = format!("{}.{}", schema_name, table_name);
        if !database.catalog.table_exists(&qualified_table_name) {
            return Err(ExecutorError::TableNotFound(qualified_table_name));
        }

        // Get table schema to validate columns
        let table_schema = database
            .catalog
            .get_table(&qualified_table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(qualified_table_name.clone()))?;

        // Validate that all indexed columns exist in the table
        for index_col in &stmt.columns {
            if table_schema.get_column(&index_col.column_name).is_none() {
                return Err(ExecutorError::ColumnNotFound {
                    column_name: index_col.column_name.clone(),
                    table_name: stmt.table_name.clone(),
                });
            }
        }

        // Check if index already exists
        let index_name = &stmt.index_name;
        if database.index_exists(index_name) {
            return Err(ExecutorError::IndexAlreadyExists(index_name.clone()));
        }

        // Create the index
        database.create_index(
            index_name.clone(),
            qualified_table_name.clone(),
            stmt.unique,
            stmt.columns.clone(),
        )?;

        Ok(format!(
            "Index '{}' created successfully on table '{}'",
            index_name, qualified_table_name
        ))
    }

    /// Execute a DROP INDEX statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The DROP INDEX statement AST node
    /// * `database` - The database to drop the index from
    ///
    /// # Returns
    ///
    /// Success message or error
    pub fn execute_drop(
        stmt: &DropIndexStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        let index_name = &stmt.index_name;

        // Check if index exists
        if !database.index_exists(index_name) {
            return Err(ExecutorError::IndexNotFound(index_name.clone()));
        }

        // Drop the index
        database.drop_index(index_name)?;

        Ok(format!("Index '{}' dropped successfully", index_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CreateTableExecutor;
    use ast::{ColumnDef, CreateTableStmt, IndexColumn, OrderDirection};
    use types::DataType;

    fn create_test_table(db: &mut Database) {
        let stmt = CreateTableStmt {
            table_name: "users".to_string(),
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
                    name: "name".to_string(),
                    data_type: DataType::Varchar { max_length: Some(100) },
                    nullable: true,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
            ],
            table_constraints: vec![],
        };

        CreateTableExecutor::execute(&stmt, db).unwrap();
    }

    #[test]
    fn test_create_simple_index() {
        let mut db = Database::new();
        create_test_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            table_name: "users".to_string(),
            unique: false,
            columns: vec![IndexColumn {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
            }],
        };

        let result = IndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            "Index 'idx_users_email' created successfully on table 'public.users'"
        );

        // Verify index exists
        assert!(db.index_exists("idx_users_email"));
    }

    #[test]
    fn test_create_unique_index() {
        let mut db = Database::new();
        create_test_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_users_email_unique".to_string(),
            table_name: "users".to_string(),
            unique: true,
            columns: vec![IndexColumn {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
            }],
        };

        let result = IndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
        assert!(db.index_exists("idx_users_email_unique"));
    }

    #[test]
    fn test_create_multi_column_index() {
        let mut db = Database::new();
        create_test_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_users_email_name".to_string(),
            table_name: "users".to_string(),
            unique: false,
            columns: vec![
                IndexColumn { column_name: "email".to_string(), direction: OrderDirection::Asc },
                IndexColumn { column_name: "name".to_string(), direction: OrderDirection::Desc },
            ],
        };

        let result = IndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_index_duplicate_name() {
        let mut db = Database::new();
        create_test_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            table_name: "users".to_string(),
            unique: false,
            columns: vec![IndexColumn {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
            }],
        };

        // First creation succeeds
        let result = IndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());

        // Second creation fails
        let result = IndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::IndexAlreadyExists(_))));
    }

    #[test]
    fn test_create_index_on_nonexistent_table() {
        let mut db = Database::new();

        let stmt = CreateIndexStmt {
            index_name: "idx_nonexistent".to_string(),
            table_name: "nonexistent_table".to_string(),
            unique: false,
            columns: vec![IndexColumn {
                column_name: "id".to_string(),
                direction: OrderDirection::Asc,
            }],
        };

        let result = IndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::TableNotFound(_))));
    }

    #[test]
    fn test_create_index_on_nonexistent_column() {
        let mut db = Database::new();
        create_test_table(&mut db);

        let stmt = CreateIndexStmt {
            index_name: "idx_users_nonexistent".to_string(),
            table_name: "users".to_string(),
            unique: false,
            columns: vec![IndexColumn {
                column_name: "nonexistent_column".to_string(),
                direction: OrderDirection::Asc,
            }],
        };

        let result = IndexExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::ColumnNotFound { .. })));
    }

    #[test]
    fn test_drop_index() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // Create index
        let create_stmt = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            table_name: "users".to_string(),
            unique: false,
            columns: vec![IndexColumn {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
            }],
        };
        IndexExecutor::execute(&create_stmt, &mut db).unwrap();

        // Drop index
        let drop_stmt = DropIndexStmt { index_name: "idx_users_email".to_string() };
        let result = IndexExecutor::execute_drop(&drop_stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Index 'idx_users_email' dropped successfully");

        // Verify index no longer exists
        assert!(!db.index_exists("idx_users_email"));
    }

    #[test]
    fn test_drop_nonexistent_index() {
        let mut db = Database::new();

        let drop_stmt = DropIndexStmt { index_name: "nonexistent_index".to_string() };
        let result = IndexExecutor::execute_drop(&drop_stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::IndexNotFound(_))));
    }
}
