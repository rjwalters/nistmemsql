//! DROP INDEX statement execution

use vibesql_ast::DropIndexStmt;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Executor for DROP INDEX statements
pub struct DropIndexExecutor;

impl DropIndexExecutor {
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
    pub fn execute(stmt: &DropIndexStmt, database: &mut Database) -> Result<String, ExecutorError> {
        let index_name = &stmt.index_name;

        // Check if index exists
        if !database.index_exists(index_name) {
            if stmt.if_exists {
                // IF EXISTS: silently succeed if index doesn't exist
                return Ok(format!("Index '{}' does not exist (skipped)", index_name));
            } else {
                return Err(ExecutorError::IndexNotFound(index_name.clone()));
            }
        }

        // Drop the index
        database.drop_index(index_name)?;

        Ok(format!("Index '{}' dropped successfully", index_name))
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{ColumnDef, CreateIndexStmt, CreateTableStmt, IndexColumn, OrderDirection};
    use vibesql_types::DataType;

    use super::*;
    use crate::{index_ddl::create_index::CreateIndexExecutor, CreateTableExecutor};

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
            table_options: vec![],
        };

        CreateTableExecutor::execute(&stmt, db).unwrap();
    }

    #[test]
    fn test_drop_index() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // Create index
        let create_stmt = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            if_not_exists: false,
            table_name: "users".to_string(),
            unique: false,
            columns: vec![IndexColumn {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
            }],
        };
        CreateIndexExecutor::execute(&create_stmt, &mut db).unwrap();

        // Drop index
        let drop_stmt =
            DropIndexStmt { index_name: "idx_users_email".to_string(), if_exists: false };
        let result = DropIndexExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Index 'idx_users_email' dropped successfully");

        // Verify index no longer exists
        assert!(!db.index_exists("idx_users_email"));
    }

    #[test]
    fn test_drop_nonexistent_index() {
        let mut db = Database::new();

        let drop_stmt =
            DropIndexStmt { index_name: "nonexistent_index".to_string(), if_exists: false };
        let result = DropIndexExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::IndexNotFound(_))));
    }

    #[test]
    fn test_drop_index_if_exists_when_exists() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // Create index
        let create_stmt = CreateIndexStmt {
            index_name: "idx_users_email".to_string(),
            if_not_exists: false,
            table_name: "users".to_string(),
            unique: false,
            columns: vec![IndexColumn {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
            }],
        };
        CreateIndexExecutor::execute(&create_stmt, &mut db).unwrap();

        // Drop with IF EXISTS should succeed
        let drop_stmt =
            DropIndexStmt { index_name: "idx_users_email".to_string(), if_exists: true };
        let result = DropIndexExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Index 'idx_users_email' dropped successfully");
        assert!(!db.index_exists("idx_users_email"));
    }

    #[test]
    fn test_drop_index_if_exists_when_not_exists() {
        let mut db = Database::new();

        // Drop non-existent index with IF EXISTS should succeed
        let drop_stmt =
            DropIndexStmt { index_name: "nonexistent_index".to_string(), if_exists: true };
        let result = DropIndexExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
        // Silently succeeds when index doesn't exist
    }

    #[test]
    fn test_case_insensitive_index_names() {
        let mut db = Database::new();
        create_test_table(&mut db);

        // Create index with lowercase name
        let create_stmt = CreateIndexStmt {
            index_name: "idx_test".to_string(),
            if_not_exists: false,
            table_name: "users".to_string(),
            unique: false,
            columns: vec![IndexColumn {
                column_name: "email".to_string(),
                direction: OrderDirection::Asc,
            }],
        };
        CreateIndexExecutor::execute(&create_stmt, &mut db).unwrap();

        // Drop with uppercase name should work (normalized to uppercase)
        let drop_stmt = DropIndexStmt { index_name: "IDX_TEST".to_string(), if_exists: false };
        let result = DropIndexExecutor::execute(&drop_stmt, &mut db);
        assert!(result.is_ok());
        assert!(!db.index_exists("idx_test"));
        assert!(!db.index_exists("IDX_TEST"));
    }
}
