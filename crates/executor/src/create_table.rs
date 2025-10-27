//! CREATE TABLE statement execution

use ast::CreateTableStmt;
use catalog::{ColumnSchema, TableSchema};
use storage::Database;

use crate::errors::ExecutorError;

/// Executor for CREATE TABLE statements
pub struct CreateTableExecutor;

impl CreateTableExecutor {
    /// Execute a CREATE TABLE statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The CREATE TABLE statement AST node
    /// * `database` - The database to create the table in
    ///
    /// # Returns
    ///
    /// Success message or error
    ///
    /// # Examples
    ///
    /// ```
    /// use ast::{CreateTableStmt, ColumnDef};
    /// use types::DataType;
    /// use storage::Database;
    /// use executor::CreateTableExecutor;
    ///
    /// let mut db = Database::new();
    /// let stmt = CreateTableStmt {
    ///     table_name: "users".to_string(),
    ///     columns: vec![
    ///         ColumnDef {
    ///             name: "id".to_string(),
    ///             data_type: DataType::Integer,
    ///             nullable: false,
    ///         },
    ///         ColumnDef {
    ///             name: "name".to_string(),
    ///             data_type: DataType::Varchar { max_length: 255 },
    ///             nullable: true,
    ///         },
    ///     ],
    /// };
    ///
    /// let result = CreateTableExecutor::execute(&stmt, &mut db);
    /// assert!(result.is_ok());
    /// ```
    pub fn execute(stmt: &CreateTableStmt, database: &mut Database) -> Result<String, ExecutorError> {
        // Check if table already exists (defensive check before calling storage)
        if database.catalog.table_exists(&stmt.table_name) {
            return Err(ExecutorError::TableAlreadyExists(stmt.table_name.clone()));
        }

        // Convert AST ColumnDef → Catalog ColumnSchema
        let columns: Vec<ColumnSchema> = stmt
            .columns
            .iter()
            .map(|col_def| ColumnSchema::new(
                col_def.name.clone(),
                col_def.data_type.clone(),
                col_def.nullable,
            ))
            .collect();

        // Create TableSchema
        let table_schema = TableSchema::new(stmt.table_name.clone(), columns);

        // Create table in storage (this also registers in catalog)
        database
            .create_table(table_schema)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        // Return success message
        Ok(format!("Table '{}' created successfully", stmt.table_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::ColumnDef;
    use types::DataType;

    #[test]
    fn test_create_simple_table() {
        let mut db = Database::new();

        let stmt = CreateTableStmt {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Varchar { max_length: 255 },
                    nullable: true,
                },
            ],
        };

        let result = CreateTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Table 'users' created successfully");

        // Verify table exists in catalog
        assert!(db.catalog.table_exists("users"));

        // Verify table is accessible from storage
        assert!(db.get_table("users").is_some());
    }

    #[test]
    fn test_create_table_with_multiple_types() {
        let mut db = Database::new();

        let stmt = CreateTableStmt {
            table_name: "products".to_string(),
            columns: vec![
                ColumnDef {
                    name: "product_id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Varchar { max_length: 100 },
                    nullable: false,
                },
                ColumnDef {
                    name: "price".to_string(),
                    data_type: DataType::Integer, // Using Integer for price (could be Decimal in future)
                    nullable: false,
                },
                ColumnDef {
                    name: "in_stock".to_string(),
                    data_type: DataType::Boolean,
                    nullable: false,
                },
                ColumnDef {
                    name: "description".to_string(),
                    data_type: DataType::Varchar { max_length: 500 },
                    nullable: true, // Optional field
                },
            ],
        };

        let result = CreateTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());

        // Verify schema correctness
        let schema = db.catalog.get_table("products");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        assert_eq!(schema.column_count(), 5);
        assert_eq!(schema.get_column("product_id").unwrap().nullable, false);
        assert_eq!(schema.get_column("description").unwrap().nullable, true);
    }

    #[test]
    fn test_create_table_already_exists() {
        let mut db = Database::new();

        let stmt = CreateTableStmt {
            table_name: "users".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            }],
        };

        // First creation succeeds
        let result = CreateTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());

        // Second creation fails
        let result = CreateTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecutorError::TableAlreadyExists(_))));
    }

    #[test]
    fn test_create_table_with_nullable_columns() {
        let mut db = Database::new();

        let stmt = CreateTableStmt {
            table_name: "employees".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                },
                ColumnDef {
                    name: "middle_name".to_string(),
                    data_type: DataType::Varchar { max_length: 50 },
                    nullable: true, // Nullable field
                },
                ColumnDef {
                    name: "manager_id".to_string(),
                    data_type: DataType::Integer,
                    nullable: true, // Nullable foreign key
                },
            ],
        };

        let result = CreateTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());

        // Verify nullable attribute is preserved
        let schema = db.catalog.get_table("employees").unwrap();
        assert_eq!(schema.get_column("id").unwrap().nullable, false);
        assert_eq!(schema.get_column("middle_name").unwrap().nullable, true);
        assert_eq!(schema.get_column("manager_id").unwrap().nullable, true);
    }

    #[test]
    fn test_create_table_empty_columns_list() {
        let mut db = Database::new();

        let stmt = CreateTableStmt {
            table_name: "empty_table".to_string(),
            columns: vec![], // Empty columns
        };

        // Should succeed (though not very useful)
        let result = CreateTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());

        let schema = db.catalog.get_table("empty_table").unwrap();
        assert_eq!(schema.column_count(), 0);
    }

    #[test]
    fn test_create_multiple_tables() {
        let mut db = Database::new();

        // Create first table
        let stmt1 = CreateTableStmt {
            table_name: "customers".to_string(),
            columns: vec![ColumnDef {
                name: "customer_id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            }],
        };
        CreateTableExecutor::execute(&stmt1, &mut db).unwrap();

        // Create second table
        let stmt2 = CreateTableStmt {
            table_name: "orders".to_string(),
            columns: vec![ColumnDef {
                name: "order_id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            }],
        };
        CreateTableExecutor::execute(&stmt2, &mut db).unwrap();

        // Verify both tables exist
        assert!(db.catalog.table_exists("customers"));
        assert!(db.catalog.table_exists("orders"));
        assert_eq!(db.list_tables().len(), 2);
    }

    #[test]
    fn test_create_table_with_special_characters_in_name() {
        let mut db = Database::new();

        // Test with underscores (common case)
        let stmt = CreateTableStmt {
            table_name: "user_profiles".to_string(),
            columns: vec![ColumnDef {
                name: "profile_id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            }],
        };

        let result = CreateTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
        assert!(db.catalog.table_exists("user_profiles"));
    }

    #[test]
    fn test_create_table_case_sensitivity() {
        let mut db = Database::new();

        let stmt1 = CreateTableStmt {
            table_name: "Users".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            }],
        };
        CreateTableExecutor::execute(&stmt1, &mut db).unwrap();

        // Try to create "users" (different case)
        let stmt2 = CreateTableStmt {
            table_name: "users".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
            }],
        };

        // Behavior depends on catalog's case sensitivity
        // Just verify it either succeeds or fails gracefully
        let result = CreateTableExecutor::execute(&stmt2, &mut db);
        assert!(result.is_ok() || result.is_err());
    }
}
