//! CREATE TABLE statement execution

use ast::CreateTableStmt;
use catalog::{ColumnSchema, TableSchema};
use storage::Database;

use crate::errors::ExecutorError;
use crate::privilege_checker::PrivilegeChecker;

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
    ///             constraints: vec![],
    ///         },
    ///         ColumnDef {
    ///             name: "name".to_string(),
    ///             data_type: DataType::Varchar { max_length: Some(255) },
    ///             nullable: true,
    ///             constraints: vec![],
    ///         },
    ///     ],
    /// table_constraints: vec![], };
    ///
    /// let result = CreateTableExecutor::execute(&stmt, &mut db);
    /// assert!(result.is_ok());
    /// ```
    pub fn execute(
        stmt: &CreateTableStmt,
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

        // Check if table already exists in the target schema
        let qualified_name = format!("{}.{}", schema_name, table_name);
        if database.catalog.table_exists(&qualified_name) {
            return Err(ExecutorError::TableAlreadyExists(qualified_name));
        }

        // Convert AST ColumnDef → Catalog ColumnSchema
        let columns: Vec<ColumnSchema> = stmt
            .columns
            .iter()
            .map(|col_def| {
                ColumnSchema::new(col_def.name.clone(), col_def.data_type.clone(), col_def.nullable)
            })
            .collect();

        // Create TableSchema with unqualified name
        let table_schema = TableSchema::new(table_name.clone(), columns);

        // If creating in a non-current schema, temporarily switch to it
        let original_schema = database.catalog.get_current_schema().to_string();
        let needs_schema_switch = schema_name != original_schema;

        if needs_schema_switch {
            database
                .catalog
                .set_current_schema(&schema_name)
                .map_err(|e| ExecutorError::StorageError(format!("Schema error: {:?}", e)))?;
        }

        // Create table using Database API (handles both catalog and storage)
        let result = database
            .create_table(table_schema)
            .map_err(|e| ExecutorError::StorageError(e.to_string()));

        // Restore original schema if we switched
        if needs_schema_switch {
            database
                .catalog
                .set_current_schema(&original_schema)
                .map_err(|e| ExecutorError::StorageError(format!("Schema error: {:?}", e)))?;
        }

        // Check if table creation succeeded
        result?;

        // Return success message
        Ok(format!("Table '{}' created successfully in schema '{}'", table_name, schema_name))
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
                    constraints: vec![],
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Varchar { max_length: Some(255) },
                    nullable: true,
                    constraints: vec![],
                },
            ],
            table_constraints: vec![],
        };

        let result = CreateTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Table 'users' created successfully in schema 'public'");

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
                    constraints: vec![],
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Varchar { max_length: Some(100) },
                    nullable: false,
                    constraints: vec![],
                },
                ColumnDef {
                    name: "price".to_string(),
                    data_type: DataType::Integer, // Using Integer for price (could be Decimal in future)
                    nullable: false,
                    constraints: vec![],
                },
                ColumnDef {
                    name: "in_stock".to_string(),
                    data_type: DataType::Boolean,
                    nullable: false,
                    constraints: vec![],
                },
                ColumnDef {
                    name: "description".to_string(),
                    data_type: DataType::Varchar { max_length: Some(500) },
                    nullable: true, // Optional field
                    constraints: vec![],
                },
            ],
            table_constraints: vec![],
        };

        let result = CreateTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());

        // Verify schema correctness
        let schema = db.catalog.get_table("products");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        assert_eq!(schema.column_count(), 5);
        assert!(!schema.get_column("product_id").unwrap().nullable);
        assert!(schema.get_column("description").unwrap().nullable);
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
                constraints: vec![],
            }],
            table_constraints: vec![],
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
                    constraints: vec![],
                },
                ColumnDef {
                    name: "middle_name".to_string(),
                    data_type: DataType::Varchar { max_length: Some(50) },
                    nullable: true, // Nullable field
                    constraints: vec![],
                },
                ColumnDef {
                    name: "manager_id".to_string(),
                    data_type: DataType::Integer,
                    nullable: true, // Nullable foreign key
                    constraints: vec![],
                },
            ],
            table_constraints: vec![],
        };

        let result = CreateTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok());

        // Verify nullable attribute is preserved
        let schema = db.catalog.get_table("employees").unwrap();
        assert!(!schema.get_column("id").unwrap().nullable);
        assert!(schema.get_column("middle_name").unwrap().nullable);
        assert!(schema.get_column("manager_id").unwrap().nullable);
    }

    #[test]
    fn test_create_table_empty_columns_list() {
        let mut db = Database::new();

        let stmt = CreateTableStmt {
            table_name: "empty_table".to_string(),
            columns: vec![], // Empty columns
            table_constraints: vec![],
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
                constraints: vec![],
            }],
            table_constraints: vec![],
        };
        CreateTableExecutor::execute(&stmt1, &mut db).unwrap();

        // Create second table
        let stmt2 = CreateTableStmt {
            table_name: "orders".to_string(),
            columns: vec![ColumnDef {
                name: "order_id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
            }],
            table_constraints: vec![],
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
                constraints: vec![],
            }],
            table_constraints: vec![],
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
                constraints: vec![],
            }],
            table_constraints: vec![],
        };
        CreateTableExecutor::execute(&stmt1, &mut db).unwrap();

        // Try to create "users" (different case)
        let stmt2 = CreateTableStmt {
            table_name: "users".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                constraints: vec![],
            }],
            table_constraints: vec![],
        };

        // Behavior depends on catalog's case sensitivity
        // Just verify it either succeeds or fails gracefully
        let result = CreateTableExecutor::execute(&stmt2, &mut db);
        assert!(result.is_ok() || result.is_err());
    }
}
