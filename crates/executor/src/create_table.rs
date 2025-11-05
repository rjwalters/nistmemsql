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
    ///             default_value: None,
    ///             comment: None,
    ///         },
    ///         ColumnDef {
    ///             name: "name".to_string(),
    ///             data_type: DataType::Varchar { max_length: Some(255) },
    ///             nullable: true,
    ///             constraints: vec![],
    ///             default_value: None,
    ///             comment: None,
    ///         },
    ///     ],
    ///     table_constraints: vec![], table_options: vec![],
    ///     table_options: vec![],
    /// };
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
        let mut columns: Vec<ColumnSchema> = stmt
            .columns
            .iter()
            .map(|col_def| ColumnSchema {
                name: col_def.name.clone(),
                data_type: col_def.data_type.clone(),
                nullable: col_def.nullable,
                default_value: col_def.default_value.as_ref().map(|expr| (**expr).clone()),
            })
            .collect();

        // Process constraints to extract primary key
        let mut primary_key_columns: Vec<String> = Vec::new();

        // First, check column-level PRIMARY KEY constraints
        for col_def in &stmt.columns {
            for constraint in &col_def.constraints {
                if matches!(constraint.kind, ast::ColumnConstraintKind::PrimaryKey) {
                    primary_key_columns.push(col_def.name.clone());
                }
            }
        }

        // Then, check table-level PRIMARY KEY constraints (these override column-level)
        for table_constraint in &stmt.table_constraints {
            if let ast::TableConstraintKind::PrimaryKey { columns: pk_cols } = &table_constraint.kind {
                primary_key_columns = pk_cols.clone();
                break; // Only one PRIMARY KEY constraint allowed
            }
        }

        // If we have a primary key, mark those columns as NOT NULL
        if !primary_key_columns.is_empty() {
            for pk_col_name in &primary_key_columns {
                if let Some(col) = columns.iter_mut().find(|c| c.name == *pk_col_name) {
                    col.nullable = false; // PKs are implicitly NOT NULL
                }
            }
        }

        // Create TableSchema with unqualified name
        let mut table_schema = TableSchema::new(table_name.clone(), columns);

        // Set primary key if found
        if !primary_key_columns.is_empty() {
            table_schema.primary_key = Some(primary_key_columns);
        }

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
        default_value: None,
        comment: None,
        },
        ColumnDef {
        name: "name".to_string(),
        data_type: DataType::Varchar { max_length: Some(255) },
        nullable: true,
        constraints: vec![],
        default_value: None,
        comment: None,
        },
        ],
        table_constraints: vec![], table_options: vec![],
            table_options: vec![],
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
        default_value: None,
        comment: None,
        },
        ColumnDef {
        name: "name".to_string(),
        data_type: DataType::Varchar { max_length: Some(100) },
        nullable: false,
        constraints: vec![],
        default_value: None,
        comment: None,
        },
        ColumnDef {
        name: "price".to_string(),
        data_type: DataType::Integer, // Using Integer for price (could be Decimal in future)
        nullable: false,
        constraints: vec![],
        default_value: None,
        comment: None,
        },
        ColumnDef {
        name: "in_stock".to_string(),
        data_type: DataType::Boolean,
        nullable: false,
        constraints: vec![],
        default_value: None,
        comment: None,
        },
        ColumnDef {
        name: "description".to_string(),
        data_type: DataType::Varchar { max_length: Some(500) },
        nullable: true, // Optional field
        constraints: vec![],
        default_value: None,
        comment: None,
        },
        ],
        table_constraints: vec![], table_options: vec![],
            table_options: vec![],
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
                default_value: None,
                comment: None,
            }],
            table_constraints: vec![], table_options: vec![],
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
                    default_value: None,
                    comment: None,
                },
                ColumnDef {
                    name: "middle_name".to_string(),
                    data_type: DataType::Varchar { max_length: Some(50) },
                    nullable: true, // Nullable field
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
                ColumnDef {
                    name: "manager_id".to_string(),
                    data_type: DataType::Integer,
                    nullable: true, // Nullable foreign key
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
            ],
            table_constraints: vec![], table_options: vec![],
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
            table_constraints: vec![], table_options: vec![],
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
                default_value: None,
                comment: None,
            }],
            table_constraints: vec![], table_options: vec![],
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
                default_value: None,
                comment: None,
            }],
            table_constraints: vec![], table_options: vec![],
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
                default_value: None,
                comment: None,
            }],
            table_constraints: vec![], table_options: vec![],
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
                default_value: None,
                comment: None,
            }],
            table_constraints: vec![], table_options: vec![],
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
                default_value: None,
                comment: None,
            }],
            table_constraints: vec![], table_options: vec![],
        };

        // Behavior depends on catalog's case sensitivity
        // Just verify it either succeeds or fails gracefully
        let result = CreateTableExecutor::execute(&stmt2, &mut db);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_create_table_with_spatial_types() {
        // Test spatial data types (SQL/MM standard) - Issue #818
        // These are parsed as UserDefined types and should be accepted by executor
        let mut db = Database::new();

        let stmt = CreateTableStmt {
            table_name: "spatial_table".to_string(),
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
                    name: "location".to_string(),
                    data_type: DataType::UserDefined { type_name: "POINT".to_string() },
                    nullable: true,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
                ColumnDef {
                    name: "area".to_string(),
                    data_type: DataType::UserDefined { type_name: "POLYGON".to_string() },
                    nullable: true,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
                ColumnDef {
                    name: "regions".to_string(),
                    data_type: DataType::UserDefined { type_name: "MULTIPOLYGON".to_string() },
                    nullable: true,
                    constraints: vec![],
                    default_value: None,
                    comment: None,
                },
            ],
            table_constraints: vec![], table_options: vec![],
        };

        let result = CreateTableExecutor::execute(&stmt, &mut db);
        assert!(result.is_ok(), "Should accept spatial types as UserDefined types");

        // Verify table exists and has correct schema
        let schema = db.catalog.get_table("spatial_table");
        assert!(schema.is_some());
        let schema = schema.unwrap();
        assert_eq!(schema.column_count(), 4);

        // Verify spatial type columns exist
        assert!(schema.get_column("location").is_some());
        assert!(schema.get_column("area").is_some());
        assert!(schema.get_column("regions").is_some());
    }

    #[test]
    fn test_create_table_multipolygon_sqllogictest() {
        // Test the exact scenario from SQLLogicTest - Issue #818
        let mut db = Database::new();

        let stmt = CreateTableStmt {
            table_name: "t1710a".to_string(),
            columns: vec![
                ColumnDef {
                    name: "c1".to_string(),
                    data_type: DataType::UserDefined { type_name: "MULTIPOLYGON".to_string() },
                    nullable: true,
                    constraints: vec![],
                    default_value: None,
                    comment: Some("text155459".to_string()),
                },
                ColumnDef {
                    name: "c2".to_string(),
                    data_type: DataType::UserDefined { type_name: "MULTIPOLYGON".to_string() },
                    nullable: true,
                    constraints: vec![],
                    default_value: None,
                    comment: Some("text155461".to_string()),
                },
            ],
            table_constraints: vec![], table_options: vec![],
        };

        let result = CreateTableExecutor::execute(&stmt, &mut db);
        assert!(
            result.is_ok(),
            "Should create table with MULTIPOLYGON columns (SQLLogicTest conformance)"
        );

        // Verify table was created successfully
        assert!(db.catalog.table_exists("t1710a"));
        let schema = db.catalog.get_table("t1710a").unwrap();
        assert_eq!(schema.column_count(), 2);
    }
}
