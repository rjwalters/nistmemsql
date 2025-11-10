//! CREATE TABLE statement execution

use vibesql_ast::CreateTableStmt;
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::Database;

use crate::{
    constraint_validator::ConstraintValidator, errors::ExecutorError,
    privilege_checker::PrivilegeChecker,
};

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
    /// use vibesql_ast::{ColumnDef, CreateTableStmt};
    /// use vibesql_executor::CreateTableExecutor;
    /// use vibesql_storage::Database;
    /// use vibesql_types::DataType;
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
    ///     table_constraints: vec![],
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

        // Process constraints using the constraint validator
        let constraint_result =
            ConstraintValidator::process_constraints(&stmt.columns, &stmt.table_constraints)?;

        // Apply constraint results to columns (updates nullability)
        ConstraintValidator::apply_to_columns(&mut columns, &constraint_result);

        // Create TableSchema with unqualified name
        let mut table_schema = TableSchema::new(table_name.clone(), columns);

        // Apply constraint results to schema (sets PK, unique, and check constraints)
        ConstraintValidator::apply_to_schema(&mut table_schema, &constraint_result);

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
