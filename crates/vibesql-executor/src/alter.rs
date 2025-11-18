//! ALTER TABLE executor

use vibesql_ast::*;
use vibesql_catalog::ColumnSchema;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use crate::{errors::ExecutorError, privilege_checker::PrivilegeChecker};

/// Executor for ALTER TABLE statements
pub struct AlterTableExecutor;

impl AlterTableExecutor {
    /// Execute an ALTER TABLE statement
    pub fn execute(
        stmt: &AlterTableStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Get table name from the statement and check ALTER privilege
        let table_name = match stmt {
            AlterTableStmt::AddColumn(s) => &s.table_name,
            AlterTableStmt::DropColumn(s) => &s.table_name,
            AlterTableStmt::AlterColumn(s) => match s {
                AlterColumnStmt::SetDefault { table_name, .. } => table_name,
                AlterColumnStmt::DropDefault { table_name, .. } => table_name,
                AlterColumnStmt::SetNotNull { table_name, .. } => table_name,
                AlterColumnStmt::DropNotNull { table_name, .. } => table_name,
            },
            AlterTableStmt::AddConstraint(s) => &s.table_name,
            AlterTableStmt::DropConstraint(s) => &s.table_name,
            AlterTableStmt::RenameTable(s) => &s.table_name,
            AlterTableStmt::ModifyColumn(s) => &s.table_name,
            AlterTableStmt::ChangeColumn(s) => &s.table_name,
        };
        PrivilegeChecker::check_alter(database, table_name)?;

        match stmt {
            AlterTableStmt::AddColumn(add_column) => Self::execute_add_column(add_column, database),
            AlterTableStmt::DropColumn(drop_column) => {
                Self::execute_drop_column(drop_column, database)
            }
            AlterTableStmt::AlterColumn(alter_column) => {
                Self::execute_alter_column(alter_column, database)
            }
            AlterTableStmt::AddConstraint(add_constraint) => {
                Self::execute_add_constraint(add_constraint, database)
            }
            AlterTableStmt::DropConstraint(drop_constraint) => {
                Self::execute_drop_constraint(drop_constraint, database)
            }
            AlterTableStmt::RenameTable(rename_table) => {
                Self::execute_rename_table(rename_table, database)
            }
            AlterTableStmt::ModifyColumn(modify_column) => {
                Self::execute_modify_column(modify_column, database)
            }
            AlterTableStmt::ChangeColumn(change_column) => {
                Self::execute_change_column(change_column, database)
            }
        }
    }

    /// Execute ADD COLUMN
    fn execute_add_column(
        stmt: &AddColumnStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        let table = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Check if column already exists
        if table.schema.has_column(&stmt.column_def.name) {
            return Err(ExecutorError::ColumnAlreadyExists(stmt.column_def.name.clone()));
        }

        // Add column to schema
        let mut new_column = ColumnSchema::new(
            stmt.column_def.name.clone(),
            stmt.column_def.data_type.clone(),
            stmt.column_def.nullable,
        );

        // Set the default value if provided
        if let Some(ref default_expr) = stmt.column_def.default_value {
            new_column.set_default(*default_expr.clone());
        }

        table.schema_mut().add_column(new_column)?;

        // Add default value (or NULL) to all existing rows
        let default_value = if let Some(ref default_expr) = stmt.column_def.default_value {
            // Evaluate the default expression for simple cases (literals)
            Self::evaluate_simple_default(default_expr)?
        } else {
            SqlValue::Null
        };

        for row in table.rows_mut() {
            row.add_value(default_value.clone());
        }

        Ok(format!("Column '{}' added to table '{}'", stmt.column_def.name, stmt.table_name))
    }

    /// Execute DROP COLUMN
    fn execute_drop_column(
        stmt: &DropColumnStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        let table = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Check if column exists
        if !stmt.if_exists && !table.schema.has_column(&stmt.column_name) {
            return Err(ExecutorError::ColumnNotFound {
                column_name: stmt.column_name.clone(),
                table_name: stmt.table_name.clone(),
                searched_tables: vec![stmt.table_name.clone()],
                available_columns: table.schema.columns.iter().map(|c| c.name.clone()).collect(),
            });
        }

        // Check if column is part of constraints
        if table.schema.is_column_in_primary_key(&stmt.column_name) {
            return Err(ExecutorError::CannotDropColumn(
                "Column is part of PRIMARY KEY".to_string(),
            ));
        }

        // Check if this is the last column in the table
        if table.schema.columns.len() <= 1 {
            return Err(ExecutorError::CannotDropColumn(
                "Cannot drop the last column in a table. Use DROP TABLE instead.".to_string(),
            ));
        }

        // Get column index
        let col_index = table.schema.get_column_index(&stmt.column_name).ok_or_else(|| {
            ExecutorError::ColumnNotFound {
                column_name: stmt.column_name.clone(),
                table_name: stmt.table_name.clone(),
                searched_tables: vec![stmt.table_name.clone()],
                available_columns: table.schema.columns.iter().map(|c| c.name.clone()).collect(),
            }
        })?;

        // Remove column from schema
        table.schema_mut().remove_column(col_index)?;

        // Remove column data from all rows
        for row in table.rows_mut() {
            let _ = row.remove_value(col_index);
        }

        Ok(format!("Column '{}' dropped from table '{}'", stmt.column_name, stmt.table_name))
    }

    /// Execute ALTER COLUMN
    fn execute_alter_column(
        stmt: &AlterColumnStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        match stmt {
            AlterColumnStmt::SetDefault { table_name, column_name, default } => {
                let table = database
                    .get_table_mut(table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

                let col_index = table.schema.get_column_index(column_name).ok_or_else(|| {
                    ExecutorError::ColumnNotFound {
                        column_name: column_name.clone(),
                        table_name: table_name.clone(),
                        searched_tables: vec![table_name.clone()],
                        available_columns: table
                            .schema
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    }
                })?;

                // Set the default value in the schema
                table.schema_mut().set_column_default(col_index, default.clone())?;

                Ok(format!("Default set for column '{}' in table '{}'", column_name, table_name))
            }
            AlterColumnStmt::DropDefault { table_name, column_name } => {
                let table = database
                    .get_table_mut(table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

                let col_index = table.schema.get_column_index(column_name).ok_or_else(|| {
                    ExecutorError::ColumnNotFound {
                        column_name: column_name.clone(),
                        table_name: table_name.clone(),
                        searched_tables: vec![table_name.clone()],
                        available_columns: table
                            .schema
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    }
                })?;

                // Drop the default value from the schema
                table.schema_mut().drop_column_default(col_index)?;

                Ok(format!(
                    "Default dropped for column '{}' in table '{}'",
                    column_name, table_name
                ))
            }
            AlterColumnStmt::SetNotNull { table_name, column_name } => {
                let table = database
                    .get_table_mut(table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

                let col_index = table.schema.get_column_index(column_name).ok_or_else(|| {
                    ExecutorError::ColumnNotFound {
                        column_name: column_name.clone(),
                        table_name: table_name.clone(),
                        searched_tables: vec![table_name.clone()],
                        available_columns: table
                            .schema
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    }
                })?;

                // Check if any existing rows have NULL in this column
                for row in table.scan() {
                    if let SqlValue::Null = &row.values[col_index] {
                        return Err(ExecutorError::ConstraintViolation(
                            "Cannot set NOT NULL: column contains NULL values".to_string(),
                        ));
                    }
                }

                // Set column as NOT NULL
                table.schema_mut().set_column_nullable(col_index, false)?;

                Ok(format!("Column '{}' set to NOT NULL in table '{}'", column_name, table_name))
            }
            AlterColumnStmt::DropNotNull { table_name, column_name } => {
                let table = database
                    .get_table_mut(table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

                let col_index = table.schema.get_column_index(column_name).ok_or_else(|| {
                    ExecutorError::ColumnNotFound {
                        column_name: column_name.clone(),
                        table_name: table_name.clone(),
                        searched_tables: vec![table_name.clone()],
                        available_columns: table
                            .schema
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    }
                })?;

                // Set column as nullable
                table.schema_mut().set_column_nullable(col_index, true)?;

                Ok(format!("Column '{}' set to nullable in table '{}'", column_name, table_name))
            }
        }
    }

    /// Execute ADD CONSTRAINT
    fn execute_add_constraint(
        stmt: &AddConstraintStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        let table = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        match &stmt.constraint.kind {
            TableConstraintKind::PrimaryKey { columns } => {
                // Extract column names from IndexColumn structs
                let column_names: Vec<String> = columns.iter().map(|c| c.column_name.clone()).collect();

                // Verify all columns exist
                for col_name in &column_names {
                    if !table.schema.has_column(col_name) {
                        return Err(ExecutorError::ColumnNotFound {
                            column_name: col_name.clone(),
                            table_name: stmt.table_name.clone(),
                            searched_tables: vec![stmt.table_name.clone()],
                            available_columns: table
                                .schema
                                .columns
                                .iter()
                                .map(|c| c.name.clone())
                                .collect(),
                        });
                    }
                }

                // Check if primary key already exists
                if table.schema.primary_key.is_some() {
                    return Err(ExecutorError::ConstraintViolation(
                        "Table already has a PRIMARY KEY constraint".to_string(),
                    ));
                }

                // Add primary key to table schema
                table.schema_mut().primary_key = Some(column_names);

                // Rebuild table indexes to create the primary key index
                table.rebuild_indexes();

                // Update catalog with modified schema by dropping and recreating
                let updated_schema = table.schema.clone();
                database.catalog.drop_table(&stmt.table_name)?;
                database.catalog.create_table(updated_schema)?;

                Ok(format!(
                    "PRIMARY KEY constraint added to table '{}'",
                    stmt.table_name
                ))
            }
            TableConstraintKind::Unique { columns } => {
                // Extract column names from IndexColumn structs
                let column_names: Vec<String> = columns.iter().map(|c| c.column_name.clone()).collect();
                table.schema_mut().add_unique_constraint(column_names)?;

                // Rebuild table indexes to create the unique constraint index
                table.rebuild_indexes();

                // Update catalog with modified schema by dropping and recreating
                let updated_schema = table.schema.clone();
                database.catalog.drop_table(&stmt.table_name)?;
                database.catalog.create_table(updated_schema)?;

                Ok(format!(
                    "UNIQUE constraint added to table '{}'",
                    stmt.table_name
                ))
            }
            TableConstraintKind::Check { expr } => {
                let constraint_name = stmt
                    .constraint
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("check_{}", table.schema.check_constraints.len()));

                table
                    .schema_mut()
                    .add_check_constraint(constraint_name.clone(), *expr.clone())?;

                Ok(format!(
                    "CHECK constraint '{}' added to table '{}'",
                    constraint_name, stmt.table_name
                ))
            }
            TableConstraintKind::ForeignKey {
                columns,
                references_table,
                references_columns,
                on_delete,
                on_update,
            } => {
                use vibesql_catalog::ForeignKeyConstraint;

                // Convert AST ReferentialAction to catalog ReferentialAction
                let convert_action = |action: &Option<vibesql_ast::ReferentialAction>| {
                    match action {
                        Some(vibesql_ast::ReferentialAction::Cascade) => {
                            vibesql_catalog::ReferentialAction::Cascade
                        }
                        Some(vibesql_ast::ReferentialAction::SetNull) => {
                            vibesql_catalog::ReferentialAction::SetNull
                        }
                        Some(vibesql_ast::ReferentialAction::SetDefault) => {
                            vibesql_catalog::ReferentialAction::SetDefault
                        }
                        Some(vibesql_ast::ReferentialAction::Restrict) => {
                            vibesql_catalog::ReferentialAction::Restrict
                        }
                        Some(vibesql_ast::ReferentialAction::NoAction) | None => {
                            vibesql_catalog::ReferentialAction::NoAction
                        }
                    }
                };

                // Get column indices
                let mut column_indices = Vec::new();
                for col_name in columns {
                    let idx = table.schema.get_column_index(col_name).ok_or_else(|| {
                        ExecutorError::ColumnNotFound {
                            column_name: col_name.clone(),
                            table_name: stmt.table_name.clone(),
                            searched_tables: vec![stmt.table_name.clone()],
                            available_columns: table
                                .schema
                                .columns
                                .iter()
                                .map(|c| c.name.clone())
                                .collect(),
                        }
                    })?;
                    column_indices.push(idx);
                }

                // Verify referenced table exists
                let ref_table = database
                    .get_table(references_table)
                    .ok_or_else(|| ExecutorError::TableNotFound(references_table.clone()))?;

                // Get referenced column indices
                let mut parent_column_indices = Vec::new();
                for col_name in references_columns {
                    let idx = ref_table.schema.get_column_index(col_name).ok_or_else(|| {
                        ExecutorError::ColumnNotFound {
                            column_name: col_name.clone(),
                            table_name: references_table.clone(),
                            searched_tables: vec![references_table.clone()],
                            available_columns: ref_table
                                .schema
                                .columns
                                .iter()
                                .map(|c| c.name.clone())
                                .collect(),
                        }
                    })?;
                    parent_column_indices.push(idx);
                }

                let fk = ForeignKeyConstraint {
                    name: stmt.constraint.name.clone(),
                    column_names: columns.clone(),
                    column_indices,
                    parent_table: references_table.clone(),
                    parent_column_names: references_columns.clone(),
                    parent_column_indices,
                    on_delete: convert_action(on_delete),
                    on_update: convert_action(on_update),
                };

                let table = database
                    .get_table_mut(&stmt.table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;
                table.schema_mut().add_foreign_key(fk)?;

                // Update catalog with modified schema by dropping and recreating
                let updated_schema = table.schema.clone();
                database.catalog.drop_table(&stmt.table_name)?;
                database.catalog.create_table(updated_schema)?;

                Ok(format!(
                    "FOREIGN KEY constraint added to table '{}'",
                    stmt.table_name
                ))
            }
            TableConstraintKind::Fulltext { index_name: _, columns: _ } => {
                // TODO: Implement FULLTEXT index creation
                // For now, just return a message indicating it's not yet implemented
                Err(ExecutorError::UnsupportedFeature(
                    "FULLTEXT index support is not yet implemented".to_string(),
                ))
            }
        }
    }

    /// Execute DROP CONSTRAINT
    fn execute_drop_constraint(
        stmt: &DropConstraintStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        let table = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Try to drop from each constraint type
        // First try check constraints
        if table.schema_mut().drop_check_constraint(&stmt.constraint_name).is_ok() {
            // Update catalog with modified schema
            let updated_schema = table.schema.clone();
            database.catalog.drop_table(&stmt.table_name)?;
            database.catalog.create_table(updated_schema)?;

            return Ok(format!(
                "CHECK constraint '{}' dropped from table '{}'",
                stmt.constraint_name, stmt.table_name
            ));
        }

        // Try foreign keys
        if table.schema_mut().drop_foreign_key(&stmt.constraint_name).is_ok() {
            // Update catalog with modified schema
            let updated_schema = table.schema.clone();
            database.catalog.drop_table(&stmt.table_name)?;
            database.catalog.create_table(updated_schema)?;

            return Ok(format!(
                "FOREIGN KEY constraint '{}' dropped from table '{}'",
                stmt.constraint_name, stmt.table_name
            ));
        }

        // Constraint not found
        Err(ExecutorError::ConstraintNotFound {
            constraint_name: stmt.constraint_name.clone(),
            table_name: stmt.table_name.clone(),
        })
    }

    /// Evaluate a simple default expression (literals and basic expressions)
    /// For more complex expressions, this would need full evaluation context
    fn evaluate_simple_default(expr: &Expression) -> Result<SqlValue, ExecutorError> {
        match expr {
            Expression::Literal(val) => Ok(val.clone()),
            Expression::UnaryOp { op, expr } => {
                let val = Self::evaluate_simple_default(expr)?;
                match op {
                    vibesql_ast::UnaryOperator::Not => match val {
                        SqlValue::Boolean(b) => Ok(SqlValue::Boolean(!b)),
                        SqlValue::Null => Ok(SqlValue::Null),
                        _ => Err(ExecutorError::TypeMismatch {
                            left: val,
                            op: "NOT".to_string(),
                            right: SqlValue::Null,
                        }),
                    },
                    vibesql_ast::UnaryOperator::Minus => match val {
                        SqlValue::Integer(i) => Ok(SqlValue::Integer(-i)),
                        SqlValue::Numeric(d) => Ok(SqlValue::Numeric(-d)),
                        SqlValue::Null => Ok(SqlValue::Null),
                        _ => Err(ExecutorError::TypeMismatch {
                            left: val,
                            op: "-".to_string(),
                            right: SqlValue::Null,
                        }),
                    },
                    _ => Err(ExecutorError::UnsupportedExpression(format!(
                        "Unsupported unary operator in default: {:?}",
                        op
                    ))),
                }
            }
            _ => Err(ExecutorError::UnsupportedExpression(
                "Complex expressions in DEFAULT not yet supported. Use simple literals.".to_string(),
            )),
        }
    }

    /// Execute RENAME TABLE
    fn execute_rename_table(
        stmt: &RenameTableStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Check if new table name already exists
        if database.get_table(&stmt.new_table_name).is_some() {
            return Err(ExecutorError::TableAlreadyExists(stmt.new_table_name.clone()));
        }

        // Get the old table to ensure it exists
        let old_table = database
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Clone the table and update its schema name
        let mut new_table = old_table.clone();
        new_table.schema_mut().name = stmt.new_table_name.clone();

        // Drop old table and create new one with the renamed schema
        // This handles indexes and spatial indexes via CASCADE
        database
            .drop_table(&stmt.table_name)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        database
            .create_table(new_table.schema.clone())
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        // Restore the data by getting the new table and setting its rows
        let restored_table = database
            .get_table_mut(&stmt.new_table_name)
            .ok_or_else(|| ExecutorError::TableAlreadyExists(stmt.new_table_name.clone()))?;

        for row in new_table.scan() {
            restored_table
                .insert(row.clone())
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
        }

        Ok(format!(
            "Table '{}' renamed to '{}'",
            stmt.table_name, stmt.new_table_name
        ))
    }

    /// Execute MODIFY COLUMN
    fn execute_modify_column(
        stmt: &ModifyColumnStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        

        let table = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Get column index
        let col_index = table.schema.get_column_index(&stmt.column_name).ok_or_else(|| {
            ExecutorError::ColumnNotFound {
                column_name: stmt.column_name.clone(),
                table_name: stmt.table_name.clone(),
                searched_tables: vec![stmt.table_name.clone()],
                available_columns: table.schema.columns.iter().map(|c| c.name.clone()).collect(),
            }
        })?;

        let old_type = &table.schema.columns[col_index].data_type;
        let new_type = &stmt.new_column_def.data_type;

        // Type conversion validation - be strict about compatibility
        let is_compatible = Self::is_type_conversion_safe(old_type, new_type);

        if !is_compatible {
            return Err(ExecutorError::TypeMismatch {
                left: SqlValue::Null, // Placeholder
                op: format!("Cannot convert column from {:?} to {:?}", old_type, new_type),
                right: SqlValue::Null,
            });
        }

        // Convert existing data
        for row in table.rows_mut() {
            if let Some(value) = row.values.get_mut(col_index) {
                *value = Self::convert_value(value.clone(), new_type)?;
            }
        }

        // Update schema
        table.schema_mut().columns[col_index].data_type = new_type.clone();
        table.schema_mut().columns[col_index].nullable = stmt.new_column_def.nullable;

        // Update default value if provided
        if let Some(ref default_expr) = stmt.new_column_def.default_value {
            table.schema_mut().set_column_default(col_index, *default_expr.clone())?;
        }

        Ok(format!("Column '{}' modified in table '{}'", stmt.column_name, stmt.table_name))
    }

    /// Execute CHANGE COLUMN (rename + modify)
    fn execute_change_column(
        stmt: &ChangeColumnStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        

        let table = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Get column index
        let col_index = table.schema.get_column_index(&stmt.old_column_name).ok_or_else(|| {
            ExecutorError::ColumnNotFound {
                column_name: stmt.old_column_name.clone(),
                table_name: stmt.table_name.clone(),
                searched_tables: vec![stmt.table_name.clone()],
                available_columns: table.schema.columns.iter().map(|c| c.name.clone()).collect(),
            }
        })?;

        let old_type = &table.schema.columns[col_index].data_type;
        let new_type = &stmt.new_column_def.data_type;

        // Type conversion validation
        let is_compatible = Self::is_type_conversion_safe(old_type, new_type);

        if !is_compatible {
            return Err(ExecutorError::TypeMismatch {
                left: SqlValue::Null,
                op: format!("Cannot convert column from {:?} to {:?}", old_type, new_type),
                right: SqlValue::Null,
            });
        }

        // Convert existing data
        for row in table.rows_mut() {
            if let Some(value) = row.values.get_mut(col_index) {
                *value = Self::convert_value(value.clone(), new_type)?;
            }
        }

        // Update schema - rename and modify
        table.schema_mut().columns[col_index].name = stmt.new_column_def.name.clone();
        table.schema_mut().columns[col_index].data_type = new_type.clone();
        table.schema_mut().columns[col_index].nullable = stmt.new_column_def.nullable;

        // Update default value if provided
        if let Some(ref default_expr) = stmt.new_column_def.default_value {
            table.schema_mut().set_column_default(col_index, *default_expr.clone())?;
        }

        Ok(format!(
            "Column '{}' changed to '{}' in table '{}'",
            stmt.old_column_name, stmt.new_column_def.name, stmt.table_name
        ))
    }

    /// Check if type conversion is safe (widening conversions only)
    fn is_type_conversion_safe(from: &vibesql_types::DataType, to: &vibesql_types::DataType) -> bool {
        use vibesql_types::DataType;

        match (from, to) {
            // Same type is always safe
            (a, b) if a == b => true,

            // Integer widenings
            (DataType::Integer, DataType::Bigint) => true,
            (DataType::Smallint, DataType::Integer) => true,
            (DataType::Smallint, DataType::Bigint) => true,

            // Numeric widenings
            (DataType::Integer, DataType::Numeric { .. }) => true,
            (DataType::Smallint, DataType::Numeric { .. }) => true,
            (DataType::Bigint, DataType::Numeric { .. }) => true,
            (DataType::Numeric { precision: p1, scale: s1 }, DataType::Numeric { precision: p2, scale: s2 }) => p2 >= p1 && s2 >= s1,

            // String widenings
            (DataType::Character { length: n1 }, DataType::Character { length: n2 }) => n2 >= n1,
            (DataType::Varchar { max_length: Some(n1) }, DataType::Varchar { max_length: Some(n2) }) => n2 >= n1,
            (DataType::Varchar { max_length: Some(_) }, DataType::Varchar { max_length: None }) => true,
            (DataType::Character { .. }, DataType::Varchar { .. }) => true,
            (DataType::Character { .. }, DataType::CharacterLargeObject) => true,
            (DataType::Varchar { .. }, DataType::CharacterLargeObject) => true,

            // Any to CLOB (always safe, just convert to string)
            (DataType::Integer, DataType::CharacterLargeObject) => true,
            (DataType::Bigint, DataType::CharacterLargeObject) => true,
            (DataType::Smallint, DataType::CharacterLargeObject) => true,
            (DataType::Numeric { .. }, DataType::CharacterLargeObject) => true,
            (DataType::Boolean, DataType::CharacterLargeObject) => true,

            // Otherwise, reject
            _ => false,
        }
    }

    /// Convert a value to a new type
    fn convert_value(value: SqlValue, target_type: &vibesql_types::DataType) -> Result<SqlValue, ExecutorError> {
        use vibesql_types::DataType;

        // NULL stays NULL
        if matches!(value, SqlValue::Null) {
            return Ok(SqlValue::Null);
        }

        match target_type {
            DataType::Integer => match &value {
                SqlValue::Integer(i) => Ok(SqlValue::Integer(*i)),
                SqlValue::Smallint(s) => Ok(SqlValue::Integer(*s as i64)),
                SqlValue::Bigint(b) => Ok(SqlValue::Integer(*b)), // May truncate
                SqlValue::Varchar(s) | SqlValue::Character(s) => {
                    s.parse::<i64>()
                        .map(SqlValue::Integer)
                        .map_err(|_| ExecutorError::TypeConversionError {
                            from: format!("{:?}", value),
                            to: "INTEGER".to_string(),
                        })
                }
                _ => Err(ExecutorError::TypeConversionError {
                    from: format!("{:?}", value),
                    to: "INTEGER".to_string(),
                }),
            },
            DataType::Bigint => match &value {
                SqlValue::Integer(i) => Ok(SqlValue::Bigint(*i)),
                SqlValue::Smallint(s) => Ok(SqlValue::Bigint(*s as i64)),
                SqlValue::Bigint(b) => Ok(SqlValue::Bigint(*b)),
                _ => Err(ExecutorError::TypeConversionError {
                    from: format!("{:?}", value),
                    to: "BIGINT".to_string(),
                }),
            },
            DataType::Smallint => match &value {
                SqlValue::Smallint(s) => Ok(SqlValue::Smallint(*s)),
                SqlValue::Integer(i) => Ok(SqlValue::Smallint(*i as i16)), // May truncate
                _ => Err(ExecutorError::TypeConversionError {
                    from: format!("{:?}", value),
                    to: "SMALLINT".to_string(),
                }),
            },
            DataType::Numeric { .. } | DataType::Decimal { .. } => match &value {
                SqlValue::Numeric(d) => Ok(SqlValue::Numeric(*d)),
                SqlValue::Integer(i) => Ok(SqlValue::Numeric(*i as f64)),
                SqlValue::Smallint(s) => Ok(SqlValue::Numeric(*s as f64)),
                SqlValue::Bigint(b) => Ok(SqlValue::Numeric(*b as f64)),
                _ => Err(ExecutorError::TypeConversionError {
                    from: format!("{:?}", value),
                    to: "NUMERIC".to_string(),
                }),
            },
            DataType::Varchar { .. } | DataType::Character { .. } | DataType::CharacterLargeObject => {
                // Convert anything to string
                match value {
                    SqlValue::Varchar(s) | SqlValue::Character(s) => {
                        Ok(SqlValue::Varchar(s))
                    }
                    SqlValue::Integer(i) => Ok(SqlValue::Varchar(i.to_string())),
                    SqlValue::Bigint(b) => Ok(SqlValue::Varchar(b.to_string())),
                    SqlValue::Smallint(s) => Ok(SqlValue::Varchar(s.to_string())),
                    SqlValue::Numeric(d) => Ok(SqlValue::Varchar(d.to_string())),
                    SqlValue::Boolean(b) => Ok(SqlValue::Varchar(b.to_string())),
                    _ => Ok(SqlValue::Varchar(format!("{:?}", value))),
                }
            }
            _ => Err(ExecutorError::TypeConversionError {
                from: format!("{:?}", value),
                to: format!("{:?}", target_type),
            }),
        }
    }
}
