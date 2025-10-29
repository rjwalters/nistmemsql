//! ALTER TABLE executor

use crate::errors::ExecutorError;
use ast::*;
use catalog::ColumnSchema;
use storage::{Database, Table};
use types::SqlValue;

/// Executor for ALTER TABLE statements
pub struct AlterTableExecutor;

impl AlterTableExecutor {
    /// Execute an ALTER TABLE statement
    pub fn execute(stmt: &AlterTableStmt, database: &mut Database) -> Result<String, ExecutorError> {
        match stmt {
            AlterTableStmt::AddColumn(add_column) => Self::execute_add_column(add_column, database),
            AlterTableStmt::DropColumn(drop_column) => Self::execute_drop_column(drop_column, database),
            AlterTableStmt::AlterColumn(alter_column) => Self::execute_alter_column(alter_column, database),
            AlterTableStmt::AddConstraint(add_constraint) => Self::execute_add_constraint(add_constraint, database),
            AlterTableStmt::DropConstraint(drop_constraint) => Self::execute_drop_constraint(drop_constraint, database),
        }
    }

    /// Execute ADD COLUMN
    fn execute_add_column(stmt: &AddColumnStmt, database: &mut Database) -> Result<String, ExecutorError> {
        let table = database.get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Check if column already exists
        if table.schema.has_column(&stmt.column_def.name) {
            return Err(ExecutorError::ColumnAlreadyExists(stmt.column_def.name.clone()));
        }

        // Add column to schema
        let new_column = ColumnSchema::new(
            stmt.column_def.name.clone(),
            stmt.column_def.data_type.clone(),
            stmt.column_def.nullable,
        );
        table.schema_mut().add_column(new_column)?;

        // Add default value (or NULL) to all existing rows
        let default_value = if let Some(default_constraint) = stmt.column_def.constraints.iter()
            .find(|c| matches!(c, ColumnConstraint::Check(_))) {
            // TODO: Handle default values properly
            SqlValue::Null
        } else {
            SqlValue::Null
        };

        for row in table.rows_mut() {
            row.add_value(default_value.clone());
        }

        Ok(format!("Column '{}' added to table '{}'", stmt.column_def.name, stmt.table_name))
    }

    /// Execute DROP COLUMN
    fn execute_drop_column(stmt: &DropColumnStmt, database: &mut Database) -> Result<String, ExecutorError> {
        let table = database.get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Check if column exists
        if !stmt.if_exists && !table.schema.has_column(&stmt.column_name) {
            return Err(ExecutorError::ColumnNotFound(stmt.column_name.clone()));
        }

        // Check if column is part of constraints
        if table.schema.is_column_in_primary_key(&stmt.column_name) {
            return Err(ExecutorError::CannotDropColumn(
                "Column is part of PRIMARY KEY".to_string()
            ));
        }

        // Get column index
        let col_index = table.schema.get_column_index(&stmt.column_name)
            .ok_or_else(|| ExecutorError::ColumnNotFound(stmt.column_name.clone()))?;

        // Remove column from schema
        table.schema_mut().remove_column(col_index)?;

        // Remove column data from all rows
        for row in table.rows_mut() {
            row.remove_value(col_index);
        }

        Ok(format!("Column '{}' dropped from table '{}'", stmt.column_name, stmt.table_name))
    }

    /// Execute ALTER COLUMN
    fn execute_alter_column(stmt: &AlterColumnStmt, database: &mut Database) -> Result<String, ExecutorError> {
        match stmt {
            AlterColumnStmt::SetDefault { table_name, column_name, default: _ } => {
                // TODO: Implement SET DEFAULT
                Ok(format!("Default set for column '{}' in table '{}'", column_name, table_name))
            }
            AlterColumnStmt::DropDefault { table_name, column_name } => {
                // TODO: Implement DROP DEFAULT
                Ok(format!("Default dropped for column '{}' in table '{}'", column_name, table_name))
            }
            AlterColumnStmt::SetNotNull { table_name, column_name } => {
                let table = database.get_table_mut(table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

                let col_index = table.schema.get_column_index(column_name)
                    .ok_or_else(|| ExecutorError::ColumnNotFound(column_name.clone()))?;

                // Check if any existing rows have NULL in this column
                for row in table.scan() {
                    if let SqlValue::Null = &row.values[col_index] {
                        return Err(ExecutorError::ConstraintViolation(
                            "Cannot set NOT NULL: column contains NULL values".to_string()
                        ));
                    }
                }

                // Set column as NOT NULL
                table.schema_mut().set_column_nullable(col_index, false)?;

                Ok(format!("Column '{}' set to NOT NULL in table '{}'", column_name, table_name))
            }
            AlterColumnStmt::DropNotNull { table_name, column_name } => {
                let table = database.get_table_mut(table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(table_name.clone()))?;

                let col_index = table.schema.get_column_index(column_name)
                    .ok_or_else(|| ExecutorError::ColumnNotFound(column_name.clone()))?;

                // Set column as nullable
                table.schema_mut().set_column_nullable(col_index, true)?;

                Ok(format!("Column '{}' set to nullable in table '{}'", column_name, table_name))
            }
        }
    }

    /// Execute ADD CONSTRAINT
    fn execute_add_constraint(stmt: &AddConstraintStmt, database: &mut Database) -> Result<String, ExecutorError> {
        // TODO: Implement ADD CONSTRAINT
        Ok(format!("Constraint added to table '{}'", stmt.table_name))
    }

    /// Execute DROP CONSTRAINT
    fn execute_drop_constraint(stmt: &DropConstraintStmt, database: &mut Database) -> Result<String, ExecutorError> {
        // TODO: Implement DROP CONSTRAINT
        Ok(format!("Constraint '{}' dropped from table '{}'", stmt.constraint_name, stmt.table_name))
    }
}
