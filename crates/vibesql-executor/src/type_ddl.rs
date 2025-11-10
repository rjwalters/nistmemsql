//! Type DDL executor

use vibesql_ast::*;
use vibesql_catalog::{TypeAttribute, TypeDefinition, TypeDefinitionKind};
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Executor for type DDL statements
pub struct TypeExecutor;

impl TypeExecutor {
    /// Execute CREATE TYPE
    pub fn execute_create_type(
        stmt: &CreateTypeStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Convert AST TypeDefinition to Catalog TypeDefinitionKind
        let catalog_def = match &stmt.definition {
            vibesql_ast::TypeDefinition::Distinct { base_type } => {
                TypeDefinitionKind::Distinct { base_type: base_type.clone() }
            }
            vibesql_ast::TypeDefinition::Structured { attributes } => {
                let catalog_attrs = attributes
                    .iter()
                    .map(|attr| TypeAttribute {
                        name: attr.name.clone(),
                        data_type: attr.data_type.clone(),
                    })
                    .collect();
                TypeDefinitionKind::Structured { attributes: catalog_attrs }
            }
            vibesql_ast::TypeDefinition::Forward => TypeDefinitionKind::Forward,
        };

        let type_def = TypeDefinition { name: stmt.type_name.clone(), definition: catalog_def };

        database
            .catalog
            .create_type(type_def)
            .map_err(|e| ExecutorError::StorageError(format!("Catalog error: {:?}", e)))?;

        Ok(format!("Type '{}' created", stmt.type_name))
    }

    /// Execute DROP TYPE
    pub fn execute_drop_type(
        stmt: &DropTypeStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        let cascade = matches!(stmt.behavior, DropBehavior::Cascade);

        database
            .catalog
            .drop_type(&stmt.type_name, cascade)
            .map_err(|e| ExecutorError::StorageError(format!("Catalog error: {:?}", e)))?;

        Ok(format!("Type '{}' dropped", stmt.type_name))
    }
}
