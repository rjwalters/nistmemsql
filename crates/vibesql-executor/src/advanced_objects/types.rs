//! Executor for TYPE objects (SQL:1999)

use vibesql_ast::*;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Execute CREATE TYPE statement (comprehensive implementation)
pub fn execute_create_type(stmt: &CreateTypeStmt, db: &mut Database) -> Result<(), ExecutorError> {
    use vibesql_catalog::{TypeAttribute, TypeDefinition, TypeDefinitionKind};

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

    db.catalog.create_type(type_def)?;
    Ok(())
}

/// Execute DROP TYPE statement (comprehensive implementation with CASCADE/RESTRICT)
pub fn execute_drop_type(stmt: &DropTypeStmt, db: &mut Database) -> Result<(), ExecutorError> {
    let cascade = matches!(stmt.behavior, DropBehavior::Cascade);
    db.catalog.drop_type(&stmt.type_name, cascade)?;
    Ok(())
}
