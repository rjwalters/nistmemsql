//! Executor for advanced SQL:1999 objects (SEQUENCE, TYPE, COLLATION, etc.)
//! Note: DOMAIN has a full implementation in domain_ddl module

use crate::errors::ExecutorError;
use ast::*;
use storage::Database;

// DOMAIN functions are in domain_ddl module with full implementation

/// Execute CREATE SEQUENCE statement
pub fn execute_create_sequence(
    stmt: &CreateSequenceStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.create_sequence(stmt.sequence_name.clone())?;
    Ok(())
}

/// Execute DROP SEQUENCE statement
pub fn execute_drop_sequence(
    stmt: &DropSequenceStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.drop_sequence(&stmt.sequence_name)?;
    Ok(())
}

/// Execute CREATE TYPE statement (comprehensive implementation)
pub fn execute_create_type(stmt: &CreateTypeStmt, db: &mut Database) -> Result<(), ExecutorError> {
    use catalog::{TypeAttribute, TypeDefinition, TypeDefinitionKind};

    // Convert AST TypeDefinition to Catalog TypeDefinitionKind
    let catalog_def = match &stmt.definition {
        ast::TypeDefinition::Distinct { base_type } => {
            TypeDefinitionKind::Distinct { base_type: base_type.clone() }
        }
        ast::TypeDefinition::Structured { attributes } => {
            let catalog_attrs = attributes
                .iter()
                .map(|attr| TypeAttribute {
                    name: attr.name.clone(),
                    data_type: attr.data_type.clone()
                })
                .collect();
            TypeDefinitionKind::Structured { attributes: catalog_attrs }
        }
    };

    let type_def = TypeDefinition {
        name: stmt.type_name.clone(),
        definition: catalog_def
    };

    db.catalog.create_type(type_def)?;
    Ok(())
}

/// Execute DROP TYPE statement (comprehensive implementation with CASCADE/RESTRICT)
pub fn execute_drop_type(stmt: &DropTypeStmt, db: &mut Database) -> Result<(), ExecutorError> {
    let cascade = matches!(stmt.behavior, DropBehavior::Cascade);
    db.catalog.drop_type(&stmt.type_name, cascade)?;
    Ok(())
}

/// Execute CREATE COLLATION statement
pub fn execute_create_collation(
    stmt: &CreateCollationStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.create_collation(stmt.collation_name.clone())?;
    Ok(())
}

/// Execute DROP COLLATION statement
pub fn execute_drop_collation(
    stmt: &DropCollationStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.drop_collation(&stmt.collation_name)?;
    Ok(())
}

/// Execute CREATE CHARACTER SET statement
pub fn execute_create_character_set(
    stmt: &CreateCharacterSetStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.create_character_set(stmt.charset_name.clone())?;
    Ok(())
}

/// Execute DROP CHARACTER SET statement
pub fn execute_drop_character_set(
    stmt: &DropCharacterSetStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.drop_character_set(&stmt.charset_name)?;
    Ok(())
}

/// Execute CREATE TRANSLATION statement
pub fn execute_create_translation(
    stmt: &CreateTranslationStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.create_translation(stmt.translation_name.clone())?;
    Ok(())
}

/// Execute DROP TRANSLATION statement
pub fn execute_drop_translation(
    stmt: &DropTranslationStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.drop_translation(&stmt.translation_name)?;
    Ok(())
}
