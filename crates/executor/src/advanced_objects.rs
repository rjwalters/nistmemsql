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
    db.catalog.create_sequence(
        stmt.sequence_name.clone(),
        stmt.start_with,
        stmt.increment_by,
        stmt.min_value,
        stmt.max_value,
        stmt.cycle,
    )?;
    Ok(())
}

/// Execute DROP SEQUENCE statement
pub fn execute_drop_sequence(
    stmt: &DropSequenceStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    // TODO: Handle CASCADE to remove sequence dependencies from columns
    db.catalog.drop_sequence(&stmt.sequence_name)?;
    Ok(())
}

/// Execute ALTER SEQUENCE statement
pub fn execute_alter_sequence(
    stmt: &AlterSequenceStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.alter_sequence(
        &stmt.sequence_name,
        stmt.restart_with,
        stmt.increment_by,
        stmt.min_value,
        stmt.max_value,
        stmt.cycle,
    )?;
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
                    data_type: attr.data_type.clone(),
                })
                .collect();
            TypeDefinitionKind::Structured { attributes: catalog_attrs }
        }
        ast::TypeDefinition::Forward => TypeDefinitionKind::Forward,
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

/// Execute CREATE COLLATION statement
pub fn execute_create_collation(
    stmt: &CreateCollationStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.create_collation(
        stmt.collation_name.clone(),
        stmt.character_set.clone(),
        stmt.source_collation.clone(),
        stmt.pad_space,
    )?;
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
    db.catalog.create_character_set(
        stmt.charset_name.clone(),
        stmt.source.clone(),
        stmt.collation.clone(),
    )?;
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
    db.catalog.create_translation(
        stmt.translation_name.clone(),
        stmt.source_charset.clone(),
        stmt.target_charset.clone(),
        stmt.translation_source.clone(),
    )?;
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

/// Execute CREATE VIEW statement
pub fn execute_create_view(stmt: &CreateViewStmt, db: &mut Database) -> Result<(), ExecutorError> {
    use catalog::ViewDefinition;

    let view = ViewDefinition::new(
        stmt.view_name.clone(),
        stmt.columns.clone(),
        (*stmt.query).clone(),
        stmt.with_check_option,
    );

    db.catalog.create_view(view)?;
    Ok(())
}

/// Execute DROP VIEW statement
pub fn execute_drop_view(stmt: &DropViewStmt, db: &mut Database) -> Result<(), ExecutorError> {
    // Check if view exists
    let view_exists = db.catalog.get_view(&stmt.view_name).is_some();

    // If IF EXISTS is specified and view doesn't exist, succeed silently
    if stmt.if_exists && !view_exists {
        return Ok(());
    }

    // TODO: Handle CASCADE to drop dependent views
    db.catalog.drop_view(&stmt.view_name)?;
    Ok(())
}

/// Execute CREATE ASSERTION statement (SQL:1999 Feature F671/F672)
pub fn execute_create_assertion(
    stmt: &CreateAssertionStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    use catalog::Assertion;

    let assertion = Assertion::new(stmt.assertion_name.clone(), (*stmt.check_condition).clone());

    db.catalog.create_assertion(assertion)?;
    Ok(())
}

/// Execute DROP ASSERTION statement (SQL:1999 Feature F671/F672)
pub fn execute_drop_assertion(
    stmt: &DropAssertionStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.drop_assertion(&stmt.assertion_name, stmt.cascade)?;
    Ok(())
}
