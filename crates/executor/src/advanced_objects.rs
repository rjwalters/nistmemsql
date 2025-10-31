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

/// Execute CREATE TYPE statement
pub fn execute_create_type(stmt: &CreateTypeStmt, db: &mut Database) -> Result<(), ExecutorError> {
    db.catalog.create_type(stmt.type_name.clone())?;
    Ok(())
}

/// Execute DROP TYPE statement
pub fn execute_drop_type(stmt: &DropTypeStmt, db: &mut Database) -> Result<(), ExecutorError> {
    db.catalog.drop_type(&stmt.type_name)?;
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
