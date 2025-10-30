//! Executor for advanced SQL:1999 objects (DOMAIN, SEQUENCE, TYPE, COLLATION, etc.)

use crate::errors::ExecutorError;
use ast::*;
use storage::Database;

/// Execute CREATE DOMAIN statement
pub fn execute_create_domain(stmt: &CreateDomainStmt, db: &mut Database) -> Result<(), ExecutorError> {
    db.catalog.create_domain(stmt.domain_name.clone())?;
    Ok(())
}

/// Execute DROP DOMAIN statement
pub fn execute_drop_domain(stmt: &DropDomainStmt, db: &mut Database) -> Result<(), ExecutorError> {
    db.catalog.drop_domain(&stmt.domain_name)?;
    Ok(())
}

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
