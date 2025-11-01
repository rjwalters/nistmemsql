//! Advanced SQL:1999 DDL objects
//!
//! This module contains AST nodes for advanced SQL:1999 features:
//! - DOMAIN
//! - SEQUENCE
//! - TYPE (distinct, structured)
//! - COLLATION
//! - CHARACTER SET
//! - TRANSLATION
//! - ASSERTION

use crate::Expression;
use types::DataType;

// ============================================================================
// DOMAIN
// ============================================================================

/// CREATE DOMAIN statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateDomainStmt {
    pub domain_name: String,
    pub data_type: DataType,
    pub default: Option<Box<Expression>>,
    pub constraints: Vec<DomainConstraint>,
}

/// Domain constraint (CHECK constraint on domain values)
#[derive(Debug, Clone, PartialEq)]
pub struct DomainConstraint {
    pub name: Option<String>,
    pub check: Box<Expression>,
}

/// DROP DOMAIN statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropDomainStmt {
    pub domain_name: String,
    pub cascade: bool, // true for CASCADE, false for RESTRICT
}

// ============================================================================
// SEQUENCE
// ============================================================================

/// CREATE SEQUENCE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSequenceStmt {
    pub sequence_name: String,
    pub start_with: Option<i64>,
    pub increment_by: i64, // default: 1
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cycle: bool, // default: false
}

/// DROP SEQUENCE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropSequenceStmt {
    pub sequence_name: String,
    pub cascade: bool, // true for CASCADE, false for RESTRICT
}

/// ALTER SEQUENCE statement
#[derive(Debug, Clone, PartialEq)]
pub struct AlterSequenceStmt {
    pub sequence_name: String,
    pub restart_with: Option<i64>,
    pub increment_by: Option<i64>,
    pub min_value: Option<Option<i64>>, // None = no change, Some(None) = NO MINVALUE, Some(Some(n)) = MINVALUE n
    pub max_value: Option<Option<i64>>, // None = no change, Some(None) = NO MAXVALUE, Some(Some(n)) = MAXVALUE n
    pub cycle: Option<bool>,
}

// ============================================================================
// TYPE
// ============================================================================

/// CREATE TYPE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTypeStmt {
    pub type_name: String,
    pub definition: TypeDefinition,
}

/// Type definition (distinct, structured, or forward)
#[derive(Debug, Clone, PartialEq)]
pub enum TypeDefinition {
    Distinct { base_type: DataType },
    Structured { attributes: Vec<TypeAttribute> },
    Forward, // Forward declaration without definition
}

/// Attribute in a structured type
#[derive(Debug, Clone, PartialEq)]
pub struct TypeAttribute {
    pub name: String,
    pub data_type: DataType,
}

/// DROP TYPE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTypeStmt {
    pub type_name: String,
    pub behavior: DropBehavior,
}

/// Drop behavior for CASCADE/RESTRICT
#[derive(Debug, Clone, PartialEq)]
pub enum DropBehavior {
    Cascade,
    Restrict,
}

// ============================================================================
// COLLATION
// ============================================================================

/// CREATE COLLATION statement
///
/// SQL:1999 Syntax:
///   CREATE COLLATION collation_name
///     [FOR character_set]
///     [FROM source_collation]
///     [PAD SPACE | NO PAD]
#[derive(Debug, Clone, PartialEq)]
pub struct CreateCollationStmt {
    pub collation_name: String,
    pub character_set: Option<String>,    // FOR character_set
    pub source_collation: Option<String>, // FROM source_collation
    pub pad_space: Option<bool>,          // PAD SPACE (true) | NO PAD (false)
}

/// DROP COLLATION statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropCollationStmt {
    pub collation_name: String,
}

// ============================================================================
// CHARACTER SET
// ============================================================================

/// CREATE CHARACTER SET statement
///
/// SQL:1999 Syntax:
///   CREATE CHARACTER SET charset_name [AS]
///     [GET source]
///     [COLLATE FROM collation]
#[derive(Debug, Clone, PartialEq)]
pub struct CreateCharacterSetStmt {
    pub charset_name: String,
    pub source: Option<String>,    // GET source
    pub collation: Option<String>, // COLLATE FROM collation
}

/// DROP CHARACTER SET statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropCharacterSetStmt {
    pub charset_name: String,
}

// ============================================================================
// TRANSLATION
// ============================================================================

/// CREATE TRANSLATION statement
///
/// SQL:1999 Syntax:
///   CREATE TRANSLATION translation_name
///     [FOR source_charset TO target_charset]
///     [FROM translation_source]
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTranslationStmt {
    pub translation_name: String,
    pub source_charset: Option<String>,     // FOR source_charset
    pub target_charset: Option<String>,     // TO target_charset
    pub translation_source: Option<String>, // FROM translation_source
}

/// DROP TRANSLATION statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTranslationStmt {
    pub translation_name: String,
}

// ============================================================================
// ASSERTION
// ============================================================================

/// CREATE ASSERTION statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateAssertionStmt {
    pub assertion_name: String,
    pub check_condition: Box<Expression>,
}

/// DROP ASSERTION statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropAssertionStmt {
    pub assertion_name: String,
    pub cascade: bool, // true for CASCADE, false for RESTRICT
}
