//! Advanced SQL object DDL parsing (SQL:1999)
//! Includes: SEQUENCE, TYPE, COLLATION, CHARACTER SET, TRANSLATION
//! Note: DOMAIN has a full implementation in domain module

use crate::keywords::Keyword;
use crate::parser::ParseError;
use crate::token::Token;
use ast::*;

// ============================================================================
// DOMAIN
// ============================================================================

// DOMAIN parsing functions are in the domain module with full implementation
// (including data types, defaults, and CHECK constraints)

// ============================================================================
// SEQUENCE
// ============================================================================

/// Parse CREATE SEQUENCE statement
///
/// Syntax: CREATE SEQUENCE sequence_name [START WITH n] [INCREMENT BY n]
/// Minimal implementation: CREATE SEQUENCE sequence_name
pub fn parse_create_sequence(
    parser: &mut crate::Parser,
) -> Result<CreateSequenceStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Sequence)?;

    let sequence_name = parser.parse_identifier()?;

    // For minimal stub: just accept the name, ignore options
    // TODO: Parse full syntax when implementing actual functionality
    parser.consume_until_semicolon_or_eof();

    Ok(CreateSequenceStmt { sequence_name })
}

/// Parse DROP SEQUENCE statement
///
/// Syntax: DROP SEQUENCE sequence_name
pub fn parse_drop_sequence(parser: &mut crate::Parser) -> Result<DropSequenceStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Sequence)?;

    let sequence_name = parser.parse_identifier()?;

    Ok(DropSequenceStmt { sequence_name })
}

// ============================================================================
// TYPE
// ============================================================================

/// Parse CREATE TYPE statement
///
/// Syntax:
///   CREATE TYPE type_name AS DISTINCT base_type
///   CREATE TYPE type_name AS (attr1 type1, attr2 type2, ...)
pub fn parse_create_type(parser: &mut crate::Parser) -> Result<CreateTypeStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Type)?;

    let type_name = parser.parse_identifier()?;

    parser.expect_keyword(Keyword::As)?;

    let definition = if parser.peek_keyword(Keyword::Distinct) {
        // DISTINCT type
        parser.advance(); // consume DISTINCT
        let base_type = parser.parse_data_type()?;
        ast::TypeDefinition::Distinct { base_type }
    } else {
        // STRUCTURED type
        parser.expect_token(Token::LParen)?;

        let mut attributes = Vec::new();
        loop {
            let attr_name = parser.parse_identifier()?;
            let data_type = parser.parse_data_type()?;

            attributes.push(ast::TypeAttribute {
                name: attr_name,
                data_type,
            });

            if !parser.try_consume(&Token::Comma) {
                break;
            }
        }

        parser.expect_token(Token::RParen)?;
        ast::TypeDefinition::Structured { attributes }
    };

    Ok(CreateTypeStmt {
        type_name,
        definition,
    })
}

/// Parse DROP TYPE statement
///
/// Syntax: DROP TYPE type_name [CASCADE | RESTRICT]
pub fn parse_drop_type(parser: &mut crate::Parser) -> Result<DropTypeStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Type)?;

    let type_name = parser.parse_identifier()?;

    // Parse optional CASCADE or RESTRICT
    let behavior = if parser.peek_keyword(Keyword::Cascade) {
        parser.advance();
        ast::DropBehavior::Cascade
    } else if parser.peek_keyword(Keyword::Restrict) {
        parser.advance();
        ast::DropBehavior::Restrict
    } else {
        ast::DropBehavior::Restrict // Default to RESTRICT per SQL:1999
    };

    Ok(DropTypeStmt {
        type_name,
        behavior,
    })
}

// ============================================================================
// COLLATION
// ============================================================================

/// Parse CREATE COLLATION statement
///
/// Syntax: CREATE COLLATION collation_name FROM 'locale' [NO PAD]
/// Minimal implementation: CREATE COLLATION collation_name
pub fn parse_create_collation(
    parser: &mut crate::Parser,
) -> Result<CreateCollationStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Collation)?;

    let collation_name = parser.parse_identifier()?;

    // For minimal stub: just accept the name, ignore FROM clause
    // TODO: Parse full syntax when implementing actual functionality
    parser.consume_until_semicolon_or_eof();

    Ok(CreateCollationStmt { collation_name })
}

/// Parse DROP COLLATION statement
///
/// Syntax: DROP COLLATION collation_name
pub fn parse_drop_collation(parser: &mut crate::Parser) -> Result<DropCollationStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Collation)?;

    let collation_name = parser.parse_identifier()?;

    Ok(DropCollationStmt { collation_name })
}

// ============================================================================
// CHARACTER SET
// ============================================================================

/// Parse CREATE CHARACTER SET statement
///
/// Syntax: CREATE CHARACTER SET charset_name [AS GET charset_source]
/// Minimal implementation: CREATE CHARACTER SET charset_name
pub fn parse_create_character_set(
    parser: &mut crate::Parser,
) -> Result<CreateCharacterSetStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Character)?;
    parser.expect_keyword(Keyword::Set)?;

    let charset_name = parser.parse_identifier()?;

    // For minimal stub: just accept the name
    // TODO: Parse full syntax when implementing actual functionality
    parser.consume_until_semicolon_or_eof();

    Ok(CreateCharacterSetStmt { charset_name })
}

/// Parse DROP CHARACTER SET statement
///
/// Syntax: DROP CHARACTER SET charset_name
pub fn parse_drop_character_set(
    parser: &mut crate::Parser,
) -> Result<DropCharacterSetStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Character)?;
    parser.expect_keyword(Keyword::Set)?;

    let charset_name = parser.parse_identifier()?;

    Ok(DropCharacterSetStmt { charset_name })
}

// ============================================================================
// TRANSLATION
// ============================================================================

/// Parse CREATE TRANSLATION statement
///
/// Syntax: CREATE TRANSLATION translation_name FROM charset1 TO charset2
/// Minimal implementation: CREATE TRANSLATION translation_name
pub fn parse_create_translation(
    parser: &mut crate::Parser,
) -> Result<CreateTranslationStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Translation)?;

    let translation_name = parser.parse_identifier()?;

    // For minimal stub: just accept the name, ignore FROM/TO clauses
    // TODO: Parse full syntax when implementing actual functionality
    parser.consume_until_semicolon_or_eof();

    Ok(CreateTranslationStmt { translation_name })
}

/// Parse DROP TRANSLATION statement
///
/// Syntax: DROP TRANSLATION translation_name
pub fn parse_drop_translation(
    parser: &mut crate::Parser,
) -> Result<DropTranslationStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Translation)?;

    let translation_name = parser.parse_identifier()?;

    Ok(DropTranslationStmt { translation_name })
}
