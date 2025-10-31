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
/// Syntax: CREATE SEQUENCE sequence_name
///         [START WITH n]
///         [INCREMENT BY n]
///         [MINVALUE n | NO MINVALUE]
///         [MAXVALUE n | NO MAXVALUE]
///         [CYCLE | NO CYCLE]
pub fn parse_create_sequence(
    parser: &mut crate::Parser,
) -> Result<CreateSequenceStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Sequence)?;

    let sequence_name = parser.parse_identifier()?;

    let mut start_with = None;
    let mut increment_by = 1; // default
    let mut min_value = None;
    let mut max_value = None;
    let mut cycle = false; // default

    // Parse optional clauses in any order
    loop {
        if parser.try_consume_keyword(Keyword::Start) {
            parser.expect_keyword(Keyword::With)?;
            let num_str = match parser.peek() {
                Token::Number(n) => n.clone(),
                _ => return Err(ParseError { message: "Expected number after START WITH".to_string() }),
            };
            parser.advance();
            start_with = Some(num_str.parse::<i64>().map_err(|_| ParseError {
                message: format!("Invalid START WITH value: {}", num_str),
            })?);
        } else if parser.try_consume_keyword(Keyword::Increment) {
            parser.expect_keyword(Keyword::By)?;
            let num_str = match parser.peek() {
                Token::Number(n) => n.clone(),
                _ => return Err(ParseError { message: "Expected number after INCREMENT BY".to_string() }),
            };
            parser.advance();
            increment_by = num_str.parse::<i64>().map_err(|_| ParseError {
                message: format!("Invalid INCREMENT BY value: {}", num_str),
            })?;
        } else if parser.try_consume_keyword(Keyword::Minvalue) {
            let num_str = match parser.peek() {
                Token::Number(n) => n.clone(),
                _ => return Err(ParseError { message: "Expected number after MINVALUE".to_string() }),
            };
            parser.advance();
            min_value = Some(num_str.parse::<i64>().map_err(|_| ParseError {
                message: format!("Invalid MINVALUE: {}", num_str),
            })?);
        } else if parser.try_consume_keyword(Keyword::Maxvalue) {
            let num_str = match parser.peek() {
                Token::Number(n) => n.clone(),
                _ => return Err(ParseError { message: "Expected number after MAXVALUE".to_string() }),
            };
            parser.advance();
            max_value = Some(num_str.parse::<i64>().map_err(|_| ParseError {
                message: format!("Invalid MAXVALUE: {}", num_str),
            })?);
        } else if parser.try_consume_keyword(Keyword::No) {
            if parser.try_consume_keyword(Keyword::Minvalue) {
                min_value = None;
            } else if parser.try_consume_keyword(Keyword::Maxvalue) {
                max_value = None;
            } else if parser.try_consume_keyword(Keyword::Cycle) {
                cycle = false;
            } else {
                return Err(ParseError {
                    message: "Expected MINVALUE, MAXVALUE, or CYCLE after NO".to_string(),
                });
            }
        } else if parser.try_consume_keyword(Keyword::Cycle) {
            cycle = true;
        } else {
            break;
        }
    }

    Ok(CreateSequenceStmt {
        sequence_name,
        start_with,
        increment_by,
        min_value,
        max_value,
        cycle,
    })
}

/// Parse DROP SEQUENCE statement
///
/// Syntax: DROP SEQUENCE sequence_name [CASCADE | RESTRICT]
pub fn parse_drop_sequence(parser: &mut crate::Parser) -> Result<DropSequenceStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Sequence)?;

    let sequence_name = parser.parse_identifier()?;

    let cascade = if parser.try_consume_keyword(Keyword::Cascade) {
        true
    } else if parser.try_consume_keyword(Keyword::Restrict) {
        false
    } else {
        false // default to RESTRICT
    };

    Ok(DropSequenceStmt {
        sequence_name,
        cascade,
    })
}

/// Parse ALTER SEQUENCE statement
///
/// Syntax: ALTER SEQUENCE sequence_name
///         [RESTART [WITH n]]
///         [INCREMENT BY n]
///         [MINVALUE n | NO MINVALUE]
///         [MAXVALUE n | NO MAXVALUE]
///         [CYCLE | NO CYCLE]
pub fn parse_alter_sequence(
    parser: &mut crate::Parser,
) -> Result<AlterSequenceStmt, ParseError> {
    parser.expect_keyword(Keyword::Alter)?;
    parser.expect_keyword(Keyword::Sequence)?;

    let sequence_name = parser.parse_identifier()?;

    let mut restart_with = None;
    let mut increment_by = None;
    let mut min_value = None;
    let mut max_value = None;
    let mut cycle = None;

    // Parse optional clauses in any order
    loop {
        if parser.try_consume_keyword(Keyword::Restart) {
            if parser.try_consume_keyword(Keyword::With) {
                let num_str = match parser.peek() {
                    Token::Number(n) => n.clone(),
                    _ => return Err(ParseError { message: "Expected number after RESTART WITH".to_string() }),
                };
                parser.advance();
                restart_with = Some(num_str.parse::<i64>().map_err(|_| ParseError {
                    message: format!("Invalid RESTART WITH value: {}", num_str),
                })?);
            } else {
                // RESTART without WITH means restart from original START WITH value
                restart_with = Some(1); // Will be handled by executor to use original start value
            }
        } else if parser.try_consume_keyword(Keyword::Increment) {
            parser.expect_keyword(Keyword::By)?;
            let num_str = match parser.peek() {
                Token::Number(n) => n.clone(),
                _ => return Err(ParseError { message: "Expected number after INCREMENT BY".to_string() }),
            };
            parser.advance();
            increment_by = Some(num_str.parse::<i64>().map_err(|_| ParseError {
                message: format!("Invalid INCREMENT BY value: {}", num_str),
            })?);
        } else if parser.try_consume_keyword(Keyword::Minvalue) {
            let num_str = match parser.peek() {
                Token::Number(n) => n.clone(),
                _ => return Err(ParseError { message: "Expected number after MINVALUE".to_string() }),
            };
            parser.advance();
            min_value = Some(Some(num_str.parse::<i64>().map_err(|_| ParseError {
                message: format!("Invalid MINVALUE: {}", num_str),
            })?));
        } else if parser.try_consume_keyword(Keyword::Maxvalue) {
            let num_str = match parser.peek() {
                Token::Number(n) => n.clone(),
                _ => return Err(ParseError { message: "Expected number after MAXVALUE".to_string() }),
            };
            parser.advance();
            max_value = Some(Some(num_str.parse::<i64>().map_err(|_| ParseError {
                message: format!("Invalid MAXVALUE: {}", num_str),
            })?));
        } else if parser.try_consume_keyword(Keyword::No) {
            if parser.try_consume_keyword(Keyword::Minvalue) {
                min_value = Some(None); // NO MINVALUE
            } else if parser.try_consume_keyword(Keyword::Maxvalue) {
                max_value = Some(None); // NO MAXVALUE
            } else if parser.try_consume_keyword(Keyword::Cycle) {
                cycle = Some(false);
            } else {
                return Err(ParseError {
                    message: "Expected MINVALUE, MAXVALUE, or CYCLE after NO".to_string(),
                });
            }
        } else if parser.try_consume_keyword(Keyword::Cycle) {
            cycle = Some(true);
        } else {
            break;
        }
    }

    Ok(AlterSequenceStmt {
        sequence_name,
        restart_with,
        increment_by,
        min_value,
        max_value,
        cycle,
    })
}

// ============================================================================
// TYPE
// ============================================================================

/// Parse CREATE TYPE statement
///
/// Syntax: CREATE TYPE type_name AS (field1 type1, field2 type2, ...)
/// Minimal implementation: CREATE TYPE type_name
pub fn parse_create_type(parser: &mut crate::Parser) -> Result<CreateTypeStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Type)?;

    let type_name = parser.parse_identifier()?;

    // For minimal stub: just accept the name, ignore AS clause
    // TODO: Parse full syntax when implementing actual functionality
    parser.consume_until_semicolon_or_eof();

    Ok(CreateTypeStmt { type_name })
}

/// Parse DROP TYPE statement
///
/// Syntax: DROP TYPE type_name
pub fn parse_drop_type(parser: &mut crate::Parser) -> Result<DropTypeStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Type)?;

    let type_name = parser.parse_identifier()?;

    Ok(DropTypeStmt { type_name })
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
