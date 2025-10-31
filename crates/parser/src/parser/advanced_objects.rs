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
pub fn parse_create_sequence(parser: &mut crate::Parser) -> Result<CreateSequenceStmt, ParseError> {
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
            let num_str = parser.parse_signed_number()?;
            start_with = Some(num_str.parse::<i64>().map_err(|_| ParseError {
                message: format!("Invalid START WITH value: {}", num_str),
            })?);
        } else if parser.try_consume_keyword(Keyword::Increment) {
            parser.expect_keyword(Keyword::By)?;
            let num_str = parser.parse_signed_number()?;
            increment_by = num_str.parse::<i64>().map_err(|_| ParseError {
                message: format!("Invalid INCREMENT BY value: {}", num_str),
            })?;
        } else if parser.try_consume_keyword(Keyword::Minvalue) {
            let num_str = parser.parse_signed_number()?;
            min_value =
                Some(num_str.parse::<i64>().map_err(|_| ParseError {
                    message: format!("Invalid MINVALUE: {}", num_str),
                })?);
        } else if parser.try_consume_keyword(Keyword::Maxvalue) {
            let num_str = parser.parse_signed_number()?;
            max_value =
                Some(num_str.parse::<i64>().map_err(|_| ParseError {
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

    Ok(CreateSequenceStmt { sequence_name, start_with, increment_by, min_value, max_value, cycle })
}

/// Parse DROP SEQUENCE statement
///
/// Syntax: DROP SEQUENCE sequence_name [CASCADE | RESTRICT]
pub fn parse_drop_sequence(parser: &mut crate::Parser) -> Result<DropSequenceStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Sequence)?;

    let sequence_name = parser.parse_identifier()?;

    // CASCADE or RESTRICT (defaults to RESTRICT if neither specified)
    let cascade = if parser.try_consume_keyword(Keyword::Cascade) {
        true
    } else {
        parser.try_consume_keyword(Keyword::Restrict); // consume if present
        false
    };

    Ok(DropSequenceStmt { sequence_name, cascade })
}

/// Parse ALTER SEQUENCE statement
///
/// Syntax: ALTER SEQUENCE sequence_name
///         [RESTART [WITH n]]
///         [INCREMENT BY n]
///         [MINVALUE n | NO MINVALUE]
///         [MAXVALUE n | NO MAXVALUE]
///         [CYCLE | NO CYCLE]
pub fn parse_alter_sequence(parser: &mut crate::Parser) -> Result<AlterSequenceStmt, ParseError> {
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
                    _ => {
                        return Err(ParseError {
                            message: "Expected number after RESTART WITH".to_string(),
                        })
                    }
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
                _ => {
                    return Err(ParseError {
                        message: "Expected number after INCREMENT BY".to_string(),
                    })
                }
            };
            parser.advance();
            increment_by = Some(num_str.parse::<i64>().map_err(|_| ParseError {
                message: format!("Invalid INCREMENT BY value: {}", num_str),
            })?);
        } else if parser.try_consume_keyword(Keyword::Minvalue) {
            let num_str = match parser.peek() {
                Token::Number(n) => n.clone(),
                _ => {
                    return Err(ParseError {
                        message: "Expected number after MINVALUE".to_string(),
                    })
                }
            };
            parser.advance();
            min_value =
                Some(Some(num_str.parse::<i64>().map_err(|_| ParseError {
                    message: format!("Invalid MINVALUE: {}", num_str),
                })?));
        } else if parser.try_consume_keyword(Keyword::Maxvalue) {
            let num_str = match parser.peek() {
                Token::Number(n) => n.clone(),
                _ => {
                    return Err(ParseError {
                        message: "Expected number after MAXVALUE".to_string(),
                    })
                }
            };
            parser.advance();
            max_value =
                Some(Some(num_str.parse::<i64>().map_err(|_| ParseError {
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

    Ok(AlterSequenceStmt { sequence_name, restart_with, increment_by, min_value, max_value, cycle })
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

            attributes.push(ast::TypeAttribute { name: attr_name, data_type });

            if !parser.try_consume(&Token::Comma) {
                break;
            }
        }

        parser.expect_token(Token::RParen)?;
        ast::TypeDefinition::Structured { attributes }
    };

    Ok(CreateTypeStmt { type_name, definition })
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

    Ok(DropTypeStmt { type_name, behavior })
}

// ============================================================================
// COLLATION
// ============================================================================

/// Parse CREATE COLLATION statement
///
/// SQL:1999 Syntax:
///   CREATE COLLATION collation_name
///     [FOR character_set]
///     [FROM source_collation]
///     [PAD SPACE | NO PAD]
pub fn parse_create_collation(
    parser: &mut crate::Parser,
) -> Result<CreateCollationStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Collation)?;

    let collation_name = parser.parse_identifier()?;

    // Optional: FOR character_set
    let character_set = if parser.try_consume_keyword(Keyword::For) {
        Some(parser.parse_identifier()?)
    } else {
        None
    };

    // Optional: FROM source_collation
    let source_collation = if parser.try_consume_keyword(Keyword::From) {
        Some(parser.parse_identifier()?)
    } else {
        None
    };

    // Optional: PAD SPACE | NO PAD
    let pad_space = if parser.try_consume_keyword(Keyword::Pad) {
        parser.expect_keyword(Keyword::Space)?;
        Some(true)
    } else if parser.try_consume_keyword(Keyword::No) {
        parser.expect_keyword(Keyword::Pad)?;
        Some(false)
    } else {
        None
    };

    Ok(CreateCollationStmt { collation_name, character_set, source_collation, pad_space })
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
/// SQL:1999 Syntax:
///   CREATE CHARACTER SET charset_name [AS]
///     [GET source]
///     [COLLATE FROM collation]
pub fn parse_create_character_set(
    parser: &mut crate::Parser,
) -> Result<CreateCharacterSetStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Character)?;
    parser.expect_keyword(Keyword::Set)?;

    let charset_name = parser.parse_identifier()?;

    // Optional: AS
    parser.try_consume_keyword(Keyword::As);

    // Optional: GET source
    let source = if parser.try_consume_keyword(Keyword::Get) {
        Some(parser.parse_identifier()?)
    } else {
        None
    };

    // Optional: COLLATE FROM collation
    let collation = if parser.try_consume_keyword(Keyword::Collate) {
        parser.expect_keyword(Keyword::From)?;
        Some(parser.parse_identifier()?)
    } else {
        None
    };

    Ok(CreateCharacterSetStmt { charset_name, source, collation })
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
/// SQL:1999 Syntax:
///   CREATE TRANSLATION translation_name
///     [FOR source_charset TO target_charset]
///     [FROM translation_source]
pub fn parse_create_translation(
    parser: &mut crate::Parser,
) -> Result<CreateTranslationStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Translation)?;

    let translation_name = parser.parse_identifier()?;

    // Optional: FOR source_charset
    let source_charset = if parser.try_consume_keyword(Keyword::For) {
        Some(parser.parse_identifier()?)
    } else {
        None
    };

    // Optional: TO target_charset (only if FOR was specified)
    let target_charset = if source_charset.is_some() && parser.try_consume_keyword(Keyword::To) {
        Some(parser.parse_identifier()?)
    } else {
        None
    };

    // Optional: FROM translation_source
    let translation_source = if parser.try_consume_keyword(Keyword::From) {
        Some(parser.parse_identifier()?)
    } else {
        None
    };

    Ok(CreateTranslationStmt {
        translation_name,
        source_charset,
        target_charset,
        translation_source,
    })
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

// ============================================================================
// ASSERTION
// ============================================================================

/// Parse CREATE ASSERTION statement
///
/// Syntax: CREATE ASSERTION assertion_name CHECK (condition)
pub fn parse_create_assertion(
    parser: &mut crate::Parser,
) -> Result<CreateAssertionStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Assertion)?;

    let assertion_name = parser.parse_identifier()?;

    // Parse CHECK keyword
    parser.expect_keyword(Keyword::Check)?;

    // Parse condition in parentheses
    parser.expect_token(Token::LParen)?;
    let check_condition = Box::new(parser.parse_expression()?);
    parser.expect_token(Token::RParen)?;

    Ok(CreateAssertionStmt { assertion_name, check_condition })
}

/// Parse DROP ASSERTION statement
///
/// Syntax: DROP ASSERTION assertion_name [CASCADE | RESTRICT]
pub fn parse_drop_assertion(
    parser: &mut crate::Parser,
) -> Result<DropAssertionStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Assertion)?;

    let assertion_name = parser.parse_identifier()?;

    // CASCADE or RESTRICT (defaults to RESTRICT if neither specified)
    let cascade = if parser.try_consume_keyword(Keyword::Cascade) {
        true
    } else {
        // Explicitly consume RESTRICT keyword if present (optional, defaults to RESTRICT)
        let _ = parser.try_consume_keyword(Keyword::Restrict);
        false // Default to RESTRICT per SQL standard
    };

    Ok(DropAssertionStmt { assertion_name, cascade })
}
