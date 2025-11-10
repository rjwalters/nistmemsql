//! Type DDL parsing (CREATE TYPE, DROP TYPE)

use crate::keywords::Keyword;
use crate::parser::ParseError;
use crate::token::Token;
use vibesql_ast::*;

/// Parse CREATE TYPE statement
///
/// Syntax:
///   CREATE TYPE typename AS (attr1 type1, attr2 type2, ...)  -- Structured type
///   CREATE TYPE typename AS DISTINCT base_type               -- Distinct type
pub fn parse_create_type(parser: &mut crate::Parser) -> Result<CreateTypeStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Type)?;

    let type_name = parser.parse_identifier()?;

    parser.expect_keyword(Keyword::As)?;

    // Check if this is a DISTINCT type or structured type
    let definition = if parser.peek_keyword(Keyword::Distinct) {
        parser.advance(); // consume DISTINCT
        let base_type = parser.parse_data_type()?;
        TypeDefinition::Distinct { base_type }
    } else {
        // Structured type: (attr1 type1, attr2 type2, ...)
        parser.expect_token(Token::LParen)?;

        let mut attributes = Vec::new();
        loop {
            let attr_name = parser.parse_identifier()?;
            let attr_type = parser.parse_data_type()?;
            attributes.push(TypeAttribute { name: attr_name, data_type: attr_type });

            if parser.peek() == &Token::Comma {
                parser.advance();
            } else {
                break;
            }
        }

        parser.expect_token(Token::RParen)?;
        TypeDefinition::Structured { attributes }
    };

    Ok(CreateTypeStmt { type_name, definition })
}

/// Parse DROP TYPE statement
///
/// Syntax:
///   DROP TYPE typename [CASCADE | RESTRICT]
pub fn parse_drop_type(parser: &mut crate::Parser) -> Result<DropTypeStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Type)?;

    let type_name = parser.parse_identifier()?;

    let behavior = if parser.peek_keyword(Keyword::Cascade) {
        parser.advance();
        DropBehavior::Cascade
    } else if parser.peek_keyword(Keyword::Restrict) {
        parser.advance();
        DropBehavior::Restrict
    } else {
        // RESTRICT is the default
        DropBehavior::Restrict
    };

    Ok(DropTypeStmt { type_name, behavior })
}
