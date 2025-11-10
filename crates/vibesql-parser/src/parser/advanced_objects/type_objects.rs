//! TYPE DDL parsing (SQL:1999)
//! Includes: CREATE/DROP TYPE

use vibesql_ast::*;

use crate::{keywords::Keyword, parser::ParseError, token::Token};

/// Parse CREATE TYPE statement
///
/// Syntax:
///   CREATE TYPE type_name;                              -- Forward declaration
///   CREATE TYPE type_name AS DISTINCT base_type         -- Distinct type
///   CREATE TYPE type_name AS (attr1 type1, attr2 type2, ...)  -- Structured type
pub fn parse_create_type(parser: &mut crate::Parser) -> Result<CreateTypeStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Type)?;

    let type_name = parser.parse_identifier()?;

    // Check if AS keyword is present
    let definition = if parser.try_consume_keyword(Keyword::As) {
        if parser.peek_keyword(Keyword::Distinct) {
            // DISTINCT type
            parser.advance(); // consume DISTINCT
            let base_type = parser.parse_data_type()?;
            vibesql_ast::TypeDefinition::Distinct { base_type }
        } else {
            // STRUCTURED type
            parser.expect_token(Token::LParen)?;

            let mut attributes = Vec::new();
            loop {
                let attr_name = parser.parse_identifier()?;
                let data_type = parser.parse_data_type()?;

                attributes.push(vibesql_ast::TypeAttribute { name: attr_name, data_type });

                if !parser.try_consume(&Token::Comma) {
                    break;
                }
            }

            parser.expect_token(Token::RParen)?;
            vibesql_ast::TypeDefinition::Structured { attributes }
        }
    } else {
        // Forward declaration without AS
        vibesql_ast::TypeDefinition::Forward
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
        vibesql_ast::DropBehavior::Cascade
    } else if parser.peek_keyword(Keyword::Restrict) {
        parser.advance();
        vibesql_ast::DropBehavior::Restrict
    } else {
        vibesql_ast::DropBehavior::Restrict // Default to RESTRICT per SQL:1999
    };

    Ok(DropTypeStmt { type_name, behavior })
}
