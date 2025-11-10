//! ASSERTION DDL parsing (SQL:1999)
//! Includes: CREATE/DROP ASSERTION

use vibesql_ast::*;

use crate::{keywords::Keyword, parser::ParseError, token::Token};

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
pub fn parse_drop_assertion(parser: &mut crate::Parser) -> Result<DropAssertionStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Assertion)?;

    let assertion_name = parser.parse_identifier()?;

    // CASCADE or RESTRICT (defaults to RESTRICT if neither specified)
    let cascade = if parser.try_consume_keyword(Keyword::Cascade) {
        true
    } else {
        parser.try_consume_keyword(Keyword::Restrict); // Consume RESTRICT if present
        false // Default to RESTRICT per SQL standard
    };

    Ok(DropAssertionStmt { assertion_name, cascade })
}
