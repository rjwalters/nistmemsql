//! Domain DDL parsing

use vibesql_ast::*;

use crate::{keywords::Keyword, parser::ParseError, token::Token};

/// Parse CREATE DOMAIN statement
///
/// Syntax: CREATE DOMAIN domain_name AS data_type [DEFAULT expr] [CHECK (condition)]
pub fn parse_create_domain(parser: &mut crate::Parser) -> Result<CreateDomainStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Domain)?;

    let domain_name = parser.parse_identifier()?;

    // Expect AS keyword
    parser.expect_keyword(Keyword::As)?;

    // Parse data type
    let data_type = parser.parse_data_type()?;

    // Parse optional DEFAULT clause
    let default = if parser.peek_keyword(Keyword::Default) {
        parser.advance(); // consume DEFAULT
        Some(Box::new(parser.parse_expression()?))
    } else {
        None
    };

    // Parse CHECK constraints (can have multiple)
    let mut constraints = Vec::new();
    while parser.peek_keyword(Keyword::Check) {
        parser.advance(); // consume CHECK

        // Parse optional constraint name (if preceded by CONSTRAINT keyword)
        // This is handled by checking if we saw CONSTRAINT before CHECK
        // For now, we'll not support named constraints in this simplified version
        // (We can add that later if needed)

        // Expect opening parenthesis
        parser.expect_token(Token::LParen)?;

        // Parse the CHECK expression
        let check_expr = Box::new(parser.parse_expression()?);

        // Expect closing parenthesis
        parser.expect_token(Token::RParen)?;

        constraints.push(DomainConstraint { name: None, check: check_expr });
    }

    Ok(CreateDomainStmt { domain_name, data_type, default, constraints })
}

/// Parse DROP DOMAIN statement
///
/// Syntax: DROP DOMAIN domain_name [CASCADE | RESTRICT]
pub fn parse_drop_domain(parser: &mut crate::Parser) -> Result<DropDomainStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Domain)?;

    let domain_name = parser.parse_identifier()?;

    // Parse CASCADE or RESTRICT (default is RESTRICT)
    let cascade = if parser.peek_keyword(Keyword::Cascade) {
        parser.advance();
        true
    } else if parser.peek_keyword(Keyword::Restrict) {
        parser.advance();
        false
    } else {
        // RESTRICT is the default
        false
    };

    Ok(DropDomainStmt { domain_name, cascade })
}
