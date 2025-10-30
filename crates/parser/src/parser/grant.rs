//! GRANT statement parsing

use crate::keywords::Keyword;
use crate::parser::ParseError;
use crate::token::Token;
use ast::*;

/// Parse GRANT statement
///
/// Phase 2.4: Supports WITH GRANT OPTION
///
/// Grammar:
/// ```text
/// GRANT privilege_list ON TABLE table_name TO grantee_list [WITH GRANT OPTION]
/// ```
pub fn parse_grant(parser: &mut crate::Parser) -> Result<GrantStmt, ParseError> {
    parser.expect_keyword(Keyword::Grant)?;

    // Parse comma-separated privilege list
    let privileges = parse_privilege_list(parser)?;

    parser.expect_keyword(Keyword::On)?;
    parser.expect_keyword(Keyword::Table)?;

    // Parse object name (supports qualified names like "schema.table")
    let object_name = parser.parse_qualified_identifier()?;

    parser.expect_keyword(Keyword::To)?;

    // Parse comma-separated grantee list
    let grantees = parse_identifier_list(parser)?;

    // Parse optional WITH GRANT OPTION clause
    let with_grant_option = if parser.peek() == &Token::Keyword(Keyword::With) {
        parser.advance(); // consume WITH
        parser.expect_keyword(Keyword::Grant)?;
        parser.expect_keyword(Keyword::Option)?;
        true
    } else {
        false
    };

    Ok(GrantStmt {
        privileges,
        object_type: ObjectType::Table,
        object_name,
        grantees,
        with_grant_option,
    })
}

/// Parse a comma-separated list of privileges
///
/// Supports: SELECT, INSERT, UPDATE, DELETE, ALL [PRIVILEGES]
fn parse_privilege_list(parser: &mut crate::Parser) -> Result<Vec<PrivilegeType>, ParseError> {
    // Check for ALL [PRIVILEGES] syntax
    if parser.peek() == &Token::Keyword(Keyword::All) {
        parser.advance(); // consume ALL

        // Optional PRIVILEGES keyword
        if parser.peek() == &Token::Keyword(Keyword::Privileges) {
            parser.advance(); // consume PRIVILEGES
        }

        return Ok(vec![PrivilegeType::AllPrivileges]);
    }

    // Otherwise parse specific privilege list
    let mut privileges = vec![];

    loop {
        let priv_type = match parser.peek() {
            Token::Keyword(Keyword::Select) => {
                parser.advance();
                PrivilegeType::Select
            }
            Token::Keyword(Keyword::Insert) => {
                parser.advance();
                PrivilegeType::Insert
            }
            Token::Keyword(Keyword::Update) => {
                parser.advance();
                PrivilegeType::Update
            }
            Token::Keyword(Keyword::Delete) => {
                parser.advance();
                PrivilegeType::Delete
            }
            _ => {
                return Err(ParseError {
                    message: format!(
                        "Expected privilege keyword (SELECT, INSERT, UPDATE, DELETE, ALL), found {:?}",
                        parser.peek()
                    ),
                })
            }
        };

        privileges.push(priv_type);

        // Check for comma indicating more privileges
        if parser.peek() == &Token::Comma {
            parser.advance(); // consume comma
        } else {
            break;
        }
    }

    Ok(privileges)
}

/// Parse a comma-separated list of identifiers
fn parse_identifier_list(parser: &mut crate::Parser) -> Result<Vec<String>, ParseError> {
    let mut identifiers = vec![];

    loop {
        identifiers.push(parser.parse_identifier()?);

        // Check for comma indicating more identifiers
        if parser.peek() == &Token::Comma {
            parser.advance(); // consume comma
        } else {
            break;
        }
    }

    Ok(identifiers)
}
