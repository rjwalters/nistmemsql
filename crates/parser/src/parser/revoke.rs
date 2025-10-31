//! REVOKE statement parsing

use crate::keywords::Keyword;
use crate::parser::ParseError;
use crate::token::Token;
use ast::*;

/// Parse REVOKE statement
///
/// Phase 3: Removes privileges from roles/users with CASCADE/RESTRICT support
///
/// Grammar:
/// ```text
/// REVOKE [GRANT OPTION FOR] privilege_list ON [TABLE | SCHEMA] object_name
/// FROM grantee_list [GRANTED BY grantor] [CASCADE | RESTRICT]
/// ```
pub fn parse_revoke(parser: &mut crate::Parser) -> Result<RevokeStmt, ParseError> {
    parser.expect_keyword(Keyword::Revoke)?;

    // Check for GRANT OPTION FOR
    let grant_option_for = if parser.peek() == &Token::Keyword(Keyword::Grant) {
        parser.advance(); // consume GRANT
        parser.expect_keyword(Keyword::Option)?;
        parser.expect_keyword(Keyword::For)?;
        true
    } else {
        false
    };

    // Parse privilege list (reuse from GRANT parser)
    let privileges = parse_privilege_list(parser)?;

    parser.expect_keyword(Keyword::On)?;

    // Detect TABLE vs SCHEMA (defaults to TABLE if not specified)
    let object_type = if parser.peek() == &Token::Keyword(Keyword::Table) {
        parser.advance(); // consume TABLE
        ObjectType::Table
    } else if parser.peek() == &Token::Keyword(Keyword::Schema) {
        parser.advance(); // consume SCHEMA
        ObjectType::Schema
    } else {
        // Default to TABLE if not specified (SQL standard behavior)
        ObjectType::Table
    };

    // Parse object name (supports qualified names like "schema.table")
    let object_name = parser.parse_qualified_identifier()?;

    parser.expect_keyword(Keyword::From)?;

    // Parse comma-separated grantee list
    let grantees = parse_identifier_list(parser)?;

    // Parse optional GRANTED BY clause
    let granted_by = if parser.peek() == &Token::Keyword(Keyword::Granted) {
        parser.advance(); // consume GRANTED
        parser.expect_keyword(Keyword::By)?;
        Some(parser.parse_identifier()?)
    } else {
        None
    };

    // Parse optional CASCADE/RESTRICT
    let cascade_option = if parser.peek() == &Token::Keyword(Keyword::Cascade) {
        parser.advance(); // consume CASCADE
        CascadeOption::Cascade
    } else if parser.peek() == &Token::Keyword(Keyword::Restrict) {
        parser.advance(); // consume RESTRICT
        CascadeOption::Restrict
    } else {
        CascadeOption::None
    };

    Ok(RevokeStmt {
        grant_option_for,
        privileges,
        object_type,
        object_name,
        grantees,
        granted_by,
        cascade_option,
    })
}

/// Parse a comma-separated list of privileges
///
/// Supports: SELECT, INSERT, UPDATE, DELETE, USAGE, CREATE, EXECUTE, TRIGGER, UNDER, ALL [PRIVILEGES]
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
            Token::Keyword(Keyword::Usage) => {
                parser.advance();
                PrivilegeType::Usage
            }
            Token::Keyword(Keyword::Create) => {
                parser.advance();
                PrivilegeType::Create
            }
            Token::Keyword(Keyword::References) => {
                parser.advance();
                PrivilegeType::References
            }
            Token::Keyword(Keyword::Execute) => {
                parser.advance();
                PrivilegeType::Execute
            }
            Token::Keyword(Keyword::Trigger) => {
                parser.advance();
                PrivilegeType::Trigger
            }
            Token::Keyword(Keyword::Under) => {
                parser.advance();
                PrivilegeType::Under
            }
            _ => {
                return Err(ParseError {
                    message: format!(
                        "Expected privilege keyword (SELECT, INSERT, UPDATE, DELETE, REFERENCES, USAGE, CREATE, EXECUTE, TRIGGER, UNDER, ALL), found {:?}",
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
