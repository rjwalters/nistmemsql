//! GRANT statement parsing

use crate::keywords::Keyword;
use crate::parser::ParseError;
use crate::token::Token;
use ast::*;

/// Parse GRANT statement
///
/// Supports TABLE, SCHEMA, FUNCTION, PROCEDURE, ROUTINE, and METHOD object types
///
/// Grammar:
/// ```text
/// GRANT privilege_list ON [TABLE | SCHEMA | FUNCTION | PROCEDURE | ROUTINE | METHOD | CONSTRUCTOR METHOD | STATIC METHOD | INSTANCE METHOD] object_name TO grantee_list [WITH GRANT OPTION]
/// ```
pub fn parse_grant(parser: &mut crate::Parser) -> Result<GrantStmt, ParseError> {
    parser.expect_keyword(Keyword::Grant)?;

    // Parse comma-separated privilege list
    let privileges = parse_privilege_list(parser)?;

    parser.expect_keyword(Keyword::On)?;

    // Parse object type (TABLE, SCHEMA, FUNCTION, PROCEDURE, etc.)
    let object_type = if parser.peek() == &Token::Keyword(Keyword::Table) {
        parser.advance(); // consume TABLE
        ObjectType::Table
    } else if parser.peek() == &Token::Keyword(Keyword::Schema) {
        parser.advance(); // consume SCHEMA
        ObjectType::Schema
    } else if parser.peek() == &Token::Keyword(Keyword::Function) {
        parser.advance(); // consume FUNCTION
        ObjectType::Function
    } else if parser.peek() == &Token::Keyword(Keyword::Procedure) {
        parser.advance(); // consume PROCEDURE
        ObjectType::Procedure
    } else if parser.peek() == &Token::Keyword(Keyword::Routine) {
        parser.advance(); // consume ROUTINE
        ObjectType::Routine
    } else if parser.peek() == &Token::Keyword(Keyword::Constructor) {
        parser.advance(); // consume CONSTRUCTOR
        parser.expect_keyword(Keyword::Method)?; // expect METHOD after CONSTRUCTOR
        ObjectType::ConstructorMethod
    } else if parser.peek() == &Token::Keyword(Keyword::Static) {
        parser.advance(); // consume STATIC
        parser.expect_keyword(Keyword::Method)?; // expect METHOD after STATIC
        ObjectType::StaticMethod
    } else if parser.peek() == &Token::Keyword(Keyword::Instance) {
        parser.advance(); // consume INSTANCE
        parser.expect_keyword(Keyword::Method)?; // expect METHOD after INSTANCE
        ObjectType::InstanceMethod
    } else if parser.peek() == &Token::Keyword(Keyword::Method) {
        parser.advance(); // consume METHOD
        ObjectType::Method
    } else {
        // When no object type is specified, infer from privilege type
        // USAGE privilege defaults to Schema (SQL:1999 E081-09)
        // EXECUTE privilege defaults to Routine (SQL:1999 P001)
        // Other privileges default to Table (SQL standard behavior)
        if privileges.contains(&PrivilegeType::Usage) {
            ObjectType::Schema
        } else if privileges.contains(&PrivilegeType::Execute) {
            ObjectType::Routine
        } else {
            ObjectType::Table
        }
    };

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

    Ok(GrantStmt { privileges, object_type, object_name, grantees, with_grant_option })
}

/// Parse a comma-separated list of privileges
///
/// Supports: SELECT, INSERT, UPDATE, DELETE, USAGE, CREATE, ALL [PRIVILEGES]
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
                // Check for optional column list
                let columns = parse_optional_column_list(parser)?;
                PrivilegeType::Update(columns)
            }
            Token::Keyword(Keyword::Delete) => {
                parser.advance();
                PrivilegeType::Delete
            }
            Token::Keyword(Keyword::References) => {
                parser.advance();
                // Check for optional column list
                let columns = parse_optional_column_list(parser)?;
                PrivilegeType::References(columns)
            }
            Token::Keyword(Keyword::Usage) => {
                parser.advance();
                PrivilegeType::Usage
            }
            Token::Keyword(Keyword::Create) => {
                parser.advance();
                PrivilegeType::Create
            }
            Token::Keyword(Keyword::Execute) => {
                parser.advance();
                PrivilegeType::Execute
            }
            _ => {
                return Err(ParseError {
                    message: format!(
                        "Expected privilege keyword (SELECT, INSERT, UPDATE, DELETE, REFERENCES, USAGE, CREATE, EXECUTE, ALL), found {:?}",
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

/// Parse optional column list for UPDATE/REFERENCES privileges
///
/// If next token is '(', parses column list and returns Some(vec).
/// Otherwise returns None for table-level privilege.
fn parse_optional_column_list(
    parser: &mut crate::Parser,
) -> Result<Option<Vec<String>>, ParseError> {
    if parser.peek() == &Token::LParen {
        parser.advance(); // consume '('

        // Parse comma-separated column list
        let columns = parse_identifier_list(parser)?;

        // Expect closing ')'
        if parser.peek() != &Token::RParen {
            return Err(ParseError {
                message: format!("Expected ')' after column list, found {:?}", parser.peek()),
            });
        }
        parser.advance(); // consume ')'

        Ok(Some(columns))
    } else {
        // No column list - table-level privilege
        Ok(None)
    }
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
