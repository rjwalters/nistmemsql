//! GRANT statement parsing

use crate::keywords::Keyword;
use crate::parser::ParseError;
use crate::token::Token;
use ast::*;

/// Parse GRANT statement
///
/// Supports all SQL:1999 Feature F031 object types
///
/// Grammar:
/// ```text
/// GRANT privilege_list ON [object_type] object_name [FOR type_name] TO grantee_list [WITH GRANT OPTION]
///
/// object_type ::= TABLE | SCHEMA | DOMAIN | COLLATION | CHARACTER SET | TRANSLATION | TYPE | SEQUENCE
///               | FUNCTION | PROCEDURE | ROUTINE | METHOD | CONSTRUCTOR METHOD | STATIC METHOD | INSTANCE METHOD
///               | SPECIFIC FUNCTION | SPECIFIC PROCEDURE | SPECIFIC ROUTINE
/// ```
pub fn parse_grant(parser: &mut crate::Parser) -> Result<GrantStmt, ParseError> {
    parser.expect_keyword(Keyword::Grant)?;

    // Parse comma-separated privilege list
    let privileges = parse_privilege_list(parser)?;

    parser.expect_keyword(Keyword::On)?;

    // Parse object type (TABLE, SCHEMA, FUNCTION, PROCEDURE, etc.)
    let object_type = if parser.peek() == &Token::Keyword(Keyword::Specific) {
        parser.advance(); // consume SPECIFIC
        // SPECIFIC FUNCTION | SPECIFIC PROCEDURE | SPECIFIC ROUTINE
        if parser.peek() == &Token::Keyword(Keyword::Function) {
            parser.advance();
            ObjectType::SpecificFunction
        } else if parser.peek() == &Token::Keyword(Keyword::Procedure) {
            parser.advance();
            ObjectType::SpecificProcedure
        } else if parser.peek() == &Token::Keyword(Keyword::Routine) {
            parser.advance();
            ObjectType::SpecificRoutine
        } else {
            return Err(ParseError {
                message: format!(
                    "Expected FUNCTION, PROCEDURE, or ROUTINE after SPECIFIC, found {:?}",
                    parser.peek()
                ),
            });
        }
    } else if parser.peek() == &Token::Keyword(Keyword::Table) {
        parser.advance();
        ObjectType::Table
    } else if parser.peek() == &Token::Keyword(Keyword::Schema) {
        parser.advance();
        ObjectType::Schema
    } else if parser.peek() == &Token::Keyword(Keyword::Domain) {
        parser.advance();
        ObjectType::Domain
    } else if parser.peek() == &Token::Keyword(Keyword::Collation) {
        parser.advance();
        ObjectType::Collation
    } else if parser.peek() == &Token::Keyword(Keyword::Character) {
        parser.advance();
        // CHARACTER SET
        parser.expect_keyword(Keyword::Set)?;
        ObjectType::CharacterSet
    } else if parser.peek() == &Token::Keyword(Keyword::Translation) {
        parser.advance();
        ObjectType::Translation
    } else if parser.peek() == &Token::Keyword(Keyword::Type) {
        parser.advance();
        ObjectType::Type
    } else if parser.peek() == &Token::Keyword(Keyword::Sequence) {
        parser.advance();
        ObjectType::Sequence
    } else if parser.peek() == &Token::Keyword(Keyword::Function) {
        parser.advance();
        ObjectType::Function
    } else if parser.peek() == &Token::Keyword(Keyword::Procedure) {
        parser.advance();
        ObjectType::Procedure
    } else if parser.peek() == &Token::Keyword(Keyword::Routine) {
        parser.advance();
        ObjectType::Routine
    } else if parser.peek() == &Token::Keyword(Keyword::Constructor) {
        parser.advance();
        parser.expect_keyword(Keyword::Method)?;
        ObjectType::ConstructorMethod
    } else if parser.peek() == &Token::Keyword(Keyword::Static) {
        parser.advance();
        parser.expect_keyword(Keyword::Method)?;
        ObjectType::StaticMethod
    } else if parser.peek() == &Token::Keyword(Keyword::Instance) {
        parser.advance();
        parser.expect_keyword(Keyword::Method)?;
        ObjectType::InstanceMethod
    } else if parser.peek() == &Token::Keyword(Keyword::Method) {
        parser.advance();
        ObjectType::Method
    } else {
        // When no object type is specified, infer from privilege type
        // USAGE privilege defaults to Schema (SQL:1999 E081-09)
        // EXECUTE privilege defaults to Routine (SQL:1999 P001)
        // Other privileges default to Table (SQL standard behavior)
        let has_usage = privileges.iter().any(|p| matches!(p, PrivilegeType::Usage));
        let has_execute = privileges.iter().any(|p| matches!(p, PrivilegeType::Execute));

        if has_usage {
            ObjectType::Schema
        } else if has_execute {
            ObjectType::Routine
        } else {
            ObjectType::Table
        }
    };

    // Parse object name (supports qualified names like "schema.table")
    let object_name = parser.parse_qualified_identifier()?;

    // Parse optional FOR type_name (for methods and routines on user-defined types)
    let for_type_name = if parser.peek() == &Token::Keyword(Keyword::For) {
        parser.advance(); // consume FOR
        Some(parser.parse_qualified_identifier()?)
    } else {
        None
    };

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
        object_type,
        object_name,
        for_type_name,
        grantees,
        with_grant_option,
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
                // Check for optional column list (SQL:1999 Feature F031-03)
                let columns = parse_optional_column_list(parser)?;
                PrivilegeType::Select(columns)
            }
            Token::Keyword(Keyword::Insert) => {
                parser.advance();
                // Check for optional column list (SQL:1999 Feature F031-03)
                let columns = parse_optional_column_list(parser)?;
                PrivilegeType::Insert(columns)
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
