//! Role DDL parsing

use crate::keywords::Keyword;
use crate::parser::ParseError;
use ast::*;

/// Parse CREATE ROLE statement
///
/// Syntax: CREATE ROLE role_name
pub fn parse_create_role(parser: &mut crate::Parser) -> Result<CreateRoleStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Role)?;

    let role_name = parser.parse_identifier()?;

    Ok(CreateRoleStmt { role_name })
}

/// Parse DROP ROLE statement
///
/// Syntax: DROP ROLE role_name
pub fn parse_drop_role(parser: &mut crate::Parser) -> Result<DropRoleStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Role)?;

    let role_name = parser.parse_identifier()?;

    Ok(DropRoleStmt { role_name })
}
