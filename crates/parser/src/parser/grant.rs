//! GRANT statement parsing

use crate::keywords::Keyword;
use crate::parser::ParseError;
use ast::*;

/// Parse GRANT statement
///
/// Phase 2.1: Only supports GRANT SELECT ON TABLE table_name TO role_name
///
/// Grammar:
/// ```text
/// GRANT SELECT ON TABLE table_name TO grantee
/// ```
pub fn parse_grant(parser: &mut crate::Parser) -> Result<GrantStmt, ParseError> {
    parser.expect_keyword(Keyword::Grant)?;

    // For Phase 2.1: Only handle SELECT
    parser.expect_keyword(Keyword::Select)?;
    let privileges = vec![PrivilegeType::Select];

    parser.expect_keyword(Keyword::On)?;
    parser.expect_keyword(Keyword::Table)?;

    // Parse object name (supports qualified names like "schema.table")
    let object_name = parser.parse_qualified_identifier()?;

    parser.expect_keyword(Keyword::To)?;
    let grantee = parser.parse_identifier()?;

    Ok(GrantStmt {
        privileges,
        object_type: ObjectType::Table,
        object_name,
        grantees: vec![grantee],
        with_grant_option: false,
    })
}
