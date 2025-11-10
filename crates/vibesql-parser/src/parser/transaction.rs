//! Transaction control statement parsing (BEGIN, COMMIT, ROLLBACK)

use crate::{keywords::Keyword, parser::ParseError};

/// Parse BEGIN [TRANSACTION] or START TRANSACTION statement
pub(super) fn parse_begin_statement(
    parser: &mut super::Parser,
) -> Result<vibesql_ast::BeginStmt, ParseError> {
    // Consume BEGIN or START
    if parser.peek_keyword(Keyword::Begin) {
        parser.consume_keyword(Keyword::Begin)?;
    } else if parser.peek_keyword(Keyword::Start) {
        parser.consume_keyword(Keyword::Start)?;
    } else {
        return Err(ParseError { message: "Expected BEGIN or START".to_string() });
    }

    // Optional TRANSACTION keyword
    if parser.peek_keyword(Keyword::Transaction) {
        parser.consume_keyword(Keyword::Transaction)?;
    }

    Ok(vibesql_ast::BeginStmt)
}

/// Parse COMMIT statement
pub(super) fn parse_commit_statement(
    parser: &mut super::Parser,
) -> Result<vibesql_ast::CommitStmt, ParseError> {
    // Consume COMMIT
    parser.consume_keyword(Keyword::Commit)?;

    Ok(vibesql_ast::CommitStmt)
}

/// Parse ROLLBACK statement
pub(super) fn parse_rollback_statement(
    parser: &mut super::Parser,
) -> Result<vibesql_ast::RollbackStmt, ParseError> {
    // Consume ROLLBACK
    parser.consume_keyword(Keyword::Rollback)?;

    Ok(vibesql_ast::RollbackStmt)
}

/// Parse SAVEPOINT statement
pub(super) fn parse_savepoint_statement(
    parser: &mut super::Parser,
) -> Result<vibesql_ast::SavepointStmt, ParseError> {
    // Consume SAVEPOINT
    parser.consume_keyword(Keyword::Savepoint)?;

    // Parse savepoint name (identifier)
    let name = parser.parse_identifier()?;

    Ok(vibesql_ast::SavepointStmt { name })
}

/// Parse ROLLBACK TO SAVEPOINT statement
pub(super) fn parse_rollback_to_savepoint_statement(
    parser: &mut super::Parser,
) -> Result<vibesql_ast::RollbackToSavepointStmt, ParseError> {
    // Consume ROLLBACK
    parser.consume_keyword(Keyword::Rollback)?;

    // Consume TO
    parser.consume_keyword(Keyword::To)?;

    // Consume SAVEPOINT
    parser.consume_keyword(Keyword::Savepoint)?;

    // Parse savepoint name (identifier)
    let name = parser.parse_identifier()?;

    Ok(vibesql_ast::RollbackToSavepointStmt { name })
}

/// Parse RELEASE SAVEPOINT statement
pub(super) fn parse_release_savepoint_statement(
    parser: &mut super::Parser,
) -> Result<vibesql_ast::ReleaseSavepointStmt, ParseError> {
    // Consume RELEASE
    parser.consume_keyword(Keyword::Release)?;

    // Consume SAVEPOINT
    parser.consume_keyword(Keyword::Savepoint)?;

    // Parse savepoint name (identifier)
    let name = parser.parse_identifier()?;

    Ok(vibesql_ast::ReleaseSavepointStmt { name })
}
