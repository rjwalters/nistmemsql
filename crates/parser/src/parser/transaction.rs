//! Transaction control statement parsing (BEGIN, COMMIT, ROLLBACK)

use crate::keywords::Keyword;
use crate::parser::ParseError;

/// Parse BEGIN [TRANSACTION] or START TRANSACTION statement
pub(super) fn parse_begin_statement(
    parser: &mut super::Parser,
) -> Result<ast::BeginStmt, ParseError> {
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

    Ok(ast::BeginStmt)
}

/// Parse COMMIT statement
pub(super) fn parse_commit_statement(
    parser: &mut super::Parser,
) -> Result<ast::CommitStmt, ParseError> {
    // Consume COMMIT
    parser.consume_keyword(Keyword::Commit)?;

    Ok(ast::CommitStmt)
}

/// Parse ROLLBACK statement
pub(super) fn parse_rollback_statement(
    parser: &mut super::Parser,
) -> Result<ast::RollbackStmt, ParseError> {
    // Consume ROLLBACK
    parser.consume_keyword(Keyword::Rollback)?;

    Ok(ast::RollbackStmt)
}

/// Parse SAVEPOINT statement
pub(super) fn parse_savepoint_statement(
    parser: &mut super::Parser,
) -> Result<ast::SavepointStmt, ParseError> {
    // Consume SAVEPOINT
    parser.consume_keyword(Keyword::Savepoint)?;

    // Parse savepoint name (identifier)
    let name = parser.parse_identifier()?;

    Ok(ast::SavepointStmt { name })
}

/// Parse ROLLBACK TO SAVEPOINT statement
pub(super) fn parse_rollback_to_savepoint_statement(
    parser: &mut super::Parser,
) -> Result<ast::RollbackToSavepointStmt, ParseError> {
    // Consume ROLLBACK
    parser.consume_keyword(Keyword::Rollback)?;

    // Consume TO
    parser.consume_keyword(Keyword::To)?;

    // Consume SAVEPOINT
    parser.consume_keyword(Keyword::Savepoint)?;

    // Parse savepoint name (identifier)
    let name = parser.parse_identifier()?;

    Ok(ast::RollbackToSavepointStmt { name })
}

/// Parse RELEASE SAVEPOINT statement
pub(super) fn parse_release_savepoint_statement(
    parser: &mut super::Parser,
) -> Result<ast::ReleaseSavepointStmt, ParseError> {
    // Consume RELEASE
    parser.consume_keyword(Keyword::Release)?;

    // Consume SAVEPOINT
    parser.consume_keyword(Keyword::Savepoint)?;

    // Parse savepoint name (identifier)
    let name = parser.parse_identifier()?;

    Ok(ast::ReleaseSavepointStmt { name })
}
