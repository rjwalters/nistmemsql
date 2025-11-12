//! Parser for CREATE INDEX, DROP INDEX, and REINDEX statements

use super::{ParseError, Parser};
use crate::{keywords::Keyword, token::Token};

impl Parser {
    /// Parse CREATE INDEX statement
    ///
    /// Syntax:
    ///   CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name ON table_name (column_list)
    pub(super) fn parse_create_index_statement(
        &mut self,
    ) -> Result<vibesql_ast::CreateIndexStmt, ParseError> {
        // Expect CREATE keyword
        self.expect_keyword(Keyword::Create)?;

        // Check for optional UNIQUE keyword
        let unique = if self.peek_keyword(Keyword::Unique) {
            self.advance(); // consume UNIQUE
            true
        } else {
            false
        };

        // Expect INDEX keyword
        self.expect_keyword(Keyword::Index)?;

        // Check for optional IF NOT EXISTS clause
        let if_not_exists = if self.peek_keyword(Keyword::If) {
            self.advance(); // consume IF
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse index name
        let index_name = self.parse_identifier()?;

        // Expect ON keyword
        self.expect_keyword(Keyword::On)?;

        // Parse table name
        let table_name = self.parse_identifier()?;

        // Expect opening parenthesis
        self.expect_token(Token::LParen)?;

        // Parse column list
        let mut columns = Vec::new();
        loop {
            // Parse column name
            let column_name = self.parse_identifier()?;

            // Check for optional ASC/DESC
            let direction = if self.peek_keyword(crate::keywords::Keyword::Asc) {
                self.advance(); // consume ASC
                vibesql_ast::OrderDirection::Asc
            } else if self.peek_keyword(crate::keywords::Keyword::Desc) {
                self.advance(); // consume DESC
                vibesql_ast::OrderDirection::Desc
            } else {
                vibesql_ast::OrderDirection::Asc // Default
            };

            columns.push(vibesql_ast::IndexColumn { column_name, direction });

            if self.peek() == &Token::Comma {
                self.advance(); // consume comma
            } else {
                break;
            }
        }

        // Expect closing parenthesis
        self.expect_token(Token::RParen)?;

        Ok(vibesql_ast::CreateIndexStmt { if_not_exists, index_name, table_name, unique, columns })
    }

    /// Parse DROP INDEX statement
    ///
    /// Syntax:
    ///   DROP INDEX [IF EXISTS] index_name
    pub(super) fn parse_drop_index_statement(&mut self) -> Result<vibesql_ast::DropIndexStmt, ParseError> {
        // Expect DROP keyword
        self.expect_keyword(Keyword::Drop)?;

        // Expect INDEX keyword
        self.expect_keyword(Keyword::Index)?;

        // Check for optional IF EXISTS clause
        let if_exists = if self.peek_keyword(Keyword::If) {
            self.advance(); // consume IF
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse index name
        let index_name = self.parse_identifier()?;

        Ok(vibesql_ast::DropIndexStmt { if_exists, index_name })
    }

    /// Parse REINDEX statement
    ///
    /// Syntax:
    ///   REINDEX [database_name | table_name | index_name]
    pub(super) fn parse_reindex_statement(&mut self) -> Result<vibesql_ast::ReindexStmt, ParseError> {
        // Expect REINDEX keyword
        self.expect_keyword(Keyword::Reindex)?;

        // Check for optional target (database, table, or index name)
        let target = if self.peek() == &Token::Semicolon || self.peek() == &Token::Eof {
            // No target specified - reindex all
            None
        } else {
            // Parse optional identifier (could be database, table, or index name)
            Some(self.parse_identifier()?)
        };

        Ok(vibesql_ast::ReindexStmt { target })
    }
}
