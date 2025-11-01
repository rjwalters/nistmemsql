//! Parser for CREATE INDEX and DROP INDEX statements

use super::{ParseError, Parser};
use crate::keywords::Keyword;
use crate::token::Token;

impl Parser {
    /// Parse CREATE INDEX statement
    ///
    /// Syntax:
    ///   CREATE [UNIQUE] INDEX index_name ON table_name (column_list)
    pub(super) fn parse_create_index_statement(
        &mut self,
    ) -> Result<ast::CreateIndexStmt, ParseError> {
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
                ast::OrderDirection::Asc
            } else if self.peek_keyword(crate::keywords::Keyword::Desc) {
                self.advance(); // consume DESC
                ast::OrderDirection::Desc
            } else {
                ast::OrderDirection::Asc // Default
            };

            columns.push(ast::IndexColumn { column_name, direction });

            if self.peek() == &Token::Comma {
                self.advance(); // consume comma
            } else {
                break;
            }
        }

        // Expect closing parenthesis
        self.expect_token(Token::RParen)?;

        Ok(ast::CreateIndexStmt { index_name, table_name, unique, columns })
    }

    /// Parse DROP INDEX statement
    ///
    /// Syntax:
    ///   DROP INDEX index_name
    pub(super) fn parse_drop_index_statement(&mut self) -> Result<ast::DropIndexStmt, ParseError> {
        // Expect DROP keyword
        self.expect_keyword(Keyword::Drop)?;

        // Expect INDEX keyword
        self.expect_keyword(Keyword::Index)?;

        // Parse index name
        let index_name = self.parse_identifier()?;

        Ok(ast::DropIndexStmt { index_name })
    }
}
