//! CREATE VIEW and DROP VIEW statement parsers

use super::{ParseError, Parser};
use crate::keywords::Keyword;
use crate::token::Token;

impl Parser {
    /// Parse CREATE VIEW statement
    ///
    /// Syntax:
    ///   CREATE VIEW view_name [(column_list)] AS select_statement [WITH CHECK OPTION]
    pub(super) fn parse_create_view_statement(&mut self) -> Result<ast::CreateViewStmt, ParseError> {
        // Expect CREATE keyword
        self.expect_keyword(Keyword::Create)?;

        // Expect VIEW keyword
        self.expect_keyword(Keyword::View)?;

        // Parse view name (supports schema.view)
        let view_name = self.parse_qualified_identifier()?;

        // Check for optional column list
        let columns = if matches!(self.peek(), Token::LParen) {
            self.advance(); // consume '('
            let mut cols = Vec::new();

            loop {
                match self.peek() {
                    Token::Identifier(name) | Token::DelimitedIdentifier(name) => {
                        cols.push(name.clone());
                        self.advance();
                    }
                    _ => return Err(ParseError {
                        message: "Expected column name in view column list".to_string(),
                    }),
                }

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }

            self.expect_token(Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        // Expect AS keyword
        self.expect_keyword(Keyword::As)?;

        // Parse the SELECT statement
        let query = Box::new(self.parse_select_statement()?);

        // Check for optional WITH CHECK OPTION
        let with_check_option = if self.peek_keyword(Keyword::With) {
            self.consume_keyword(Keyword::With)?;
            self.expect_keyword(Keyword::Check)?;
            self.expect_keyword(Keyword::Option)?;
            true
        } else {
            false
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(ast::CreateViewStmt {
            view_name,
            columns,
            query,
            with_check_option,
        })
    }

    /// Parse DROP VIEW statement
    ///
    /// Syntax:
    ///   DROP VIEW [IF EXISTS] view_name [CASCADE | RESTRICT]
    pub(super) fn parse_drop_view_statement(&mut self) -> Result<ast::DropViewStmt, ParseError> {
        // Expect DROP keyword
        self.expect_keyword(Keyword::Drop)?;

        // Expect VIEW keyword
        self.expect_keyword(Keyword::View)?;

        // Check for optional IF EXISTS
        let if_exists = if self.peek_keyword(Keyword::If) {
            self.consume_keyword(Keyword::If)?;
            self.expect_keyword(Keyword::Exists)?;
            true
        } else {
            false
        };

        // Parse view name (supports schema.view)
        let view_name = self.parse_qualified_identifier()?;

        // Check for optional CASCADE or RESTRICT
        let cascade = if self.peek_keyword(Keyword::Cascade) {
            self.consume_keyword(Keyword::Cascade)?;
            true
        } else if self.peek_keyword(Keyword::Restrict) {
            self.consume_keyword(Keyword::Restrict)?;
            false
        } else {
            false // RESTRICT is the default
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(ast::DropViewStmt {
            view_name,
            if_exists,
            cascade,
        })
    }
}
