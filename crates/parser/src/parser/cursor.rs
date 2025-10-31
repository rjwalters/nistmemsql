//! Cursor declaration parsing (SQL:1999 Feature E121)

use crate::keywords::Keyword;
use crate::parser::{ParseError, Parser};
use crate::token::Token;
use ast::{CursorUpdatability, DeclareCursorStmt};

impl Parser {
    /// Parse DECLARE CURSOR statement
    ///
    /// Syntax:
    /// ```sql
    /// DECLARE cursor_name [INSENSITIVE] [SCROLL] CURSOR
    ///   [WITH HOLD | WITHOUT HOLD]
    ///   FOR query_expression
    ///   [FOR {READ ONLY | UPDATE [OF column_list]}]
    /// ```
    pub(super) fn parse_declare_cursor_statement(
        &mut self,
    ) -> Result<DeclareCursorStmt, ParseError> {
        // DECLARE keyword
        self.expect_keyword(Keyword::Declare)?;

        // Cursor name
        let cursor_name = self.parse_identifier()?;

        // Optional INSENSITIVE
        let insensitive = self.try_consume_keyword(Keyword::Insensitive);

        // Optional SCROLL
        let scroll = self.try_consume_keyword(Keyword::Scroll);

        // Required CURSOR keyword
        self.expect_keyword(Keyword::Cursor)?;

        // Optional WITH HOLD | WITHOUT HOLD
        let hold = if self.try_consume_keyword(Keyword::With) {
            self.expect_keyword(Keyword::Hold)?;
            Some(true)
        } else if self.try_consume_keyword(Keyword::Without) {
            self.expect_keyword(Keyword::Hold)?;
            Some(false)
        } else {
            None
        };

        // Required FOR keyword
        self.expect_keyword(Keyword::For)?;

        // Query expression (SELECT statement)
        let query = Box::new(self.parse_select_statement()?);

        // Optional FOR {READ ONLY | UPDATE [OF column_list]}
        let updatability = if self.try_consume_keyword(Keyword::For) {
            if self.try_consume_keyword(Keyword::Read) {
                self.expect_keyword(Keyword::Only)?;
                CursorUpdatability::ReadOnly
            } else if self.try_consume_keyword(Keyword::Update) {
                // Check for OF column_list
                let columns = if self.try_consume_keyword(Keyword::Of) {
                    let mut cols = Vec::new();
                    cols.push(self.parse_identifier()?);
                    while self.try_consume(&Token::Comma) {
                        cols.push(self.parse_identifier()?);
                    }
                    Some(cols)
                } else {
                    None
                };
                CursorUpdatability::Update { columns }
            } else {
                return Err(ParseError {
                    message: "Expected READ ONLY or UPDATE after FOR".to_string(),
                });
            }
        } else {
            CursorUpdatability::Unspecified
        };

        Ok(DeclareCursorStmt { cursor_name, insensitive, scroll, hold, query, updatability })
    }
}
