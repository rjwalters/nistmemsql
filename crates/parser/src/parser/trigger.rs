//! CREATE TRIGGER and DROP TRIGGER statement parsers

use super::{ParseError, Parser};
use crate::keywords::Keyword;
use crate::token::Token;

impl Parser {
    /// Parse CREATE TRIGGER statement
    ///
    /// Syntax:
    ///   CREATE TRIGGER trigger_name
    ///   {BEFORE | AFTER | INSTEAD OF} {INSERT | UPDATE | DELETE}
    ///   ON table_name
    ///   [FOR EACH {ROW | STATEMENT}]
    ///   [WHEN (condition)]
    ///   triggered_action
    pub(super) fn parse_create_trigger_statement(
        &mut self,
    ) -> Result<ast::CreateTriggerStmt, ParseError> {
        // Expect CREATE keyword
        self.expect_keyword(Keyword::Create)?;

        // Expect TRIGGER keyword
        self.expect_keyword(Keyword::Trigger)?;

        // Parse trigger name
        let trigger_name = self.parse_identifier()?;

        // Parse timing: BEFORE | AFTER | INSTEAD OF
        let timing = if self.try_consume_keyword(Keyword::Before) {
            ast::TriggerTiming::Before
        } else if self.try_consume_keyword(Keyword::After) {
            ast::TriggerTiming::After
        } else if self.try_consume_keyword(Keyword::Instead) {
            self.expect_keyword(Keyword::Of)?;
            ast::TriggerTiming::InsteadOf
        } else {
            return Err(ParseError {
                message: "Expected BEFORE, AFTER, or INSTEAD OF after trigger name".to_string(),
            });
        };

        // Parse event: INSERT | UPDATE [OF columns] | DELETE
        let event = if self.try_consume_keyword(Keyword::Insert) {
            ast::TriggerEvent::Insert
        } else if self.try_consume_keyword(Keyword::Update) {
            // Check for optional OF column_list
            let columns = if self.try_consume_keyword(Keyword::Of) {
                let mut cols = Vec::new();
                self.expect_token(Token::LParen)?;
                loop {
                    let col = self.parse_identifier()?;
                    cols.push(col);

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
            ast::TriggerEvent::Update(columns)
        } else if self.try_consume_keyword(Keyword::Delete) {
            ast::TriggerEvent::Delete
        } else {
            return Err(ParseError {
                message: "Expected INSERT, UPDATE, or DELETE after trigger timing".to_string(),
            });
        };

        // Expect ON keyword
        self.expect_keyword(Keyword::On)?;

        // Parse table name
        let table_name = self.parse_identifier()?;

        // Parse optional FOR EACH ROW/STATEMENT
        let granularity = if self.try_consume_keyword(Keyword::For) {
            self.expect_keyword(Keyword::Each)?;
            if self.try_consume_keyword(Keyword::Row) {
                ast::TriggerGranularity::Row
            } else if self.try_consume_keyword(Keyword::Statement) {
                ast::TriggerGranularity::Statement
            } else {
                return Err(ParseError {
                    message: "Expected ROW or STATEMENT after FOR EACH".to_string(),
                });
            }
        } else {
            // Default per SQL:1999
            ast::TriggerGranularity::Statement
        };

        // Parse optional WHEN condition
        let when_condition = if self.try_consume_keyword(Keyword::When) {
            self.expect_token(Token::LParen)?;
            let expr = self.parse_expression()?;
            self.expect_token(Token::RParen)?;
            Some(Box::new(expr))
        } else {
            None
        };

        // Parse triggered action
        // For now, we'll store the action as raw SQL
        // We expect BEGIN...END block or a simple statement
        let triggered_action = self.parse_trigger_action()?;

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(ast::CreateTriggerStmt {
            trigger_name,
            timing,
            event,
            table_name,
            granularity,
            when_condition,
            triggered_action,
        })
    }

    /// Parse triggered action (simplified: just collect tokens until semicolon or EOF)
    fn parse_trigger_action(&mut self) -> Result<ast::TriggerAction, ParseError> {
        // Simplified implementation: store raw SQL as a string
        // A full implementation would parse procedural SQL (BEGIN...END blocks)

        // Expect BEGIN keyword
        self.expect_keyword(Keyword::Begin)?;

        let mut sql_parts = vec!["BEGIN".to_string()];
        let mut depth = 1; // Track BEGIN/END nesting

        // Collect tokens until matching END
        loop {
            match self.peek() {
                Token::Keyword(Keyword::Begin) => {
                    sql_parts.push("BEGIN".to_string());
                    depth += 1;
                    self.advance();
                }
                Token::Keyword(Keyword::End) => {
                    sql_parts.push("END".to_string());
                    depth -= 1;
                    self.advance();
                    if depth == 0 {
                        break;
                    }
                }
                Token::Eof => {
                    return Err(ParseError {
                        message: "Unexpected end of input in trigger action".to_string(),
                    });
                }
                Token::Semicolon => {
                    // Semicolon inside trigger body, not end of statement
                    sql_parts.push(";".to_string());
                    self.advance();
                }
                token => {
                    // Collect other tokens
                    sql_parts.push(format!("{:?}", token));
                    self.advance();
                }
            }
        }

        let raw_sql = sql_parts.join(" ");
        Ok(ast::TriggerAction::RawSql(raw_sql))
    }

    /// Parse DROP TRIGGER statement
    ///
    /// Syntax:
    ///   DROP TRIGGER trigger_name [CASCADE | RESTRICT]
    pub(super) fn parse_drop_trigger_statement(
        &mut self,
    ) -> Result<ast::DropTriggerStmt, ParseError> {
        // Expect DROP keyword
        self.expect_keyword(Keyword::Drop)?;

        // Expect TRIGGER keyword
        self.expect_keyword(Keyword::Trigger)?;

        // Parse trigger name
        let trigger_name = self.parse_identifier()?;

        // Parse optional CASCADE or RESTRICT
        let cascade = if self.try_consume_keyword(Keyword::Cascade) {
            true
        } else if self.try_consume_keyword(Keyword::Restrict) {
            false
        } else {
            // Default to RESTRICT per SQL:1999
            false
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(ast::DropTriggerStmt { trigger_name, cascade })
    }
}
