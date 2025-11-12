use super::*;

impl Parser {
    /// Parse special SQL forms (CASE, CAST, EXISTS, NOT EXISTS, CURRENT_DATE/TIME/TIMESTAMP)
    pub(super) fn parse_special_form(&mut self) -> Result<Option<vibesql_ast::Expression>, ParseError> {
        match self.peek() {
            // CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP (as identifiers)
            Token::Identifier(ref id) if id.to_uppercase() == "CURRENT_DATE" => {
                self.advance();
                Ok(Some(vibesql_ast::Expression::Function {
                    name: "CURRENT_DATE".to_string(),
                    args: vec![],
                    character_unit: None,
                }))
            }
            Token::Identifier(ref id) if id.to_uppercase() == "CURRENT_TIME" => {
                self.advance();
                Ok(Some(vibesql_ast::Expression::Function {
                    name: "CURRENT_TIME".to_string(),
                    args: vec![],
                    character_unit: None,
                }))
            }
            Token::Identifier(ref id) if id.to_uppercase() == "CURRENT_TIMESTAMP" => {
                self.advance();
                Ok(Some(vibesql_ast::Expression::Function {
                    name: "CURRENT_TIMESTAMP".to_string(),
                    args: vec![],
                    character_unit: None,
                }))
            }
            // CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP (multi-token form)
            // The lexer tokenizes CURRENT_DATE as two tokens when CURRENT is a keyword:
            //   Token::Keyword(Current) + Token::Identifier("_DATE")
            // This branch handles that tokenization pattern.
            Token::Keyword(Keyword::Current) => {
                self.advance(); // consume CURRENT

                // Check for underscore followed by DATE/TIME/TIMESTAMP
                if let Token::Identifier(ref id) = self.peek() {
                    let function_name = match id.to_uppercase().as_str() {
                        "_DATE" => {
                            self.advance(); // consume _DATE
                            "CURRENT_DATE"
                        }
                        "_TIME" => {
                            self.advance(); // consume _TIME
                            "CURRENT_TIME"
                        }
                        "_TIMESTAMP" => {
                            self.advance(); // consume _TIMESTAMP
                            "CURRENT_TIMESTAMP"
                        }
                        _ => {
                            return Err(ParseError {
                                message: format!(
                                    "Expected DATE, TIME, or TIMESTAMP after CURRENT, found {}",
                                    id
                                ),
                            })
                        }
                    };

                    Ok(Some(vibesql_ast::Expression::Function {
                        name: function_name.to_string(),
                        args: vec![],
                        character_unit: None,
                    }))
                } else {
                    Err(ParseError {
                        message: format!(
                            "Expected identifier after CURRENT, found {:?}",
                            self.peek()
                        ),
                    })
                }
            }
            // CAST expression: CAST(expr AS data_type)
            // CASE expression: both simple and searched forms
            Token::Keyword(Keyword::Case) => {
                self.advance(); // consume CASE

                // Try to parse operand for simple CASE
                // If next token is WHEN, it's a searched CASE (no operand)
                let operand = if !self.peek_keyword(Keyword::When) {
                    Some(Box::new(self.parse_expression()?))
                } else {
                    None
                };

                // Parse WHEN clauses
                let mut when_clauses = Vec::new();
                while self.peek_keyword(Keyword::When) {
                    self.advance(); // consume WHEN

                    // Parse first condition
                    let mut conditions = vec![self.parse_expression()?];

                    // Parse additional comma-separated conditions
                    while matches!(self.peek(), Token::Comma) {
                        self.advance(); // consume comma
                        conditions.push(self.parse_expression()?);
                    }

                    self.expect_keyword(Keyword::Then)?;
                    let result = self.parse_expression()?;

                    when_clauses.push(vibesql_ast::CaseWhen { conditions, result });
                }

                // Ensure at least one WHEN clause exists
                if when_clauses.is_empty() {
                    return Err(ParseError {
                        message: "CASE expression requires at least one WHEN clause".to_string(),
                    });
                }

                // Parse optional ELSE clause
                let else_result = if self.peek_keyword(Keyword::Else) {
                    self.advance(); // consume ELSE
                    Some(Box::new(self.parse_expression()?))
                } else {
                    None
                };

                // Expect END keyword
                self.expect_keyword(Keyword::End)?;

                Ok(Some(vibesql_ast::Expression::Case { operand, when_clauses, else_result }))
            }
            Token::Keyword(Keyword::Cast) => {
                self.advance(); // consume CAST

                // Expect opening parenthesis
                self.expect_token(Token::LParen)?;

                // Parse the expression to cast
                let expr = self.parse_expression()?;

                // Expect AS keyword
                self.expect_keyword(Keyword::As)?;

                // Parse the target data type
                let data_type = self.parse_data_type()?;

                // Expect closing parenthesis
                self.expect_token(Token::RParen)?;

                Ok(Some(vibesql_ast::Expression::Cast { expr: Box::new(expr), data_type }))
            }
            // EXISTS expression: EXISTS (SELECT ...)
            Token::Keyword(Keyword::Exists) => {
                self.advance(); // consume EXISTS

                // Expect opening parenthesis
                self.expect_token(Token::LParen)?;

                // Parse the subquery (parse_select_statement will consume SELECT keyword)
                let subquery = self.parse_select_statement()?;

                // Expect closing parenthesis
                self.expect_token(Token::RParen)?;

                Ok(Some(vibesql_ast::Expression::Exists { subquery: Box::new(subquery), negated: false }))
            }
            // DEFAULT keyword: DEFAULT
            Token::Keyword(Keyword::Default) => {
                self.advance(); // consume DEFAULT
                Ok(Some(vibesql_ast::Expression::Default))
            }
            // NOT keyword - could be NOT EXISTS or unary NOT
            Token::Keyword(Keyword::Not) => {
                self.advance(); // consume NOT

                // Check if it's NOT EXISTS
                if self.peek_keyword(Keyword::Exists) {
                    self.advance(); // consume EXISTS

                    // Expect opening parenthesis
                    self.expect_token(Token::LParen)?;

                    // Parse the subquery
                    let subquery = self.parse_select_statement()?;

                    // Expect closing parenthesis
                    self.expect_token(Token::RParen)?;

                    Ok(Some(vibesql_ast::Expression::Exists {
                        subquery: Box::new(subquery),
                        negated: true,
                    }))
                } else {
                    // It's a unary NOT operator on another expression
                    // Parse the inner expression (including unary operators like +/-)
                    let expr = self.parse_unary_expression()?;

                    Ok(Some(vibesql_ast::Expression::UnaryOp {
                        op: vibesql_ast::UnaryOperator::Not,
                        expr: Box::new(expr),
                    }))
                }
            }
            _ => Ok(None),
        }
    }

    /// Parse current date/time functions (CURRENT_DATE, CURRENT_TIME[(precision)],
    /// CURRENT_TIMESTAMP[(precision)])
    pub(super) fn parse_current_datetime_function(
        &mut self,
    ) -> Result<Option<vibesql_ast::Expression>, ParseError> {
        match self.peek() {
            Token::Keyword(Keyword::CurrentDate) => {
                self.advance(); // consume CURRENT_DATE
                Ok(Some(vibesql_ast::Expression::CurrentDate))
            }
            Token::Keyword(Keyword::CurrentTime) => {
                self.advance(); // consume CURRENT_TIME
                let precision = if self.try_consume(&Token::LParen) {
                    let prec_str = match self.peek() {
                        Token::Number(n) => n.clone(),
                        _ => {
                            return Err(ParseError {
                                message: "Expected integer precision for CURRENT_TIME".to_string(),
                            })
                        }
                    };
                    let prec: u32 = prec_str.parse().map_err(|_| ParseError {
                        message: format!("Invalid precision value: {}", prec_str),
                    })?;
                    if prec > 9 {
                        return Err(ParseError {
                            message: format!(
                                "CURRENT_TIME precision must be between 0 and 9, got {}",
                                prec
                            ),
                        });
                    }
                    self.advance(); // consume the number
                    self.expect_token(Token::RParen)?;
                    Some(prec)
                } else {
                    None
                };
                Ok(Some(vibesql_ast::Expression::CurrentTime { precision }))
            }
            Token::Keyword(Keyword::CurrentTimestamp) => {
                self.advance(); // consume CURRENT_TIMESTAMP
                let precision = if self.try_consume(&Token::LParen) {
                    let prec_str = match self.peek() {
                        Token::Number(n) => n.clone(),
                        _ => {
                            return Err(ParseError {
                                message: "Expected integer precision for CURRENT_TIMESTAMP"
                                    .to_string(),
                            })
                        }
                    };
                    let prec: u32 = prec_str.parse().map_err(|_| ParseError {
                        message: format!("Invalid precision value: {}", prec_str),
                    })?;
                    if prec > 9 {
                        return Err(ParseError {
                            message: format!(
                                "CURRENT_TIMESTAMP precision must be between 0 and 9, got {}",
                                prec
                            ),
                        });
                    }
                    self.advance(); // consume the number
                    self.expect_token(Token::RParen)?;
                    Some(prec)
                } else {
                    None
                };
                Ok(Some(vibesql_ast::Expression::CurrentTimestamp { precision }))
            }
            _ => Ok(None),
        }
    }

    /// Parse NEXT VALUE FOR expression
    /// Syntax: NEXT VALUE FOR sequence_name
    pub(super) fn parse_sequence_value_function(
        &mut self,
    ) -> Result<Option<vibesql_ast::Expression>, ParseError> {
        if matches!(self.peek(), Token::Keyword(Keyword::Next)) {
            self.advance(); // consume NEXT

            // Parse "VALUE" as identifier (not a reserved keyword)
            match self.peek() {
                Token::Identifier(s) if s.eq_ignore_ascii_case("VALUE") => {
                    self.advance();
                }
                _ => return Err(ParseError { message: "Expected VALUE after NEXT".to_string() }),
            }

            self.expect_keyword(Keyword::For)?;
            let sequence_name = self.parse_identifier()?;
            Ok(Some(vibesql_ast::Expression::NextValue { sequence_name }))
        } else if self.peek_keyword(Keyword::Match) {
            // MATCH...AGAINST full-text search
            self.advance(); // consume MATCH
            self.expect_token(Token::LParen)?;

            // Parse column list
            let mut columns = Vec::new();
            loop {
                let col = self.parse_identifier()?;
                columns.push(col);
                if !matches!(self.peek(), Token::Comma) {
                    break;
                }
                self.advance(); // consume comma
            }

            self.expect_token(Token::RParen)?;

            // Expect AGAINST keyword
            self.expect_keyword(Keyword::Against)?;
            self.expect_token(Token::LParen)?;

            // Parse search string (primary expression, not full expression with operators)
            // This prevents IN keyword from being parsed as an IN operator
            let search_modifier = Box::new(self.parse_primary_expression()?);

            // Check for search mode modifier
            let mode = if self.peek_keyword(Keyword::In) {
                self.advance(); // consume IN
                if self.peek_keyword(Keyword::Boolean) {
                    self.advance(); // consume BOOLEAN
                    // MODE is a required keyword after BOOLEAN in MySQL syntax
                    // It might be a keyword or identifier depending on lexer
                    if matches!(self.peek(), Token::Identifier(s) | Token::DelimitedIdentifier(s) if s.eq_ignore_ascii_case("MODE")) {
                        self.advance(); // consume MODE
                    } else if self.peek_keyword(Keyword::Mode) {
                        self.advance(); // consume MODE keyword if it exists
                    }
                    vibesql_ast::FulltextMode::Boolean
                } else {
                    return Err(ParseError {
                        message: "Expected BOOLEAN after IN".to_string(),
                    });
                }
            } else if self.peek_keyword(Keyword::With) {
                self.advance(); // consume WITH
                self.expect_keyword(Keyword::Query)?;
                self.expect_keyword(Keyword::Expansion)?;
                vibesql_ast::FulltextMode::QueryExpansion
            } else {
                vibesql_ast::FulltextMode::NaturalLanguage
            };

            self.expect_token(Token::RParen)?;

            Ok(Some(vibesql_ast::Expression::MatchAgainst {
                columns,
                search_modifier,
                mode,
            }))
        } else {
            Ok(None)
        }
    }
}
