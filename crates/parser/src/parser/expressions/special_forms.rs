use super::*;

impl Parser {
    /// Parse special SQL forms (CASE, CAST, EXISTS, NOT EXISTS, CURRENT_DATE/TIME/TIMESTAMP)
    pub(super) fn parse_special_form(&mut self) -> Result<Option<ast::Expression>, ParseError> {
        match self.peek() {
            // CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP (as identifiers)
            Token::Identifier(ref id) if id.to_uppercase() == "CURRENT_DATE" => {
                self.advance();
                Ok(Some(ast::Expression::Function {
                    name: "CURRENT_DATE".to_string(),
                    args: vec![],
                    character_unit: None,
                }))
            }
            Token::Identifier(ref id) if id.to_uppercase() == "CURRENT_TIME" => {
                self.advance();
                Ok(Some(ast::Expression::Function {
                    name: "CURRENT_TIME".to_string(),
                    args: vec![],
                    character_unit: None,
                }))
            }
            Token::Identifier(ref id) if id.to_uppercase() == "CURRENT_TIMESTAMP" => {
                self.advance();
                Ok(Some(ast::Expression::Function {
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

                    Ok(Some(ast::Expression::Function {
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

                    when_clauses.push(ast::CaseWhen { conditions, result });
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

                Ok(Some(ast::Expression::Case { operand, when_clauses, else_result }))
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

                Ok(Some(ast::Expression::Cast { expr: Box::new(expr), data_type }))
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

                Ok(Some(ast::Expression::Exists { subquery: Box::new(subquery), negated: false }))
            }
            // DEFAULT keyword: DEFAULT
            Token::Keyword(Keyword::Default) => {
                self.advance(); // consume DEFAULT
                Ok(Some(ast::Expression::Default))
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

                    Ok(Some(ast::Expression::Exists {
                        subquery: Box::new(subquery),
                        negated: true,
                    }))
                } else {
                    // It's a unary NOT operator on another expression
                    // Parse the inner expression
                    let expr = self.parse_primary_expression()?;

                    Ok(Some(ast::Expression::UnaryOp {
                        op: ast::UnaryOperator::Not,
                        expr: Box::new(expr),
                    }))
                }
            }
            _ => Ok(None),
        }
    }

    /// Parse current date/time functions (CURRENT_DATE, CURRENT_TIME[(precision)], CURRENT_TIMESTAMP[(precision)])
    pub(super) fn parse_current_datetime_function(
        &mut self,
    ) -> Result<Option<ast::Expression>, ParseError> {
        match self.peek() {
            Token::Keyword(Keyword::CurrentDate) => {
                self.advance(); // consume CURRENT_DATE
                Ok(Some(ast::Expression::CurrentDate))
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
                Ok(Some(ast::Expression::CurrentTime { precision }))
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
                Ok(Some(ast::Expression::CurrentTimestamp { precision }))
            }
            _ => Ok(None),
        }
    }

    /// Parse NEXT VALUE FOR expression
    /// Syntax: NEXT VALUE FOR sequence_name
    pub(super) fn parse_sequence_value_function(
        &mut self,
    ) -> Result<Option<ast::Expression>, ParseError> {
        if matches!(self.peek(), Token::Keyword(Keyword::Next)) {
            self.advance(); // consume NEXT
            self.expect_keyword(Keyword::Value)?;
            self.expect_keyword(Keyword::For)?;
            let sequence_name = self.parse_identifier()?;
            Ok(Some(ast::Expression::NextValue { sequence_name }))
        } else {
            Ok(None)
        }
    }
}
