use super::*;

impl Parser {
    /// Parse literal expressions (numbers, strings, booleans, NULL)
    pub(super) fn parse_literal(&mut self) -> Result<Option<ast::Expression>, ParseError> {
        match self.peek() {
            Token::Number(n) => {
                let num_str = n.clone();
                self.advance();

                // Try to parse as integer first
                if let Ok(i) = num_str.parse::<i64>() {
                    Ok(Some(ast::Expression::Literal(types::SqlValue::Integer(i))))
                } else {
                    // For now, store as numeric string
                    Ok(Some(ast::Expression::Literal(types::SqlValue::Numeric(num_str))))
                }
            }
            Token::String(s) => {
                let string_val = s.clone();
                self.advance();
                Ok(Some(ast::Expression::Literal(types::SqlValue::Varchar(string_val))))
            }
            Token::Keyword(Keyword::True) => {
                self.advance();
                Ok(Some(ast::Expression::Literal(types::SqlValue::Boolean(true))))
            }
            Token::Keyword(Keyword::False) => {
                self.advance();
                Ok(Some(ast::Expression::Literal(types::SqlValue::Boolean(false))))
            }
            Token::Keyword(Keyword::Null) => {
                self.advance();
                Ok(Some(ast::Expression::Literal(types::SqlValue::Null)))
            }
            // Typed literals: DATE 'string', TIME 'string', TIMESTAMP 'string'
            Token::Keyword(Keyword::Date) => {
                self.advance();
                match self.peek() {
                    Token::String(s) => {
                        let date_str = s.clone();
                        self.advance();
                        Ok(Some(ast::Expression::Literal(types::SqlValue::Date(date_str))))
                    }
                    _ => Err(ParseError {
                        message: "Expected string literal after DATE keyword".to_string(),
                    }),
                }
            }
            Token::Keyword(Keyword::Time) => {
                self.advance();
                match self.peek() {
                    Token::String(s) => {
                        let time_str = s.clone();
                        self.advance();
                        Ok(Some(ast::Expression::Literal(types::SqlValue::Time(time_str))))
                    }
                    _ => Err(ParseError {
                        message: "Expected string literal after TIME keyword".to_string(),
                    }),
                }
            }
            Token::Keyword(Keyword::Timestamp) => {
                self.advance();
                match self.peek() {
                    Token::String(s) => {
                        let timestamp_str = s.clone();
                        self.advance();
                        Ok(Some(ast::Expression::Literal(types::SqlValue::Timestamp(
                            timestamp_str,
                        ))))
                    }
                    _ => Err(ParseError {
                        message: "Expected string literal after TIMESTAMP keyword".to_string(),
                    }),
                }
            }
            Token::Keyword(Keyword::Interval) => {
                self.advance();
                // Parse INTERVAL 'value' field [TO field]
                match self.peek() {
                    Token::String(interval_str) => {
                        let value_str = interval_str.clone();
                        self.advance();

                        // Parse interval field (YEAR, MONTH, DAY, etc.)
                        let start_field = match self.peek() {
                            Token::Identifier(field) => field.to_uppercase(),
                            _ => {
                                return Err(ParseError {
                                    message: "Expected interval field after INTERVAL value"
                                        .to_string(),
                                })
                            }
                        };
                        self.advance();

                        // Check for TO (multi-field interval)
                        let interval_spec = match self.peek() {
                            Token::Keyword(Keyword::To) => {
                                self.advance(); // consume TO keyword
                                let end_field = match self.peek() {
                                    Token::Identifier(field) => field.to_uppercase(),
                                    _ => {
                                        return Err(ParseError {
                                            message: "Expected interval field after TO".to_string(),
                                        })
                                    }
                                };
                                self.advance();
                                format!("{} {} TO {}", value_str, start_field, end_field)
                            }
                            Token::Identifier(word) if word.to_uppercase() == "TO" => {
                                self.advance(); // consume TO identifier (backward compat)
                                let end_field = match self.peek() {
                                    Token::Identifier(field) => field.to_uppercase(),
                                    _ => {
                                        return Err(ParseError {
                                            message: "Expected interval field after TO".to_string(),
                                        })
                                    }
                                };
                                self.advance();
                                format!("{} {} TO {}", value_str, start_field, end_field)
                            }
                            _ => format!("{} {}", value_str, start_field),
                        };

                        Ok(Some(ast::Expression::Literal(types::SqlValue::Interval(interval_spec))))
                    }
                    _ => Err(ParseError {
                        message: "Expected string literal after INTERVAL keyword".to_string(),
                    }),
                }
            }
            _ => Ok(None),
        }
    }
}
