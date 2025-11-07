use super::*;

impl Parser {
    /// Parse FROM clause
    pub(crate) fn parse_from_clause(&mut self) -> Result<ast::FromClause, ParseError> {
        // Parse the first table reference
        let mut left = self.parse_table_reference()?;

        // Check for JOINs (left-associative)
        while self.is_join_keyword() {
            let join_type = self.parse_join_type()?;

            // Parse right table reference
            let right = self.parse_table_reference()?;

            // Parse ON condition
            let condition = if self.peek_keyword(Keyword::On) {
                self.consume_keyword(Keyword::On)?;
                Some(self.parse_expression()?)
            } else {
                None
            };

            // Build JOIN node
            left = ast::FromClause::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type,
                condition,
            };
        }

        Ok(left)
    }

    /// Parse a single table reference (table name, subquery, or derived table with alias)
    pub(crate) fn parse_table_reference(&mut self) -> Result<ast::FromClause, ParseError> {
        match self.peek() {
            Token::LParen => {
                // Parenthesized expression: could be a subquery or a JOIN expression
                self.advance(); // Consume '('

                // Check if this is a subquery (starts with SELECT) or a table reference/JOIN
                let result = if self.peek_keyword(Keyword::Select) {
                    // Parse the SELECT statement (subquery)
                    let query = Box::new(self.parse_select_statement()?);

                    // Expect closing ')'
                    match self.peek() {
                        Token::RParen => {
                            self.advance();
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected ')' after subquery".to_string(),
                            })
                        }
                    }

                    // SQL:1999 requires AS alias for derived tables
                    if !self.peek_keyword(Keyword::As) {
                        return Err(ParseError {
                            message: "Derived table must have AS alias (SQL:1999 requirement)"
                                .to_string(),
                        });
                    }
                    self.consume_keyword(Keyword::As)?;

                    // Parse alias
                    let alias = match self.peek() {
                        Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                            let alias = id.clone();
                            self.advance();
                            alias
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected alias after AS keyword".to_string(),
                            })
                        }
                    };

                    ast::FromClause::Subquery { query, alias }
                } else {
                    // Parenthesized table reference or JOIN expression
                    // Parse as a FROM clause (which handles JOINs)
                    let from_clause = self.parse_from_clause()?;

                    // Expect closing ')'
                    match self.peek() {
                        Token::RParen => {
                            self.advance();
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected ')' after parenthesized table reference"
                                    .to_string(),
                            })
                        }
                    }

                    from_clause
                };

                Ok(result)
            }
            Token::Identifier(_) | Token::DelimitedIdentifier(_) => {
                let name = self.parse_qualified_identifier()?;

                // Check for optional alias
                let alias = if self.peek_keyword(Keyword::As) {
                    self.consume_keyword(Keyword::As)?;
                    match self.peek() {
                        Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                            let alias = id.clone();
                            self.advance();
                            Some(alias)
                        }
                        _ => None,
                    }
                } else if matches!(
                    self.peek(),
                    Token::Identifier(_) | Token::DelimitedIdentifier(_)
                ) && !self.is_join_keyword()
                {
                    // Implicit alias (no AS keyword) - but not a JOIN keyword
                    match self.peek() {
                        Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                            let alias = id.clone();
                            self.advance();
                            Some(alias)
                        }
                        _ => None,
                    }
                } else {
                    None
                };

                Ok(ast::FromClause::Table { name, alias })
            }
            _ => Err(ParseError {
                message: "Expected table name or subquery in FROM clause".to_string(),
            }),
        }
    }

    /// Check if current token is a JOIN keyword
    pub(crate) fn is_join_keyword(&self) -> bool {
        matches!(
            self.peek(),
            Token::Keyword(Keyword::Join)
                | Token::Keyword(Keyword::Inner)
                | Token::Keyword(Keyword::Left)
                | Token::Keyword(Keyword::Right)
                | Token::Keyword(Keyword::Cross)
                | Token::Keyword(Keyword::Full)
        )
    }

    /// Parse JOIN type (INNER JOIN, LEFT JOIN, etc.)
    pub(crate) fn parse_join_type(&mut self) -> Result<ast::JoinType, ParseError> {
        match self.peek() {
            Token::Keyword(Keyword::Join) => {
                self.advance();
                Ok(ast::JoinType::Inner) // Default JOIN is INNER JOIN
            }
            Token::Keyword(Keyword::Inner) => {
                self.advance();
                self.expect_keyword(Keyword::Join)?;
                Ok(ast::JoinType::Inner)
            }
            Token::Keyword(Keyword::Left) => {
                self.advance();
                // Optional OUTER keyword
                if self.peek_keyword(Keyword::Outer) {
                    self.consume_keyword(Keyword::Outer)?;
                }
                self.expect_keyword(Keyword::Join)?;
                Ok(ast::JoinType::LeftOuter)
            }
            Token::Keyword(Keyword::Right) => {
                self.advance();
                // Optional OUTER keyword
                if self.peek_keyword(Keyword::Outer) {
                    self.consume_keyword(Keyword::Outer)?;
                }
                self.expect_keyword(Keyword::Join)?;
                Ok(ast::JoinType::RightOuter)
            }
            Token::Keyword(Keyword::Cross) => {
                self.advance();
                self.expect_keyword(Keyword::Join)?;
                Ok(ast::JoinType::Cross)
            }
            Token::Keyword(Keyword::Full) => {
                self.advance();
                // Optional OUTER keyword
                if self.peek_keyword(Keyword::Outer) {
                    self.consume_keyword(Keyword::Outer)?;
                }
                self.expect_keyword(Keyword::Join)?;
                Ok(ast::JoinType::FullOuter)
            }
            _ => Err(ParseError { message: "Expected JOIN keyword".to_string() }),
        }
    }
}
