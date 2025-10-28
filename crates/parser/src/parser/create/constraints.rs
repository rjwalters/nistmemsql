//! Constraint parsing for CREATE TABLE

use super::super::*;

impl Parser {
    /// Parse column-level constraints (PRIMARY KEY, UNIQUE, CHECK, REFERENCES)
    pub(in crate::parser) fn parse_column_constraints(&mut self) -> Result<Vec<ast::ColumnConstraint>, ParseError> {
        let mut constraints = Vec::new();

        loop {
            match self.peek() {
                Token::Keyword(Keyword::Primary) => {
                    self.advance(); // consume PRIMARY
                    self.expect_keyword(Keyword::Key)?;
                    constraints.push(ast::ColumnConstraint::PrimaryKey);
                }
                Token::Keyword(Keyword::Unique) => {
                    self.advance(); // consume UNIQUE
                    constraints.push(ast::ColumnConstraint::Unique);
                }
                Token::Keyword(Keyword::Check) => {
                    self.advance(); // consume CHECK
                    self.expect_token(Token::LParen)?;
                    let expr = self.parse_expression()?;
                    self.expect_token(Token::RParen)?;
                    constraints.push(ast::ColumnConstraint::Check(Box::new(expr)));
                }
                Token::Keyword(Keyword::References) => {
                    self.advance(); // consume REFERENCES
                    let table = match self.peek() {
                        Token::Identifier(t) => {
                            let table_name = t.clone();
                            self.advance();
                            table_name
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected table name after REFERENCES".to_string(),
                            })
                        }
                    };

                    self.expect_token(Token::LParen)?;
                    let column = match self.peek() {
                        Token::Identifier(c) => {
                            let col_name = c.clone();
                            self.advance();
                            col_name
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected column name in REFERENCES".to_string(),
                            })
                        }
                    };
                    self.expect_token(Token::RParen)?;

                    constraints.push(ast::ColumnConstraint::References { table, column });
                }
                _ => break,
            }
        }

        Ok(constraints)
    }

    /// Parse table-level constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK)
    pub(in crate::parser) fn parse_table_constraint(&mut self) -> Result<ast::TableConstraint, ParseError> {
        match self.peek() {
            Token::Keyword(Keyword::Primary) => {
                self.advance(); // consume PRIMARY
                self.expect_keyword(Keyword::Key)?;
                self.expect_token(Token::LParen)?;

                let mut columns = Vec::new();
                loop {
                    match self.peek() {
                        Token::Identifier(col) => {
                            columns.push(col.clone());
                            self.advance();
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected column name in PRIMARY KEY".to_string(),
                            })
                        }
                    }

                    if matches!(self.peek(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }

                self.expect_token(Token::RParen)?;
                Ok(ast::TableConstraint::PrimaryKey { columns })
            }
            Token::Keyword(Keyword::Foreign) => {
                self.advance(); // consume FOREIGN
                self.expect_keyword(Keyword::Key)?;
                self.expect_token(Token::LParen)?;

                let mut columns = Vec::new();
                loop {
                    match self.peek() {
                        Token::Identifier(col) => {
                            columns.push(col.clone());
                            self.advance();
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected column name in FOREIGN KEY".to_string(),
                            })
                        }
                    }

                    if matches!(self.peek(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }

                self.expect_token(Token::RParen)?;
                self.expect_keyword(Keyword::References)?;

                let references_table = match self.peek() {
                    Token::Identifier(t) => {
                        let table_name = t.clone();
                        self.advance();
                        table_name
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected table name after REFERENCES".to_string(),
                        })
                    }
                };

                self.expect_token(Token::LParen)?;

                let mut references_columns = Vec::new();
                loop {
                    match self.peek() {
                        Token::Identifier(col) => {
                            references_columns.push(col.clone());
                            self.advance();
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected column name in REFERENCES".to_string(),
                            })
                        }
                    }

                    if matches!(self.peek(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }

                self.expect_token(Token::RParen)?;

                Ok(ast::TableConstraint::ForeignKey {
                    columns,
                    references_table,
                    references_columns,
                })
            }
            Token::Keyword(Keyword::Unique) => {
                self.advance(); // consume UNIQUE
                self.expect_token(Token::LParen)?;

                let mut columns = Vec::new();
                loop {
                    match self.peek() {
                        Token::Identifier(col) => {
                            columns.push(col.clone());
                            self.advance();
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected column name in UNIQUE".to_string(),
                            })
                        }
                    }

                    if matches!(self.peek(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }

                self.expect_token(Token::RParen)?;
                Ok(ast::TableConstraint::Unique { columns })
            }
            Token::Keyword(Keyword::Check) => {
                self.advance(); // consume CHECK
                self.expect_token(Token::LParen)?;
                let expr = self.parse_expression()?;
                self.expect_token(Token::RParen)?;
                Ok(ast::TableConstraint::Check {
                    expr: Box::new(expr),
                })
            }
            _ => Err(ParseError {
                message: "Expected table constraint keyword (PRIMARY, FOREIGN, UNIQUE, CHECK)"
                    .to_string(),
            }),
        }
    }
}
