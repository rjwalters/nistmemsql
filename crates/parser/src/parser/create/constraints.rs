//! Constraint parsing for CREATE TABLE

use super::super::*;

impl Parser {
    /// Parse column-level constraints (PRIMARY KEY, UNIQUE, CHECK, REFERENCES)
    pub(in crate::parser) fn parse_column_constraints(&mut self) -> Result<Vec<ast::ColumnConstraint>, ParseError> {
        let mut constraints = Vec::new();

        loop {
            // Check for optional CONSTRAINT keyword
            let name = if self.peek_keyword(Keyword::Constraint) {
                self.advance(); // consume CONSTRAINT
                match self.peek() {
                    Token::Identifier(n) => {
                        let constraint_name = n.clone();
                        self.advance();
                        Some(constraint_name)
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected constraint name after CONSTRAINT".to_string(),
                        })
                    }
                }
            } else {
                None
            };

            match self.peek() {
                Token::Keyword(Keyword::Not) => {
                    self.advance(); // consume NOT
                    self.expect_keyword(Keyword::Null)?;
                    constraints.push(ast::ColumnConstraint {
                        name,
                        kind: ast::ColumnConstraintKind::NotNull,
                    });
                }
                Token::Keyword(Keyword::Primary) => {
                    self.advance(); // consume PRIMARY
                    self.expect_keyword(Keyword::Key)?;
                    constraints.push(ast::ColumnConstraint {
                        name,
                        kind: ast::ColumnConstraintKind::PrimaryKey,
                    });
                }
                Token::Keyword(Keyword::Unique) => {
                    self.advance(); // consume UNIQUE
                    constraints.push(ast::ColumnConstraint {
                        name,
                        kind: ast::ColumnConstraintKind::Unique,
                    });
                }
                Token::Keyword(Keyword::Check) => {
                    self.advance(); // consume CHECK
                    self.expect_token(Token::LParen)?;
                    let expr = self.parse_expression()?;
                    self.expect_token(Token::RParen)?;
                    constraints.push(ast::ColumnConstraint {
                        name,
                        kind: ast::ColumnConstraintKind::Check(Box::new(expr)),
                    });
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

                    constraints.push(ast::ColumnConstraint {
                        name,
                        kind: ast::ColumnConstraintKind::References { table, column },
                    });
                }
                _ => {
                    // If we parsed a CONSTRAINT name but no constraint type, error
                    if name.is_some() {
                        return Err(ParseError {
                            message: "Expected constraint type after CONSTRAINT name".to_string(),
                        });
                    }
                    break;
                }
            }
        }

        Ok(constraints)
    }

    /// Parse table-level constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK)
    pub(in crate::parser) fn parse_table_constraint(&mut self) -> Result<ast::TableConstraint, ParseError> {
        // Check for optional CONSTRAINT keyword
        let name = if self.peek_keyword(Keyword::Constraint) {
            self.advance(); // consume CONSTRAINT
            match self.peek() {
                Token::Identifier(n) => {
                    let constraint_name = n.clone();
                    self.advance();
                    Some(constraint_name)
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected constraint name after CONSTRAINT".to_string(),
                    })
                }
            }
        } else {
            None
        };

        let kind = match self.peek() {
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
                ast::TableConstraintKind::PrimaryKey { columns }
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

                ast::TableConstraintKind::ForeignKey {
                    columns,
                    references_table,
                    references_columns,
                }
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
                ast::TableConstraintKind::Unique { columns }
            }
            Token::Keyword(Keyword::Check) => {
                self.advance(); // consume CHECK
                self.expect_token(Token::LParen)?;
                let expr = self.parse_expression()?;
                self.expect_token(Token::RParen)?;
                ast::TableConstraintKind::Check {
                    expr: Box::new(expr),
                }
            }
            _ => {
                return Err(ParseError {
                    message: "Expected table constraint keyword (PRIMARY, FOREIGN, UNIQUE, CHECK)"
                        .to_string(),
                })
            }
        };

        Ok(ast::TableConstraint { name, kind })
    }
}
