//! Constraint parsing for CREATE TABLE

use super::super::*;

impl Parser {
    /// Parse ON DELETE/UPDATE referential actions
    fn parse_referential_actions(
        &mut self,
    ) -> Result<(Option<ast::ReferentialAction>, Option<ast::ReferentialAction>), ParseError> {
        let mut on_delete = None;
        let mut on_update = None;

        // Parse optional ON DELETE clause
        if self.peek_keyword(Keyword::On) {
            self.advance(); // consume ON
            if self.peek_keyword(Keyword::Delete) {
                self.advance(); // consume DELETE
                on_delete = Some(self.parse_referential_action()?);
            } else if self.peek_keyword(Keyword::Update) {
                self.advance(); // consume UPDATE
                on_update = Some(self.parse_referential_action()?);
            } else {
                return Err(ParseError {
                    message: "Expected DELETE or UPDATE after ON".to_string(),
                });
            }
        }

        // Parse optional ON UPDATE clause (if not already parsed above)
        if self.peek_keyword(Keyword::On) {
            self.advance(); // consume ON
            if self.peek_keyword(Keyword::Update) {
                self.advance(); // consume UPDATE
                on_update = Some(self.parse_referential_action()?);
            } else {
                return Err(ParseError { message: "Expected UPDATE after ON".to_string() });
            }
        }

        Ok((on_delete, on_update))
    }

    /// Parse a single referential action (NO ACTION, CASCADE, SET NULL, SET DEFAULT)
    fn parse_referential_action(&mut self) -> Result<ast::ReferentialAction, ParseError> {
        if self.peek_keyword(Keyword::No) {
            self.advance(); // consume NO
            self.expect_keyword(Keyword::Action)?;
            Ok(ast::ReferentialAction::NoAction)
        } else if self.peek_keyword(Keyword::Cascade) {
            self.advance(); // consume CASCADE
            Ok(ast::ReferentialAction::Cascade)
        } else if self.peek_keyword(Keyword::Set) {
            self.advance(); // consume SET
            if self.peek_keyword(Keyword::Null) {
                self.advance(); // consume NULL
                Ok(ast::ReferentialAction::SetNull)
            } else if self.peek_keyword(Keyword::Default) {
                self.advance(); // consume DEFAULT
                Ok(ast::ReferentialAction::SetDefault)
            } else {
                return Err(ParseError {
                    message: "Expected NULL or DEFAULT after SET".to_string(),
                });
            }
        } else {
            return Err(ParseError {
                message: "Expected NO ACTION, CASCADE, SET NULL, or SET DEFAULT".to_string(),
            });
        }
    }

    /// Parse column-level constraints (PRIMARY KEY, UNIQUE, CHECK, REFERENCES)
    pub(in crate::parser) fn parse_column_constraints(
        &mut self,
    ) -> Result<Vec<ast::ColumnConstraint>, ParseError> {
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

                    let (on_delete, on_update) = self.parse_referential_actions()?;

                    constraints.push(ast::ColumnConstraint {
                        name,
                        kind: ast::ColumnConstraintKind::References {
                            table,
                            column,
                            on_delete,
                            on_update,
                        },
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
    pub(in crate::parser) fn parse_table_constraint(
        &mut self,
    ) -> Result<ast::TableConstraint, ParseError> {
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

                let (on_delete, on_update) = self.parse_referential_actions()?;

                ast::TableConstraintKind::ForeignKey {
                    columns,
                    references_table,
                    references_columns,
                    on_delete,
                    on_update,
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
                ast::TableConstraintKind::Check { expr: Box::new(expr) }
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
