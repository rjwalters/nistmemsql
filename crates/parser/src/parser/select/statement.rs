use super::*;

impl Parser {
    /// Parse SELECT statement
    pub(crate) fn parse_select_statement(&mut self) -> Result<ast::SelectStmt, ParseError> {
        self.expect_keyword(Keyword::Select)?;

        // Parse SELECT list
        let select_list = self.parse_select_list()?;

        // Parse optional FROM clause
        let from = if self.peek_keyword(Keyword::From) {
            self.consume_keyword(Keyword::From)?;
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        // Parse optional WHERE clause
        let where_clause = if self.peek_keyword(Keyword::Where) {
            self.consume_keyword(Keyword::Where)?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse optional GROUP BY clause
        let group_by = if self.peek_keyword(Keyword::Group) {
            self.consume_keyword(Keyword::Group)?;
            self.expect_keyword(Keyword::By)?;

            // Parse comma-separated list of expressions
            let mut group_exprs = Vec::new();
            loop {
                let expr = self.parse_expression()?;
                group_exprs.push(expr);

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }

            Some(group_exprs)
        } else {
            None
        };

        // Parse optional HAVING clause (only valid with GROUP BY)
        let having = if self.peek_keyword(Keyword::Having) {
            self.consume_keyword(Keyword::Having)?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse optional ORDER BY clause
        let order_by = if self.peek_keyword(Keyword::Order) {
            self.consume_keyword(Keyword::Order)?;
            self.expect_keyword(Keyword::By)?;

            // Parse comma-separated list of order items
            let mut order_items = Vec::new();
            loop {
                let expr = self.parse_expression()?;

                // Check for optional ASC/DESC
                let direction = if self.peek_keyword(Keyword::Asc) {
                    self.consume_keyword(Keyword::Asc)?;
                    ast::OrderDirection::Asc
                } else if self.peek_keyword(Keyword::Desc) {
                    self.consume_keyword(Keyword::Desc)?;
                    ast::OrderDirection::Desc
                } else {
                    ast::OrderDirection::Asc // Default
                };

                order_items.push(ast::OrderByItem { expr, direction });

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }

            Some(order_items)
        } else {
            None
        };

        // Parse LIMIT clause
        let limit = if self.peek_keyword(Keyword::Limit) {
            self.consume_keyword(Keyword::Limit)?;
            match self.peek() {
                Token::Number(n) => {
                    let limit_value = n.parse::<usize>().map_err(|_| ParseError {
                        message: format!("Invalid LIMIT value: {}", n),
                    })?;

                    self.advance();
                    Some(limit_value)
                }
                _ => return Err(ParseError { message: "Expected number after LIMIT".to_string() }),
            }
        } else {
            None
        };

        // Parse OFFSET clause
        let offset = if self.peek_keyword(Keyword::Offset) {
            self.consume_keyword(Keyword::Offset)?;
            match self.peek() {
                Token::Number(n) => {
                    let offset_value = n.parse::<usize>().map_err(|_| ParseError {
                        message: format!("Invalid OFFSET value: {}", n),
                    })?;
                    self.advance();
                    Some(offset_value)
                }
                _ => {
                    return Err(ParseError { message: "Expected number after OFFSET".to_string() })
                }
            }
        } else {
            None
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(ast::SelectStmt {
            select_list,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }
}
