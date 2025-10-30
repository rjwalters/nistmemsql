use super::*;

impl Parser {
    /// Parse SELECT statement (public entry point)
    pub(crate) fn parse_select_statement(&mut self) -> Result<ast::SelectStmt, ParseError> {
        self.parse_select_statement_internal(true)
    }

    /// Internal SELECT parser with control over ORDER BY/LIMIT parsing
    ///
    /// The `allow_order_limit` parameter controls whether ORDER BY, LIMIT, and OFFSET
    /// are parsed. This is set to false when parsing the right-hand side of set operations
    /// to ensure these clauses only apply to the outermost query.
    fn parse_select_statement_internal(
        &mut self,
        allow_order_limit: bool,
    ) -> Result<ast::SelectStmt, ParseError> {
        // Parse optional WITH clause (CTEs)
        let with_clause = if self.peek_keyword(Keyword::With) {
            self.consume_keyword(Keyword::With)?;
            Some(self.parse_cte_list()?)
        } else {
            None
        };

        self.expect_keyword(Keyword::Select)?;

        // Parse optional DISTINCT keyword
        let distinct = if self.peek_keyword(Keyword::Distinct) {
            self.consume_keyword(Keyword::Distinct)?;
            true
        } else {
            false
        };

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

        // Parse set operations (UNION, INTERSECT, EXCEPT) before ORDER BY/LIMIT
        // This ensures ORDER BY/LIMIT apply to the entire set operation result
        let set_operation = if self.peek_keyword(Keyword::Union)
            || self.peek_keyword(Keyword::Intersect)
            || self.peek_keyword(Keyword::Except)
        {
            let op = if self.peek_keyword(Keyword::Union) {
                self.consume_keyword(Keyword::Union)?;
                ast::SetOperator::Union
            } else if self.peek_keyword(Keyword::Intersect) {
                self.consume_keyword(Keyword::Intersect)?;
                ast::SetOperator::Intersect
            } else {
                self.consume_keyword(Keyword::Except)?;
                ast::SetOperator::Except
            };

            // Check for ALL keyword (default is DISTINCT)
            let all = if self.peek_keyword(Keyword::All) {
                self.consume_keyword(Keyword::All)?;
                true
            } else {
                false
            };

            // Parse the right-hand side SELECT statement
            // Don't allow ORDER BY/LIMIT on the right side - they should only apply to the final result
            let right = Box::new(self.parse_select_statement_internal(false)?);

            Some(ast::SetOperation { op, all, right })
        } else {
            None
        };

        // Parse ORDER BY, LIMIT, OFFSET after set operations (only if allowed)
        // These apply to the entire result (including set operations)

        // Parse optional ORDER BY clause
        let order_by = if allow_order_limit && self.peek_keyword(Keyword::Order) {
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
        let limit = if allow_order_limit && self.peek_keyword(Keyword::Limit) {
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
        let offset = if allow_order_limit && self.peek_keyword(Keyword::Offset) {
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
            with_clause,
            distinct,
            select_list,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            set_operation,
        })
    }

    /// Parse a comma-separated list of CTEs
    ///
    /// Syntax: cte_name [(col1, col2, ...)] AS (SELECT ...) [, ...]
    fn parse_cte_list(&mut self) -> Result<Vec<ast::CommonTableExpr>, ParseError> {
        let mut ctes = Vec::new();

        loop {
            ctes.push(self.parse_cte()?);

            // Check for more CTEs
            if matches!(self.peek(), Token::Comma) {
                self.advance(); // consume comma
            } else {
                break;
            }
        }

        Ok(ctes)
    }

    /// Parse a single CTE definition
    ///
    /// Syntax: cte_name [(col1, col2, ...)] AS (SELECT ...)
    fn parse_cte(&mut self) -> Result<ast::CommonTableExpr, ParseError> {
        // Parse CTE name
        let name = match self.peek() {
            Token::Identifier(id) => {
                let name = id.clone();
                self.advance();
                name
            }
            _ => return Err(ParseError { message: "Expected CTE name (identifier)".to_string() }),
        };

        // Parse optional column list: (col1, col2, ...)
        let columns = if matches!(self.peek(), Token::LParen) {
            self.advance(); // consume '('

            // Check for empty column list
            if matches!(self.peek(), Token::RParen) {
                return Err(ParseError { message: "CTE column list cannot be empty".to_string() });
            }

            let mut cols = Vec::new();
            loop {
                match self.peek() {
                    Token::Identifier(col) => {
                        cols.push(col.clone());
                        self.advance();
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected column name in CTE column list".to_string(),
                        })
                    }
                }

                if matches!(self.peek(), Token::Comma) {
                    self.advance(); // consume comma
                } else {
                    break;
                }
            }

            // Expect closing paren
            if !matches!(self.peek(), Token::RParen) {
                return Err(ParseError {
                    message: "Expected ')' after CTE column list".to_string(),
                });
            }
            self.advance(); // consume ')'

            Some(cols)
        } else {
            None
        };

        // Expect AS keyword
        self.expect_keyword(Keyword::As)?;

        // Expect opening paren for subquery
        if !matches!(self.peek(), Token::LParen) {
            return Err(ParseError {
                message: "Expected '(' after AS in CTE definition".to_string(),
            });
        }
        self.advance(); // consume '('

        // Parse the SELECT statement
        let query = Box::new(self.parse_select_statement()?);

        // Expect closing paren
        if !matches!(self.peek(), Token::RParen) {
            return Err(ParseError { message: "Expected ')' after CTE query".to_string() });
        }
        self.advance(); // consume ')'

        Ok(ast::CommonTableExpr { name, columns, query })
    }
}
