use super::*;

impl Parser {
    /// Parse an expression (entry point)
    pub(super) fn parse_expression(&mut self) -> Result<ast::Expression, ParseError> {
        self.parse_or_expression()
    }

    /// Parse OR expression (lowest precedence)
    fn parse_or_expression(&mut self) -> Result<ast::Expression, ParseError> {
        let mut left = self.parse_and_expression()?;

        while self.peek_keyword(Keyword::Or) {
            self.consume_keyword(Keyword::Or)?;
            let right = self.parse_and_expression()?;
            left = ast::Expression::BinaryOp {
                op: ast::BinaryOperator::Or,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse AND expression
    fn parse_and_expression(&mut self) -> Result<ast::Expression, ParseError> {
        let mut left = self.parse_additive_expression()?;

        while self.peek_keyword(Keyword::And) {
            self.consume_keyword(Keyword::And)?;
            let right = self.parse_additive_expression()?;
            left = ast::Expression::BinaryOp {
                op: ast::BinaryOperator::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse additive expression (handles + and -)
    fn parse_additive_expression(&mut self) -> Result<ast::Expression, ParseError> {
        let mut left = self.parse_multiplicative_expression()?;

        while matches!(self.peek(), Token::Symbol('+') | Token::Symbol('-')) {
            let op = match self.peek() {
                Token::Symbol('+') => ast::BinaryOperator::Plus,
                Token::Symbol('-') => ast::BinaryOperator::Minus,
                _ => unreachable!(),
            };
            self.advance();

            let right = self.parse_multiplicative_expression()?;
            left = ast::Expression::BinaryOp { op, left: Box::new(left), right: Box::new(right) };
        }

        Ok(left)
    }

    /// Parse multiplicative expression (handles * and /)
    fn parse_multiplicative_expression(&mut self) -> Result<ast::Expression, ParseError> {
        let mut left = self.parse_comparison_expression()?;

        while matches!(self.peek(), Token::Symbol('*') | Token::Symbol('/')) {
            let op = match self.peek() {
                Token::Symbol('*') => ast::BinaryOperator::Multiply,
                Token::Symbol('/') => ast::BinaryOperator::Divide,
                _ => unreachable!(),
            };
            self.advance();

            let right = self.parse_comparison_expression()?;
            left = ast::Expression::BinaryOp { op, left: Box::new(left), right: Box::new(right) };
        }

        Ok(left)
    }

    /// Parse comparison expression (handles =, <, >, <=, >=, !=, <>, IN)
    fn parse_comparison_expression(&mut self) -> Result<ast::Expression, ParseError> {
        let mut left = self.parse_primary_expression()?;

        // Check for IN operator (including NOT IN) and BETWEEN (including NOT BETWEEN)
        if self.peek_keyword(Keyword::Not) {
            // Peek ahead to see if it's "NOT IN" or "NOT BETWEEN"
            let saved_pos = self.position;
            self.advance(); // consume NOT

            if self.peek_keyword(Keyword::In) {
                // It's NOT IN
                self.consume_keyword(Keyword::In)?;

                // Expect (SELECT ...)
                let subquery = self.parse_subquery()?;

                return Ok(ast::Expression::In {
                    expr: Box::new(left),
                    subquery: Box::new(subquery),
                    negated: true,
                });
            } else if self.peek_keyword(Keyword::Between) {
                // It's NOT BETWEEN
                self.consume_keyword(Keyword::Between)?;

                // Parse low AND high
                let low = self.parse_additive_expression()?;
                self.consume_keyword(Keyword::And)?;
                let high = self.parse_additive_expression()?;

                return Ok(ast::Expression::Between {
                    expr: Box::new(left),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated: true,
                });
            } else {
                // Not "NOT IN" or "NOT BETWEEN", restore position and continue
                self.position = saved_pos;
            }
        } else if self.peek_keyword(Keyword::In) {
            // It's IN (not negated)
            self.consume_keyword(Keyword::In)?;

            // Expect (SELECT ...)
            let subquery = self.parse_subquery()?;

            return Ok(ast::Expression::In {
                expr: Box::new(left),
                subquery: Box::new(subquery),
                negated: false,
            });
        } else if self.peek_keyword(Keyword::Between) {
            // It's BETWEEN (not negated)
            self.consume_keyword(Keyword::Between)?;

            // Parse low AND high
            let low = self.parse_additive_expression()?;
            self.consume_keyword(Keyword::And)?;
            let high = self.parse_additive_expression()?;

            return Ok(ast::Expression::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated: false,
            });
        }

        // Check for comparison operators (both single-char and multi-char)
        let is_comparison = matches!(
            self.peek(),
            Token::Symbol('=') | Token::Symbol('<') | Token::Symbol('>') | Token::Operator(_)
        );

        if is_comparison {
            let op = match self.peek() {
                Token::Symbol('=') => ast::BinaryOperator::Equal,
                Token::Symbol('<') => ast::BinaryOperator::LessThan,
                Token::Symbol('>') => ast::BinaryOperator::GreaterThan,
                Token::Operator(ref s) => match s.as_str() {
                    "<=" => ast::BinaryOperator::LessThanOrEqual,
                    ">=" => ast::BinaryOperator::GreaterThanOrEqual,
                    "!=" => ast::BinaryOperator::NotEqual,
                    "<>" => ast::BinaryOperator::NotEqual, // SQL standard
                    _ => return Err(ParseError { message: format!("Unexpected operator: {}", s) }),
                },
                _ => unreachable!(),
            };
            self.advance();

            let right = self.parse_primary_expression()?;
            left = ast::Expression::BinaryOp { op, left: Box::new(left), right: Box::new(right) };
        }

        Ok(left)
    }

    /// Parse primary expression (literals, identifiers, parenthesized expressions)
    fn parse_primary_expression(&mut self) -> Result<ast::Expression, ParseError> {
        match self.peek() {
            Token::Number(n) => {
                let num_str = n.clone();
                self.advance();

                // Try to parse as integer first
                if let Ok(i) = num_str.parse::<i64>() {
                    Ok(ast::Expression::Literal(types::SqlValue::Integer(i)))
                } else {
                    // For now, store as numeric string
                    Ok(ast::Expression::Literal(types::SqlValue::Numeric(num_str)))
                }
            }
            Token::String(s) => {
                let string_val = s.clone();
                self.advance();
                Ok(ast::Expression::Literal(types::SqlValue::Varchar(string_val)))
            }
            Token::Keyword(Keyword::True) => {
                self.advance();
                Ok(ast::Expression::Literal(types::SqlValue::Boolean(true)))
            }
            Token::Keyword(Keyword::False) => {
                self.advance();
                Ok(ast::Expression::Literal(types::SqlValue::Boolean(false)))
            }
            Token::Keyword(Keyword::Null) => {
                self.advance();
                Ok(ast::Expression::Literal(types::SqlValue::Null))
            }
            Token::Identifier(id) => {
                let first = id.clone();
                self.advance();

                // Check for function call (identifier followed by '(')
                if matches!(self.peek(), Token::LParen) {
                    self.advance(); // consume '('

                    // Parse function arguments
                    let mut args = Vec::new();

                    // Check for empty argument list or '*'
                    if matches!(self.peek(), Token::RParen) {
                        // No arguments: func()
                        self.advance();
                        return Ok(ast::Expression::Function { name: first, args });
                    } else if matches!(self.peek(), Token::Symbol('*')) {
                        // Special case for COUNT(*)
                        self.advance(); // consume '*'
                        self.expect_token(Token::RParen)?;
                        // Represent * as a special wildcard expression
                        args.push(ast::Expression::ColumnRef {
                            table: None,
                            column: "*".to_string(),
                        });
                        return Ok(ast::Expression::Function { name: first, args });
                    }

                    // Parse comma-separated argument list
                    loop {
                        let arg = self.parse_expression()?;
                        args.push(arg);

                        if matches!(self.peek(), Token::Comma) {
                            self.advance();
                        } else {
                            break;
                        }
                    }

                    self.expect_token(Token::RParen)?;
                    Ok(ast::Expression::Function { name: first, args })
                }
                // Check for qualified column reference (table.column)
                else if matches!(self.peek(), Token::Symbol('.')) {
                    self.advance(); // consume '.'
                    match self.peek() {
                        Token::Identifier(col) => {
                            let column = col.clone();
                            self.advance();
                            Ok(ast::Expression::ColumnRef { table: Some(first), column })
                        }
                        _ => Err(ParseError {
                            message: "Expected column name after '.'".to_string(),
                        }),
                    }
                } else {
                    // Simple column reference
                    Ok(ast::Expression::ColumnRef { table: None, column: first })
                }
            }
            Token::LParen => {
                self.advance(); // consume '('

                // Check if this is a scalar subquery by peeking at next token
                if self.peek_keyword(Keyword::Select) {
                    // It's a scalar subquery: (SELECT ...)
                    let select_stmt = self.parse_select_statement()?;
                    self.expect_token(Token::RParen)?;
                    Ok(ast::Expression::ScalarSubquery(Box::new(select_stmt)))
                } else {
                    // Regular parenthesized expression: (expr)
                    let expr = self.parse_expression()?;
                    self.expect_token(Token::RParen)?;
                    Ok(expr)
                }
            }
            _ => {
                Err(ParseError { message: format!("Expected expression, found {:?}", self.peek()) })
            }
        }
    }

    /// Parse a subquery: (SELECT ...)
    /// Used for scalar subqueries and IN operator
    pub(super) fn parse_subquery(&mut self) -> Result<ast::SelectStmt, ParseError> {
        // Expect opening paren
        self.expect_token(Token::LParen)?;

        // Parse SELECT statement
        let select_stmt = self.parse_select_statement()?;

        // Expect closing paren
        self.expect_token(Token::RParen)?;

        Ok(select_stmt)
    }
}
