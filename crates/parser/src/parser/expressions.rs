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

                // Expect opening paren
                self.expect_token(Token::LParen)?;

                // Check if it's a subquery (SELECT ...) or a value list
                if self.peek_keyword(Keyword::Select) {
                    // It's a subquery: NOT IN (SELECT ...)
                    let subquery = self.parse_select_statement()?;
                    self.expect_token(Token::RParen)?;

                    return Ok(ast::Expression::In {
                        expr: Box::new(left),
                        subquery: Box::new(subquery),
                        negated: true,
                    });
                } else {
                    // It's a value list: NOT IN (val1, val2, ...)
                    let values = self.parse_expression_list()?;
                    self.expect_token(Token::RParen)?;

                    return Ok(ast::Expression::InList {
                        expr: Box::new(left),
                        values,
                        negated: true,
                    });
                }
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
            } else if self.peek_keyword(Keyword::Like) {
                // It's NOT LIKE
                self.consume_keyword(Keyword::Like)?;

                // Parse pattern expression
                let pattern = self.parse_additive_expression()?;

                return Ok(ast::Expression::Like {
                    expr: Box::new(left),
                    pattern: Box::new(pattern),
                    negated: true,
                });
            } else {
                // Not "NOT IN", "NOT BETWEEN", or "NOT LIKE", restore position and continue
                self.position = saved_pos;
            }
        } else if self.peek_keyword(Keyword::In) {
            // It's IN (not negated)
            self.consume_keyword(Keyword::In)?;

            // Expect opening paren
            self.expect_token(Token::LParen)?;

            // Check if it's a subquery (SELECT ...) or a value list
            if self.peek_keyword(Keyword::Select) {
                // It's a subquery: IN (SELECT ...)
                let subquery = self.parse_select_statement()?;
                self.expect_token(Token::RParen)?;

                return Ok(ast::Expression::In {
                    expr: Box::new(left),
                    subquery: Box::new(subquery),
                    negated: false,
                });
            } else {
                // It's a value list: IN (val1, val2, ...)
                let values = self.parse_expression_list()?;
                self.expect_token(Token::RParen)?;

                return Ok(ast::Expression::InList {
                    expr: Box::new(left),
                    values,
                    negated: false,
                });
            }
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
        } else if self.peek_keyword(Keyword::Like) {
            // It's LIKE (not negated)
            self.consume_keyword(Keyword::Like)?;

            // Parse pattern expression
            let pattern = self.parse_additive_expression()?;

            return Ok(ast::Expression::Like {
                expr: Box::new(left),
                pattern: Box::new(pattern),
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
            // Typed literals: DATE 'string', TIME 'string', TIMESTAMP 'string'
            Token::Keyword(Keyword::Date) => {
                self.advance();
                match self.peek() {
                    Token::String(s) => {
                        let date_str = s.clone();
                        self.advance();
                        Ok(ast::Expression::Literal(types::SqlValue::Date(date_str)))
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
                        Ok(ast::Expression::Literal(types::SqlValue::Time(time_str)))
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
                        Ok(ast::Expression::Literal(types::SqlValue::Timestamp(timestamp_str)))
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
                        let interval_spec = if let Token::Identifier(word) = self.peek() {
                            if word.to_uppercase() == "TO" {
                                self.advance(); // consume TO
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
                            } else {
                                format!("{} {}", value_str, start_field)
                            }
                        } else {
                            format!("{} {}", value_str, start_field)
                        };

                        Ok(ast::Expression::Literal(types::SqlValue::Interval(interval_spec)))
                    }
                    _ => Err(ParseError {
                        message: "Expected string literal after INTERVAL keyword".to_string(),
                    }),
                }
            }
            // CAST expression: CAST(expr AS data_type)
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

                Ok(ast::Expression::Cast { expr: Box::new(expr), data_type })
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

    /// Parse a comma-separated list of expressions
    /// Used for IN (val1, val2, ...) and function arguments
    /// Does NOT consume the opening or closing parentheses
    fn parse_expression_list(&mut self) -> Result<Vec<ast::Expression>, ParseError> {
        let mut expressions = Vec::new();

        // Empty list check
        if matches!(self.peek(), Token::RParen) {
            // Empty list - SQL standard requires at least one value in IN list
            return Err(ParseError {
                message: "Expected at least one value in list".to_string(),
            });
        }

        // Parse first expression
        expressions.push(self.parse_expression()?);

        // Parse remaining expressions
        while matches!(self.peek(), Token::Comma) {
            self.advance(); // consume comma
            expressions.push(self.parse_expression()?);
        }

        Ok(expressions)
    }
}
