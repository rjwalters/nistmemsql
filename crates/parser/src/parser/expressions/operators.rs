use super::*;

impl Parser {
    /// Parse OR expression (lowest precedence)
    pub(super) fn parse_or_expression(&mut self) -> Result<ast::Expression, ParseError> {
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
    pub(super) fn parse_and_expression(&mut self) -> Result<ast::Expression, ParseError> {
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

    /// Parse additive expression (handles +, -, and ||)
    pub(super) fn parse_additive_expression(&mut self) -> Result<ast::Expression, ParseError> {
        let mut left = self.parse_multiplicative_expression()?;

        loop {
            let op = match self.peek() {
                Token::Symbol('+') => ast::BinaryOperator::Plus,
                Token::Symbol('-') => ast::BinaryOperator::Minus,
                Token::Operator(ref s) if s == "||" => ast::BinaryOperator::Concat,
                _ => break,
            };
            self.advance();

            let right = self.parse_multiplicative_expression()?;
            left = ast::Expression::BinaryOp { op, left: Box::new(left), right: Box::new(right) };
        }

        Ok(left)
    }

    /// Parse multiplicative expression (handles * and /)
    pub(super) fn parse_multiplicative_expression(&mut self) -> Result<ast::Expression, ParseError> {
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
    pub(super) fn parse_comparison_expression(&mut self) -> Result<ast::Expression, ParseError> {
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
                // Note: NOT EXISTS is handled in parse_primary_expression()
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
        // Note: Exclude || (concat) operator which should be handled in additive expression
        let is_comparison = match self.peek() {
            Token::Symbol('=') | Token::Symbol('<') | Token::Symbol('>') => true,
            Token::Operator(ref s) => matches!(s.as_str(), "<=" | ">=" | "!=" | "<>"),
            _ => false,
        };

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

            // Check for quantified comparison (ALL, ANY, SOME)
            if self.peek_keyword(Keyword::All)
                || self.peek_keyword(Keyword::Any)
                || self.peek_keyword(Keyword::Some)
            {
                let quantifier = if self.peek_keyword(Keyword::All) {
                    self.consume_keyword(Keyword::All)?;
                    ast::Quantifier::All
                } else if self.peek_keyword(Keyword::Any) {
                    self.consume_keyword(Keyword::Any)?;
                    ast::Quantifier::Any
                } else {
                    self.consume_keyword(Keyword::Some)?;
                    ast::Quantifier::Some
                };

                // Expect opening paren
                self.expect_token(Token::LParen)?;

                // Parse subquery
                let subquery = self.parse_select_statement()?;

                // Expect closing paren
                self.expect_token(Token::RParen)?;

                return Ok(ast::Expression::QuantifiedComparison {
                    expr: Box::new(left),
                    op,
                    quantifier,
                    subquery: Box::new(subquery),
                });
            }

            let right = self.parse_primary_expression()?;
            left = ast::Expression::BinaryOp { op, left: Box::new(left), right: Box::new(right) };
        }

        Ok(left)
    }

    /// Parse a comma-separated list of expressions
    /// Used for IN (val1, val2, ...) and function arguments
    /// Does NOT consume the opening or closing parentheses
    pub(super) fn parse_expression_list(&mut self) -> Result<Vec<ast::Expression>, ParseError> {
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
