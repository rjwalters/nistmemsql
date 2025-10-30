use super::*;

impl Parser {
    /// Parse special SQL forms (CASE, CAST, EXISTS, NOT EXISTS)
    pub(super) fn parse_special_form(&mut self) -> Result<Option<ast::Expression>, ParseError> {
        match self.peek() {
            // CAST expression: CAST(expr AS data_type)
            // CASE expression: both simple and searched forms
            Token::Keyword(Keyword::Case) => {
                self.advance(); // consume CASE

                // Try to parse operand for simple CASE
                // If next token is WHEN, it's a searched CASE (no operand)
                let operand = if !self.peek_keyword(Keyword::When) {
                    Some(Box::new(self.parse_expression()?))
                } else {
                    None
                };

                // Parse WHEN clauses
                let mut when_clauses = Vec::new();
                while self.peek_keyword(Keyword::When) {
                    self.advance(); // consume WHEN
                    let condition = self.parse_expression()?;
                    self.expect_keyword(Keyword::Then)?;
                    let result = self.parse_expression()?;
                    when_clauses.push((condition, result));
                }

                // Ensure at least one WHEN clause exists
                if when_clauses.is_empty() {
                    return Err(ParseError {
                        message: "CASE expression requires at least one WHEN clause".to_string(),
                    });
                }

                // Parse optional ELSE clause
                let else_result = if self.peek_keyword(Keyword::Else) {
                    self.advance(); // consume ELSE
                    Some(Box::new(self.parse_expression()?))
                } else {
                    None
                };

                // Expect END keyword
                self.expect_keyword(Keyword::End)?;

                Ok(Some(ast::Expression::Case { operand, when_clauses, else_result }))
            }
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

                Ok(Some(ast::Expression::Cast { expr: Box::new(expr), data_type }))
            }
            // EXISTS expression: EXISTS (SELECT ...)
            Token::Keyword(Keyword::Exists) => {
                self.advance(); // consume EXISTS

                // Expect opening parenthesis
                self.expect_token(Token::LParen)?;

                // Parse the subquery (parse_select_statement will consume SELECT keyword)
                let subquery = self.parse_select_statement()?;

                // Expect closing parenthesis
                self.expect_token(Token::RParen)?;

                Ok(Some(ast::Expression::Exists { subquery: Box::new(subquery), negated: false }))
            }
            // DEFAULT keyword: DEFAULT
            Token::Keyword(Keyword::Default) => {
                self.advance(); // consume DEFAULT
                Ok(Some(ast::Expression::Default))
            }
            // NOT keyword - could be NOT EXISTS or unary NOT
            Token::Keyword(Keyword::Not) => {
                self.advance(); // consume NOT

                // Check if it's NOT EXISTS
                if self.peek_keyword(Keyword::Exists) {
                    self.advance(); // consume EXISTS

                    // Expect opening parenthesis
                    self.expect_token(Token::LParen)?;

                    // Parse the subquery
                    let subquery = self.parse_select_statement()?;

                    // Expect closing parenthesis
                    self.expect_token(Token::RParen)?;

                    Ok(Some(ast::Expression::Exists {
                        subquery: Box::new(subquery),
                        negated: true,
                    }))
                } else {
                    // It's a unary NOT operator on another expression
                    // Parse the inner expression
                    let expr = self.parse_primary_expression()?;

                    Ok(Some(ast::Expression::UnaryOp {
                        op: ast::UnaryOperator::Not,
                        expr: Box::new(expr),
                    }))
                }
            }
            _ => Ok(None),
        }
    }
}
