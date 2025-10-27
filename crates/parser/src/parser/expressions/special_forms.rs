use super::*;

impl Parser {
    /// Parse special SQL forms (CAST, EXISTS, NOT EXISTS)
    pub(super) fn parse_special_form(&mut self) -> Result<Option<ast::Expression>, ParseError> {
        match self.peek() {
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

                    Ok(Some(ast::Expression::Exists { subquery: Box::new(subquery), negated: true }))
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
