use super::*;

impl Parser {
    /// Parse function call expressions
    pub(super) fn parse_function_call(&mut self) -> Result<Option<ast::Expression>, ParseError> {
        match self.peek() {
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
                        return Ok(Some(ast::Expression::Function { name: first, args }));
                    } else if matches!(self.peek(), Token::Symbol('*')) {
                        // Special case for COUNT(*)
                        self.advance(); // consume '*'
                        self.expect_token(Token::RParen)?;
                        // Represent * as a special wildcard expression
                        args.push(ast::Expression::ColumnRef {
                            table: None,
                            column: "*".to_string(),
                        });
                        return Ok(Some(ast::Expression::Function { name: first, args }));
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
                    Ok(Some(ast::Expression::Function { name: first, args }))
                } else {
                    // Not a function call, rewind
                    self.position -= 1;
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }
}
