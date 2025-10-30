use super::*;

impl Parser {
    /// Peek at current token without consuming.
    pub(super) fn peek(&self) -> &Token {
        if self.position < self.tokens.len() {
            &self.tokens[self.position]
        } else {
            &Token::Eof
        }
    }

    /// Peek at next token (position + 1) without consuming.
    pub(super) fn peek_next(&self) -> &Token {
        if self.position + 1 < self.tokens.len() {
            &self.tokens[self.position + 1]
        } else {
            &Token::Eof
        }
    }

    /// Advance to next token.
    pub(super) fn advance(&mut self) {
        if self.position < self.tokens.len() {
            self.position += 1;
        }
    }

    /// Check if current token is a specific keyword.
    pub(super) fn peek_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.peek(), Token::Keyword(k) if k == &keyword)
    }

    /// Check if next token is a specific keyword.
    pub(super) fn peek_next_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.peek_next(), Token::Keyword(k) if k == &keyword)
    }

    /// Expect and consume a specific keyword.
    pub(super) fn expect_keyword(&mut self, keyword: Keyword) -> Result<(), ParseError> {
        if self.peek_keyword(keyword.clone()) {
            self.advance();
            Ok(())
        } else {
            Err(ParseError {
                message: format!("Expected keyword {:?}, found {:?}", keyword, self.peek()),
            })
        }
    }

    /// Consume a specific keyword.
    pub(super) fn consume_keyword(&mut self, keyword: Keyword) -> Result<(), ParseError> {
        self.expect_keyword(keyword)
    }

    /// Expect a specific token.
    pub(super) fn expect_token(&mut self, expected: Token) -> Result<(), ParseError> {
        if self.peek() == &expected {
            self.advance();
            Ok(())
        } else {
            Err(ParseError { message: format!("Expected {:?}, found {:?}", expected, self.peek()) })
        }
    }

    /// Parse an identifier token.
    pub(super) fn parse_identifier(&mut self) -> Result<String, ParseError> {
        match self.peek() {
            Token::Identifier(name) => {
                let identifier = name.clone();
                self.advance();
                Ok(identifier)
            }
            _ => Err(ParseError {
                message: "Expected identifier".to_string(),
            }),
        }
    }

    /// Try to consume a keyword, returning true if successful.
    pub(super) fn try_consume_keyword(&mut self, keyword: Keyword) -> bool {
        if self.peek_keyword(keyword) {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Try to consume a specific token, returning true if successful.
    pub(super) fn try_consume(&mut self, token: &Token) -> bool {
        if self.peek() == token {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Parse a qualified identifier (schema.table or just table)
    pub(super) fn parse_qualified_identifier(&mut self) -> Result<String, ParseError> {
        // Parse first identifier
        let first_part = match self.peek() {
            Token::Identifier(name) => {
                let identifier = name.clone();
                self.advance();
                identifier
            }
            _ => {
                return Err(ParseError {
                    message: "Expected identifier".to_string(),
                })
            }
        };

        // Check if there's a dot followed by another identifier
        if self.peek() == &Token::Symbol('.') {
            self.advance(); // consume the dot
            let second_part = match self.peek() {
                Token::Identifier(name) => {
                    let identifier = name.clone();
                    self.advance();
                    identifier
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected identifier after '.'".to_string(),
                    })
                }
            };
            Ok(format!("{}.{}", first_part, second_part))
        } else {
            Ok(first_part)
        }
    }
}
