use super::*;

impl Parser {
    /// Parse INSERT statement
    pub(super) fn parse_insert_statement(&mut self) -> Result<ast::InsertStmt, ParseError> {
        self.expect_keyword(Keyword::Insert)?;
        self.expect_keyword(Keyword::Into)?;

        // Parse table name
        let table_name = match self.peek() {
            Token::Identifier(name) => {
                let table = name.clone();
                self.advance();
                table
            }
            _ => {
                return Err(ParseError {
                    message: "Expected table name after INSERT INTO".to_string(),
                })
            }
        };

        // Parse column list (optional in SQL, but we'll require it for now)
        let columns = if matches!(self.peek(), Token::LParen) {
            self.advance(); // consume (
            let mut cols = Vec::new();
            loop {
                match self.peek() {
                    Token::Identifier(col) => {
                        cols.push(col.clone());
                        self.advance();
                    }
                    _ => return Err(ParseError { message: "Expected column name".to_string() }),
                }

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            self.expect_token(Token::RParen)?;
            cols
        } else {
            Vec::new() // No columns specified
        };

        // Parse VALUES
        self.expect_keyword(Keyword::Values)?;

        // Parse value lists
        let mut values = Vec::new();
        loop {
            self.expect_token(Token::LParen)?;
            let mut row = Vec::new();
            loop {
                let expr = self.parse_expression()?;
                row.push(expr);

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            self.expect_token(Token::RParen)?;
            values.push(row);

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(ast::InsertStmt { table_name, columns, values })
    }
}
