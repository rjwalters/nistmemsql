use super::*;

impl Parser {
    /// Parse INSERT statement
    pub(super) fn parse_insert_statement(&mut self) -> Result<vibesql_ast::InsertStmt, ParseError> {
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

        // Parse source: VALUES or SELECT
        let source = if self.peek_keyword(Keyword::Values) {
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
            vibesql_ast::InsertSource::Values(values)
        } else if self.peek_keyword(Keyword::Select) || self.peek_keyword(Keyword::With) {
            // Parse SELECT
            let select_stmt = self.parse_select_statement()?;
            vibesql_ast::InsertSource::Select(Box::new(select_stmt))
        } else {
            return Err(ParseError {
                message: "Expected VALUES or SELECT after INSERT".to_string(),
            });
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(vibesql_ast::InsertStmt { table_name, columns, source })
    }
}
