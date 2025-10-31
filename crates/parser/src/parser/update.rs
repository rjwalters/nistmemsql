use super::*;

impl Parser {
    /// Parse UPDATE statement
    pub(super) fn parse_update_statement(&mut self) -> Result<ast::UpdateStmt, ParseError> {
        self.expect_keyword(Keyword::Update)?;

        // Parse table name
        let table_name = match self.peek() {
            Token::Identifier(name) => {
                let table = name.clone();
                self.advance();
                table
            }
            _ => {
                return Err(ParseError { message: "Expected table name after UPDATE".to_string() })
            }
        };

        // Parse SET keyword
        self.expect_keyword(Keyword::Set)?;

        // Parse assignments
        let mut assignments = Vec::new();
        loop {
            // Parse column name
            let column = match self.peek() {
                Token::Identifier(col) => {
                    let c = col.clone();
                    self.advance();
                    c
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected column name in SET clause".to_string(),
                    })
                }
            };

            // Expect =
            self.expect_token(Token::Symbol('='))?;

            // Parse value expression
            let value = self.parse_expression()?;

            assignments.push(ast::Assignment { column, value });

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        // Parse optional WHERE clause
        let where_clause = if self.peek_keyword(Keyword::Where) {
            self.consume_keyword(Keyword::Where)?;
            // Check for WHERE CURRENT OF cursor_name
            if self.try_consume_keyword(Keyword::Current) {
                self.expect_keyword(Keyword::Of)?;
                let cursor_name = self.parse_identifier()?;
                Some(ast::WhereClause::CurrentOf(cursor_name))
            } else {
                Some(ast::WhereClause::Condition(self.parse_expression()?))
            }
        } else {
            None
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(ast::UpdateStmt { table_name, assignments, where_clause })
    }
}
