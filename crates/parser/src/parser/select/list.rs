use super::*;

impl Parser {
    /// Parse SELECT list (items after SELECT keyword)
    pub(crate) fn parse_select_list(&mut self) -> Result<Vec<ast::SelectItem>, ParseError> {
        let mut items = Vec::new();

        loop {
            let item = self.parse_select_item()?;
            items.push(item);

            // Check if there's a comma (more items)
            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(items)
    }

    /// Parse derived column list: AS (col1, col2, ...)
    /// SQL:1999 Feature E051-07/08
    fn parse_derived_column_list(&mut self) -> Result<Option<Vec<String>>, ParseError> {
        if !self.peek_keyword(Keyword::As) {
            return Ok(None);
        }

        self.consume_keyword(Keyword::As)?;
        self.expect_token(Token::Symbol('('))?;

        let mut columns = Vec::new();
        loop {
            match self.peek() {
                Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                    columns.push(id.clone());
                    self.advance();
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected column name in derived column list".to_string(),
                    })
                }
            }

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        self.expect_token(Token::Symbol(')'))?;

        if columns.is_empty() {
            return Err(ParseError {
                message: "Derived column list cannot be empty".to_string(),
            });
        }

        Ok(Some(columns))
    }

    /// Parse a single SELECT item
    pub(crate) fn parse_select_item(&mut self) -> Result<ast::SelectItem, ParseError> {
        // Check for qualified wildcard (table.* or alias.*)
        let saved_position = self.position;
        let qualifier = if let Token::Identifier(ref qualifier)
        | Token::DelimitedIdentifier(ref qualifier) = self.peek()
        {
            Some(qualifier.clone())
        } else {
            None
        };

        if let Some(qualifier) = qualifier {
            self.advance(); // consume identifier

            if matches!(self.peek(), Token::Symbol('.')) {
                self.advance(); // consume dot

                if matches!(self.peek(), Token::Symbol('*')) {
                    self.advance(); // consume asterisk
                    let alias = self.parse_derived_column_list()?;
                    return Ok(ast::SelectItem::QualifiedWildcard { qualifier, alias });
                }
            }
        }

        // Not a qualified wildcard, backtrack
        self.position = saved_position;

        // Check for wildcard (*)
        if matches!(self.peek(), Token::Symbol('*')) {
            self.advance();
            let alias = self.parse_derived_column_list()?;
            return Ok(ast::SelectItem::Wildcard { alias });
        }

        // Parse expression
        let expr = self.parse_expression()?;

        // Check for optional AS alias
        let alias = if self.peek_keyword(Keyword::As) {
            self.consume_keyword(Keyword::As)?;
            match self.peek() {
                Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                    let alias = id.clone();
                    self.advance();
                    Some(alias)
                }
                _ => {
                    return Err(ParseError { message: "Expected identifier after AS".to_string() })
                }
            }
        } else {
            None
        };

        Ok(ast::SelectItem::Expression { expr, alias })
    }
}
