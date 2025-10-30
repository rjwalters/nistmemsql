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

    /// Parse a single SELECT item
    pub(crate) fn parse_select_item(&mut self) -> Result<ast::SelectItem, ParseError> {
        // Check for wildcard (*)
        if matches!(self.peek(), Token::Symbol('*')) {
            self.advance();
            return Ok(ast::SelectItem::Wildcard);
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
