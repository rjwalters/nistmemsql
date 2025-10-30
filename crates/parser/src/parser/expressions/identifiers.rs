use super::*;

impl Parser {
    /// Parse identifier-based expressions (column references, qualified names)
    pub(super) fn parse_identifier_expression(
        &mut self,
    ) -> Result<Option<ast::Expression>, ParseError> {
        match self.peek() {
            Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                let first = id.clone();
                self.advance();

                // Check for function call (identifier followed by '(')
                if matches!(self.peek(), Token::LParen) {
                    // This is handled by parse_function_call, return None
                    // We need to rewind
                    self.position -= 1;
                    return Ok(None);
                }
                // Check for qualified column reference (table.column)
                else if matches!(self.peek(), Token::Symbol('.')) {
                    self.advance(); // consume '.'
                    match self.peek() {
                        Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                            let column = col.clone();
                            self.advance();
                            Ok(Some(ast::Expression::ColumnRef { table: Some(first), column }))
                        }
                        _ => Err(ParseError {
                            message: "Expected column name after '.'".to_string(),
                        }),
                    }
                } else {
                    // Simple column reference
                    Ok(Some(ast::Expression::ColumnRef { table: None, column: first }))
                }
            }
            _ => Ok(None),
        }
    }
}
