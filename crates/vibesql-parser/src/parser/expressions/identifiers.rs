use super::*;

impl Parser {
    /// Parse identifier-based expressions (column references, qualified names)
    pub(super) fn parse_identifier_expression(
        &mut self,
    ) -> Result<Option<vibesql_ast::Expression>, ParseError> {
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
                if matches!(self.peek(), Token::Symbol('.')) {
                    self.advance(); // consume '.'
                    match self.peek() {
                        Token::Identifier(col) | Token::DelimitedIdentifier(col) => {
                            let column = col.clone();
                            self.advance();

                            // Check for pseudo-variable (OLD.column or NEW.column)
                            let first_upper = first.to_uppercase();
                            if first_upper == "OLD" || first_upper == "NEW" {
                                let pseudo_table = if first_upper == "OLD" {
                                    vibesql_ast::PseudoTable::Old
                                } else {
                                    vibesql_ast::PseudoTable::New
                                };
                                Ok(Some(vibesql_ast::Expression::PseudoVariable {
                                    pseudo_table,
                                    column,
                                }))
                            } else {
                                Ok(Some(vibesql_ast::Expression::ColumnRef { table: Some(first), column }))
                            }
                        }
                        _ => Err(ParseError {
                            message: "Expected column name after '.'".to_string(),
                        }),
                    }
                } else {
                    // Simple column reference
                    Ok(Some(vibesql_ast::Expression::ColumnRef { table: None, column: first }))
                }
            }
            _ => Ok(None),
        }
    }
}
