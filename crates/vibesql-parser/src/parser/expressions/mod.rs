use super::*;

// Submodules
mod functions;
mod identifiers;
mod literals;
mod operators;
mod special_forms;
mod subqueries;

impl Parser {
    /// Parse an expression (entry point)
    pub(super) fn parse_expression(&mut self) -> Result<vibesql_ast::Expression, ParseError> {
        self.parse_or_expression()
    }

    /// Parse primary expression (literals, identifiers, parenthesized expressions)
    pub(super) fn parse_primary_expression(&mut self) -> Result<vibesql_ast::Expression, ParseError> {
        // Try to parse as a literal
        if let Some(expr) = self.parse_literal()? {
            return Ok(expr);
        }

        // Try to parse as a special form (CAST, EXISTS, NOT EXISTS)
        if let Some(expr) = self.parse_special_form()? {
            return Ok(expr);
        }

        // Try to parse as current date/time functions
        if let Some(expr) = self.parse_current_datetime_function()? {
            return Ok(expr);
        }

        // Try to parse as NEXT VALUE FOR sequence expression
        if let Some(expr) = self.parse_sequence_value_function()? {
            return Ok(expr);
        }

        // Try to parse as a function call
        if let Some(expr) = self.parse_function_call()? {
            return Ok(expr);
        }

        // Try to parse as an identifier (column reference or qualified name)
        if let Some(expr) = self.parse_identifier_expression()? {
            return Ok(expr);
        }

        // Try to parse as a parenthesized expression or scalar subquery
        if let Some(expr) = self.parse_parenthesized()? {
            return Ok(expr);
        }

        Err(ParseError { message: format!("Expected expression, found {:?}", self.peek()) })
    }
}
