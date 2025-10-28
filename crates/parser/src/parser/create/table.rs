//! CREATE TABLE parsing

use super::super::*;

impl Parser {
    /// Parse CREATE TABLE statement
    pub(in crate::parser) fn parse_create_table_statement(
        &mut self,
    ) -> Result<ast::CreateTableStmt, ParseError> {
        self.expect_keyword(Keyword::Create)?;
        self.expect_keyword(Keyword::Table)?;

        // Parse table name
        let table_name = match self.peek() {
            Token::Identifier(name) => {
                let table = name.clone();
                self.advance();
                table
            }
            _ => {
                return Err(ParseError {
                    message: "Expected table name after CREATE TABLE".to_string(),
                })
            }
        };

        // Parse column definitions and table constraints
        self.expect_token(Token::LParen)?;
        let mut columns = Vec::new();
        let mut table_constraints = Vec::new();

        loop {
            // Check if this is a table-level constraint
            if self.peek_keyword(Keyword::Primary)
                || self.peek_keyword(Keyword::Foreign)
                || self.peek_keyword(Keyword::Unique)
                || self.peek_keyword(Keyword::Check)
            {
                table_constraints.push(self.parse_table_constraint()?);
                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                    continue;
                } else {
                    break;
                }
            }

            // Parse column name
            let name = match self.peek() {
                Token::Identifier(col) => {
                    let c = col.clone();
                    self.advance();
                    c
                }
                _ => return Err(ParseError { message: "Expected column name".to_string() }),
            };

            // Parse data type
            let data_type = self.parse_data_type()?;

            // Parse optional NOT NULL (default is nullable)
            let nullable = if self.peek_keyword(Keyword::Not) {
                self.consume_keyword(Keyword::Not)?;
                self.expect_keyword(Keyword::Null)?;
                false
            } else {
                true
            };

            // Parse column constraints
            let constraints = self.parse_column_constraints()?;

            columns.push(ast::ColumnDef {
                name,
                data_type,
                nullable,
                constraints,
            });

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        self.expect_token(Token::RParen)?;

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(ast::CreateTableStmt {
            table_name,
            columns,
            table_constraints,
        })
    }
}
