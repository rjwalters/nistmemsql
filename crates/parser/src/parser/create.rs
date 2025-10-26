use super::*;

impl Parser {
    /// Parse CREATE TABLE statement
    pub(super) fn parse_create_table_statement(
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

        // Parse column definitions
        self.expect_token(Token::LParen)?;
        let mut columns = Vec::new();

        loop {
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

            columns.push(ast::ColumnDef { name, data_type, nullable });

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

        Ok(ast::CreateTableStmt { table_name, columns })
    }

    /// Parse data type
    fn parse_data_type(&mut self) -> Result<types::DataType, ParseError> {
        let type_upper = match self.peek() {
            Token::Identifier(type_name) => type_name.to_uppercase(),
            _ => return Err(ParseError { message: "Expected data type".to_string() }),
        };
        self.advance();

        match type_upper.as_str() {
            "INTEGER" | "INT" => Ok(types::DataType::Integer),
            "SMALLINT" => Ok(types::DataType::Smallint),
            "BIGINT" => Ok(types::DataType::Bigint),
            "BOOLEAN" | "BOOL" => Ok(types::DataType::Boolean),
            "DATE" => Ok(types::DataType::Date),
            "VARCHAR" => {
                // Parse VARCHAR(n)
                self.expect_token(Token::LParen)?;
                let max_length = match self.peek() {
                    Token::Number(n) => {
                        let len = n.parse::<usize>().map_err(|_| ParseError {
                            message: "Invalid VARCHAR length".to_string(),
                        })?;
                        self.advance();
                        len
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected number after VARCHAR(".to_string(),
                        })
                    }
                };
                self.expect_token(Token::RParen)?;
                Ok(types::DataType::Varchar { max_length })
            }
            "CHAR" | "CHARACTER" => {
                // Parse CHAR(n)
                self.expect_token(Token::LParen)?;
                let length = match self.peek() {
                    Token::Number(n) => {
                        let len = n.parse::<usize>().map_err(|_| ParseError {
                            message: "Invalid CHAR length".to_string(),
                        })?;
                        self.advance();
                        len
                    }
                    _ => {
                        return Err(ParseError {
                            message: "Expected number after CHAR(".to_string(),
                        })
                    }
                };
                self.expect_token(Token::RParen)?;
                Ok(types::DataType::Character { length })
            }
            _ => Err(ParseError { message: format!("Unknown data type: {}", type_upper) }),
        }
    }
}
