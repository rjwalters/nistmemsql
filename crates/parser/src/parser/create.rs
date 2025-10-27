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
    pub(super) fn parse_data_type(&mut self) -> Result<types::DataType, ParseError> {
        let type_upper = match self.peek() {
            Token::Identifier(type_name) => type_name.to_uppercase(),
            Token::Keyword(Keyword::Date) => "DATE".to_string(),
            Token::Keyword(Keyword::Time) => "TIME".to_string(),
            Token::Keyword(Keyword::Timestamp) => "TIMESTAMP".to_string(),
            Token::Keyword(Keyword::Interval) => "INTERVAL".to_string(),
            _ => return Err(ParseError { message: "Expected data type".to_string() }),
        };
        self.advance();

        match type_upper.as_str() {
            "INTEGER" | "INT" => Ok(types::DataType::Integer),
            "SMALLINT" => Ok(types::DataType::Smallint),
            "BIGINT" => Ok(types::DataType::Bigint),
            "BOOLEAN" | "BOOL" => Ok(types::DataType::Boolean),
            "FLOAT" => Ok(types::DataType::Float),
            "REAL" => Ok(types::DataType::Real),
            "DOUBLE" => {
                // Check for DOUBLE PRECISION
                if let Token::Identifier(next) = self.peek() {
                    if next.to_uppercase() == "PRECISION" {
                        self.advance();
                        return Ok(types::DataType::DoublePrecision);
                    }
                }
                // Just DOUBLE without PRECISION - treat as DOUBLE PRECISION
                Ok(types::DataType::DoublePrecision)
            }
            "NUMERIC" | "DECIMAL" => {
                // Parse NUMERIC(precision, scale) or NUMERIC(precision)
                // NUMERIC without parameters defaults to implementation-defined precision/scale
                if matches!(self.peek(), Token::LParen) {
                    self.advance(); // consume (

                    let precision = match self.peek() {
                        Token::Number(n) => {
                            let p = n.parse::<u8>().map_err(|_| ParseError {
                                message: "Invalid NUMERIC precision".to_string(),
                            })?;
                            self.advance();
                            p
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected precision after NUMERIC(".to_string(),
                            })
                        }
                    };

                    let scale = if matches!(self.peek(), Token::Comma) {
                        self.advance(); // consume comma
                        match self.peek() {
                            Token::Number(n) => {
                                let s = n.parse::<u8>().map_err(|_| ParseError {
                                    message: "Invalid NUMERIC scale".to_string(),
                                })?;
                                self.advance();
                                s
                            }
                            _ => {
                                return Err(ParseError {
                                    message: "Expected scale after NUMERIC(precision,".to_string(),
                                })
                            }
                        }
                    } else {
                        0 // Default scale is 0
                    };

                    self.expect_token(Token::RParen)?;

                    if type_upper == "DECIMAL" {
                        Ok(types::DataType::Decimal { precision, scale })
                    } else {
                        Ok(types::DataType::Numeric { precision, scale })
                    }
                } else {
                    // NUMERIC without parameters - use defaults (38, 0) per SQL standard
                    if type_upper == "DECIMAL" {
                        Ok(types::DataType::Decimal { precision: 38, scale: 0 })
                    } else {
                        Ok(types::DataType::Numeric { precision: 38, scale: 0 })
                    }
                }
            }
            "DATE" => Ok(types::DataType::Date),
            "TIME" => {
                // Parse optional WITH TIME ZONE or WITHOUT TIME ZONE
                let with_timezone = self.parse_timezone_modifier()?;
                Ok(types::DataType::Time { with_timezone })
            }
            "TIMESTAMP" => {
                // Parse optional WITH TIME ZONE or WITHOUT TIME ZONE
                let with_timezone = self.parse_timezone_modifier()?;
                Ok(types::DataType::Timestamp { with_timezone })
            }
            "INTERVAL" => {
                // Parse INTERVAL start_field [TO end_field]
                let start_field = self.parse_interval_field()?;

                // Check for TO keyword (multi-field interval)
                let end_field = if let Token::Identifier(word) = self.peek() {
                    if word.to_uppercase() == "TO" {
                        self.advance(); // consume TO
                        Some(self.parse_interval_field()?)
                    } else {
                        None
                    }
                } else {
                    None
                };

                Ok(types::DataType::Interval { start_field, end_field })
            }
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

    /// Parse interval field (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)
    fn parse_interval_field(&mut self) -> Result<types::IntervalField, ParseError> {
        let field_upper = match self.peek() {
            Token::Identifier(field) => field.to_uppercase(),
            _ => {
                return Err(ParseError {
                    message: "Expected interval field (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)"
                        .to_string(),
                })
            }
        };
        self.advance();

        match field_upper.as_str() {
            "YEAR" => Ok(types::IntervalField::Year),
            "MONTH" => Ok(types::IntervalField::Month),
            "DAY" => Ok(types::IntervalField::Day),
            "HOUR" => Ok(types::IntervalField::Hour),
            "MINUTE" => Ok(types::IntervalField::Minute),
            "SECOND" => Ok(types::IntervalField::Second),
            _ => Err(ParseError {
                message: format!("Unknown interval field: {}", field_upper),
            }),
        }
    }

    /// Parse optional timezone modifier (WITH TIME ZONE or WITHOUT TIME ZONE)
    /// Returns true if WITH TIME ZONE, false if WITHOUT TIME ZONE or no modifier
    fn parse_timezone_modifier(&mut self) -> Result<bool, ParseError> {
        if let Token::Identifier(word) = self.peek() {
            let word_upper = word.to_uppercase();
            if word_upper == "WITH" {
                self.advance(); // consume WITH

                // Expect TIME (as keyword or identifier)
                let is_time = match self.peek() {
                    Token::Keyword(Keyword::Time) => true,
                    Token::Identifier(next) if next.to_uppercase() == "TIME" => true,
                    _ => false,
                };

                if is_time {
                    self.advance(); // consume TIME

                    // Expect ZONE
                    if let Token::Identifier(zone) = self.peek() {
                        if zone.to_uppercase() == "ZONE" {
                            self.advance(); // consume ZONE
                            return Ok(true); // WITH TIME ZONE
                        }
                    }
                    return Err(ParseError {
                        message: "Expected ZONE after WITH TIME".to_string(),
                    });
                }
                return Err(ParseError {
                    message: "Expected TIME after WITH".to_string(),
                });
            } else if word_upper == "WITHOUT" {
                self.advance(); // consume WITHOUT

                // Expect TIME (as keyword or identifier)
                let is_time = match self.peek() {
                    Token::Keyword(Keyword::Time) => true,
                    Token::Identifier(next) if next.to_uppercase() == "TIME" => true,
                    _ => false,
                };

                if is_time {
                    self.advance(); // consume TIME

                    // Expect ZONE
                    if let Token::Identifier(zone) = self.peek() {
                        if zone.to_uppercase() == "ZONE" {
                            self.advance(); // consume ZONE
                            return Ok(false); // WITHOUT TIME ZONE
                        }
                    }
                    return Err(ParseError {
                        message: "Expected ZONE after WITHOUT TIME".to_string(),
                    });
                }
                return Err(ParseError {
                    message: "Expected TIME after WITHOUT".to_string(),
                });
            }
        }

        // No timezone modifier - default to WITHOUT TIME ZONE
        Ok(false)
    }
}
