//! Data type parsing

use super::super::*;

impl Parser {
    /// Parse data type
    pub(in crate::parser) fn parse_data_type(&mut self) -> Result<types::DataType, ParseError> {
        let type_upper = match self.peek() {
            Token::Identifier(type_name) => type_name.to_uppercase(),
            Token::Keyword(Keyword::Date) => "DATE".to_string(),
            Token::Keyword(Keyword::Time) => "TIME".to_string(),
            Token::Keyword(Keyword::Timestamp) => "TIMESTAMP".to_string(),
            Token::Keyword(Keyword::Interval) => "INTERVAL".to_string(),
            Token::Keyword(Keyword::Character) => "CHARACTER".to_string(),
            _ => return Err(ParseError { message: "Expected data type".to_string() }),
        };
        self.advance();

        match type_upper.as_str() {
        "INTEGER" | "INT" => Ok(types::DataType::Integer),
        "SIGNED" => Ok(types::DataType::Integer), // MySQL-specific: SIGNED is equivalent to INTEGER
        "SMALLINT" => Ok(types::DataType::Smallint),
            "BIGINT" => Ok(types::DataType::Bigint),
            "BOOLEAN" | "BOOL" => Ok(types::DataType::Boolean),
            "FLOAT" => {
                // Parse FLOAT(precision) or FLOAT
                // SQL:1999 allows FLOAT with optional precision parameter
                if matches!(self.peek(), Token::LParen) {
                    self.advance(); // consume (
                    let precision = match self.peek() {
                        Token::Number(n) => {
                            let p = n.parse::<u8>().map_err(|_| ParseError {
                                message: "Invalid FLOAT precision".to_string(),
                            })?;
                            self.advance();
                            p
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected precision after FLOAT(".to_string(),
                            })
                        }
                    };
                    self.expect_token(Token::RParen)?;
                    Ok(types::DataType::Float { precision })
                } else {
                    // FLOAT without parameters defaults to 53-bit precision (IEEE 754 double)
                    Ok(types::DataType::Float { precision: 53 })
                }
            }
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
            "NUMERIC" | "DECIMAL" | "DEC" => {
                // Parse NUMERIC(precision, scale) or NUMERIC(precision)
                // NUMERIC, DECIMAL, and DEC are all aliases per SQL:1999
                // All map to DataType::Numeric internally
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

                    // DEC, DECIMAL, and NUMERIC all map to DataType::Numeric
                    Ok(types::DataType::Numeric { precision, scale })
                } else {
                    // NUMERIC/DECIMAL/DEC without parameters - use defaults (38, 0) per SQL standard
                    Ok(types::DataType::Numeric { precision: 38, scale: 0 })
                }
            }
            "DATE" => Ok(types::DataType::Date),
            "NAME" => Ok(types::DataType::Name),
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
                let end_field = match self.peek() {
                    Token::Keyword(Keyword::To) => {
                        self.advance(); // consume TO keyword
                        Some(self.parse_interval_field()?)
                    }
                    Token::Identifier(word) if word.to_uppercase() == "TO" => {
                        self.advance(); // consume TO identifier (backward compat)
                        Some(self.parse_interval_field()?)
                    }
                    _ => None,
                };

                Ok(types::DataType::Interval { start_field, end_field })
            }
            "VARCHAR" => {
                // Parse VARCHAR or VARCHAR(n) or VARCHAR(n CHARACTERS) or VARCHAR(n OCTETS)
                // Length is optional - if not specified, defaults to None (unlimited)
                let max_length = if matches!(self.peek(), Token::LParen) {
                    self.advance(); // consume LParen
                    let len = match self.peek() {
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

                    // Check for CHARACTERS or OCTETS modifier
                    // For MVP, we accept both but treat them the same (as character count)
                    if self.try_consume_keyword(Keyword::Characters)
                        || self.try_consume_keyword(Keyword::Octets)
                    {
                        // Modifier consumed, continue
                    }

                    self.expect_token(Token::RParen)?;
                    Some(len)
                } else {
                    None // No length specified, use default
                };
                Ok(types::DataType::Varchar { max_length })
            }
            "CHAR" | "CHARACTER" => {
                // Check for VARYING keyword (CHARACTER VARYING = VARCHAR)
                // Also support deprecated VARING identifier (SQL:1999 conformance tests)
                let is_varying = self.try_consume_keyword(Keyword::Varying);
                let is_varing = if !is_varying {
                    // Check for VARING as identifier (deprecated SQL:1999 variant)
                    if let Token::Identifier(next) = self.peek() {
                        if next.to_uppercase() == "VARING" {
                            self.advance(); // consume VARING
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                };

                if is_varying || is_varing {
                    // Parse as VARCHAR (CHARACTER VARYING or CHAR VARING)
                    let max_length = if self.peek() == &Token::LParen {
                        self.advance();
                        let len = match self.peek() {
                            Token::Number(n) => {
                                let parsed = n.parse::<usize>().map_err(|_| ParseError {
                                    message: "Invalid VARCHAR length".to_string(),
                                })?;
                                self.advance();
                                Some(parsed)
                            }
                            _ => {
                                return Err(ParseError {
                                    message: "Expected number after CHARACTER VARYING(".to_string(),
                                })
                            }
                        };

                        // Check for CHARACTERS or OCTETS modifier
                        if self.try_consume_keyword(Keyword::Characters)
                            || self.try_consume_keyword(Keyword::Octets)
                        {
                            // Modifier consumed, continue
                        }

                        self.expect_token(Token::RParen)?;
                        len
                    } else {
                        None // No length specified, use default
                    };
                    return Ok(types::DataType::Varchar { max_length });
                }

                // Otherwise parse as CHAR
                // Length is optional - if not specified, defaults to 1 per SQL:1999
                let length = if matches!(self.peek(), Token::LParen) {
                    self.advance(); // consume (
                    let len = match self.peek() {
                        Token::Number(n) => {
                            let parsed = n.parse::<usize>().map_err(|_| ParseError {
                                message: "Invalid CHAR length".to_string(),
                            })?;
                            self.advance();
                            parsed
                        }
                        _ => {
                            return Err(ParseError {
                                message: "Expected number after CHAR(".to_string(),
                            })
                        }
                    };

                    // Check for CHARACTERS or OCTETS modifier
                    if self.try_consume_keyword(Keyword::Characters)
                        || self.try_consume_keyword(Keyword::Octets)
                    {
                        // Modifier consumed, continue
                    }

                    self.expect_token(Token::RParen)?;
                    len
                } else {
                    1 // Default length is 1 per SQL:1999 standard
                };

                Ok(types::DataType::Character { length })
            }
            "TEXT" => {
                // TEXT is SQLite-style unlimited VARCHAR
                // Maps to VARCHAR without length constraint (unlimited)
                Ok(types::DataType::Varchar { max_length: None })
            }
            _ => {
                // Check if this is a spatial/geometric type (SQL/MM standard)
                // These are outside SQL:1999 scope but should parse gracefully as user-defined types
                if Self::is_spatial_type(&type_upper) {
                    Ok(types::DataType::UserDefined { type_name: type_upper })
                } else {
                    Err(ParseError { message: format!("Unknown data type: {}", type_upper) })
                }
            }
        }
    }

    /// Check if a type name is a spatial/geometric type from SQL/MM standard
    /// These types are not part of SQL:1999 but appear in SQLLogicTest suite
    fn is_spatial_type(type_name: &str) -> bool {
        matches!(
            type_name,
            // 2D basic types
            "POINT" | "LINESTRING" | "POLYGON" |
            // Multi types
            "MULTIPOINT" | "MULTILINESTRING" | "MULTIPOLYGON" |
            // Collection types
            "GEOMETRY" | "GEOMETRYCOLLECTION"
        )
    }

    /// Parse interval field (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)
    pub(in crate::parser) fn parse_interval_field(
        &mut self,
    ) -> Result<types::IntervalField, ParseError> {
        let field_upper = match self.peek() {
            Token::Identifier(field) => field.to_uppercase(),
            Token::Keyword(Keyword::Hour) => "HOUR".to_string(),
            Token::Keyword(Keyword::Minute) => "MINUTE".to_string(),
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
            _ => Err(ParseError { message: format!("Unknown interval field: {}", field_upper) }),
        }
    }

    /// Parse optional timezone modifier (WITH TIME ZONE or WITHOUT TIME ZONE)
    /// Returns true if WITH TIME ZONE, false if WITHOUT TIME ZONE or no modifier
    pub(in crate::parser) fn parse_timezone_modifier(&mut self) -> Result<bool, ParseError> {
        // Check for WITH keyword
        if matches!(self.peek(), Token::Keyword(Keyword::With)) {
            self.advance(); // consume WITH

            // Expect TIME keyword
            self.expect_keyword(Keyword::Time)?;

            // Expect ZONE keyword
            self.expect_keyword(Keyword::Zone)?;
            return Ok(true); // WITH TIME ZONE
        }

        // Check for WITHOUT keyword
        if matches!(self.peek(), Token::Keyword(Keyword::Without)) {
            self.advance(); // consume WITHOUT

            // Expect TIME keyword
            self.expect_keyword(Keyword::Time)?;

            // Expect ZONE keyword
            self.expect_keyword(Keyword::Zone)?;
            return Ok(false); // WITHOUT TIME ZONE
        }

        // No timezone modifier - default to WITHOUT TIME ZONE
        Ok(false)
    }
}
