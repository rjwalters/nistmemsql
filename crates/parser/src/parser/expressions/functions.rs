use super::*;

impl Parser {
    /// Parse function call expressions (including window functions)
    pub(super) fn parse_function_call(&mut self) -> Result<Option<ast::Expression>, ParseError> {
        // Try to match either an identifier or specific keywords that can be function names
        let function_name = match self.peek() {
            Token::Identifier(id) => {
                let name = id.clone();
                self.advance();
                // Check if followed by '('
                if !matches!(self.peek(), Token::LParen) {
                    // Not a function call - rewind
                    self.position -= 1;
                    return Ok(None);
                }
                name
            }
            // Allow LEFT and RIGHT keywords as function names
            // These are reserved for LEFT JOIN and RIGHT JOIN but can also be functions
            Token::Keyword(Keyword::Left) | Token::Keyword(Keyword::Right) => {
                // Peek ahead to see if this is followed by '('
                // Don't consume the keyword unless we're sure it's a function
                let keyword_name = match self.peek() {
                    Token::Keyword(Keyword::Left) => "LEFT",
                    Token::Keyword(Keyword::Right) => "RIGHT",
                    _ => unreachable!(),
                };

                // Look ahead to next token
                if self.position + 1 < self.tokens.len() {
                    if matches!(self.tokens[self.position + 1], Token::LParen) {
                        // Yes, it's a function call
                        self.advance(); // consume keyword
                        keyword_name.to_string()
                    } else {
                        // Not a function call, don't consume
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            }
            _ => return Ok(None),
        };

        self.advance(); // consume '('
        let first = function_name;

        // Special case for POSITION(substring IN string [USING unit])
        // SQL:1999 standard syntax
        if first.to_uppercase() == "POSITION" {
            // Parse substring at primary level (literals, identifiers, function calls)
            // to avoid IN operator consumption at comparison level
            let substring = self.parse_primary_expression()?;

            // Expect IN keyword
            self.expect_keyword(Keyword::In)?;

            // Parse string expression at primary level
            let string = self.parse_primary_expression()?;

            // Parse optional USING clause
            let character_unit = if matches!(self.peek(), Token::Keyword(Keyword::Using)) {
                self.advance(); // consume USING
                Some(self.parse_character_unit()?)
            } else {
                None
            };

            // Expect closing parenthesis
            self.expect_token(Token::RParen)?;

            return Ok(Some(ast::Expression::Position {
                substring: Box::new(substring),
                string: Box::new(string),
                character_unit,
            }));
        }

        // Special case for TRIM([position] [removal_char FROM] string)
        // SQL:1999 standard syntax
        // Forms:
        //   TRIM(string)                              -- remove spaces from both sides
        //   TRIM('x' FROM string)                     -- remove 'x' from both sides
        //   TRIM(BOTH 'x' FROM string)                -- remove 'x' from both sides
        //   TRIM(LEADING 'x' FROM string)             -- remove 'x' from start
        //   TRIM(TRAILING 'x' FROM string)            -- remove 'x' from end
        if first.to_uppercase() == "TRIM" {
            let mut position: Option<ast::TrimPosition> = None;
            let removal_char: Option<Box<ast::Expression>>;

            // Check for BOTH/LEADING/TRAILING keywords
            match self.peek() {
                Token::Keyword(Keyword::Both) => {
                    self.advance();
                    position = Some(ast::TrimPosition::Both);
                }
                Token::Keyword(Keyword::Leading) => {
                    self.advance();
                    position = Some(ast::TrimPosition::Leading);
                }
                Token::Keyword(Keyword::Trailing) => {
                    self.advance();
                    position = Some(ast::TrimPosition::Trailing);
                }
                _ => {}
            }

            // Try to parse the first expression (could be removal_char or string)
            let first_expr = self.parse_primary_expression()?;

            // Check if this is followed by FROM keyword
            if matches!(self.peek(), Token::Keyword(Keyword::From)) {
                self.advance(); // consume FROM
                                // first_expr is the removal_char, now parse the string
                removal_char = Some(Box::new(first_expr));
                let string = self.parse_primary_expression()?;

                self.expect_token(Token::RParen)?;

                return Ok(Some(ast::Expression::Trim {
                    position,
                    removal_char,
                    string: Box::new(string),
                }));
            } else {
                // No FROM keyword, so first_expr is the string
                // removal_char defaults to None (which means space)
                self.expect_token(Token::RParen)?;

                return Ok(Some(ast::Expression::Trim {
                    position,
                    removal_char: None,
                    string: Box::new(first_expr),
                }));
            }
        }

        // Special case for SUBSTRING(string FROM start [FOR length] [USING unit])
        // SQL:1999 standard syntax - alternative to comma syntax
        if first.to_uppercase() == "SUBSTRING" {
            // Parse the string expression
            let string_expr = self.parse_expression()?;

            // Check which syntax: comma or FROM keyword
            let (start_expr, length_expr) = if self.try_consume_keyword(Keyword::From) {
                // FROM/FOR syntax: SUBSTRING(string FROM start [FOR length])
                let start = self.parse_expression()?;
                let length = if self.try_consume_keyword(Keyword::For) {
                    Some(self.parse_expression()?)
                } else {
                    None
                };
                (start, length)
            } else {
                // Comma syntax: SUBSTRING(string, start [, length])
                self.expect_token(Token::Comma)?;
                let start = self.parse_expression()?;
                let length = if matches!(self.peek(), Token::Comma) {
                    self.advance(); // consume comma
                    Some(self.parse_expression()?)
                } else {
                    None
                };
                (start, length)
            };

            // Parse optional USING clause
            let character_unit = if matches!(self.peek(), Token::Keyword(Keyword::Using)) {
                self.advance(); // consume USING
                Some(self.parse_character_unit()?)
            } else {
                None
            };

            self.expect_token(Token::RParen)?;

            // For SUBSTRING, we represent it as a function call with 2 or 3 arguments
            // The executor will handle the semantics
            let mut args = vec![string_expr, start_expr];
            if let Some(length) = length_expr {
                args.push(length);
            }

            return Ok(Some(ast::Expression::Function { name: first, args, character_unit }));
        }

        // Check if this is an aggregate function
        let function_name_upper = first.to_uppercase();
        let is_aggregate =
            matches!(function_name_upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX");

        // Parse optional DISTINCT for aggregate functions
        let distinct = if is_aggregate && matches!(self.peek(), Token::Keyword(Keyword::Distinct)) {
            self.advance(); // consume DISTINCT
            true
        } else {
            false
        };

        // Parse function arguments
        let mut args = Vec::new();

        // Check for empty argument list or '*'
        if matches!(self.peek(), Token::RParen) {
            // No arguments: func()
            self.advance();
        } else if matches!(self.peek(), Token::Symbol('*')) {
            // Special case for COUNT(*)
            self.advance(); // consume '*'
            self.expect_token(Token::RParen)?;
            // Represent * as a special wildcard expression
            args.push(ast::Expression::ColumnRef { table: None, column: "*".to_string() });
        } else {
            // Parse comma-separated argument list
            loop {
                let arg = self.parse_expression()?;
                args.push(arg);

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }

            self.expect_token(Token::RParen)?;
        }

        // Parse optional USING clause for string functions
        let character_unit = if matches!(function_name_upper.as_str(), "CHARACTER_LENGTH" | "CHAR_LENGTH") {
            if matches!(self.peek(), Token::Keyword(Keyword::Using)) {
                self.advance(); // consume USING
                Some(self.parse_character_unit()?)
            } else {
                None
            }
        } else {
            None
        };

        // Check for OVER clause (window function)
        if matches!(self.peek(), Token::Keyword(Keyword::Over)) {
            self.advance(); // consume OVER

            // Parse window specification
            let window_spec = self.parse_window_spec()?;

            // Determine window function type based on function name
            let function_spec = self.classify_window_function(&first, args);

            return Ok(Some(ast::Expression::WindowFunction {
                function: function_spec,
                over: window_spec,
            }));
        }

        // Return appropriate expression type
        if is_aggregate {
            Ok(Some(ast::Expression::AggregateFunction { name: first, distinct, args }))
        } else {
            Ok(Some(ast::Expression::Function { name: first, args, character_unit }))
        }
    }

    /// Classify a function as aggregate, ranking, or value window function
    fn classify_window_function(
        &self,
        name: &str,
        args: Vec<ast::Expression>,
    ) -> ast::WindowFunctionSpec {
        let name_upper = name.to_uppercase();

        match name_upper.as_str() {
            // Ranking functions
            "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" => {
                ast::WindowFunctionSpec::Ranking { name: name_upper, args }
            }

            // Value functions
            "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" => {
                ast::WindowFunctionSpec::Value { name: name_upper, args }
            }

            // Aggregate functions (SUM, AVG, COUNT, MIN, MAX, etc.)
            _ => ast::WindowFunctionSpec::Aggregate { name: name_upper, args },
        }
    }

    /// Parse window specification (OVER clause contents)
    fn parse_window_spec(&mut self) -> Result<ast::WindowSpec, ParseError> {
        // OVER ( [PARTITION BY expr_list] [ORDER BY order_list] [frame_clause] )
        self.expect_token(Token::LParen)?;

        let mut partition_by = None;
        let mut order_by = None;
        let mut frame = None;

        // Check for empty OVER()
        if matches!(self.peek(), Token::RParen) {
            self.advance();
            return Ok(ast::WindowSpec { partition_by, order_by, frame });
        }

        // Parse PARTITION BY clause
        if matches!(self.peek(), Token::Keyword(Keyword::Partition)) {
            self.advance(); // consume PARTITION
            self.expect_keyword(Keyword::By)?;

            let mut expressions = vec![self.parse_expression()?];

            while matches!(self.peek(), Token::Comma) {
                self.advance();
                expressions.push(self.parse_expression()?);
            }

            partition_by = Some(expressions);
        }

        // Parse ORDER BY clause
        if matches!(self.peek(), Token::Keyword(Keyword::Order)) {
            self.advance(); // consume ORDER
            self.expect_keyword(Keyword::By)?;

            let mut order_items = Vec::new();
            loop {
                let expr = self.parse_expression()?;

                // Check for optional ASC/DESC
                let direction = if matches!(self.peek(), Token::Keyword(Keyword::Asc)) {
                    self.advance();
                    ast::OrderDirection::Asc
                } else if matches!(self.peek(), Token::Keyword(Keyword::Desc)) {
                    self.advance();
                    ast::OrderDirection::Desc
                } else {
                    ast::OrderDirection::Asc // Default
                };

                order_items.push(ast::OrderByItem { expr, direction });

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }

            order_by = Some(order_items);
        }

        // Parse frame clause (ROWS/RANGE)
        if matches!(self.peek(), Token::Keyword(Keyword::Rows) | Token::Keyword(Keyword::Range)) {
            frame = Some(self.parse_frame_clause()?);
        }

        self.expect_token(Token::RParen)?;

        Ok(ast::WindowSpec { partition_by, order_by, frame })
    }

    /// Parse frame clause (ROWS/RANGE BETWEEN ... AND ...)
    fn parse_frame_clause(&mut self) -> Result<ast::WindowFrame, ParseError> {
        // Parse frame unit (ROWS or RANGE)
        let unit = match self.peek() {
            Token::Keyword(Keyword::Rows) => {
                self.advance();
                ast::FrameUnit::Rows
            }
            Token::Keyword(Keyword::Range) => {
                self.advance();
                ast::FrameUnit::Range
            }
            _ => {
                return Err(ParseError {
                    message: format!(
                        "Expected ROWS or RANGE in frame clause, found {:?}",
                        self.peek()
                    ),
                })
            }
        };

        // Parse BETWEEN ... AND ... or single bound
        if matches!(self.peek(), Token::Keyword(Keyword::Between)) {
            self.advance(); // consume BETWEEN

            let start = self.parse_frame_bound()?;

            self.expect_keyword(Keyword::And)?;

            let end = self.parse_frame_bound()?;

            Ok(ast::WindowFrame { unit, start, end: Some(end) })
        } else {
            // Single bound (defaults to CURRENT ROW as end)
            let start = self.parse_frame_bound()?;

            Ok(ast::WindowFrame { unit, start, end: None })
        }
    }

    /// Parse a single frame boundary
    fn parse_frame_bound(&mut self) -> Result<ast::FrameBound, ParseError> {
        match self.peek() {
            Token::Keyword(Keyword::Unbounded) => {
                self.advance(); // consume UNBOUNDED

                match self.peek() {
                    Token::Keyword(Keyword::Preceding) => {
                        self.advance();
                        Ok(ast::FrameBound::UnboundedPreceding)
                    }
                    Token::Keyword(Keyword::Following) => {
                        self.advance();
                        Ok(ast::FrameBound::UnboundedFollowing)
                    }
                    _ => Err(ParseError {
                        message: format!(
                            "Expected PRECEDING or FOLLOWING after UNBOUNDED, found {:?}",
                            self.peek()
                        ),
                    }),
                }
            }

            Token::Keyword(Keyword::Current) => {
                self.advance(); // consume CURRENT
                                // Expect ROW (note: not ROWS, this is "CURRENT ROW" singular)
                                // We need to match against an identifier "ROW" since there's no Keyword::Row
                if let Token::Identifier(ref id) = self.peek() {
                    if id.to_uppercase() == "ROW" {
                        self.advance();
                        Ok(ast::FrameBound::CurrentRow)
                    } else {
                        Err(ParseError {
                            message: format!(
                                "Expected ROW after CURRENT in frame bound, found {:?}",
                                self.peek()
                            ),
                        })
                    }
                } else {
                    Err(ParseError {
                        message: format!(
                            "Expected ROW after CURRENT in frame bound, found {:?}",
                            self.peek()
                        ),
                    })
                }
            }

            // N PRECEDING or N FOLLOWING
            _ => {
                let offset = self.parse_primary_expression()?;

                match self.peek() {
                    Token::Keyword(Keyword::Preceding) => {
                        self.advance();
                        Ok(ast::FrameBound::Preceding(Box::new(offset)))
                    }
                    Token::Keyword(Keyword::Following) => {
                        self.advance();
                        Ok(ast::FrameBound::Following(Box::new(offset)))
                    }
                    _ => Err(ParseError {
                        message: format!(
                            "Expected PRECEDING or FOLLOWING in frame bound, found {:?}",
                            self.peek()
                        ),
                    }),
                }
            }
        }
    }

    /// Parse CHARACTERS or OCTETS keyword for USING clause
    /// SQL:1999 Section 6.29: String value functions
    pub(super) fn parse_character_unit(&mut self) -> Result<ast::CharacterUnit, ParseError> {
        match self.peek() {
            Token::Keyword(Keyword::Characters) => {
                self.advance();
                Ok(ast::CharacterUnit::Characters)
            }
            Token::Keyword(Keyword::Octets) => {
                self.advance();
                Ok(ast::CharacterUnit::Octets)
            }
            _ => Err(ParseError {
                message: format!(
                    "Expected CHARACTERS or OCTETS after USING, found {:?}",
                    self.peek()
                ),
            }),
        }
    }
}
