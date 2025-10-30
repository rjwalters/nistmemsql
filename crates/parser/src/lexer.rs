use crate::keywords::Keyword;
use crate::token::Token;
use std::fmt;

/// Lexer error returned when tokenization fails.
#[derive(Debug, Clone, PartialEq)]
pub struct LexerError {
    pub message: String,
    pub position: usize,
}

impl fmt::Display for LexerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Lexer error at position {}: {}", self.position, self.message)
    }
}

/// SQL Lexer - converts SQL text into tokens.
pub struct Lexer {
    input: Vec<char>,
    position: usize,
}

impl Lexer {
    /// Create a new lexer from SQL input.
    pub fn new(input: &str) -> Self {
        Lexer { input: input.chars().collect(), position: 0 }
    }

    /// Tokenize the entire input.
    pub fn tokenize(&mut self) -> Result<Vec<Token>, LexerError> {
        let mut tokens = Vec::new();

        loop {
            self.skip_whitespace_and_comments();

            if self.is_eof() {
                tokens.push(Token::Eof);
                break;
            }

            let token = self.next_token()?;
            tokens.push(token);
        }

        Ok(tokens)
    }

    /// Get the next token.
    fn next_token(&mut self) -> Result<Token, LexerError> {
        let ch = self.current_char();

        match ch {
            ';' => {
                self.advance();
                Ok(Token::Semicolon)
            }
            ',' => {
                self.advance();
                Ok(Token::Comma)
            }
            '(' => {
                self.advance();
                Ok(Token::LParen)
            }
            ')' => {
                self.advance();
                Ok(Token::RParen)
            }
            '=' | '<' | '>' | '!' => {
                self.advance();
                if !self.is_eof() {
                    let next_ch = self.current_char();
                    match (ch, next_ch) {
                        ('<', '=') => {
                            self.advance();
                            Ok(Token::Operator("<=".to_string()))
                        }
                        ('>', '=') => {
                            self.advance();
                            Ok(Token::Operator(">=".to_string()))
                        }
                        ('!', '=') => {
                            self.advance();
                            Ok(Token::Operator("!=".to_string()))
                        }
                        ('<', '>') => {
                            self.advance();
                            Ok(Token::Operator("<>".to_string()))
                        }
                        _ => Ok(Token::Symbol(ch)),
                    }
                } else {
                    Ok(Token::Symbol(ch))
                }
            }
            '|' => {
                self.advance();
                if !self.is_eof() && self.current_char() == '|' {
                    self.advance();
                    Ok(Token::Operator("||".to_string()))
                } else {
                    Err(LexerError {
                        message: "Unexpected character: '|' (did you mean '||'?)".to_string(),
                        position: self.position - 1,
                    })
                }
            }
            '.' => {
                // Check if this is the start of a decimal number (e.g., .2, .5E+10)
                if !self.is_eof() && self.peek(1).map(|c| c.is_ascii_digit()).unwrap_or(false) {
                    self.tokenize_number()
                } else {
                    self.advance();
                    Ok(Token::Symbol('.'))
                }
            }
            '+' | '-' | '*' | '/' => {
                let symbol = ch;
                self.advance();
                Ok(Token::Symbol(symbol))
            }
            '\'' | '"' => self.tokenize_string(),
            '0'..='9' => self.tokenize_number(),
            'a'..='z' | 'A'..='Z' | '_' => self.tokenize_identifier_or_keyword(),
            _ => Err(LexerError {
                message: format!("Unexpected character: '{}'", ch),
                position: self.position,
            }),
        }
    }

    /// Tokenize an identifier or keyword.
    fn tokenize_identifier_or_keyword(&mut self) -> Result<Token, LexerError> {
        let start = self.position;
        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_alphanumeric() || ch == '_' {
                self.advance();
            } else {
                break;
            }
        }

        let text: String = self.input[start..self.position].iter().collect();
        let upper_text = text.to_uppercase();

        let token = match upper_text.as_str() {
            "SELECT" => Token::Keyword(Keyword::Select),
            "DISTINCT" => Token::Keyword(Keyword::Distinct),
            "FROM" => Token::Keyword(Keyword::From),
            "WHERE" => Token::Keyword(Keyword::Where),
            "INSERT" => Token::Keyword(Keyword::Insert),
            "INTO" => Token::Keyword(Keyword::Into),
            "UPDATE" => Token::Keyword(Keyword::Update),
            "DELETE" => Token::Keyword(Keyword::Delete),
            "CREATE" => Token::Keyword(Keyword::Create),
            "TABLE" => Token::Keyword(Keyword::Table),
            "DROP" => Token::Keyword(Keyword::Drop),
            "ADD" => Token::Keyword(Keyword::Add),
            "ALTER" => Token::Keyword(Keyword::Alter),
            "AND" => Token::Keyword(Keyword::And),
            "OR" => Token::Keyword(Keyword::Or),
            "NOT" => Token::Keyword(Keyword::Not),
            "NULL" => Token::Keyword(Keyword::Null),
            "TRUE" => Token::Keyword(Keyword::True),
            "FALSE" => Token::Keyword(Keyword::False),
            "AS" => Token::Keyword(Keyword::As),
            "JOIN" => Token::Keyword(Keyword::Join),
            "LEFT" => Token::Keyword(Keyword::Left),
            "RIGHT" => Token::Keyword(Keyword::Right),
            "INNER" => Token::Keyword(Keyword::Inner),
            "OUTER" => Token::Keyword(Keyword::Outer),
            "CROSS" => Token::Keyword(Keyword::Cross),
            "FULL" => Token::Keyword(Keyword::Full),
            "ON" => Token::Keyword(Keyword::On),
            "GROUP" => Token::Keyword(Keyword::Group),
            "BY" => Token::Keyword(Keyword::By),
            "HAVING" => Token::Keyword(Keyword::Having),
            "ORDER" => Token::Keyword(Keyword::Order),
            "ASC" => Token::Keyword(Keyword::Asc),
            "DESC" => Token::Keyword(Keyword::Desc),
            "LIMIT" => Token::Keyword(Keyword::Limit),
            "OFFSET" => Token::Keyword(Keyword::Offset),
            "SET" => Token::Keyword(Keyword::Set),
            "VALUES" => Token::Keyword(Keyword::Values),
            "IN" => Token::Keyword(Keyword::In),
            "BETWEEN" => Token::Keyword(Keyword::Between),
            "LIKE" => Token::Keyword(Keyword::Like),
            "EXISTS" => Token::Keyword(Keyword::Exists),
            "IF" => Token::Keyword(Keyword::If),
            "IS" => Token::Keyword(Keyword::Is),
            "ALL" => Token::Keyword(Keyword::All),
            "ANY" => Token::Keyword(Keyword::Any),
            "SOME" => Token::Keyword(Keyword::Some),
            "UNION" => Token::Keyword(Keyword::Union),
            "INTERSECT" => Token::Keyword(Keyword::Intersect),
            "EXCEPT" => Token::Keyword(Keyword::Except),
            "WITH" => Token::Keyword(Keyword::With),
            "RECURSIVE" => Token::Keyword(Keyword::Recursive),
            "DATE" => Token::Keyword(Keyword::Date),
            "DEFAULT" => Token::Keyword(Keyword::Default),
            "TIME" => Token::Keyword(Keyword::Time),
            "TIMESTAMP" => Token::Keyword(Keyword::Timestamp),
            "INTERVAL" => Token::Keyword(Keyword::Interval),
            "CAST" => Token::Keyword(Keyword::Cast),
            // CASE expression keywords
            "CASE" => Token::Keyword(Keyword::Case),
            "WHEN" => Token::Keyword(Keyword::When),
            "THEN" => Token::Keyword(Keyword::Then),
            "ELSE" => Token::Keyword(Keyword::Else),
            "END" => Token::Keyword(Keyword::End),
            // Window function keywords
            "OVER" => Token::Keyword(Keyword::Over),
            "PARTITION" => Token::Keyword(Keyword::Partition),
            "ROWS" => Token::Keyword(Keyword::Rows),
            "RANGE" => Token::Keyword(Keyword::Range),
            "PRECEDING" => Token::Keyword(Keyword::Preceding),
            "FOLLOWING" => Token::Keyword(Keyword::Following),
            "UNBOUNDED" => Token::Keyword(Keyword::Unbounded),
            "CURRENT" => Token::Keyword(Keyword::Current),
            // Note: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD are treated as
            // identifiers (function names), not keywords. They are classified as
            // window functions by the parser's classify_window_function() method.
            // Transaction keywords
            "BEGIN" => Token::Keyword(Keyword::Begin),
            "COLUMN" => Token::Keyword(Keyword::Column),
            "COMMIT" => Token::Keyword(Keyword::Commit),
            "CONSTRAINT" => Token::Keyword(Keyword::Constraint),
            "ROLLBACK" => Token::Keyword(Keyword::Rollback),
            "START" => Token::Keyword(Keyword::Start),
            "TRANSACTION" => Token::Keyword(Keyword::Transaction),
            // Constraint keywords
            "PRIMARY" => Token::Keyword(Keyword::Primary),
            "FOREIGN" => Token::Keyword(Keyword::Foreign),
            "KEY" => Token::Keyword(Keyword::Key),
            "UNIQUE" => Token::Keyword(Keyword::Unique),
            "CHECK" => Token::Keyword(Keyword::Check),
            "REFERENCES" => Token::Keyword(Keyword::References),
            // TRIM function keywords
            "BOTH" => Token::Keyword(Keyword::Both),
            "LEADING" => Token::Keyword(Keyword::Leading),
            "TRAILING" => Token::Keyword(Keyword::Trailing),
            // Data type keywords
            "VARYING" => Token::Keyword(Keyword::Varying),
            "CHARACTERS" => Token::Keyword(Keyword::Characters),
            "OCTETS" => Token::Keyword(Keyword::Octets),
            // SUBSTRING function keywords
            "FOR" => Token::Keyword(Keyword::For),
            _ => Token::Identifier(text),
        };

        Ok(token)
    }

    /// Tokenize a number literal.
    /// Supports integers, decimals, and scientific notation (E-notation).
    /// Examples: 42, 3.14, 2.5E+10, 1.2E-5, .2E+2
    fn tokenize_number(&mut self) -> Result<Token, LexerError> {
        let start = self.position;
        let mut has_dot = false;

        // Handle leading decimal point (e.g., .5)
        if !self.is_eof() && self.current_char() == '.' {
            has_dot = true;
            self.advance();
        }

        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_ascii_digit() {
                self.advance();
            } else if ch == '.' && !has_dot {
                has_dot = true;
                self.advance();
            } else {
                break;
            }
        }

        // Check for scientific notation (E-notation)
        if !self.is_eof() {
            let ch = self.current_char();
            if ch == 'E' || ch == 'e' {
                self.advance();

                // Optional sign (+/-)
                if !self.is_eof() {
                    let sign = self.current_char();
                    if sign == '+' || sign == '-' {
                        self.advance();
                    }
                }

                // Exponent digits (required)
                let exp_start = self.position;
                while !self.is_eof() && self.current_char().is_ascii_digit() {
                    self.advance();
                }

                // Verify we got at least one exponent digit
                if self.position == exp_start {
                    return Err(LexerError {
                        message: "Invalid scientific notation: expected digits after 'E'".to_string(),
                        position: self.position,
                    });
                }
            }
        }

        let number: String = self.input[start..self.position].iter().collect();
        Ok(Token::Number(number))
    }

    /// Tokenize a string literal enclosed in single or double quotes.
    /// Supports SQL-standard escaped quotes (e.g., 'O''Reilly' becomes "O'Reilly")
    fn tokenize_string(&mut self) -> Result<Token, LexerError> {
        let quote = self.current_char();
        self.advance();

        let mut string_content = String::new();
        while !self.is_eof() {
            let ch = self.current_char();
            if ch == quote {
                // Check if this is an escaped quote (two consecutive quotes)
                self.advance();
                if !self.is_eof() && self.current_char() == quote {
                    // Escaped quote - add a single quote to the result and continue
                    string_content.push(quote);
                    self.advance();
                } else {
                    // End of string
                    return Ok(Token::String(string_content));
                }
            } else {
                string_content.push(ch);
                self.advance();
            }
        }

        Err(LexerError { message: "Unterminated string literal".to_string(), position: self.position })
    }

    /// Skip whitespace characters.
    fn skip_whitespace(&mut self) {
        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    /// Skip whitespace and SQL comments.
    /// SQL supports line comments starting with -- until end of line.
    fn skip_whitespace_and_comments(&mut self) {
        loop {
            self.skip_whitespace();

            if self.is_eof() {
                break;
            }

            // Check for -- line comment
            if self.current_char() == '-' && self.peek(1) == Some('-') {
                // Skip until end of line
                while !self.is_eof() && self.current_char() != '\n' {
                    self.advance();
                }
                // Continue loop to skip the newline and any following whitespace/comments
                continue;
            }

            // No more whitespace or comments
            break;
        }
    }

    /// Get current character without advancing.
    fn current_char(&self) -> char {
        if self.is_eof() {
            '\0'
        } else {
            self.input[self.position]
        }
    }

    /// Peek ahead n characters without advancing.
    fn peek(&self, n: usize) -> Option<char> {
        let peek_pos = self.position + n;
        if peek_pos < self.input.len() {
            Some(self.input[peek_pos])
        } else {
            None
        }
    }

    /// Advance to next character.
    fn advance(&mut self) {
        if !self.is_eof() {
            self.position += 1;
        }
    }

    /// Check if we've reached end of input.
    fn is_eof(&self) -> bool {
        self.position >= self.input.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scientific_notation() {
        // Test various E-notation formats
        let test_cases = vec![
            ("2.5E+10", "2.5E+10"),
            ("1.2E-5", "1.2E-5"),
            (".2E+2", ".2E+2"),
            ("2E10", "2E10"),
            ("2.E-5", "2.E-5"),
            ("2.5e10", "2.5e10"),  // lowercase e
            (".5E2", ".5E2"),
            ("123.456E-78", "123.456E-78"),
        ];

        for (input, expected) in test_cases {
            let mut lexer = Lexer::new(input);
            let tokens = lexer.tokenize().expect(&format!("Failed to tokenize: {}", input));

            // Should have exactly 2 tokens: the number and EOF
            assert_eq!(tokens.len(), 2, "Input: {}", input);

            match &tokens[0] {
                Token::Number(n) => assert_eq!(n, expected, "Input: {}", input),
                other => panic!("Expected Number token for {}, got {:?}", input, other),
            }

            assert_eq!(tokens[1], Token::Eof);
        }
    }

    #[test]
    fn test_decimal_point_start() {
        // Test numbers starting with decimal point
        let test_cases = vec![
            (".5", ".5"),
            (".123", ".123"),
            (".2E+2", ".2E+2"),
        ];

        for (input, expected) in test_cases {
            let mut lexer = Lexer::new(input);
            let tokens = lexer.tokenize().expect(&format!("Failed to tokenize: {}", input));

            assert_eq!(tokens.len(), 2, "Input: {}", input);

            match &tokens[0] {
                Token::Number(n) => assert_eq!(n, expected, "Input: {}", input),
                other => panic!("Expected Number token for {}, got {:?}", input, other),
            }
        }
    }

    #[test]
    fn test_invalid_scientific_notation() {
        // Test invalid E-notation should fail
        let invalid_cases = vec![
            "2E",      // No exponent digits
            "2E+",     // No exponent digits after sign
            "2E-",     // No exponent digits after sign
        ];

        for input in invalid_cases {
            let mut lexer = Lexer::new(input);
            let result = lexer.tokenize();
            assert!(result.is_err(), "Expected error for input: {}", input);
        }
    }
}
