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
            '+' | '-' | '*' | '/' | '.' => {
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
            "ALL" => Token::Keyword(Keyword::All),
            "ANY" => Token::Keyword(Keyword::Any),
            "SOME" => Token::Keyword(Keyword::Some),
            "UNION" => Token::Keyword(Keyword::Union),
            "INTERSECT" => Token::Keyword(Keyword::Intersect),
            "EXCEPT" => Token::Keyword(Keyword::Except),
            "WITH" => Token::Keyword(Keyword::With),
            "DATE" => Token::Keyword(Keyword::Date),
            "TIME" => Token::Keyword(Keyword::Time),
            "TIMESTAMP" => Token::Keyword(Keyword::Timestamp),
            "INTERVAL" => Token::Keyword(Keyword::Interval),
            "CAST" => Token::Keyword(Keyword::Cast),
            // Window function keywords
            "OVER" => Token::Keyword(Keyword::Over),
            "PARTITION" => Token::Keyword(Keyword::Partition),
            "ROWS" => Token::Keyword(Keyword::Rows),
            "RANGE" => Token::Keyword(Keyword::Range),
            "PRECEDING" => Token::Keyword(Keyword::Preceding),
            "FOLLOWING" => Token::Keyword(Keyword::Following),
            "UNBOUNDED" => Token::Keyword(Keyword::Unbounded),
            "CURRENT" => Token::Keyword(Keyword::Current),
            // Window function names
            "ROW_NUMBER" => Token::Keyword(Keyword::RowNumber),
            "RANK" => Token::Keyword(Keyword::Rank),
            "DENSE_RANK" => Token::Keyword(Keyword::DenseRank),
            "NTILE" => Token::Keyword(Keyword::Ntile),
            "LAG" => Token::Keyword(Keyword::Lag),
            "LEAD" => Token::Keyword(Keyword::Lead),
            _ => Token::Identifier(text),
        };

        Ok(token)
    }

    /// Tokenize a number literal.
    fn tokenize_number(&mut self) -> Result<Token, LexerError> {
        let start = self.position;
        let mut has_dot = false;

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

        let number: String = self.input[start..self.position].iter().collect();
        Ok(Token::Number(number))
    }

    /// Tokenize a string literal enclosed in single or double quotes.
    fn tokenize_string(&mut self) -> Result<Token, LexerError> {
        let quote = self.current_char();
        self.advance();

        let start = self.position;
        while !self.is_eof() {
            let ch = self.current_char();
            if ch == quote {
                let string_content: String = self.input[start..self.position].iter().collect();
                self.advance();
                return Ok(Token::String(string_content));
            }
            self.advance();
        }

        Err(LexerError { message: "Unterminated string literal".to_string(), position: start - 1 })
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
