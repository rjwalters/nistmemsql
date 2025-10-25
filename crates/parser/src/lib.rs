//! SQL:1999 Parser - Lexer and Parser
//!
//! This crate provides lexical analysis (tokenization) and parsing of SQL:1999
//! statements into AST structures.
//!
//! Implementation follows hand-written recursive descent approach per ADR-0002.

use std::fmt;

// ============================================================================
// Token Types
// ============================================================================

/// SQL Keywords
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Keyword {
    Select,
    From,
    Where,
    Insert,
    Into,
    Update,
    Delete,
    Create,
    Table,
    Drop,
    Alter,
    And,
    Or,
    Not,
    Null,
    True,
    False,
    As,
    Join,
    Left,
    Right,
    Inner,
    Outer,
    On,
    Group,
    By,
    Having,
    Order,
    Asc,
    Desc,
    Limit,
    Offset,
    // Add more keywords as needed
}

impl fmt::Display for Keyword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keyword_str = match self {
            Keyword::Select => "SELECT",
            Keyword::From => "FROM",
            Keyword::Where => "WHERE",
            Keyword::Insert => "INSERT",
            Keyword::Into => "INTO",
            Keyword::Update => "UPDATE",
            Keyword::Delete => "DELETE",
            Keyword::Create => "CREATE",
            Keyword::Table => "TABLE",
            Keyword::Drop => "DROP",
            Keyword::Alter => "ALTER",
            Keyword::And => "AND",
            Keyword::Or => "OR",
            Keyword::Not => "NOT",
            Keyword::Null => "NULL",
            Keyword::True => "TRUE",
            Keyword::False => "FALSE",
            Keyword::As => "AS",
            Keyword::Join => "JOIN",
            Keyword::Left => "LEFT",
            Keyword::Right => "RIGHT",
            Keyword::Inner => "INNER",
            Keyword::Outer => "OUTER",
            Keyword::On => "ON",
            Keyword::Group => "GROUP",
            Keyword::By => "BY",
            Keyword::Having => "HAVING",
            Keyword::Order => "ORDER",
            Keyword::Asc => "ASC",
            Keyword::Desc => "DESC",
            Keyword::Limit => "LIMIT",
            Keyword::Offset => "OFFSET",
        };
        write!(f, "{}", keyword_str)
    }
}

/// SQL Token
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// SQL keyword (SELECT, FROM, etc.)
    Keyword(Keyword),

    /// Identifier (table name, column name, etc.)
    Identifier(String),

    /// Numeric literal (42, 3.14, etc.)
    Number(String),

    /// String literal ('hello', "world")
    String(String),

    /// Single character symbols (+, -, *, /, =, <, >, etc.)
    Symbol(char),

    /// Multi-character operators (<=, >=, !=, <>, ||)
    Operator(String),

    /// Semicolon (statement terminator)
    Semicolon,

    /// Comma (separator)
    Comma,

    /// Left parenthesis
    LParen,

    /// Right parenthesis
    RParen,

    /// End of input
    Eof,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Keyword(kw) => write!(f, "Keyword({})", kw),
            Token::Identifier(id) => write!(f, "Identifier({})", id),
            Token::Number(n) => write!(f, "Number({})", n),
            Token::String(s) => write!(f, "String('{}')", s),
            Token::Symbol(c) => write!(f, "Symbol({})", c),
            Token::Operator(op) => write!(f, "Operator({})", op),
            Token::Semicolon => write!(f, "Semicolon"),
            Token::Comma => write!(f, "Comma"),
            Token::LParen => write!(f, "LParen"),
            Token::RParen => write!(f, "RParen"),
            Token::Eof => write!(f, "Eof"),
        }
    }
}

// ============================================================================
// Lexer
// ============================================================================

/// Lexer error
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

/// SQL Lexer - converts SQL text into tokens
pub struct Lexer {
    input: Vec<char>,
    position: usize,
}

impl Lexer {
    /// Create a new lexer from SQL input
    pub fn new(input: &str) -> Self {
        Lexer { input: input.chars().collect(), position: 0 }
    }

    /// Tokenize the entire input
    pub fn tokenize(&mut self) -> Result<Vec<Token>, LexerError> {
        let mut tokens = Vec::new();

        loop {
            self.skip_whitespace();

            if self.is_eof() {
                tokens.push(Token::Eof);
                break;
            }

            let token = self.next_token()?;
            tokens.push(token);
        }

        Ok(tokens)
    }

    /// Get the next token
    fn next_token(&mut self) -> Result<Token, LexerError> {
        let ch = self.current_char();

        // Handle single-character tokens
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
            '+' | '-' | '*' | '/' | '=' | '<' | '>' => {
                // For now, treat as single-char symbols
                // TODO: Handle multi-char operators (<=, >=, !=, <>)
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

    /// Tokenize an identifier or keyword
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

        // Check if it's a keyword
        let token = match upper_text.as_str() {
            "SELECT" => Token::Keyword(Keyword::Select),
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
            _ => Token::Identifier(text),
        };

        Ok(token)
    }

    /// Tokenize a number
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

    /// Tokenize a string literal
    fn tokenize_string(&mut self) -> Result<Token, LexerError> {
        let quote = self.current_char();
        self.advance(); // Skip opening quote

        let start = self.position;
        while !self.is_eof() {
            let ch = self.current_char();
            if ch == quote {
                let string_content: String = self.input[start..self.position].iter().collect();
                self.advance(); // Skip closing quote
                return Ok(Token::String(string_content));
            }
            self.advance();
        }

        Err(LexerError { message: "Unterminated string literal".to_string(), position: start - 1 })
    }

    /// Skip whitespace characters
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

    /// Get current character without advancing
    fn current_char(&self) -> char {
        if self.is_eof() {
            '\0'
        } else {
            self.input[self.position]
        }
    }

    /// Advance to next character
    fn advance(&mut self) {
        if !self.is_eof() {
            self.position += 1;
        }
    }

    /// Check if we've reached end of input
    fn is_eof(&self) -> bool {
        self.position >= self.input.len()
    }
}

// ============================================================================
// Parser
// ============================================================================

/// Parser error
#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    pub message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Parse error: {}", self.message)
    }
}

/// SQL Parser - converts tokens into AST
pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    /// Create a new parser from tokens
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, position: 0 }
    }

    /// Parse SQL input string into a Statement
    pub fn parse_sql(input: &str) -> Result<ast::Statement, ParseError> {
        let mut lexer = Lexer::new(input);
        let tokens =
            lexer.tokenize().map_err(|e| ParseError { message: format!("Lexer error: {}", e) })?;

        let mut parser = Parser::new(tokens);
        parser.parse_statement()
    }

    /// Parse a statement
    pub fn parse_statement(&mut self) -> Result<ast::Statement, ParseError> {
        match self.peek() {
            Token::Keyword(Keyword::Select) => {
                let select_stmt = self.parse_select_statement()?;
                Ok(ast::Statement::Select(select_stmt))
            }
            _ => {
                Err(ParseError { message: format!("Expected statement, found {:?}", self.peek()) })
            }
        }
    }

    /// Parse SELECT statement
    fn parse_select_statement(&mut self) -> Result<ast::SelectStmt, ParseError> {
        self.expect_keyword(Keyword::Select)?;

        // Parse SELECT list
        let select_list = self.parse_select_list()?;

        // Parse optional FROM clause
        let from = if self.peek_keyword(Keyword::From) {
            self.consume_keyword(Keyword::From)?;
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        // Parse optional WHERE clause
        let where_clause = if self.peek_keyword(Keyword::Where) {
            self.consume_keyword(Keyword::Where)?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        // For now, skip GROUP BY, HAVING, ORDER BY
        // TODO: Implement these clauses

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(ast::SelectStmt {
            select_list,
            from,
            where_clause,
            group_by: None,
            having: None,
            order_by: None,
        })
    }

    /// Parse SELECT list (items after SELECT keyword)
    fn parse_select_list(&mut self) -> Result<Vec<ast::SelectItem>, ParseError> {
        let mut items = Vec::new();

        loop {
            let item = self.parse_select_item()?;
            items.push(item);

            // Check if there's a comma (more items)
            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(items)
    }

    /// Parse a single SELECT item
    fn parse_select_item(&mut self) -> Result<ast::SelectItem, ParseError> {
        // Check for wildcard (*)
        if matches!(self.peek(), Token::Symbol('*')) {
            self.advance();
            return Ok(ast::SelectItem::Wildcard);
        }

        // Parse expression
        let expr = self.parse_expression()?;

        // Check for optional AS alias
        let alias = if self.peek_keyword(Keyword::As) {
            self.consume_keyword(Keyword::As)?;
            match self.peek() {
                Token::Identifier(id) => {
                    let alias = id.clone();
                    self.advance();
                    Some(alias)
                }
                _ => {
                    return Err(ParseError { message: "Expected identifier after AS".to_string() })
                }
            }
        } else {
            None
        };

        Ok(ast::SelectItem::Expression { expr, alias })
    }

    /// Parse FROM clause
    fn parse_from_clause(&mut self) -> Result<ast::FromClause, ParseError> {
        // For now, just parse a simple table reference
        match self.peek() {
            Token::Identifier(table_name) => {
                let name = table_name.clone();
                self.advance();

                // Check for optional alias
                let alias = if self.peek_keyword(Keyword::As) {
                    self.consume_keyword(Keyword::As)?;
                    match self.peek() {
                        Token::Identifier(id) => {
                            let alias = id.clone();
                            self.advance();
                            Some(alias)
                        }
                        _ => None,
                    }
                } else if matches!(self.peek(), Token::Identifier(_)) {
                    // Implicit alias (no AS keyword)
                    match self.peek() {
                        Token::Identifier(id) => {
                            let alias = id.clone();
                            self.advance();
                            Some(alias)
                        }
                        _ => None,
                    }
                } else {
                    None
                };

                Ok(ast::FromClause::Table { name, alias })
            }
            _ => Err(ParseError { message: "Expected table name in FROM clause".to_string() }),
        }
    }

    /// Parse an expression (simplified for now - just literals and column refs)
    fn parse_expression(&mut self) -> Result<ast::Expression, ParseError> {
        self.parse_additive_expression()
    }

    /// Parse additive expression (handles + and -)
    fn parse_additive_expression(&mut self) -> Result<ast::Expression, ParseError> {
        let mut left = self.parse_multiplicative_expression()?;

        while matches!(self.peek(), Token::Symbol('+') | Token::Symbol('-')) {
            let op = match self.peek() {
                Token::Symbol('+') => ast::BinaryOperator::Plus,
                Token::Symbol('-') => ast::BinaryOperator::Minus,
                _ => unreachable!(),
            };
            self.advance();

            let right = self.parse_multiplicative_expression()?;
            left = ast::Expression::BinaryOp { op, left: Box::new(left), right: Box::new(right) };
        }

        Ok(left)
    }

    /// Parse multiplicative expression (handles * and /)
    fn parse_multiplicative_expression(&mut self) -> Result<ast::Expression, ParseError> {
        let mut left = self.parse_comparison_expression()?;

        while matches!(self.peek(), Token::Symbol('*') | Token::Symbol('/')) {
            let op = match self.peek() {
                Token::Symbol('*') => ast::BinaryOperator::Multiply,
                Token::Symbol('/') => ast::BinaryOperator::Divide,
                _ => unreachable!(),
            };
            self.advance();

            let right = self.parse_comparison_expression()?;
            left = ast::Expression::BinaryOp { op, left: Box::new(left), right: Box::new(right) };
        }

        Ok(left)
    }

    /// Parse comparison expression (handles =, <, >, etc.)
    fn parse_comparison_expression(&mut self) -> Result<ast::Expression, ParseError> {
        let mut left = self.parse_primary_expression()?;

        if matches!(self.peek(), Token::Symbol('=') | Token::Symbol('<') | Token::Symbol('>')) {
            let op = match self.peek() {
                Token::Symbol('=') => ast::BinaryOperator::Equal,
                Token::Symbol('<') => ast::BinaryOperator::LessThan,
                Token::Symbol('>') => ast::BinaryOperator::GreaterThan,
                _ => unreachable!(),
            };
            self.advance();

            let right = self.parse_primary_expression()?;
            left = ast::Expression::BinaryOp { op, left: Box::new(left), right: Box::new(right) };
        }

        Ok(left)
    }

    /// Parse primary expression (literals, identifiers, parenthesized expressions)
    fn parse_primary_expression(&mut self) -> Result<ast::Expression, ParseError> {
        match self.peek() {
            Token::Number(n) => {
                let num_str = n.clone();
                self.advance();

                // Try to parse as integer first
                if let Ok(i) = num_str.parse::<i64>() {
                    Ok(ast::Expression::Literal(types::SqlValue::Integer(i)))
                } else {
                    // For now, store as numeric string
                    Ok(ast::Expression::Literal(types::SqlValue::Numeric(num_str)))
                }
            }
            Token::String(s) => {
                let string_val = s.clone();
                self.advance();
                Ok(ast::Expression::Literal(types::SqlValue::Varchar(string_val)))
            }
            Token::Keyword(Keyword::True) => {
                self.advance();
                Ok(ast::Expression::Literal(types::SqlValue::Boolean(true)))
            }
            Token::Keyword(Keyword::False) => {
                self.advance();
                Ok(ast::Expression::Literal(types::SqlValue::Boolean(false)))
            }
            Token::Keyword(Keyword::Null) => {
                self.advance();
                Ok(ast::Expression::Literal(types::SqlValue::Null))
            }
            Token::Identifier(id) => {
                let column = id.clone();
                self.advance();

                // Check for qualified column reference (table.column)
                // For now, just return simple column reference
                Ok(ast::Expression::ColumnRef { table: None, column })
            }
            Token::LParen => {
                self.advance();
                let expr = self.parse_expression()?;
                self.expect_token(Token::RParen)?;
                Ok(expr)
            }
            _ => {
                Err(ParseError { message: format!("Expected expression, found {:?}", self.peek()) })
            }
        }
    }

    // ============================================================================
    // Helper methods
    // ============================================================================

    /// Peek at current token without consuming
    fn peek(&self) -> &Token {
        if self.position < self.tokens.len() {
            &self.tokens[self.position]
        } else {
            &Token::Eof
        }
    }

    /// Advance to next token
    fn advance(&mut self) {
        if self.position < self.tokens.len() {
            self.position += 1;
        }
    }

    /// Check if current token is a specific keyword
    fn peek_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.peek(), Token::Keyword(k) if k == &keyword)
    }

    /// Expect and consume a specific keyword
    fn expect_keyword(&mut self, keyword: Keyword) -> Result<(), ParseError> {
        if self.peek_keyword(keyword.clone()) {
            self.advance();
            Ok(())
        } else {
            Err(ParseError {
                message: format!("Expected keyword {:?}, found {:?}", keyword, self.peek()),
            })
        }
    }

    /// Consume a specific keyword
    fn consume_keyword(&mut self, keyword: Keyword) -> Result<(), ParseError> {
        self.expect_keyword(keyword)
    }

    /// Expect a specific token
    fn expect_token(&mut self, expected: Token) -> Result<(), ParseError> {
        if self.peek() == &expected {
            self.advance();
            Ok(())
        } else {
            Err(ParseError { message: format!("Expected {:?}, found {:?}", expected, self.peek()) })
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // Keyword Tokenization Tests
    // ============================================================================

    #[test]
    fn test_tokenize_select_keyword() {
        let mut lexer = Lexer::new("SELECT");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens.len(), 2); // SELECT + EOF
        assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
        assert_eq!(tokens[1], Token::Eof);
    }

    #[test]
    fn test_tokenize_select_lowercase() {
        let mut lexer = Lexer::new("select");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
    }

    #[test]
    fn test_tokenize_select_mixed_case() {
        let mut lexer = Lexer::new("SeLeCt");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
    }

    #[test]
    fn test_tokenize_from_keyword() {
        let mut lexer = Lexer::new("FROM");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Keyword(Keyword::From));
    }

    #[test]
    fn test_tokenize_where_keyword() {
        let mut lexer = Lexer::new("WHERE");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Keyword(Keyword::Where));
    }

    #[test]
    fn test_tokenize_multiple_keywords() {
        let mut lexer = Lexer::new("SELECT FROM WHERE");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens.len(), 4); // 3 keywords + EOF
        assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
        assert_eq!(tokens[1], Token::Keyword(Keyword::From));
        assert_eq!(tokens[2], Token::Keyword(Keyword::Where));
        assert_eq!(tokens[3], Token::Eof);
    }

    // ============================================================================
    // Identifier Tokenization Tests
    // ============================================================================

    #[test]
    fn test_tokenize_simple_identifier() {
        let mut lexer = Lexer::new("users");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Identifier("users".to_string()));
    }

    #[test]
    fn test_tokenize_identifier_with_underscore() {
        let mut lexer = Lexer::new("user_id");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Identifier("user_id".to_string()));
    }

    #[test]
    fn test_tokenize_identifier_with_numbers() {
        let mut lexer = Lexer::new("table123");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Identifier("table123".to_string()));
    }

    #[test]
    fn test_tokenize_identifier_starting_with_underscore() {
        let mut lexer = Lexer::new("_internal");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Identifier("_internal".to_string()));
    }

    // ============================================================================
    // Number Tokenization Tests
    // ============================================================================

    #[test]
    fn test_tokenize_integer() {
        let mut lexer = Lexer::new("42");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Number("42".to_string()));
    }

    #[test]
    fn test_tokenize_decimal() {
        let mut lexer = Lexer::new("3.14");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Number("3.14".to_string()));
    }

    #[test]
    fn test_tokenize_zero() {
        let mut lexer = Lexer::new("0");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Number("0".to_string()));
    }

    #[test]
    fn test_tokenize_large_number() {
        let mut lexer = Lexer::new("999999");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Number("999999".to_string()));
    }

    // ============================================================================
    // String Tokenization Tests
    // ============================================================================

    #[test]
    fn test_tokenize_single_quoted_string() {
        let mut lexer = Lexer::new("'hello'");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::String("hello".to_string()));
    }

    #[test]
    fn test_tokenize_double_quoted_string() {
        let mut lexer = Lexer::new("\"world\"");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::String("world".to_string()));
    }

    #[test]
    fn test_tokenize_empty_string() {
        let mut lexer = Lexer::new("''");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::String("".to_string()));
    }

    #[test]
    fn test_tokenize_string_with_spaces() {
        let mut lexer = Lexer::new("'hello world'");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::String("hello world".to_string()));
    }

    #[test]
    fn test_tokenize_unterminated_string() {
        let mut lexer = Lexer::new("'hello");
        let result = lexer.tokenize();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.message, "Unterminated string literal");
    }

    // ============================================================================
    // Symbol and Punctuation Tests
    // ============================================================================

    #[test]
    fn test_tokenize_semicolon() {
        let mut lexer = Lexer::new(";");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Semicolon);
    }

    #[test]
    fn test_tokenize_comma() {
        let mut lexer = Lexer::new(",");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Comma);
    }

    #[test]
    fn test_tokenize_parentheses() {
        let mut lexer = Lexer::new("()");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::LParen);
        assert_eq!(tokens[1], Token::RParen);
    }

    #[test]
    fn test_tokenize_arithmetic_symbols() {
        let mut lexer = Lexer::new("+ - * /");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Symbol('+'));
        assert_eq!(tokens[1], Token::Symbol('-'));
        assert_eq!(tokens[2], Token::Symbol('*'));
        assert_eq!(tokens[3], Token::Symbol('/'));
    }

    #[test]
    fn test_tokenize_comparison_symbols() {
        let mut lexer = Lexer::new("= < >");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Symbol('='));
        assert_eq!(tokens[1], Token::Symbol('<'));
        assert_eq!(tokens[2], Token::Symbol('>'));
    }

    // ============================================================================
    // Complex SQL Statement Tests
    // ============================================================================

    #[test]
    fn test_tokenize_select_42() {
        let mut lexer = Lexer::new("SELECT 42;");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens.len(), 4); // SELECT, 42, ;, EOF
        assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
        assert_eq!(tokens[1], Token::Number("42".to_string()));
        assert_eq!(tokens[2], Token::Semicolon);
        assert_eq!(tokens[3], Token::Eof);
    }

    #[test]
    fn test_tokenize_select_string() {
        let mut lexer = Lexer::new("SELECT 'hello';");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
        assert_eq!(tokens[1], Token::String("hello".to_string()));
        assert_eq!(tokens[2], Token::Semicolon);
    }

    #[test]
    fn test_tokenize_select_with_arithmetic() {
        let mut lexer = Lexer::new("SELECT 1 + 2;");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens.len(), 6); // SELECT, 1, +, 2, ;, EOF
        assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
        assert_eq!(tokens[1], Token::Number("1".to_string()));
        assert_eq!(tokens[2], Token::Symbol('+'));
        assert_eq!(tokens[3], Token::Number("2".to_string()));
        assert_eq!(tokens[4], Token::Semicolon);
    }

    #[test]
    fn test_tokenize_select_from_table() {
        let mut lexer = Lexer::new("SELECT * FROM users;");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens.len(), 6); // SELECT, *, FROM, users, ;, EOF
        assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
        assert_eq!(tokens[1], Token::Symbol('*'));
        assert_eq!(tokens[2], Token::Keyword(Keyword::From));
        assert_eq!(tokens[3], Token::Identifier("users".to_string()));
        assert_eq!(tokens[4], Token::Semicolon);
    }

    #[test]
    fn test_tokenize_select_columns() {
        let mut lexer = Lexer::new("SELECT id, name, age FROM users;");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
        assert_eq!(tokens[1], Token::Identifier("id".to_string()));
        assert_eq!(tokens[2], Token::Comma);
        assert_eq!(tokens[3], Token::Identifier("name".to_string()));
        assert_eq!(tokens[4], Token::Comma);
        assert_eq!(tokens[5], Token::Identifier("age".to_string()));
        assert_eq!(tokens[6], Token::Keyword(Keyword::From));
        assert_eq!(tokens[7], Token::Identifier("users".to_string()));
    }

    #[test]
    fn test_tokenize_select_with_where() {
        let mut lexer = Lexer::new("SELECT name FROM users WHERE id = 1;");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
        assert_eq!(tokens[1], Token::Identifier("name".to_string()));
        assert_eq!(tokens[2], Token::Keyword(Keyword::From));
        assert_eq!(tokens[3], Token::Identifier("users".to_string()));
        assert_eq!(tokens[4], Token::Keyword(Keyword::Where));
        assert_eq!(tokens[5], Token::Identifier("id".to_string()));
        assert_eq!(tokens[6], Token::Symbol('='));
        assert_eq!(tokens[7], Token::Number("1".to_string()));
        assert_eq!(tokens[8], Token::Semicolon);
    }

    // ============================================================================
    // Whitespace Handling Tests
    // ============================================================================

    #[test]
    fn test_tokenize_with_multiple_spaces() {
        let mut lexer = Lexer::new("SELECT    42");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens.len(), 3); // SELECT, 42, EOF
        assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
        assert_eq!(tokens[1], Token::Number("42".to_string()));
    }

    #[test]
    fn test_tokenize_with_tabs() {
        let mut lexer = Lexer::new("SELECT\t42");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
        assert_eq!(tokens[1], Token::Number("42".to_string()));
    }

    #[test]
    fn test_tokenize_with_newlines() {
        let mut lexer = Lexer::new("SELECT\n42");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
        assert_eq!(tokens[1], Token::Number("42".to_string()));
    }

    // ============================================================================
    // Error Handling Tests
    // ============================================================================

    #[test]
    fn test_tokenize_invalid_character() {
        let mut lexer = Lexer::new("SELECT @");
        let result = lexer.tokenize();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Unexpected character"));
    }

    // ============================================================================
    // Parser Tests - Parsing SQL into AST
    // ============================================================================

    #[test]
    fn test_parse_select_42() {
        let result = Parser::parse_sql("SELECT 42;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert_eq!(select.select_list.len(), 1);
                match &select.select_list[0] {
                    ast::SelectItem::Expression { expr, alias } => {
                        assert!(alias.is_none());
                        match expr {
                            ast::Expression::Literal(types::SqlValue::Integer(42)) => {} // Success
                            _ => panic!("Expected Integer(42), got {:?}", expr),
                        }
                    }
                    _ => panic!("Expected Expression select item"),
                }
                assert!(select.from.is_none());
                assert!(select.where_clause.is_none());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_string() {
        let result = Parser::parse_sql("SELECT 'hello';");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert_eq!(select.select_list.len(), 1);
                match &select.select_list[0] {
                    ast::SelectItem::Expression { expr, .. } => match expr {
                        ast::Expression::Literal(types::SqlValue::Varchar(s)) if s == "hello" => {} // Success
                        _ => panic!("Expected Varchar('hello'), got {:?}", expr),
                    },
                    _ => panic!("Expected Expression select item"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_arithmetic() {
        let result = Parser::parse_sql("SELECT 1 + 2;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert_eq!(select.select_list.len(), 1);
                match &select.select_list[0] {
                    ast::SelectItem::Expression { expr, .. } => match expr {
                        ast::Expression::BinaryOp { op, left, right } => {
                            assert_eq!(*op, ast::BinaryOperator::Plus);
                            match **left {
                                ast::Expression::Literal(types::SqlValue::Integer(1)) => {}
                                _ => panic!("Expected left = 1"),
                            }
                            match **right {
                                ast::Expression::Literal(types::SqlValue::Integer(2)) => {}
                                _ => panic!("Expected right = 2"),
                            }
                        }
                        _ => panic!("Expected BinaryOp, got {:?}", expr),
                    },
                    _ => panic!("Expected Expression select item"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_star() {
        let result = Parser::parse_sql("SELECT *;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert_eq!(select.select_list.len(), 1);
                match &select.select_list[0] {
                    ast::SelectItem::Wildcard => {} // Success
                    _ => panic!("Expected Wildcard select item"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_from_table() {
        let result = Parser::parse_sql("SELECT * FROM users;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert!(select.from.is_some());
                match &select.from.as_ref().unwrap() {
                    ast::FromClause::Table { name, alias } => {
                        assert_eq!(name, "users");
                        assert!(alias.is_none());
                    }
                    _ => panic!("Expected table in FROM clause"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_columns() {
        let result = Parser::parse_sql("SELECT id, name, age FROM users;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert_eq!(select.select_list.len(), 3);

                // Check first column (id)
                match &select.select_list[0] {
                    ast::SelectItem::Expression { expr, .. } => match expr {
                        ast::Expression::ColumnRef { column, .. } if column == "id" => {}
                        _ => panic!("Expected id column"),
                    },
                    _ => panic!("Expected Expression select item"),
                }

                // Check second column (name)
                match &select.select_list[1] {
                    ast::SelectItem::Expression { expr, .. } => match expr {
                        ast::Expression::ColumnRef { column, .. } if column == "name" => {}
                        _ => panic!("Expected name column"),
                    },
                    _ => panic!("Expected Expression select item"),
                }

                // Check third column (age)
                match &select.select_list[2] {
                    ast::SelectItem::Expression { expr, .. } => match expr {
                        ast::Expression::ColumnRef { column, .. } if column == "age" => {}
                        _ => panic!("Expected age column"),
                    },
                    _ => panic!("Expected Expression select item"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let result = Parser::parse_sql("SELECT name FROM users WHERE id = 1;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert!(select.where_clause.is_some());
                match &select.where_clause.as_ref().unwrap() {
                    ast::Expression::BinaryOp { op, left, right } => {
                        assert_eq!(*op, ast::BinaryOperator::Equal);
                        match **left {
                            ast::Expression::ColumnRef { ref column, .. } if column == "id" => {}
                            _ => panic!("Expected id column in WHERE"),
                        }
                        match **right {
                            ast::Expression::Literal(types::SqlValue::Integer(1)) => {}
                            _ => panic!("Expected Integer(1) in WHERE"),
                        }
                    }
                    _ => panic!("Expected BinaryOp in WHERE clause"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_with_alias() {
        let result = Parser::parse_sql("SELECT id AS user_id FROM users;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert_eq!(select.select_list.len(), 1);
                match &select.select_list[0] {
                    ast::SelectItem::Expression { alias, .. } => {
                        assert_eq!(alias.as_ref().unwrap(), "user_id");
                    }
                    _ => panic!("Expected Expression select item"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_precedence() {
        // Test that 1 + 2 * 3 parses as 1 + (2 * 3)
        let result = Parser::parse_sql("SELECT 1 + 2 * 3;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                match &select.select_list[0] {
                    ast::SelectItem::Expression { expr, .. } => match expr {
                        ast::Expression::BinaryOp { op, left, right } => {
                            assert_eq!(*op, ast::BinaryOperator::Plus);
                            // Left should be 1
                            match **left {
                                ast::Expression::Literal(types::SqlValue::Integer(1)) => {}
                                _ => panic!("Expected left = 1"),
                            }
                            // Right should be 2 * 3
                            match **right {
                                ast::Expression::BinaryOp {
                                    op: ast::BinaryOperator::Multiply,
                                    ..
                                } => {}
                                _ => panic!("Expected right = 2 * 3"),
                            }
                        }
                        _ => panic!("Expected BinaryOp"),
                    },
                    _ => panic!("Expected Expression"),
                }
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_parentheses() {
        // Test that (1 + 2) * 3 parses correctly
        let result = Parser::parse_sql("SELECT (1 + 2) * 3;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                match &select.select_list[0] {
                    ast::SelectItem::Expression { expr, .. } => match expr {
                        ast::Expression::BinaryOp { op, left, right } => {
                            assert_eq!(*op, ast::BinaryOperator::Multiply);
                            // Left should be (1 + 2)
                            match **left {
                                ast::Expression::BinaryOp {
                                    op: ast::BinaryOperator::Plus, ..
                                } => {}
                                _ => panic!("Expected left = 1 + 2"),
                            }
                            // Right should be 3
                            match **right {
                                ast::Expression::Literal(types::SqlValue::Integer(3)) => {}
                                _ => panic!("Expected right = 3"),
                            }
                        }
                        _ => panic!("Expected BinaryOp"),
                    },
                    _ => panic!("Expected Expression"),
                }
            }
            _ => panic!("Expected SELECT"),
        }
    }
}
