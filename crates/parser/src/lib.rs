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
    Set,
    Values,
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
            Keyword::Set => "SET",
            Keyword::Values => "VALUES",
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
            '=' | '<' | '>' | '!' => {
                // Check for multi-character operators (<=, >=, !=, <>)
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
                // Single-character symbols
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
            "SET" => Token::Keyword(Keyword::Set),
            "VALUES" => Token::Keyword(Keyword::Values),
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
            Token::Keyword(Keyword::Insert) => {
                let insert_stmt = self.parse_insert_statement()?;
                Ok(ast::Statement::Insert(insert_stmt))
            }
            Token::Keyword(Keyword::Update) => {
                let update_stmt = self.parse_update_statement()?;
                Ok(ast::Statement::Update(update_stmt))
            }
            Token::Keyword(Keyword::Delete) => {
                let delete_stmt = self.parse_delete_statement()?;
                Ok(ast::Statement::Delete(delete_stmt))
            }
            Token::Keyword(Keyword::Create) => {
                let create_stmt = self.parse_create_table_statement()?;
                Ok(ast::Statement::CreateTable(create_stmt))
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

        // Parse optional GROUP BY clause
        let group_by = if self.peek_keyword(Keyword::Group) {
            self.consume_keyword(Keyword::Group)?;
            self.expect_keyword(Keyword::By)?;

            // Parse comma-separated list of expressions
            let mut group_exprs = Vec::new();
            loop {
                let expr = self.parse_expression()?;
                group_exprs.push(expr);

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }

            Some(group_exprs)
        } else {
            None
        };

        // Parse optional HAVING clause (only valid with GROUP BY)
        let having = if self.peek_keyword(Keyword::Having) {
            self.consume_keyword(Keyword::Having)?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse optional ORDER BY clause
        let order_by = if self.peek_keyword(Keyword::Order) {
            self.consume_keyword(Keyword::Order)?;
            self.expect_keyword(Keyword::By)?;

            // Parse comma-separated list of order items
            let mut order_items = Vec::new();
            loop {
                let expr = self.parse_expression()?;

                // Check for optional ASC/DESC
                let direction = if self.peek_keyword(Keyword::Asc) {
                    self.consume_keyword(Keyword::Asc)?;
                    ast::OrderDirection::Asc
                } else if self.peek_keyword(Keyword::Desc) {
                    self.consume_keyword(Keyword::Desc)?;
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

            Some(order_items)
        } else {
            None
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(ast::SelectStmt { select_list, from, where_clause, group_by, having, order_by })
    }

    /// Parse INSERT statement
    fn parse_insert_statement(&mut self) -> Result<ast::InsertStmt, ParseError> {
        self.expect_keyword(Keyword::Insert)?;
        self.expect_keyword(Keyword::Into)?;

        // Parse table name
        let table_name = match self.peek() {
            Token::Identifier(name) => {
                let table = name.clone();
                self.advance();
                table
            }
            _ => {
                return Err(ParseError {
                    message: "Expected table name after INSERT INTO".to_string(),
                })
            }
        };

        // Parse column list (optional in SQL, but we'll require it for now)
        let columns = if matches!(self.peek(), Token::LParen) {
            self.advance(); // consume (
            let mut cols = Vec::new();
            loop {
                match self.peek() {
                    Token::Identifier(col) => {
                        cols.push(col.clone());
                        self.advance();
                    }
                    _ => return Err(ParseError { message: "Expected column name".to_string() }),
                }

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            self.expect_token(Token::RParen)?;
            cols
        } else {
            Vec::new() // No columns specified
        };

        // Parse VALUES
        self.expect_keyword(Keyword::Values)?;

        // Parse value lists
        let mut values = Vec::new();
        loop {
            self.expect_token(Token::LParen)?;
            let mut row = Vec::new();
            loop {
                let expr = self.parse_expression()?;
                row.push(expr);

                if matches!(self.peek(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            self.expect_token(Token::RParen)?;
            values.push(row);

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(ast::InsertStmt { table_name, columns, values })
    }

    /// Parse UPDATE statement
    fn parse_update_statement(&mut self) -> Result<ast::UpdateStmt, ParseError> {
        self.expect_keyword(Keyword::Update)?;

        // Parse table name
        let table_name = match self.peek() {
            Token::Identifier(name) => {
                let table = name.clone();
                self.advance();
                table
            }
            _ => {
                return Err(ParseError { message: "Expected table name after UPDATE".to_string() })
            }
        };

        // Parse SET keyword
        self.expect_keyword(Keyword::Set)?;

        // Parse assignments
        let mut assignments = Vec::new();
        loop {
            // Parse column name
            let column = match self.peek() {
                Token::Identifier(col) => {
                    let c = col.clone();
                    self.advance();
                    c
                }
                _ => {
                    return Err(ParseError {
                        message: "Expected column name in SET clause".to_string(),
                    })
                }
            };

            // Expect =
            self.expect_token(Token::Symbol('='))?;

            // Parse value expression
            let value = self.parse_expression()?;

            assignments.push(ast::Assignment { column, value });

            if matches!(self.peek(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        // Parse optional WHERE clause
        let where_clause = if self.peek_keyword(Keyword::Where) {
            self.consume_keyword(Keyword::Where)?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(ast::UpdateStmt { table_name, assignments, where_clause })
    }

    /// Parse DELETE statement
    fn parse_delete_statement(&mut self) -> Result<ast::DeleteStmt, ParseError> {
        self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;

        // Parse table name
        let table_name = match self.peek() {
            Token::Identifier(name) => {
                let table = name.clone();
                self.advance();
                table
            }
            _ => {
                return Err(ParseError {
                    message: "Expected table name after DELETE FROM".to_string(),
                })
            }
        };

        // Parse optional WHERE clause
        let where_clause = if self.peek_keyword(Keyword::Where) {
            self.consume_keyword(Keyword::Where)?;
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Expect semicolon or EOF
        if matches!(self.peek(), Token::Semicolon) {
            self.advance();
        }

        Ok(ast::DeleteStmt { table_name, where_clause })
    }

    /// Parse CREATE TABLE statement
    fn parse_create_table_statement(&mut self) -> Result<ast::CreateTableStmt, ParseError> {
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
        // Parse the first table reference
        let mut left = self.parse_table_reference()?;

        // Check for JOINs (left-associative)
        while self.is_join_keyword() {
            let join_type = self.parse_join_type()?;

            // Parse right table reference
            let right = self.parse_table_reference()?;

            // Parse ON condition
            let condition = if self.peek_keyword(Keyword::On) {
                self.consume_keyword(Keyword::On)?;
                Some(self.parse_expression()?)
            } else {
                None
            };

            // Build JOIN node
            left = ast::FromClause::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type,
                condition,
            };
        }

        Ok(left)
    }

    /// Parse a single table reference (table name with optional alias)
    fn parse_table_reference(&mut self) -> Result<ast::FromClause, ParseError> {
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
                } else if matches!(self.peek(), Token::Identifier(_)) && !self.is_join_keyword() {
                    // Implicit alias (no AS keyword) - but not a JOIN keyword
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

    /// Check if current token is a JOIN keyword
    fn is_join_keyword(&self) -> bool {
        matches!(
            self.peek(),
            Token::Keyword(Keyword::Join)
                | Token::Keyword(Keyword::Inner)
                | Token::Keyword(Keyword::Left)
                | Token::Keyword(Keyword::Right)
        )
    }

    /// Parse JOIN type (INNER JOIN, LEFT JOIN, etc.)
    fn parse_join_type(&mut self) -> Result<ast::JoinType, ParseError> {
        match self.peek() {
            Token::Keyword(Keyword::Join) => {
                self.advance();
                Ok(ast::JoinType::Inner) // Default JOIN is INNER JOIN
            }
            Token::Keyword(Keyword::Inner) => {
                self.advance();
                self.expect_keyword(Keyword::Join)?;
                Ok(ast::JoinType::Inner)
            }
            Token::Keyword(Keyword::Left) => {
                self.advance();
                // Optional OUTER keyword
                if self.peek_keyword(Keyword::Outer) {
                    self.consume_keyword(Keyword::Outer)?;
                }
                self.expect_keyword(Keyword::Join)?;
                Ok(ast::JoinType::LeftOuter)
            }
            Token::Keyword(Keyword::Right) => {
                self.advance();
                // Optional OUTER keyword
                if self.peek_keyword(Keyword::Outer) {
                    self.consume_keyword(Keyword::Outer)?;
                }
                self.expect_keyword(Keyword::Join)?;
                Ok(ast::JoinType::RightOuter)
            }
            _ => Err(ParseError { message: "Expected JOIN keyword".to_string() }),
        }
    }

    /// Parse an expression (entry point)
    fn parse_expression(&mut self) -> Result<ast::Expression, ParseError> {
        self.parse_or_expression()
    }

    /// Parse OR expression (lowest precedence)
    fn parse_or_expression(&mut self) -> Result<ast::Expression, ParseError> {
        let mut left = self.parse_and_expression()?;

        while self.peek_keyword(Keyword::Or) {
            self.consume_keyword(Keyword::Or)?;
            let right = self.parse_and_expression()?;
            left = ast::Expression::BinaryOp {
                op: ast::BinaryOperator::Or,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse AND expression
    fn parse_and_expression(&mut self) -> Result<ast::Expression, ParseError> {
        let mut left = self.parse_additive_expression()?;

        while self.peek_keyword(Keyword::And) {
            self.consume_keyword(Keyword::And)?;
            let right = self.parse_additive_expression()?;
            left = ast::Expression::BinaryOp {
                op: ast::BinaryOperator::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        }

        Ok(left)
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

    /// Parse comparison expression (handles =, <, >, <=, >=, !=, <>)
    fn parse_comparison_expression(&mut self) -> Result<ast::Expression, ParseError> {
        let mut left = self.parse_primary_expression()?;

        // Check for comparison operators (both single-char and multi-char)
        let is_comparison = matches!(
            self.peek(),
            Token::Symbol('=') | Token::Symbol('<') | Token::Symbol('>') | Token::Operator(_)
        );

        if is_comparison {
            let op = match self.peek() {
                Token::Symbol('=') => ast::BinaryOperator::Equal,
                Token::Symbol('<') => ast::BinaryOperator::LessThan,
                Token::Symbol('>') => ast::BinaryOperator::GreaterThan,
                Token::Operator(ref s) => match s.as_str() {
                    "<=" => ast::BinaryOperator::LessThanOrEqual,
                    ">=" => ast::BinaryOperator::GreaterThanOrEqual,
                    "!=" => ast::BinaryOperator::NotEqual,
                    "<>" => ast::BinaryOperator::NotEqual, // SQL standard
                    _ => {
                        return Err(ParseError {
                            message: format!("Unexpected operator: {}", s),
                        })
                    }
                },
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
                let first = id.clone();
                self.advance();

                // Check for function call (identifier followed by '(')
                if matches!(self.peek(), Token::LParen) {
                    self.advance(); // consume '('

                    // Parse function arguments
                    let mut args = Vec::new();

                    // Check for empty argument list or '*'
                    if matches!(self.peek(), Token::RParen) {
                        // No arguments: func()
                        self.advance();
                        return Ok(ast::Expression::Function { name: first, args });
                    } else if matches!(self.peek(), Token::Symbol('*')) {
                        // Special case for COUNT(*)
                        self.advance(); // consume '*'
                        self.expect_token(Token::RParen)?;
                        // Represent * as a special wildcard expression
                        args.push(ast::Expression::ColumnRef {
                            table: None,
                            column: "*".to_string(),
                        });
                        return Ok(ast::Expression::Function { name: first, args });
                    }

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
                    Ok(ast::Expression::Function { name: first, args })
                }
                // Check for qualified column reference (table.column)
                else if matches!(self.peek(), Token::Symbol('.')) {
                    self.advance(); // consume '.'
                    match self.peek() {
                        Token::Identifier(col) => {
                            let column = col.clone();
                            self.advance();
                            Ok(ast::Expression::ColumnRef { table: Some(first), column })
                        }
                        _ => Err(ParseError {
                            message: "Expected column name after '.'".to_string(),
                        }),
                    }
                } else {
                    // Simple column reference
                    Ok(ast::Expression::ColumnRef { table: None, column: first })
                }
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

    #[test]
    fn test_tokenize_multi_char_operators() {
        let mut lexer = Lexer::new("<= >= != <>");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Operator("<=".to_string()));
        assert_eq!(tokens[1], Token::Operator(">=".to_string()));
        assert_eq!(tokens[2], Token::Operator("!=".to_string()));
        assert_eq!(tokens[3], Token::Operator("<>".to_string()));
    }

    #[test]
    fn test_tokenize_operators_without_spaces() {
        // Test that >= is tokenized as one operator, not two
        let mut lexer = Lexer::new("age>=18");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Identifier("age".to_string()));
        assert_eq!(tokens[1], Token::Operator(">=".to_string()));
        assert_eq!(tokens[2], Token::Number("18".to_string()));
    }

    #[test]
    fn test_tokenize_single_vs_multi_char() {
        // Test that > and = are separate when not adjacent
        let mut lexer = Lexer::new("> =");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Symbol('>'));
        assert_eq!(tokens[1], Token::Symbol('='));

        // But >= is one token when adjacent
        let mut lexer2 = Lexer::new(">=");
        let tokens2 = lexer2.tokenize().unwrap();
        assert_eq!(tokens2[0], Token::Operator(">=".to_string()));
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

    #[test]
    fn test_parse_and_operator() {
        let result = Parser::parse_sql("SELECT * FROM users WHERE age > 18 AND status = 'active';");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert!(select.where_clause.is_some());
                match select.where_clause.as_ref().unwrap() {
                    ast::Expression::BinaryOp { op, .. } => {
                        assert_eq!(*op, ast::BinaryOperator::And);
                    }
                    _ => panic!("Expected AND expression"),
                }
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_or_operator() {
        let result =
            Parser::parse_sql("SELECT * FROM users WHERE status = 'active' OR status = 'pending';");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert!(select.where_clause.is_some());
                match select.where_clause.as_ref().unwrap() {
                    ast::Expression::BinaryOp { op, .. } => {
                        assert_eq!(*op, ast::BinaryOperator::Or);
                    }
                    _ => panic!("Expected OR expression"),
                }
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_complex_where() {
        // Test: age > 18 AND (status = 'active' OR status = 'pending')
        let result = Parser::parse_sql(
            "SELECT * FROM users WHERE age > 18 AND (status = 'active' OR status = 'pending');",
        );
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert!(select.where_clause.is_some());
                // Outer should be AND
                match select.where_clause.as_ref().unwrap() {
                    ast::Expression::BinaryOp { op, right, .. } => {
                        assert_eq!(*op, ast::BinaryOperator::And);
                        // Right side should be OR (in parentheses)
                        match **right {
                            ast::Expression::BinaryOp { op: ast::BinaryOperator::Or, .. } => {} // Success
                            _ => panic!("Expected OR in right side"),
                        }
                    }
                    _ => panic!("Expected AND expression"),
                }
            }
            _ => panic!("Expected SELECT"),
        }
    }

    // ========================================================================
    // INSERT Statement Tests
    // ========================================================================

    #[test]
    fn test_parse_insert_basic() {
        let result = Parser::parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Insert(insert) => {
                assert_eq!(insert.table_name, "users");
                assert_eq!(insert.columns.len(), 2);
                assert_eq!(insert.columns[0], "id");
                assert_eq!(insert.columns[1], "name");
                assert_eq!(insert.values.len(), 1); // One row
                assert_eq!(insert.values[0].len(), 2); // Two values
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn test_parse_insert_multiple_rows() {
        let result =
            Parser::parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Insert(insert) => {
                assert_eq!(insert.table_name, "users");
                assert_eq!(insert.values.len(), 2); // Two rows
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    // ========================================================================
    // UPDATE Statement Tests
    // ========================================================================

    #[test]
    fn test_parse_update_basic() {
        let result = Parser::parse_sql("UPDATE users SET name = 'Bob' WHERE id = 1;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Update(update) => {
                assert_eq!(update.table_name, "users");
                assert_eq!(update.assignments.len(), 1);
                assert_eq!(update.assignments[0].column, "name");
                assert!(update.where_clause.is_some());
            }
            _ => panic!("Expected UPDATE statement"),
        }
    }

    #[test]
    fn test_parse_update_multiple_columns() {
        let result = Parser::parse_sql("UPDATE users SET name = 'Bob', age = 30;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Update(update) => {
                assert_eq!(update.table_name, "users");
                assert_eq!(update.assignments.len(), 2);
                assert_eq!(update.assignments[0].column, "name");
                assert_eq!(update.assignments[1].column, "age");
            }
            _ => panic!("Expected UPDATE statement"),
        }
    }

    // ========================================================================
    // DELETE Statement Tests
    // ========================================================================

    #[test]
    fn test_parse_delete_basic() {
        let result = Parser::parse_sql("DELETE FROM users WHERE id = 1;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Delete(delete) => {
                assert_eq!(delete.table_name, "users");
                assert!(delete.where_clause.is_some());
            }
            _ => panic!("Expected DELETE statement"),
        }
    }

    #[test]
    fn test_parse_delete_no_where() {
        let result = Parser::parse_sql("DELETE FROM users;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Delete(delete) => {
                assert_eq!(delete.table_name, "users");
                assert!(delete.where_clause.is_none());
            }
            _ => panic!("Expected DELETE statement"),
        }
    }

    // ========================================================================
    // CREATE TABLE Statement Tests
    // ========================================================================

    #[test]
    fn test_parse_create_table_basic() {
        let result = Parser::parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR(100));");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::CreateTable(create) => {
                assert_eq!(create.table_name, "users");
                assert_eq!(create.columns.len(), 2);
                assert_eq!(create.columns[0].name, "id");
                assert_eq!(create.columns[1].name, "name");
                match create.columns[0].data_type {
                    types::DataType::Integer => {} // Success
                    _ => panic!("Expected Integer data type"),
                }
                match create.columns[1].data_type {
                    types::DataType::Varchar { max_length: 100 } => {} // Success
                    _ => panic!("Expected VARCHAR(100) data type"),
                }
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_create_table_various_types() {
        let result = Parser::parse_sql(
            "CREATE TABLE test (id INT, flag BOOLEAN, birth DATE, code CHAR(5));",
        );
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::CreateTable(create) => {
                assert_eq!(create.table_name, "test");
                assert_eq!(create.columns.len(), 4);
                match create.columns[0].data_type {
                    types::DataType::Integer => {} // Success
                    _ => panic!("Expected Integer"),
                }
                match create.columns[1].data_type {
                    types::DataType::Boolean => {} // Success
                    _ => panic!("Expected Boolean"),
                }
                match create.columns[2].data_type {
                    types::DataType::Date => {} // Success
                    _ => panic!("Expected Date"),
                }
                match create.columns[3].data_type {
                    types::DataType::Character { length: 5 } => {} // Success
                    _ => panic!("Expected CHAR(5)"),
                }
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    // ========================================================================
    // JOIN Operation Tests
    // ========================================================================

    #[test]
    fn test_parse_simple_join() {
        let result =
            Parser::parse_sql("SELECT * FROM users JOIN orders ON users.id = orders.user_id;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert!(select.from.is_some());
                match select.from.as_ref().unwrap() {
                    ast::FromClause::Join { join_type, left, right, condition } => {
                        // Default JOIN is INNER JOIN
                        assert_eq!(*join_type, ast::JoinType::Inner);

                        // Left should be users table
                        match **left {
                            ast::FromClause::Table { ref name, .. } if name == "users" => {} // Success
                            _ => panic!("Expected left table to be 'users'"),
                        }

                        // Right should be orders table
                        match **right {
                            ast::FromClause::Table { ref name, .. } if name == "orders" => {} // Success
                            _ => panic!("Expected right table to be 'orders'"),
                        }

                        // Should have ON condition
                        assert!(condition.is_some());
                    }
                    _ => panic!("Expected JOIN in FROM clause"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_inner_join() {
        let result = Parser::parse_sql(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id;",
        );
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
                ast::FromClause::Join { join_type, .. } => {
                    assert_eq!(*join_type, ast::JoinType::Inner);
                }
                _ => panic!("Expected JOIN"),
            },
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_left_join() {
        let result =
            Parser::parse_sql("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
                ast::FromClause::Join { join_type, .. } => {
                    assert_eq!(*join_type, ast::JoinType::LeftOuter);
                }
                _ => panic!("Expected JOIN"),
            },
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_left_outer_join() {
        let result = Parser::parse_sql(
            "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id;",
        );
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
                ast::FromClause::Join { join_type, .. } => {
                    assert_eq!(*join_type, ast::JoinType::LeftOuter);
                }
                _ => panic!("Expected JOIN"),
            },
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_right_join() {
        let result = Parser::parse_sql(
            "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id;",
        );
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
                ast::FromClause::Join { join_type, .. } => {
                    assert_eq!(*join_type, ast::JoinType::RightOuter);
                }
                _ => panic!("Expected JOIN"),
            },
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_multiple_joins() {
        let result = Parser::parse_sql(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN products ON orders.product_id = products.id;"
        );
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                // Should have nested JOINs
                match select.from.as_ref().unwrap() {
                    ast::FromClause::Join { left, .. } => {
                        // Left should also be a JOIN
                        match **left {
                            ast::FromClause::Join { .. } => {} // Success - nested JOIN
                            _ => panic!("Expected nested JOIN"),
                        }
                    }
                    _ => panic!("Expected JOIN"),
                }
            }
            _ => panic!("Expected SELECT"),
        }
    }

    // ========================================================================
    // Aggregate Function Tests
    // ========================================================================

    #[test]
    fn test_parse_count_star() {
        let result = Parser::parse_sql("SELECT COUNT(*) FROM users;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert_eq!(select.select_list.len(), 1);
                match &select.select_list[0] {
                    ast::SelectItem::Expression { expr, .. } => match expr {
                        ast::Expression::Function { name, args } => {
                            assert_eq!(name, "COUNT");
                            assert_eq!(args.len(), 1);
                            // COUNT(*) is represented as a special wildcard expression
                        }
                        _ => panic!("Expected function call"),
                    },
                    _ => panic!("Expected expression"),
                }
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_count_column() {
        let result = Parser::parse_sql("SELECT COUNT(id) FROM users;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => match &select.select_list[0] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::Function { name, args } => {
                        assert_eq!(name, "COUNT");
                        assert_eq!(args.len(), 1);
                        match &args[0] {
                            ast::Expression::ColumnRef { column, .. } if column == "id" => {}
                            _ => panic!("Expected column reference"),
                        }
                    }
                    _ => panic!("Expected function call"),
                },
                _ => panic!("Expected expression"),
            },
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_sum_function() {
        let result = Parser::parse_sql("SELECT SUM(amount) FROM orders;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => match &select.select_list[0] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::Function { name, args } => {
                        assert_eq!(name, "SUM");
                        assert_eq!(args.len(), 1);
                    }
                    _ => panic!("Expected SUM function"),
                },
                _ => panic!("Expected expression"),
            },
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_avg_function() {
        let result = Parser::parse_sql("SELECT AVG(price) FROM products;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => match &select.select_list[0] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::Function { name, .. } => {
                        assert_eq!(name, "AVG");
                    }
                    _ => panic!("Expected AVG function"),
                },
                _ => panic!("Expected expression"),
            },
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_min_max_functions() {
        let result = Parser::parse_sql("SELECT MIN(price), MAX(price) FROM products;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert_eq!(select.select_list.len(), 2);

                // Check MIN
                match &select.select_list[0] {
                    ast::SelectItem::Expression { expr, .. } => match expr {
                        ast::Expression::Function { name, .. } => {
                            assert_eq!(name, "MIN");
                        }
                        _ => panic!("Expected MIN function"),
                    },
                    _ => panic!("Expected expression"),
                }

                // Check MAX
                match &select.select_list[1] {
                    ast::SelectItem::Expression { expr, .. } => match expr {
                        ast::Expression::Function { name, .. } => {
                            assert_eq!(name, "MAX");
                        }
                        _ => panic!("Expected MAX function"),
                    },
                    _ => panic!("Expected expression"),
                }
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_aggregate_with_alias() {
        let result = Parser::parse_sql("SELECT COUNT(*) AS total FROM users;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias } => {
                    match expr {
                        ast::Expression::Function { name, .. } => {
                            assert_eq!(name, "COUNT");
                        }
                        _ => panic!("Expected function"),
                    }
                    assert_eq!(alias.as_ref().unwrap(), "total");
                }
                _ => panic!("Expected expression with alias"),
            },
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_multiple_aggregates() {
        let result = Parser::parse_sql("SELECT COUNT(*), SUM(amount), AVG(amount) FROM orders;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert_eq!(select.select_list.len(), 3);

                // Verify all are functions
                for item in &select.select_list {
                    match item {
                        ast::SelectItem::Expression { expr, .. } => match expr {
                            ast::Expression::Function { .. } => {} // Success
                            _ => panic!("Expected function"),
                        },
                        _ => panic!("Expected expression"),
                    }
                }
            }
            _ => panic!("Expected SELECT"),
        }
    }

    // ========================================================================
    // GROUP BY and HAVING Tests
    // ========================================================================

    #[test]
    fn test_parse_group_by_single_column() {
        let result = Parser::parse_sql("SELECT name, COUNT(*) FROM users GROUP BY name;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert!(select.group_by.is_some());
                let group_by = select.group_by.unwrap();
                assert_eq!(group_by.len(), 1);
                match &group_by[0] {
                    ast::Expression::ColumnRef { column, .. } if column == "name" => {}
                    _ => panic!("Expected column reference 'name'"),
                }
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_group_by_multiple_columns() {
        let result =
            Parser::parse_sql("SELECT dept, role, COUNT(*) FROM users GROUP BY dept, role;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert!(select.group_by.is_some());
                let group_by = select.group_by.unwrap();
                assert_eq!(group_by.len(), 2);
                match &group_by[0] {
                    ast::Expression::ColumnRef { column, .. } if column == "dept" => {}
                    _ => panic!("Expected column reference 'dept'"),
                }
                match &group_by[1] {
                    ast::Expression::ColumnRef { column, .. } if column == "role" => {}
                    _ => panic!("Expected column reference 'role'"),
                }
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_having_clause() {
        let result = Parser::parse_sql(
            "SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 5;",
        );
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert!(select.group_by.is_some());
                assert!(select.having.is_some());

                // HAVING should contain a comparison expression
                match select.having.as_ref().unwrap() {
                    ast::Expression::BinaryOp { op, .. } => {
                        assert_eq!(*op, ast::BinaryOperator::GreaterThan);
                    }
                    _ => panic!("Expected comparison in HAVING clause"),
                }
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_group_by_with_where_and_having() {
        let result = Parser::parse_sql(
            "SELECT dept, COUNT(*) FROM users WHERE active = true GROUP BY dept HAVING COUNT(*) > 10;"
        );
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                // Should have WHERE, GROUP BY, and HAVING
                assert!(select.where_clause.is_some());
                assert!(select.group_by.is_some());
                assert!(select.having.is_some());
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_group_by_qualified_columns() {
        let result = Parser::parse_sql("SELECT u.dept, COUNT(*) FROM users u GROUP BY u.dept;");
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                assert!(select.group_by.is_some());
                let group_by = select.group_by.unwrap();
                assert_eq!(group_by.len(), 1);
                match &group_by[0] {
                    ast::Expression::ColumnRef { table, column } => {
                        assert_eq!(table.as_ref().unwrap(), "u");
                        assert_eq!(column, "dept");
                    }
                    _ => panic!("Expected qualified column reference"),
                }
            }
            _ => panic!("Expected SELECT"),
        }
    }

    #[test]
    fn test_parse_group_by_with_order_by() {
        let result = Parser::parse_sql(
            "SELECT name, COUNT(*) as cnt FROM users GROUP BY name ORDER BY cnt DESC;",
        );
        assert!(result.is_ok());
        let stmt = result.unwrap();

        match stmt {
            ast::Statement::Select(select) => {
                // Should have both GROUP BY and ORDER BY
                assert!(select.group_by.is_some());
                assert!(select.order_by.is_some());
            }
            _ => panic!("Expected SELECT"),
        }
    }
}
