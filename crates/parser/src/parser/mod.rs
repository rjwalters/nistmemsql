use crate::keywords::Keyword;
use crate::lexer::Lexer;
use crate::token::Token;
use std::fmt;

mod create;
mod delete;
mod expressions;
mod helpers;
mod insert;
mod select;
mod update;

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
}
