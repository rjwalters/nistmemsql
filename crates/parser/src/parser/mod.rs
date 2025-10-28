use crate::keywords::Keyword;
use crate::lexer::Lexer;
use crate::token::Token;
use std::fmt;

mod create;
mod delete;
mod drop;
mod expressions;
mod helpers;
mod insert;
mod select;
mod transaction;
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
            Token::Keyword(Keyword::Select) | Token::Keyword(Keyword::With) => {
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
            Token::Keyword(Keyword::Drop) => {
                let drop_stmt = self.parse_drop_table_statement()?;
                Ok(ast::Statement::DropTable(drop_stmt))
            }
            Token::Keyword(Keyword::Begin) | Token::Keyword(Keyword::Start) => {
                let begin_stmt = self.parse_begin_statement()?;
                Ok(ast::Statement::BeginTransaction(begin_stmt))
            }
            Token::Keyword(Keyword::Commit) => {
                let commit_stmt = self.parse_commit_statement()?;
                Ok(ast::Statement::Commit(commit_stmt))
            }
            Token::Keyword(Keyword::Rollback) => {
                let rollback_stmt = self.parse_rollback_statement()?;
                Ok(ast::Statement::Rollback(rollback_stmt))
            }
            _ => {
                Err(ParseError { message: format!("Expected statement, found {:?}", self.peek()) })
            }
        }
    }

    /// Parse BEGIN [TRANSACTION] statement
    pub fn parse_begin_statement(&mut self) -> Result<ast::BeginStmt, ParseError> {
        transaction::parse_begin_statement(self)
    }

    /// Parse COMMIT statement
    pub fn parse_commit_statement(&mut self) -> Result<ast::CommitStmt, ParseError> {
        transaction::parse_commit_statement(self)
    }

    /// Parse ROLLBACK statement
    pub fn parse_rollback_statement(&mut self) -> Result<ast::RollbackStmt, ParseError> {
        transaction::parse_rollback_statement(self)
    }
}
