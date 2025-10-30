use crate::keywords::Keyword;
use crate::lexer::Lexer;
use crate::token::Token;
use std::fmt;

mod alter;
mod create;
mod delete;
mod drop;
mod expressions;
mod grant;
mod helpers;
mod insert;
mod revoke;
mod role;
mod schema;
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
            Token::Keyword(Keyword::Create) => match self.peek_next_keyword(Keyword::Table) {
                true => {
                    let create_stmt = self.parse_create_table_statement()?;
                    Ok(ast::Statement::CreateTable(create_stmt))
                }
                false => match self.peek_next_keyword(Keyword::Schema) {
                    true => {
                        let create_stmt = self.parse_create_schema_statement()?;
                        Ok(ast::Statement::CreateSchema(create_stmt))
                    }
                    false => match self.peek_next_keyword(Keyword::Role) {
                        true => {
                            let create_stmt = self.parse_create_role_statement()?;
                            Ok(ast::Statement::CreateRole(create_stmt))
                        }
                        false => Err(ParseError {
                            message: "Expected TABLE, SCHEMA, or ROLE after CREATE".to_string(),
                        }),
                    },
                },
            },
            Token::Keyword(Keyword::Drop) => match self.peek_next_keyword(Keyword::Table) {
                true => {
                    let drop_stmt = self.parse_drop_table_statement()?;
                    Ok(ast::Statement::DropTable(drop_stmt))
                }
                false => match self.peek_next_keyword(Keyword::Schema) {
                    true => {
                        let drop_stmt = self.parse_drop_schema_statement()?;
                        Ok(ast::Statement::DropSchema(drop_stmt))
                    }
                    false => match self.peek_next_keyword(Keyword::Role) {
                        true => {
                            let drop_stmt = self.parse_drop_role_statement()?;
                            Ok(ast::Statement::DropRole(drop_stmt))
                        }
                        false => Err(ParseError {
                            message: "Expected TABLE, SCHEMA, or ROLE after DROP".to_string(),
                        }),
                    },
                },
            },
            Token::Keyword(Keyword::Alter) => {
                let alter_stmt = self.parse_alter_table_statement()?;
                Ok(ast::Statement::AlterTable(alter_stmt))
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
                // Check if this is ROLLBACK TO SAVEPOINT by looking ahead
                let saved_position = self.position;
                self.advance(); // consume ROLLBACK
                if self.peek_keyword(Keyword::To) {
                    // Reset and parse as ROLLBACK TO SAVEPOINT
                    self.position = saved_position;
                    let rollback_to_stmt = self.parse_rollback_to_savepoint_statement()?;
                    Ok(ast::Statement::RollbackToSavepoint(rollback_to_stmt))
                } else {
                    // Reset and parse as regular ROLLBACK
                    self.position = saved_position;
                    let rollback_stmt = self.parse_rollback_statement()?;
                    Ok(ast::Statement::Rollback(rollback_stmt))
                }
            }
            Token::Keyword(Keyword::Savepoint) => {
                let savepoint_stmt = self.parse_savepoint_statement()?;
                Ok(ast::Statement::Savepoint(savepoint_stmt))
            }
            Token::Keyword(Keyword::Release) => {
                let release_stmt = self.parse_release_savepoint_statement()?;
                Ok(ast::Statement::ReleaseSavepoint(release_stmt))
            }
            Token::Keyword(Keyword::Set) => match self.peek_keyword(Keyword::Schema) {
                true => {
                    let set_stmt = self.parse_set_schema_statement()?;
                    Ok(ast::Statement::SetSchema(set_stmt))
                }
                false => Err(ParseError { message: "Expected SCHEMA after SET".to_string() }),
            },
            Token::Keyword(Keyword::Grant) => {
                let grant_stmt = self.parse_grant_statement()?;
                Ok(ast::Statement::Grant(grant_stmt))
            }
            Token::Keyword(Keyword::Revoke) => {
                let revoke_stmt = self.parse_revoke_statement()?;
                Ok(ast::Statement::Revoke(revoke_stmt))
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

    /// Parse ALTER TABLE statement
    pub fn parse_alter_table_statement(&mut self) -> Result<ast::AlterTableStmt, ParseError> {
        alter::parse_alter_table(self)
    }

    /// Parse SAVEPOINT statement
    pub fn parse_savepoint_statement(&mut self) -> Result<ast::SavepointStmt, ParseError> {
        transaction::parse_savepoint_statement(self)
    }

    /// Parse ROLLBACK TO SAVEPOINT statement
    pub fn parse_rollback_to_savepoint_statement(
        &mut self,
    ) -> Result<ast::RollbackToSavepointStmt, ParseError> {
        transaction::parse_rollback_to_savepoint_statement(self)
    }

    /// Parse RELEASE SAVEPOINT statement
    pub fn parse_release_savepoint_statement(
        &mut self,
    ) -> Result<ast::ReleaseSavepointStmt, ParseError> {
        transaction::parse_release_savepoint_statement(self)
    }

    /// Parse CREATE SCHEMA statement
    pub fn parse_create_schema_statement(&mut self) -> Result<ast::CreateSchemaStmt, ParseError> {
        schema::parse_create_schema(self)
    }

    /// Parse DROP SCHEMA statement
    pub fn parse_drop_schema_statement(&mut self) -> Result<ast::DropSchemaStmt, ParseError> {
        schema::parse_drop_schema(self)
    }

    /// Parse SET SCHEMA statement
    pub fn parse_set_schema_statement(&mut self) -> Result<ast::SetSchemaStmt, ParseError> {
        schema::parse_set_schema(self)
    }

    /// Parse GRANT statement
    pub fn parse_grant_statement(&mut self) -> Result<ast::GrantStmt, ParseError> {
        grant::parse_grant(self)
    }

    /// Parse REVOKE statement
    pub fn parse_revoke_statement(&mut self) -> Result<ast::RevokeStmt, ParseError> {
        revoke::parse_revoke(self)
    }

    /// Parse CREATE ROLE statement
    pub fn parse_create_role_statement(&mut self) -> Result<ast::CreateRoleStmt, ParseError> {
        role::parse_create_role(self)
    }

    /// Parse DROP ROLE statement
    pub fn parse_drop_role_statement(&mut self) -> Result<ast::DropRoleStmt, ParseError> {
        role::parse_drop_role(self)
    }
}
