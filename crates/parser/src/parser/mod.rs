use crate::keywords::Keyword;
use crate::lexer::Lexer;
use crate::token::Token;
use std::fmt;

mod advanced_objects;
mod alter;
mod create;
mod delete;
mod domain;
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
mod view;

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
                if self.peek_next_keyword(Keyword::Table) {
                    Ok(ast::Statement::CreateTable(self.parse_create_table_statement()?))
                } else if self.peek_next_keyword(Keyword::Schema) {
                    Ok(ast::Statement::CreateSchema(self.parse_create_schema_statement()?))
                } else if self.peek_next_keyword(Keyword::Role) {
                    Ok(ast::Statement::CreateRole(self.parse_create_role_statement()?))
                } else if self.peek_next_keyword(Keyword::Domain) {
                    Ok(ast::Statement::CreateDomain(self.parse_create_domain_statement()?))
                } else if self.peek_next_keyword(Keyword::Sequence) {
                    Ok(ast::Statement::CreateSequence(self.parse_create_sequence_statement()?))
                } else if self.peek_next_keyword(Keyword::Type) {
                    Ok(ast::Statement::CreateType(self.parse_create_type_statement()?))
                } else if self.peek_next_keyword(Keyword::Collation) {
                    Ok(ast::Statement::CreateCollation(self.parse_create_collation_statement()?))
                } else if self.peek_next_keyword(Keyword::Character) {
                    Ok(ast::Statement::CreateCharacterSet(
                        self.parse_create_character_set_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::Translation) {
                    Ok(ast::Statement::CreateTranslation(
                        self.parse_create_translation_statement()?,
                    ))
                } else if self.peek_next_keyword(Keyword::View) {
                    Ok(ast::Statement::CreateView(self.parse_create_view_statement()?))
                } else {
                    Err(ParseError {
                        message:
                            "Expected TABLE, SCHEMA, ROLE, DOMAIN, SEQUENCE, TYPE, COLLATION, CHARACTER, TRANSLATION, or VIEW after CREATE"
                                .to_string(),
                    })
                }
            }
            Token::Keyword(Keyword::Drop) => {
                if self.peek_next_keyword(Keyword::Table) {
                    Ok(ast::Statement::DropTable(self.parse_drop_table_statement()?))
                } else if self.peek_next_keyword(Keyword::Schema) {
                    Ok(ast::Statement::DropSchema(self.parse_drop_schema_statement()?))
                } else if self.peek_next_keyword(Keyword::Role) {
                    Ok(ast::Statement::DropRole(self.parse_drop_role_statement()?))
                } else if self.peek_next_keyword(Keyword::Domain) {
                    Ok(ast::Statement::DropDomain(self.parse_drop_domain_statement()?))
                } else if self.peek_next_keyword(Keyword::Sequence) {
                    Ok(ast::Statement::DropSequence(self.parse_drop_sequence_statement()?))
                } else if self.peek_next_keyword(Keyword::Type) {
                    Ok(ast::Statement::DropType(self.parse_drop_type_statement()?))
                } else if self.peek_next_keyword(Keyword::Collation) {
                    Ok(ast::Statement::DropCollation(self.parse_drop_collation_statement()?))
                } else if self.peek_next_keyword(Keyword::Character) {
                    Ok(ast::Statement::DropCharacterSet(self.parse_drop_character_set_statement()?))
                } else if self.peek_next_keyword(Keyword::Translation) {
                    Ok(ast::Statement::DropTranslation(self.parse_drop_translation_statement()?))
                } else if self.peek_next_keyword(Keyword::View) {
                    Ok(ast::Statement::DropView(self.parse_drop_view_statement()?))
                } else {
                    Err(ParseError {
                        message:
                            "Expected TABLE, SCHEMA, ROLE, DOMAIN, SEQUENCE, TYPE, COLLATION, CHARACTER, TRANSLATION, or VIEW after DROP"
                                .to_string(),
                    })
                }
            }
            Token::Keyword(Keyword::Alter) => {
                if self.peek_next_keyword(Keyword::Table) {
                    let alter_stmt = self.parse_alter_table_statement()?;
                    Ok(ast::Statement::AlterTable(alter_stmt))
                } else if self.peek_next_keyword(Keyword::Sequence) {
                    let alter_stmt = self.parse_alter_sequence_statement()?;
                    Ok(ast::Statement::AlterSequence(alter_stmt))
                } else {
                    Err(ParseError {
                        message: "Expected TABLE or SEQUENCE after ALTER".to_string(),
                    })
                }
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

    // ========================================================================
    // Advanced SQL Object Parsers (SQL:1999)
    // ========================================================================

    /// Parse CREATE DOMAIN statement (uses full implementation from domain module)
    pub fn parse_create_domain_statement(&mut self) -> Result<ast::CreateDomainStmt, ParseError> {
        domain::parse_create_domain(self)
    }

    /// Parse DROP DOMAIN statement (uses full implementation from domain module)
    pub fn parse_drop_domain_statement(&mut self) -> Result<ast::DropDomainStmt, ParseError> {
        domain::parse_drop_domain(self)
    }

    /// Parse CREATE SEQUENCE statement
    pub fn parse_create_sequence_statement(
        &mut self,
    ) -> Result<ast::CreateSequenceStmt, ParseError> {
        advanced_objects::parse_create_sequence(self)
    }

    /// Parse DROP SEQUENCE statement
    pub fn parse_drop_sequence_statement(&mut self) -> Result<ast::DropSequenceStmt, ParseError> {
        advanced_objects::parse_drop_sequence(self)
    }

    /// Parse ALTER SEQUENCE statement
    pub fn parse_alter_sequence_statement(&mut self) -> Result<ast::AlterSequenceStmt, ParseError> {
        advanced_objects::parse_alter_sequence(self)
    }

    /// Parse CREATE TYPE statement
    pub fn parse_create_type_statement(&mut self) -> Result<ast::CreateTypeStmt, ParseError> {
        advanced_objects::parse_create_type(self)
    }

    /// Parse DROP TYPE statement
    pub fn parse_drop_type_statement(&mut self) -> Result<ast::DropTypeStmt, ParseError> {
        advanced_objects::parse_drop_type(self)
    }

    /// Parse CREATE COLLATION statement
    pub fn parse_create_collation_statement(
        &mut self,
    ) -> Result<ast::CreateCollationStmt, ParseError> {
        advanced_objects::parse_create_collation(self)
    }

    /// Parse DROP COLLATION statement
    pub fn parse_drop_collation_statement(&mut self) -> Result<ast::DropCollationStmt, ParseError> {
        advanced_objects::parse_drop_collation(self)
    }

    /// Parse CREATE CHARACTER SET statement
    pub fn parse_create_character_set_statement(
        &mut self,
    ) -> Result<ast::CreateCharacterSetStmt, ParseError> {
        advanced_objects::parse_create_character_set(self)
    }

    /// Parse DROP CHARACTER SET statement
    pub fn parse_drop_character_set_statement(
        &mut self,
    ) -> Result<ast::DropCharacterSetStmt, ParseError> {
        advanced_objects::parse_drop_character_set(self)
    }

    /// Parse CREATE TRANSLATION statement
    pub fn parse_create_translation_statement(
        &mut self,
    ) -> Result<ast::CreateTranslationStmt, ParseError> {
        advanced_objects::parse_create_translation(self)
    }

    /// Parse DROP TRANSLATION statement
    pub fn parse_drop_translation_statement(
        &mut self,
    ) -> Result<ast::DropTranslationStmt, ParseError> {
        advanced_objects::parse_drop_translation(self)
    }
}
