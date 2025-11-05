use crate::keywords::Keyword;
use crate::lexer::Lexer;
use crate::token::Token;
use std::fmt;

mod advanced_objects;
mod alter;
mod create;
mod cursor;
mod delete;
mod domain;
mod drop;
mod expressions;
mod grant;
mod helpers;
mod index;
mod insert;
mod revoke;
mod role;
mod schema;
mod select;
mod transaction;
mod trigger;
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
                } else if self.peek_next_keyword(Keyword::Trigger) {
                    Ok(ast::Statement::CreateTrigger(self.parse_create_trigger_statement()?))
                } else if self.peek_next_keyword(Keyword::Index)
                    || self.peek_next_keyword(Keyword::Unique)
                {
                    Ok(ast::Statement::CreateIndex(self.parse_create_index_statement()?))
                } else if self.peek_next_keyword(Keyword::Assertion) {
                    Ok(ast::Statement::CreateAssertion(self.parse_create_assertion_statement()?))
                } else {
                    Err(ParseError {
                        message:
                            "Expected TABLE, SCHEMA, ROLE, DOMAIN, SEQUENCE, TYPE, COLLATION, CHARACTER, TRANSLATION, VIEW, TRIGGER, INDEX, or ASSERTION after CREATE"
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
                } else if self.peek_next_keyword(Keyword::Trigger) {
                    Ok(ast::Statement::DropTrigger(self.parse_drop_trigger_statement()?))
                } else if self.peek_next_keyword(Keyword::Index) {
                    Ok(ast::Statement::DropIndex(self.parse_drop_index_statement()?))
                } else if self.peek_next_keyword(Keyword::Assertion) {
                    Ok(ast::Statement::DropAssertion(self.parse_drop_assertion_statement()?))
                } else {
                    Err(ParseError {
                        message:
                            "Expected TABLE, SCHEMA, ROLE, DOMAIN, SEQUENCE, TYPE, COLLATION, CHARACTER, TRANSLATION, VIEW, TRIGGER, INDEX, or ASSERTION after DROP"
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
            Token::Keyword(Keyword::Set) => {
                // Look ahead to determine which SET statement this is
                if self.peek_next_keyword(Keyword::Schema) {
                    let set_stmt = self.parse_set_schema_statement()?;
                    Ok(ast::Statement::SetSchema(set_stmt))
                } else if self.peek_next_keyword(Keyword::Catalog) {
                    let set_stmt = schema::parse_set_catalog(self)?;
                    Ok(ast::Statement::SetCatalog(set_stmt))
                } else if self.peek_next_keyword(Keyword::Names) {
                    let set_stmt = schema::parse_set_names(self)?;
                    Ok(ast::Statement::SetNames(set_stmt))
                } else if self.peek_next_keyword(Keyword::Time) {
                    let set_stmt = schema::parse_set_time_zone(self)?;
                    Ok(ast::Statement::SetTimeZone(set_stmt))
                } else if self.peek_next_keyword(Keyword::Transaction) {
                    let set_stmt = self.parse_set_transaction_statement()?;
                    Ok(ast::Statement::SetTransaction(set_stmt))
                } else if self.peek_next_keyword(Keyword::Local) {
                    // SET LOCAL TRANSACTION
                    let set_stmt = self.parse_set_transaction_statement()?;
                    Ok(ast::Statement::SetTransaction(set_stmt))
                } else {
                    Err(ParseError {
                        message:
                            "Expected SCHEMA, CATALOG, NAMES, TIME ZONE, TRANSACTION, or LOCAL after SET"
                                .to_string(),
                    })
                }
            }
            Token::Keyword(Keyword::Grant) => {
                let grant_stmt = self.parse_grant_statement()?;
                Ok(ast::Statement::Grant(grant_stmt))
            }
            Token::Keyword(Keyword::Revoke) => {
                let revoke_stmt = self.parse_revoke_statement()?;
                Ok(ast::Statement::Revoke(revoke_stmt))
            }
            Token::Keyword(Keyword::Declare) => {
                let declare_cursor_stmt = self.parse_declare_cursor_statement()?;
                Ok(ast::Statement::DeclareCursor(declare_cursor_stmt))
            }
            Token::Keyword(Keyword::Open) => {
                let open_cursor_stmt = self.parse_open_cursor_statement()?;
                Ok(ast::Statement::OpenCursor(open_cursor_stmt))
            }
            Token::Keyword(Keyword::Fetch) => {
                let fetch_stmt = self.parse_fetch_statement()?;
                Ok(ast::Statement::Fetch(fetch_stmt))
            }
            Token::Keyword(Keyword::Close) => {
                let close_cursor_stmt = self.parse_close_cursor_statement()?;
                Ok(ast::Statement::CloseCursor(close_cursor_stmt))
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

    /// Parse SET TRANSACTION statement
    pub fn parse_set_transaction_statement(
        &mut self,
    ) -> Result<ast::SetTransactionStmt, ParseError> {
        // SET keyword
        self.expect_keyword(Keyword::Set)?;

        // Optional LOCAL keyword
        let local = self.try_consume_keyword(Keyword::Local);

        // TRANSACTION keyword
        self.expect_keyword(Keyword::Transaction)?;

        // Parse optional characteristics
        let mut isolation_level = None;
        let mut access_mode = None;

        loop {
            if self.try_consume_keyword(Keyword::Serializable) {
                isolation_level = Some(ast::IsolationLevel::Serializable);
            } else if self.try_consume_keyword(Keyword::Read) {
                if self.try_consume_keyword(Keyword::Only) {
                    access_mode = Some(ast::TransactionAccessMode::ReadOnly);
                } else if self.try_consume_keyword(Keyword::Write) {
                    access_mode = Some(ast::TransactionAccessMode::ReadWrite);
                } else {
                    return Err(ParseError {
                        message: "Expected ONLY or WRITE after READ".to_string(),
                    });
                }
            } else if self.try_consume_keyword(Keyword::Isolation) {
                self.expect_keyword(Keyword::Level)?;
                if self.try_consume_keyword(Keyword::Serializable) {
                    isolation_level = Some(ast::IsolationLevel::Serializable);
                } else {
                    return Err(ParseError {
                        message: "Expected SERIALIZABLE after ISOLATION LEVEL".to_string(),
                    });
                }
            } else {
                break;
            }

            // Check for comma (more characteristics)
            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(ast::SetTransactionStmt { local, isolation_level, access_mode })
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

    /// Parse CREATE ASSERTION statement
    pub fn parse_create_assertion_statement(
        &mut self,
    ) -> Result<ast::CreateAssertionStmt, ParseError> {
        advanced_objects::parse_create_assertion(self)
    }

    /// Parse DROP ASSERTION statement
    pub fn parse_drop_assertion_statement(&mut self) -> Result<ast::DropAssertionStmt, ParseError> {
        advanced_objects::parse_drop_assertion(self)
    }

    /// Parse a numeric value that can be integer or float (converts float to int by truncation)
    fn parse_numeric_value(&mut self) -> Result<Option<i64>, ParseError> {
        if let Token::Number(n) = self.peek() {
            // Try parsing as i64 first, then as f64 and truncate
            let val = if let Ok(int_val) = n.parse::<i64>() {
                int_val
            } else if let Ok(float_val) = n.parse::<f64>() {
                float_val as i64
            } else {
                return Err(ParseError {
                    message: "Invalid numeric value".to_string(),
                });
            };
            self.advance();
            Ok(Some(val))
        } else {
            Ok(None)
        }
    }

    /// Parse MySQL table options for CREATE TABLE
    pub fn parse_table_options(&mut self) -> Result<Vec<ast::TableOption>, ParseError> {
        let mut options = Vec::new();

        loop {
            // Check for table option keywords
            let option = if self.try_consume_keyword(Keyword::KeyBlockSize) {
                self.parse_key_block_size_option()?
            } else if self.try_consume_keyword(Keyword::Connection) {
                self.parse_connection_option()?
            } else if self.try_consume_keyword(Keyword::InsertMethod) {
                self.parse_insert_method_option()?
            } else if self.try_consume_keyword(Keyword::Union) {
                self.parse_union_option()?
            } else if self.try_consume_keyword(Keyword::RowFormat) {
                self.parse_row_format_option()?
            } else if self.try_consume_keyword(Keyword::DelayKeyWrite) {
                self.parse_delay_key_write_option()?
            } else if self.try_consume_keyword(Keyword::TableChecksum) || self.try_consume_keyword(Keyword::Checksum) {
                self.parse_table_checksum_option()?
            } else if self.try_consume_keyword(Keyword::StatsSamplePages) {
                self.parse_stats_sample_pages_option()?
            } else if self.try_consume_keyword(Keyword::Password) {
                self.parse_password_option()?
            } else if self.try_consume_keyword(Keyword::AvgRowLength) {
                self.parse_avg_row_length_option()?
            } else if self.try_consume_keyword(Keyword::MinRows) {
                self.parse_min_rows_option()?
            } else if self.try_consume_keyword(Keyword::MaxRows) {
                self.parse_max_rows_option()?
            } else if self.try_consume_keyword(Keyword::SecondaryEngine) {
                self.parse_secondary_engine_option()?
            } else if self.try_consume_keyword(Keyword::Collate) {
                self.parse_collate_option()?
            } else if self.try_consume_keyword(Keyword::Comment) {
                self.parse_comment_option()?
            } else {
                // No more table options
                break;
            };

            options.push(option);

            // Table options can be separated by commas (MySQL style) or just spaces
            self.try_consume(&Token::Comma);
        }

        Ok(options)
    }

    fn parse_key_block_size_option(&mut self) -> Result<ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse value
        let value = self.parse_numeric_value()?;
        Ok(ast::TableOption::KeyBlockSize(value))
    }

    fn parse_connection_option(&mut self) -> Result<ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse string value
        match self.peek() {
            Token::String(s) => {
                let val = s.clone();
                self.advance();
                Ok(ast::TableOption::Connection(Some(val)))
            }
            _ => Err(ParseError {
                message: "Expected string value for CONNECTION".to_string(),
            }),
        }
    }

    fn parse_insert_method_option(&mut self) -> Result<ast::TableOption, ParseError> {
        self.expect_token(Token::Symbol('='))?;
        let method = if self.try_consume_keyword(Keyword::First) {
            ast::InsertMethod::First
        } else if self.try_consume_keyword(Keyword::Last) {
            ast::InsertMethod::Last
        } else if self.try_consume_keyword(Keyword::No) {
            ast::InsertMethod::No
        } else {
            return Err(ParseError {
                message: "Expected FIRST, LAST, or NO for INSERT_METHOD".to_string(),
            });
        };
        Ok(ast::TableOption::InsertMethod(method))
    }

    fn parse_union_option(&mut self) -> Result<ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Optional parentheses
        self.try_consume(&Token::LParen);
        let mut tables = Vec::new();
        if !self.try_consume(&Token::RParen) {
            loop {
                match self.peek() {
                    Token::Identifier(id) | Token::DelimitedIdentifier(id) => {
                        tables.push(id.clone());
                        self.advance();
                    }
                    _ => return Err(ParseError {
                        message: "Expected table name in UNION".to_string(),
                    }),
                }
                if self.try_consume(&Token::Comma) {
                    continue;
                } else {
                    break;
                }
            }
            self.expect_token(Token::RParen)?;
        }
        Ok(ast::TableOption::Union(Some(tables)))
    }

    fn parse_row_format_option(&mut self) -> Result<ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        let format = if self.try_consume_keyword(Keyword::Default) {
            ast::RowFormat::Default
        } else if self.try_consume_keyword(Keyword::Dynamic) {
            ast::RowFormat::Dynamic
        } else if self.try_consume_keyword(Keyword::Fixed) {
            ast::RowFormat::Fixed
        } else if self.try_consume_keyword(Keyword::Compressed) {
            ast::RowFormat::Compressed
        } else if self.try_consume_keyword(Keyword::Redundant) {
            ast::RowFormat::Redundant
        } else if self.try_consume_keyword(Keyword::Compact) {
            ast::RowFormat::Compact
        } else {
            return Err(ParseError {
                message: "Expected row format value".to_string(),
            });
        };
        Ok(ast::TableOption::RowFormat(Some(format)))
    }

    fn parse_delay_key_write_option(&mut self) -> Result<ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse numeric value
        let value = self.parse_numeric_value()?;
        Ok(ast::TableOption::DelayKeyWrite(value))
    }

    fn parse_table_checksum_option(&mut self) -> Result<ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse numeric value
        let value = self.parse_numeric_value()?;
        Ok(ast::TableOption::TableChecksum(value))
    }

    fn parse_stats_sample_pages_option(&mut self) -> Result<ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse numeric value
        let value = self.parse_numeric_value()?;
        Ok(ast::TableOption::StatsSamplePages(value))
    }

    fn parse_password_option(&mut self) -> Result<ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse string value
        match self.peek() {
            Token::String(s) => {
                let val = s.clone();
                self.advance();
                Ok(ast::TableOption::Password(Some(val)))
            }
            _ => Err(ParseError {
                message: "Expected string value for PASSWORD".to_string(),
            }),
        }
    }

    fn parse_avg_row_length_option(&mut self) -> Result<ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse numeric value
        let value = self.parse_numeric_value()?;
        Ok(ast::TableOption::AvgRowLength(value))
    }

    fn parse_min_rows_option(&mut self) -> Result<ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse numeric value
        let value = self.parse_numeric_value()?;
        Ok(ast::TableOption::MinRows(value))
    }

    fn parse_max_rows_option(&mut self) -> Result<ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse numeric value
        let value = self.parse_numeric_value()?;
        Ok(ast::TableOption::MaxRows(value))
    }

    fn parse_secondary_engine_option(&mut self) -> Result<ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse identifier or NULL
        let value = if self.try_consume_keyword(Keyword::Null) {
            Some("NULL".to_string())
        } else if let Token::Identifier(id) = self.peek() {
            let val = id.clone();
            self.advance();
            Some(val)
        } else {
            None
        };
        Ok(ast::TableOption::SecondaryEngine(value))
    }

    fn parse_collate_option(&mut self) -> Result<ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse collation name
        match self.peek() {
            Token::Identifier(id) => {
                let val = id.clone();
                self.advance();
                Ok(ast::TableOption::Collate(Some(val)))
            }
            _ => Err(ParseError {
                message: "Expected collation name".to_string(),
            }),
        }
    }

    fn parse_comment_option(&mut self) -> Result<ast::TableOption, ParseError> {
        // Optional = sign
        self.try_consume(&Token::Symbol('='));
        // Parse string value
        match self.peek() {
            Token::String(s) => {
                let val = s.clone();
                self.advance();
                Ok(ast::TableOption::Comment(Some(val)))
            }
            _ => Err(ParseError {
                message: "Expected string value for COMMENT".to_string(),
            }),
        }
    }
}
