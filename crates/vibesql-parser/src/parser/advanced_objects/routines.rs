//! Stored procedures and functions parsing (SQL:1999 Feature P001)
//!
//! This module parses:
//! - CREATE PROCEDURE statements
//! - CREATE FUNCTION statements
//! - CALL statements
//! - DROP PROCEDURE statements
//! - DROP FUNCTION statements
//!
//! Syntax (MySQL-compatible):
//!
//! CREATE PROCEDURE procedure_name ([param_list])
//! BEGIN
//!   -- statements
//! END;
//!
//! CREATE FUNCTION function_name ([param_list])
//! RETURNS data_type
//! BEGIN
//!   -- statements
//!   RETURN value;
//! END;
//!
//! CALL procedure_name([args]);
//!
//! DROP PROCEDURE [IF EXISTS] procedure_name;
//! DROP FUNCTION [IF EXISTS] function_name;

use crate::keywords::Keyword;
use crate::parser::{ParseError, Parser};
use crate::token::Token;
use vibesql_ast::{
    CallStmt, CreateFunctionStmt, CreateProcedureStmt, DropFunctionStmt, DropProcedureStmt,
    FunctionParameter, ParameterMode, ProcedureBody, ProcedureParameter, ProceduralStatement,
};

impl Parser {
    /// Parse CREATE PROCEDURE statement
    ///
    /// Syntax: CREATE PROCEDURE proc_name ([param_list]) BEGIN ... END;
    pub fn parse_create_procedure(&mut self) -> Result<CreateProcedureStmt, ParseError> {
        // Already consumed CREATE PROCEDURE
        let procedure_name = self.parse_identifier()?;

        self.expect_token(Token::LParen)?;
        let parameters = self.parse_procedure_parameters()?;
        self.expect_token(Token::RParen)?;

        let body = self.parse_procedure_body()?;

        Ok(CreateProcedureStmt {
            procedure_name,
            parameters,
            body,
        })
    }

    /// Parse CREATE FUNCTION statement
    ///
    /// Syntax: CREATE FUNCTION func_name ([param_list]) RETURNS data_type BEGIN ... END;
    pub fn parse_create_function(&mut self) -> Result<CreateFunctionStmt, ParseError> {
        // Already consumed CREATE FUNCTION
        let function_name = self.parse_identifier()?;

        self.expect_token(Token::LParen)?;
        let parameters = self.parse_function_parameters()?;
        self.expect_token(Token::RParen)?;

        self.expect_keyword(Keyword::Returns)?;
        let return_type = self.parse_data_type()?;

        let body = self.parse_procedure_body()?;

        Ok(CreateFunctionStmt {
            function_name,
            parameters,
            return_type,
            body,
        })
    }

    /// Parse DROP PROCEDURE statement
    ///
    /// Syntax: DROP PROCEDURE [IF EXISTS] proc_name;
    pub fn parse_drop_procedure(&mut self) -> Result<DropProcedureStmt, ParseError> {
        // Already consumed DROP PROCEDURE
        let if_exists = self.try_consume_keyword(Keyword::If);
        if if_exists {
            self.expect_keyword(Keyword::Exists)?;
        }

        let procedure_name = self.parse_identifier()?;

        Ok(DropProcedureStmt {
            procedure_name,
            if_exists,
        })
    }

    /// Parse DROP FUNCTION statement
    ///
    /// Syntax: DROP FUNCTION [IF EXISTS] func_name;
    pub fn parse_drop_function(&mut self) -> Result<DropFunctionStmt, ParseError> {
        // Already consumed DROP FUNCTION
        let if_exists = self.try_consume_keyword(Keyword::If);
        if if_exists {
            self.expect_keyword(Keyword::Exists)?;
        }

        let function_name = self.parse_identifier()?;

        Ok(DropFunctionStmt {
            function_name,
            if_exists,
        })
    }

    /// Parse CALL statement
    ///
    /// Syntax: CALL procedure_name([args]);
    pub fn parse_call(&mut self) -> Result<CallStmt, ParseError> {
        // Already consumed CALL
        let procedure_name = self.parse_identifier()?;

        self.expect_token(Token::LParen)?;
        let arguments = self.parse_expression_list()?;
        self.expect_token(Token::RParen)?;

        Ok(CallStmt {
            procedure_name,
            arguments,
        })
    }

    /// Parse procedure parameters: [param_mode] name data_type [, ...]
    ///
    /// Example: IN user_id INT, OUT result VARCHAR(100), INOUT status CHAR
    fn parse_procedure_parameters(&mut self) -> Result<Vec<ProcedureParameter>, ParseError> {
        let mut parameters = Vec::new();

        // Empty parameter list
        if self.peek() == &Token::RParen {
            return Ok(parameters);
        }

        loop {
            // Parse parameter mode (IN, OUT, INOUT)
            let mode = if self.try_consume_keyword(Keyword::In) {
                ParameterMode::In
            } else if self.try_consume_keyword(Keyword::Out) {
                ParameterMode::Out
            } else if self.try_consume_keyword(Keyword::InOut) {
                ParameterMode::InOut
            } else {
                // Default to IN if not specified
                ParameterMode::In
            };

            let name = self.parse_identifier()?;
            let data_type = self.parse_data_type()?;

            parameters.push(ProcedureParameter {
                mode,
                name,
                data_type,
            });

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(parameters)
    }

    /// Parse function parameters: name data_type [, ...]
    ///
    /// Functions typically only have IN parameters (no mode specification)
    /// Example: user_id INT, price DECIMAL(10,2)
    fn parse_function_parameters(&mut self) -> Result<Vec<FunctionParameter>, ParseError> {
        let mut parameters = Vec::new();

        // Empty parameter list
        if self.peek() == &Token::RParen {
            return Ok(parameters);
        }

        loop {
            let name = self.parse_identifier()?;
            let data_type = self.parse_data_type()?;

            parameters.push(FunctionParameter { name, data_type });

            if !self.try_consume(&Token::Comma) {
                break;
            }
        }

        Ok(parameters)
    }

    /// Parse procedure body
    ///
    /// For now, we support two formats:
    /// 1. BEGIN ... END; (procedural block)
    /// 2. RawSql for simple cases
    fn parse_procedure_body(&mut self) -> Result<ProcedureBody, ParseError> {
        if self.try_consume_keyword(Keyword::Begin) {
            // Parse BEGIN ... END block
            let statements = self.parse_procedural_statements()?;
            self.expect_keyword(Keyword::End)?;
            Ok(ProcedureBody::BeginEnd(statements))
        } else {
            // For now, error - require BEGIN/END block
            Err(ParseError {
                message: "Expected BEGIN keyword for procedure body".to_string(),
            })
        }
    }

    /// Parse procedural statements (statements within BEGIN/END block)
    ///
    /// Supported statements:
    /// - SQL statements (SELECT, INSERT, UPDATE, DELETE, etc.)
    /// - DECLARE variable declarations
    /// - SET variable assignments
    /// - IF/ELSE conditional
    /// - WHILE loops
    /// - LOOP statements
    /// - REPEAT/UNTIL loops
    /// - RETURN (for functions)
    /// - LEAVE/ITERATE (for loop control)
    fn parse_procedural_statements(&mut self) -> Result<Vec<ProceduralStatement>, ParseError> {
        let mut statements = Vec::new();

        while !self.peek_keyword(Keyword::End) && self.peek() != &Token::Eof {
            let stmt = if self.try_consume_keyword(Keyword::Declare) {
                self.parse_declare_statement()?
            } else if self.try_consume_keyword(Keyword::Set) {
                self.parse_set_statement()?
            } else if self.try_consume_keyword(Keyword::If) {
                self.parse_if_statement()?
            } else if self.try_consume_keyword(Keyword::While) {
                self.parse_while_statement()?
            } else if self.try_consume_keyword(Keyword::Loop) {
                self.parse_loop_statement()?
            } else if self.try_consume_keyword(Keyword::Repeat) {
                self.parse_repeat_statement()?
            } else if self.try_consume_keyword(Keyword::Return) {
                let expr = self.parse_expression()?;
                self.expect_token(Token::Semicolon)?;
                ProceduralStatement::Return(Box::new(expr))
            } else if self.try_consume_keyword(Keyword::Leave) {
                let label = self.parse_identifier()?;
                self.expect_token(Token::Semicolon)?;
                ProceduralStatement::Leave(label)
            } else if self.try_consume_keyword(Keyword::Iterate) {
                let label = self.parse_identifier()?;
                self.expect_token(Token::Semicolon)?;
                ProceduralStatement::Iterate(label)
            } else {
                // Try to parse as a SQL statement
                let sql_stmt = self.parse_statement()?;
                ProceduralStatement::Sql(Box::new(sql_stmt))
            };

            statements.push(stmt);
        }

        Ok(statements)
    }

    /// Parse DECLARE statement
    ///
    /// Syntax: DECLARE var_name data_type [DEFAULT expr];
    fn parse_declare_statement(&mut self) -> Result<ProceduralStatement, ParseError> {
        let name = self.parse_identifier()?;
        let data_type = self.parse_data_type()?;

        let default_value = if self.try_consume_keyword(Keyword::Default) {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        self.expect_token(Token::Semicolon)?;

        Ok(ProceduralStatement::Declare {
            name,
            data_type,
            default_value,
        })
    }

    /// Parse SET statement
    ///
    /// Syntax: SET var_name = expr;
    fn parse_set_statement(&mut self) -> Result<ProceduralStatement, ParseError> {
        let name = self.parse_identifier()?;
        self.expect_token(Token::Symbol('='))?;
        let value = self.parse_expression()?;
        self.expect_token(Token::Semicolon)?;

        Ok(ProceduralStatement::Set {
            name,
            value: Box::new(value),
        })
    }

    /// Parse IF statement
    ///
    /// Syntax: IF condition THEN ... [ELSE ...] END IF;
    fn parse_if_statement(&mut self) -> Result<ProceduralStatement, ParseError> {
        let condition = self.parse_expression()?;
        self.expect_keyword(Keyword::Then)?;

        let then_statements = self.parse_procedural_statements_until(&[Keyword::Else, Keyword::End])?;

        let else_statements = if self.try_consume_keyword(Keyword::Else) {
            Some(self.parse_procedural_statements_until(&[Keyword::End])?)
        } else {
            None
        };

        self.expect_keyword(Keyword::End)?;
        self.expect_keyword(Keyword::If)?;
        self.expect_token(Token::Semicolon)?;

        Ok(ProceduralStatement::If {
            condition: Box::new(condition),
            then_statements,
            else_statements,
        })
    }

    /// Parse WHILE statement
    ///
    /// Syntax: WHILE condition DO ... END WHILE;
    fn parse_while_statement(&mut self) -> Result<ProceduralStatement, ParseError> {
        let condition = self.parse_expression()?;
        self.expect_keyword(Keyword::Do)?;

        let statements = self.parse_procedural_statements_until(&[Keyword::End])?;

        self.expect_keyword(Keyword::End)?;
        self.expect_keyword(Keyword::While)?;
        self.expect_token(Token::Semicolon)?;

        Ok(ProceduralStatement::While {
            condition: Box::new(condition),
            statements,
        })
    }

    /// Parse LOOP statement
    ///
    /// Syntax: LOOP ... END LOOP;
    fn parse_loop_statement(&mut self) -> Result<ProceduralStatement, ParseError> {
        let statements = self.parse_procedural_statements_until(&[Keyword::End])?;

        self.expect_keyword(Keyword::Loop)?;
        self.expect_token(Token::Semicolon)?;

        Ok(ProceduralStatement::Loop { statements })
    }

    /// Parse REPEAT statement
    ///
    /// Syntax: REPEAT ... UNTIL condition END REPEAT;
    fn parse_repeat_statement(&mut self) -> Result<ProceduralStatement, ParseError> {
        let statements = self.parse_procedural_statements_until(&[Keyword::Until])?;

        self.expect_keyword(Keyword::Until)?;
        let condition = self.parse_expression()?;
        self.expect_keyword(Keyword::End)?;
        self.expect_keyword(Keyword::Repeat)?;
        self.expect_token(Token::Semicolon)?;

        Ok(ProceduralStatement::Repeat {
            statements,
            condition: Box::new(condition),
        })
    }

    /// Parse procedural statements until one of the keywords is encountered
    ///
    /// This helper stops parsing when it encounters any of the given keywords,
    /// without consuming them.
    fn parse_procedural_statements_until(
        &mut self,
        stop_keywords: &[Keyword],
    ) -> Result<Vec<ProceduralStatement>, ParseError> {
        let mut statements = Vec::new();

        while self.peek() != &Token::Eof {
            // Check if we've hit a stop keyword
            if stop_keywords.iter().any(|kw| self.peek_keyword(*kw)) {
                break;
            }

            let stmt = if self.try_consume_keyword(Keyword::Declare) {
                self.parse_declare_statement()?
            } else if self.try_consume_keyword(Keyword::Set) {
                self.parse_set_statement()?
            } else if self.try_consume_keyword(Keyword::If) {
                self.parse_if_statement()?
            } else if self.try_consume_keyword(Keyword::While) {
                self.parse_while_statement()?
            } else if self.try_consume_keyword(Keyword::Loop) {
                self.parse_loop_statement()?
            } else if self.try_consume_keyword(Keyword::Repeat) {
                self.parse_repeat_statement()?
            } else if self.try_consume_keyword(Keyword::Return) {
                let expr = self.parse_expression()?;
                self.expect_token(Token::Semicolon)?;
                ProceduralStatement::Return(Box::new(expr))
            } else if self.try_consume_keyword(Keyword::Leave) {
                let label = self.parse_identifier()?;
                self.expect_token(Token::Semicolon)?;
                ProceduralStatement::Leave(label)
            } else if self.try_consume_keyword(Keyword::Iterate) {
                let label = self.parse_identifier()?;
                self.expect_token(Token::Semicolon)?;
                ProceduralStatement::Iterate(label)
            } else {
                // Try to parse as a SQL statement
                let sql_stmt = self.parse_statement()?;
                ProceduralStatement::Sql(Box::new(sql_stmt))
            };

            statements.push(stmt);
        }

        Ok(statements)
    }
}
