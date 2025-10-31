//! ALTER TABLE parser

use crate::keywords::Keyword;
use crate::parser::ParseError;
use crate::token::Token;
use ast::*;
use types::SqlValue;

/// Parse ALTER TABLE statement
pub fn parse_alter_table(parser: &mut crate::Parser) -> Result<AlterTableStmt, ParseError> {
    // ALTER TABLE
    parser.expect_keyword(Keyword::Alter)?;
    parser.expect_keyword(Keyword::Table)?;

    let table_name = parser.parse_identifier()?;

    // Dispatch based on operation
    match parser.peek() {
        Token::Keyword(Keyword::Add) => {
            parser.advance();
            match parser.peek() {
                Token::Keyword(Keyword::Column) => {
                    parser.advance();
                    parse_add_column(parser, table_name)
                }
                // SQL:1999 allows adding constraints with or without CONSTRAINT keyword
                Token::Keyword(
                    Keyword::Constraint
                    | Keyword::Check
                    | Keyword::Unique
                    | Keyword::Primary
                    | Keyword::Foreign,
                ) => parse_add_constraint(parser, table_name),
                _ => Err(ParseError {
                    message: "Expected COLUMN, CONSTRAINT, or constraint type after ADD"
                        .to_string(),
                }),
            }
        }
        Token::Keyword(Keyword::Drop) => {
            parser.advance();
            match parser.peek() {
                Token::Keyword(Keyword::Column) => {
                    parser.advance();
                    parse_drop_column(parser, table_name)
                }
                Token::Keyword(Keyword::Constraint) => {
                    parser.advance();
                    parse_drop_constraint(parser, table_name)
                }
                _ => Err(ParseError {
                    message: "Expected COLUMN or CONSTRAINT after DROP".to_string(),
                }),
            }
        }
        Token::Keyword(Keyword::Alter) => {
            parser.advance();
            parser.expect_keyword(Keyword::Column)?;
            parse_alter_column(parser, table_name)
        }
        _ => {
            Err(ParseError { message: "Expected ADD, DROP, or ALTER after table name".to_string() })
        }
    }
}

/// Parse ADD COLUMN
fn parse_add_column(
    parser: &mut crate::Parser,
    table_name: String,
) -> Result<AlterTableStmt, ParseError> {
    let column_name = parser.parse_identifier()?;
    let data_type = parser.parse_data_type()?;

    // Parse optional DEFAULT clause
    let default_value = if parser.peek_keyword(Keyword::Default) {
        parser.advance(); // consume DEFAULT
        Some(Box::new(parser.parse_expression()?))
    } else {
        None
    };

    // Parse column constraints
    let mut nullable = true;
    let mut constraints = Vec::new();

    loop {
        match parser.peek() {
            Token::Keyword(Keyword::Not) => {
                parser.advance();
                parser.expect_keyword(Keyword::Null)?;
                nullable = false;
            }
            Token::Keyword(Keyword::Primary) => {
                parser.advance();
                parser.expect_keyword(Keyword::Key)?;
                constraints
                    .push(ColumnConstraint { name: None, kind: ColumnConstraintKind::PrimaryKey });
            }
            Token::Keyword(Keyword::Unique) => {
                parser.advance();
                constraints
                    .push(ColumnConstraint { name: None, kind: ColumnConstraintKind::Unique });
            }
            Token::Keyword(Keyword::References) => {
                parser.advance();
                let ref_table = parser.parse_identifier()?;
                parser.expect_token(crate::token::Token::LParen)?;
                let ref_column = parser.parse_identifier()?;
                parser.expect_token(crate::token::Token::RParen)?;
                constraints.push(ColumnConstraint {
                    name: None,
                    kind: ColumnConstraintKind::References {
                        table: ref_table,
                        column: ref_column,
                        on_delete: None,
                        on_update: None,
                    },
                });
            }
            _ => break,
        }
    }

    let column_def =
        ColumnDef { name: column_name, data_type, nullable, constraints, default_value };

    Ok(AlterTableStmt::AddColumn(AddColumnStmt { table_name, column_def }))
}

/// Parse DROP COLUMN
fn parse_drop_column(
    parser: &mut crate::Parser,
    table_name: String,
) -> Result<AlterTableStmt, ParseError> {
    let if_exists =
        parser.try_consume_keyword(Keyword::If) && parser.try_consume_keyword(Keyword::Exists);

    let column_name = parser.parse_identifier()?;

    Ok(AlterTableStmt::DropColumn(DropColumnStmt { table_name, column_name, if_exists }))
}

/// Parse ALTER COLUMN
fn parse_alter_column(
    parser: &mut crate::Parser,
    table_name: String,
) -> Result<AlterTableStmt, ParseError> {
    let column_name = parser.parse_identifier()?;

    match parser.peek() {
        Token::Keyword(Keyword::Set) => {
            parser.advance();
            match parser.peek() {
                Token::Keyword(Keyword::Default) => {
                    parser.advance();
                    // TODO: Parse default expression
                    // For now, create a placeholder expression
                    let default = Expression::Literal(SqlValue::Null);
                    Ok(AlterTableStmt::AlterColumn(AlterColumnStmt::SetDefault {
                        table_name,
                        column_name,
                        default,
                    }))
                }
                Token::Keyword(Keyword::Not) => {
                    parser.advance();
                    parser.expect_keyword(Keyword::Null)?;
                    Ok(AlterTableStmt::AlterColumn(AlterColumnStmt::SetNotNull {
                        table_name,
                        column_name,
                    }))
                }
                _ => Err(ParseError {
                    message: "Expected DEFAULT or NOT NULL after SET".to_string(),
                }),
            }
        }
        Token::Keyword(Keyword::Drop) => {
            parser.advance();
            match parser.peek() {
                Token::Keyword(Keyword::Default) => {
                    parser.advance();
                    Ok(AlterTableStmt::AlterColumn(AlterColumnStmt::DropDefault {
                        table_name,
                        column_name,
                    }))
                }
                Token::Keyword(Keyword::Not) => {
                    parser.advance();
                    parser.expect_keyword(Keyword::Null)?;
                    Ok(AlterTableStmt::AlterColumn(AlterColumnStmt::DropNotNull {
                        table_name,
                        column_name,
                    }))
                }
                _ => Err(ParseError {
                    message: "Expected DEFAULT or NOT NULL after DROP".to_string(),
                }),
            }
        }
        _ => Err(ParseError { message: "Expected SET or DROP after column name".to_string() }),
    }
}

/// Parse ADD CONSTRAINT
fn parse_add_constraint(
    parser: &mut crate::Parser,
    table_name: String,
) -> Result<AlterTableStmt, ParseError> {
    // Use the existing parse_table_constraint method which handles all constraint types
    let constraint = parser.parse_table_constraint()?;

    Ok(AlterTableStmt::AddConstraint(AddConstraintStmt { table_name, constraint }))
}

/// Parse DROP CONSTRAINT
fn parse_drop_constraint(
    parser: &mut crate::Parser,
    table_name: String,
) -> Result<AlterTableStmt, ParseError> {
    let constraint_name = parser.parse_identifier()?;

    Ok(AlterTableStmt::DropConstraint(DropConstraintStmt { table_name, constraint_name }))
}
