//! Schema DDL parsing

use crate::keywords::Keyword;
use crate::parser::ParseError;
use ast::*;

/// Parse CREATE SCHEMA statement
pub fn parse_create_schema(parser: &mut crate::Parser) -> Result<CreateSchemaStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Schema)?;

    let if_not_exists = parser.peek_keyword(Keyword::If) && {
        parser.advance();
        parser.expect_keyword(Keyword::Not)?;
        parser.expect_keyword(Keyword::Exists)?;
        true
    };

    let schema_name = parser.parse_qualified_identifier()?;

    // Parse optional schema elements (CREATE TABLE, etc.)
    let mut schema_elements = Vec::new();
    while parser.peek_keyword(Keyword::Create) {
        // Look ahead to see what kind of CREATE statement this is
        let saved_position = parser.position;
        parser.advance(); // Skip CREATE keyword

        if parser.peek_keyword(Keyword::Table) {
            // Reset to before CREATE and parse the full CREATE TABLE statement
            parser.position = saved_position;
            let table_stmt = parser.parse_create_table_statement()?;
            schema_elements.push(ast::SchemaElement::CreateTable(table_stmt));
        } else {
            // Unsupported schema element - restore and break
            parser.position = saved_position;
            break;
        }
    }

    Ok(CreateSchemaStmt {
        schema_name,
        if_not_exists,
        schema_elements,
    })
}

/// Parse DROP SCHEMA statement
pub fn parse_drop_schema(parser: &mut crate::Parser) -> Result<DropSchemaStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Schema)?;

    let if_exists = parser.peek_keyword(Keyword::If) && {
        parser.advance();
        parser.expect_keyword(Keyword::Exists)?;
        true
    };

    let schema_name = parser.parse_qualified_identifier()?;

    let cascade = if parser.peek_keyword(Keyword::Cascade) {
        parser.advance();
        true
    } else if parser.peek_keyword(Keyword::Restrict) {
        parser.advance();
        false
    } else {
        // RESTRICT is the default
        false
    };

    Ok(DropSchemaStmt { schema_name, if_exists, cascade })
}

/// Parse SET SCHEMA statement
pub fn parse_set_schema(parser: &mut crate::Parser) -> Result<SetSchemaStmt, ParseError> {
    // This could be "SET SCHEMA schema_name" or "SET search_path TO schema_name"
    // For now, we'll support the simpler "SET SCHEMA schema_name" form

    parser.expect_keyword(Keyword::Set)?;
    parser.expect_keyword(Keyword::Schema)?;

    let schema_name = parser.parse_qualified_identifier()?;

    Ok(SetSchemaStmt { schema_name })
}
