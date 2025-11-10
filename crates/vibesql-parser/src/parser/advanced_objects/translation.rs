//! TRANSLATION DDL parsing (SQL:1999)
//! Includes: CREATE/DROP TRANSLATION

use vibesql_ast::*;

use crate::{keywords::Keyword, parser::ParseError};

/// Parse CREATE TRANSLATION statement
///
/// SQL:1999 Syntax:
///   CREATE TRANSLATION translation_name
///     [FOR source_charset TO target_charset]
///     [FROM translation_source]
pub fn parse_create_translation(
    parser: &mut crate::Parser,
) -> Result<CreateTranslationStmt, ParseError> {
    parser.expect_keyword(Keyword::Create)?;
    parser.expect_keyword(Keyword::Translation)?;

    let translation_name = parser.parse_identifier()?;

    // Optional: FOR source_charset
    let source_charset = if parser.try_consume_keyword(Keyword::For) {
        Some(parser.parse_identifier()?)
    } else {
        None
    };

    // Optional: TO target_charset (only if FOR was specified)
    let target_charset = if source_charset.is_some() && parser.try_consume_keyword(Keyword::To) {
        Some(parser.parse_identifier()?)
    } else {
        None
    };

    // Optional: FROM translation_source
    let translation_source = if parser.try_consume_keyword(Keyword::From) {
        Some(parser.parse_identifier()?)
    } else {
        None
    };

    Ok(CreateTranslationStmt {
        translation_name,
        source_charset,
        target_charset,
        translation_source,
    })
}

/// Parse DROP TRANSLATION statement
///
/// Syntax: DROP TRANSLATION translation_name
pub fn parse_drop_translation(
    parser: &mut crate::Parser,
) -> Result<DropTranslationStmt, ParseError> {
    parser.expect_keyword(Keyword::Drop)?;
    parser.expect_keyword(Keyword::Translation)?;

    let translation_name = parser.parse_identifier()?;

    Ok(DropTranslationStmt { translation_name })
}
