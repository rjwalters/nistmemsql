use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_simple_identifier() {
    let mut lexer = Lexer::new("users");
    let tokens = lexer.tokenize().unwrap();
    // Regular identifiers are normalized to uppercase
    assert_eq!(tokens[0], Token::Identifier("USERS".to_string()));
}

#[test]
fn test_tokenize_identifier_with_underscore() {
    let mut lexer = Lexer::new("user_id");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("USER_ID".to_string()));
}

#[test]
fn test_tokenize_identifier_with_numbers() {
    let mut lexer = Lexer::new("table123");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("TABLE123".to_string()));
}

#[test]
fn test_tokenize_identifier_starting_with_underscore() {
    let mut lexer = Lexer::new("_internal");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("_INTERNAL".to_string()));
}

// ============================================================================
// Delimited Identifier Tests
// ============================================================================

#[test]
fn test_tokenize_delimited_identifier_simple() {
    let mut lexer = Lexer::new(r#""columnName""#);
    let tokens = lexer.tokenize().unwrap();
    // Delimited identifiers preserve case
    assert_eq!(tokens[0], Token::DelimitedIdentifier("columnName".to_string()));
}

#[test]
fn test_tokenize_delimited_identifier_uppercase() {
    let mut lexer = Lexer::new(r#""A""#);
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::DelimitedIdentifier("A".to_string()));
}

#[test]
fn test_tokenize_delimited_identifier_lowercase() {
    let mut lexer = Lexer::new(r#""a""#);
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::DelimitedIdentifier("a".to_string()));
}

#[test]
fn test_tokenize_delimited_identifier_with_spaces() {
    let mut lexer = Lexer::new(r#""First Name""#);
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::DelimitedIdentifier("First Name".to_string()));
}

#[test]
fn test_tokenize_delimited_identifier_reserved_word() {
    let mut lexer = Lexer::new(r#""SELECT""#);
    let tokens = lexer.tokenize().unwrap();
    // Reserved words can be used as delimited identifiers
    assert_eq!(tokens[0], Token::DelimitedIdentifier("SELECT".to_string()));
}

#[test]
fn test_tokenize_delimited_identifier_with_escaped_quotes() {
    let mut lexer = Lexer::new(r#""O""Reilly""#);
    let tokens = lexer.tokenize().unwrap();
    // Doubled quotes become single quote in the identifier
    assert_eq!(tokens[0], Token::DelimitedIdentifier(r#"O"Reilly"#.to_string()));
}

#[test]
fn test_tokenize_empty_delimited_identifier_error() {
    let mut lexer = Lexer::new(r#""""#);
    let result = lexer.tokenize();
    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("Empty delimited identifier"));
}

#[test]
fn test_tokenize_unterminated_delimited_identifier_error() {
    let mut lexer = Lexer::new(r#""unterminated"#);
    let result = lexer.tokenize();
    assert!(result.is_err());
    assert!(result.unwrap_err().message.contains("Unterminated delimited identifier"));
}

#[test]
fn test_tokenize_mixed_identifiers() {
    let mut lexer = Lexer::new(r#"SELECT "columnName", regularColumn FROM table"#);
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Keyword(Keyword::Select));
    assert_eq!(tokens[1], Token::DelimitedIdentifier("columnName".to_string()));
    assert_eq!(tokens[2], Token::Comma);
    assert_eq!(tokens[3], Token::Identifier("REGULARCOLUMN".to_string()));
    assert_eq!(tokens[4], Token::Keyword(Keyword::From));
    assert_eq!(tokens[5], Token::Identifier("TABLE".to_string()));
}

// ============================================================================
