use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_simple_identifier() {
    let mut lexer = Lexer::new("users");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("users".to_string()));
}

#[test]
fn test_tokenize_identifier_with_underscore() {
    let mut lexer = Lexer::new("user_id");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("user_id".to_string()));
}

#[test]
fn test_tokenize_identifier_with_numbers() {
    let mut lexer = Lexer::new("table123");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("table123".to_string()));
}

#[test]
fn test_tokenize_identifier_starting_with_underscore() {
    let mut lexer = Lexer::new("_internal");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("_internal".to_string()));
}

// ============================================================================
