use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_single_quoted_string() {
    let mut lexer = Lexer::new("'hello'");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::String("hello".to_string()));
}

#[test]
fn test_tokenize_double_quoted_string() {
    let mut lexer = Lexer::new("\"world\"");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::String("world".to_string()));
}

#[test]
fn test_tokenize_empty_string() {
    let mut lexer = Lexer::new("''");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::String("".to_string()));
}

#[test]
fn test_tokenize_string_with_spaces() {
    let mut lexer = Lexer::new("'hello world'");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::String("hello world".to_string()));
}

#[test]
fn test_tokenize_unterminated_string() {
    let mut lexer = Lexer::new("'hello");
    let result = lexer.tokenize();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.message, "Unterminated string literal");
}

// ============================================================================
