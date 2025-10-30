use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_integer() {
    let mut lexer = Lexer::new("42");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Number("42".to_string()));
}

#[test]
fn test_tokenize_decimal() {
    let mut lexer = Lexer::new("3.14");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Number("3.14".to_string()));
}

#[test]
fn test_tokenize_zero() {
    let mut lexer = Lexer::new("0");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Number("0".to_string()));
}

#[test]
fn test_tokenize_large_number() {
    let mut lexer = Lexer::new("999999");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Number("999999".to_string()));
}

// ============================================================================
