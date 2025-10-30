use super::super::*;

// ============================================================================

#[test]
fn test_tokenize_semicolon() {
    let mut lexer = Lexer::new(";");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Semicolon);
}

#[test]
fn test_tokenize_comma() {
    let mut lexer = Lexer::new(",");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Comma);
}

#[test]
fn test_tokenize_parentheses() {
    let mut lexer = Lexer::new("()");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::LParen);
    assert_eq!(tokens[1], Token::RParen);
}

#[test]
fn test_tokenize_arithmetic_symbols() {
    let mut lexer = Lexer::new("+ - * /");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Symbol('+'));
    assert_eq!(tokens[1], Token::Symbol('-'));
    assert_eq!(tokens[2], Token::Symbol('*'));
    assert_eq!(tokens[3], Token::Symbol('/'));
}

#[test]
fn test_tokenize_comparison_symbols() {
    let mut lexer = Lexer::new("= < >");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Symbol('='));
    assert_eq!(tokens[1], Token::Symbol('<'));
    assert_eq!(tokens[2], Token::Symbol('>'));
}

#[test]
fn test_tokenize_multi_char_operators() {
    let mut lexer = Lexer::new("<= >= != <>");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Operator("<=".to_string()));
    assert_eq!(tokens[1], Token::Operator(">=".to_string()));
    assert_eq!(tokens[2], Token::Operator("!=".to_string()));
    assert_eq!(tokens[3], Token::Operator("<>".to_string()));
}

#[test]
fn test_tokenize_operators_without_spaces() {
    // Test that >= is tokenized as one operator, not two
    let mut lexer = Lexer::new("age>=18");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Identifier("AGE".to_string()));
    assert_eq!(tokens[1], Token::Operator(">=".to_string()));
    assert_eq!(tokens[2], Token::Number("18".to_string()));
}

#[test]
fn test_tokenize_single_vs_multi_char() {
    // Test that > and = are separate when not adjacent
    let mut lexer = Lexer::new("> =");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Symbol('>'));
    assert_eq!(tokens[1], Token::Symbol('='));

    // But >= is one token when adjacent
    let mut lexer2 = Lexer::new(">=");
    let tokens2 = lexer2.tokenize().unwrap();
    assert_eq!(tokens2[0], Token::Operator(">=".to_string()));
}

// ============================================================================
