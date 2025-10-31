use crate::token::Token;
use super::{Lexer, LexerError};

impl Lexer {
    /// Tokenize comparison and logical operators.
    /// Handles multi-character operators like <=, >=, !=, <>, ||
    pub(super) fn tokenize_operator(&mut self, ch: char) -> Result<Token, LexerError> {
        match ch {
            '=' | '<' | '>' | '!' => {
                self.advance();
                if !self.is_eof() {
                    let next_ch = self.current_char();
                    match (ch, next_ch) {
                        ('<', '=') => {
                            self.advance();
                            Ok(Token::Operator("<=".to_string()))
                        }
                        ('>', '=') => {
                            self.advance();
                            Ok(Token::Operator(">=".to_string()))
                        }
                        ('!', '=') => {
                            self.advance();
                            Ok(Token::Operator("!=".to_string()))
                        }
                        ('<', '>') => {
                            self.advance();
                            Ok(Token::Operator("<>".to_string()))
                        }
                        _ => Ok(Token::Symbol(ch)),
                    }
                } else {
                    Ok(Token::Symbol(ch))
                }
            }
            '|' => {
                self.advance();
                if !self.is_eof() && self.current_char() == '|' {
                    self.advance();
                    Ok(Token::Operator("||".to_string()))
                } else {
                    Err(LexerError {
                        message: "Unexpected character: '|' (did you mean '||'?)".to_string(),
                        position: self.position - 1,
                    })
                }
            }
            _ => {
                self.advance();
                Ok(Token::Symbol(ch))
            }
        }
    }
}
