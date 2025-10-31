use crate::token::Token;
use super::{Lexer, LexerError, keywords};

impl Lexer {
    /// Tokenize an identifier or keyword.
    pub(super) fn tokenize_identifier_or_keyword(&mut self) -> Result<Token, LexerError> {
        let start = self.position;
        while !self.is_eof() {
            let ch = self.current_char();
            if ch.is_alphanumeric() || ch == '_' {
                self.advance();
            } else {
                break;
            }
        }

        let text: String = self.input[start..self.position].iter().collect();
        let upper_text = text.to_uppercase();

        Ok(keywords::map_keyword(upper_text))
    }

    /// Tokenize a delimited identifier enclosed in double quotes.
    /// Delimited identifiers are case-sensitive and can contain reserved words.
    /// Supports SQL-standard escaped quotes (e.g., "O""Reilly" becomes O"Reilly)
    pub(super) fn tokenize_delimited_identifier(&mut self) -> Result<Token, LexerError> {
        self.advance(); // Skip opening quote

        let mut identifier = String::new();
        while !self.is_eof() {
            let ch = self.current_char();
            if ch == '"' {
                self.advance();
                // Check for escaped quote ("")
                if !self.is_eof() && self.current_char() == '"' {
                    // Escaped quote - add a single quote to the identifier
                    identifier.push('"');
                    self.advance();
                } else {
                    // End of delimited identifier
                    // Reject empty delimited identifiers
                    if identifier.is_empty() {
                        return Err(LexerError {
                            message: "Empty delimited identifier is not allowed".to_string(),
                            position: self.position,
                        });
                    }
                    return Ok(Token::DelimitedIdentifier(identifier));
                }
            } else {
                identifier.push(ch);
                self.advance();
            }
        }

        Err(LexerError {
            message: "Unterminated delimited identifier".to_string(),
            position: self.position,
        })
    }
}
