use std::fmt;

#[allow(dead_code)]
#[derive(Debug)]
pub enum CliError {
    Database(String),
    Parser(String),
    Execution(String),
    Io(String),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::Database(msg) => write!(f, "Database error: {}", msg),
            CliError::Parser(msg) => write!(f, "Parse error: {}", msg),
            CliError::Execution(msg) => write!(f, "Execution error: {}", msg),
            CliError::Io(msg) => write!(f, "IO error: {}", msg),
        }
    }
}

impl std::error::Error for CliError {}
