use std::fmt;

#[derive(Debug)]
pub enum CliError {
    DatabaseError(String),
    ParserError(String),
    ExecutionError(String),
    IoError(String),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            CliError::ParserError(msg) => write!(f, "Parse error: {}", msg),
            CliError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
            CliError::IoError(msg) => write!(f, "IO error: {}", msg),
        }
    }
}

impl std::error::Error for CliError {}
