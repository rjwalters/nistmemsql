#[derive(Debug, Clone, PartialEq)]
pub enum ExecutorError {
    TableNotFound(String),
    ColumnNotFound(String),
    ColumnIndexOutOfBounds { index: usize },
    TypeMismatch { left: types::SqlValue, op: String, right: types::SqlValue },
    DivisionByZero,
    InvalidWhereClause(String),
    UnsupportedExpression(String),
    UnsupportedFeature(String),
}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutorError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
            ExecutorError::ColumnNotFound(name) => write!(f, "Column '{}' not found", name),
            ExecutorError::ColumnIndexOutOfBounds { index } => {
                write!(f, "Column index {} out of bounds", index)
            }
            ExecutorError::TypeMismatch { left, op, right } => {
                write!(f, "Type mismatch: {:?} {} {:?}", left, op, right)
            }
            ExecutorError::DivisionByZero => write!(f, "Division by zero"),
            ExecutorError::InvalidWhereClause(msg) => write!(f, "Invalid WHERE clause: {}", msg),
            ExecutorError::UnsupportedExpression(msg) => {
                write!(f, "Unsupported expression: {}", msg)
            }
            ExecutorError::UnsupportedFeature(msg) => write!(f, "Unsupported feature: {}", msg),
        }
    }
}

impl std::error::Error for ExecutorError {}
