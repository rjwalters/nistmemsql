#[derive(Debug, Clone, PartialEq)]
pub enum ExecutorError {
    TableNotFound(String),
    TableAlreadyExists(String),
    ColumnNotFound(String),
    ColumnAlreadyExists(String),
    SchemaNotFound(String),
    SchemaAlreadyExists(String),
    SchemaNotEmpty(String),
    ColumnIndexOutOfBounds { index: usize },
    TypeMismatch { left: types::SqlValue, op: String, right: types::SqlValue },
    DivisionByZero,
    InvalidWhereClause(String),
    UnsupportedExpression(String),
    UnsupportedFeature(String),
    StorageError(String),
    SubqueryReturnedMultipleRows { expected: usize, actual: usize },
    SubqueryColumnCountMismatch { expected: usize, actual: usize },
    CastError { from_type: String, to_type: String },
    ConstraintViolation(String),
    CannotDropColumn(String),
}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutorError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
            ExecutorError::TableAlreadyExists(name) => write!(f, "Table '{}' already exists", name),
            ExecutorError::ColumnNotFound(name) => write!(f, "Column '{}' not found", name),
            ExecutorError::ColumnAlreadyExists(name) => write!(f, "Column '{}' already exists", name),
            ExecutorError::SchemaNotFound(name) => write!(f, "Schema '{}' not found", name),
            ExecutorError::SchemaAlreadyExists(name) => write!(f, "Schema '{}' already exists", name),
            ExecutorError::SchemaNotEmpty(name) => write!(f, "Cannot drop schema '{}': schema is not empty", name),
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
            ExecutorError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            ExecutorError::SubqueryReturnedMultipleRows { expected, actual } => {
                write!(f, "Scalar subquery returned {} rows, expected {}", actual, expected)
            }
            ExecutorError::SubqueryColumnCountMismatch { expected, actual } => {
                write!(f, "Subquery returned {} columns, expected {}", actual, expected)
            }
            ExecutorError::CastError { from_type, to_type } => {
                write!(f, "Cannot cast {} to {}", from_type, to_type)
            }
            ExecutorError::ConstraintViolation(msg) => {
                write!(f, "Constraint violation: {}", msg)
            }
            ExecutorError::CannotDropColumn(msg) => {
                write!(f, "Cannot drop column: {}", msg)
            }
        }
    }
}

impl std::error::Error for ExecutorError {}

impl From<catalog::CatalogError> for ExecutorError {
    fn from(err: catalog::CatalogError) -> Self {
        match err {
            catalog::CatalogError::TableAlreadyExists(name) => ExecutorError::TableAlreadyExists(name),
            catalog::CatalogError::TableNotFound(name) => ExecutorError::TableNotFound(name),
            catalog::CatalogError::ColumnAlreadyExists(name) => ExecutorError::ColumnAlreadyExists(name),
            catalog::CatalogError::ColumnNotFound(name) => ExecutorError::ColumnNotFound(name),
            catalog::CatalogError::SchemaNotFound(name) => ExecutorError::SchemaNotFound(name),
            catalog::CatalogError::SchemaAlreadyExists(name) => ExecutorError::SchemaAlreadyExists(name),
            catalog::CatalogError::SchemaNotEmpty(name) => ExecutorError::SchemaNotEmpty(name),
        }
    }
}
