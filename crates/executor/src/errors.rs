#[derive(Debug, Clone, PartialEq)]
pub enum ExecutorError {
    TableNotFound(String),
    TableAlreadyExists(String),
    ColumnNotFound(String),
    ColumnAlreadyExists(String),
    SchemaNotFound(String),
    SchemaAlreadyExists(String),
    SchemaNotEmpty(String),
    RoleNotFound(String),
    TypeNotFound(String),
    TypeAlreadyExists(String),
    TypeInUse(String),
    DependentPrivilegesExist(String),
    PermissionDenied { role: String, privilege: String, object: String },
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
    Other(String),
}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutorError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
            ExecutorError::TableAlreadyExists(name) => write!(f, "Table '{}' already exists", name),
            ExecutorError::ColumnNotFound(name) => write!(f, "Column '{}' not found", name),
            ExecutorError::ColumnAlreadyExists(name) => {
                write!(f, "Column '{}' already exists", name)
            }
            ExecutorError::SchemaNotFound(name) => write!(f, "Schema '{}' not found", name),
            ExecutorError::SchemaAlreadyExists(name) => {
                write!(f, "Schema '{}' already exists", name)
            }
            ExecutorError::SchemaNotEmpty(name) => {
                write!(f, "Cannot drop schema '{}': schema is not empty", name)
            }
            ExecutorError::RoleNotFound(name) => write!(f, "Role '{}' not found", name),
            ExecutorError::TypeNotFound(name) => write!(f, "Type '{}' not found", name),
            ExecutorError::TypeAlreadyExists(name) => write!(f, "Type '{}' already exists", name),
            ExecutorError::TypeInUse(name) => {
                write!(f, "Cannot drop type '{}': type is still in use", name)
            }
            ExecutorError::DependentPrivilegesExist(msg) => {
                write!(f, "Dependent privileges exist: {}", msg)
            }
            ExecutorError::PermissionDenied { role, privilege, object } => {
                write!(
                    f,
                    "Permission denied: role '{}' lacks {} privilege on {}",
                    role, privilege, object
                )
            }
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
            ExecutorError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for ExecutorError {}

impl From<catalog::CatalogError> for ExecutorError {
    fn from(err: catalog::CatalogError) -> Self {
        match err {
            catalog::CatalogError::TableAlreadyExists(name) => {
                ExecutorError::TableAlreadyExists(name)
            }
            catalog::CatalogError::TableNotFound(name) => ExecutorError::TableNotFound(name),
            catalog::CatalogError::ColumnAlreadyExists(name) => {
                ExecutorError::ColumnAlreadyExists(name)
            }
            catalog::CatalogError::ColumnNotFound(name) => ExecutorError::ColumnNotFound(name),
            catalog::CatalogError::SchemaNotFound(name) => ExecutorError::SchemaNotFound(name),
            catalog::CatalogError::SchemaAlreadyExists(name) => {
                ExecutorError::SchemaAlreadyExists(name)
            }
            catalog::CatalogError::SchemaNotEmpty(name) => ExecutorError::SchemaNotEmpty(name),
            catalog::CatalogError::RoleAlreadyExists(name) => {
                ExecutorError::StorageError(format!("Role '{}' already exists", name))
            }
            catalog::CatalogError::RoleNotFound(name) => ExecutorError::RoleNotFound(name),
            // Advanced SQL:1999 objects
            catalog::CatalogError::DomainAlreadyExists(name) => {
                ExecutorError::Other(format!("Domain '{}' already exists", name))
            }
            catalog::CatalogError::DomainNotFound(name) => {
                ExecutorError::Other(format!("Domain '{}' not found", name))
            }
            catalog::CatalogError::SequenceAlreadyExists(name) => {
                ExecutorError::Other(format!("Sequence '{}' already exists", name))
            }
            catalog::CatalogError::SequenceNotFound(name) => {
                ExecutorError::Other(format!("Sequence '{}' not found", name))
            }
            catalog::CatalogError::TypeAlreadyExists(name) => {
                ExecutorError::TypeAlreadyExists(name)
            }
            catalog::CatalogError::TypeNotFound(name) => ExecutorError::TypeNotFound(name),
            catalog::CatalogError::TypeInUse(name) => ExecutorError::TypeInUse(name),
            catalog::CatalogError::CollationAlreadyExists(name) => {
                ExecutorError::Other(format!("Collation '{}' already exists", name))
            }
            catalog::CatalogError::CollationNotFound(name) => {
                ExecutorError::Other(format!("Collation '{}' not found", name))
            }
            catalog::CatalogError::CharacterSetAlreadyExists(name) => {
                ExecutorError::Other(format!("Character set '{}' already exists", name))
            }
            catalog::CatalogError::CharacterSetNotFound(name) => {
                ExecutorError::Other(format!("Character set '{}' not found", name))
            }
            catalog::CatalogError::TranslationAlreadyExists(name) => {
                ExecutorError::Other(format!("Translation '{}' already exists", name))
            }
            catalog::CatalogError::TranslationNotFound(name) => {
                ExecutorError::Other(format!("Translation '{}' not found", name))
            }
            catalog::CatalogError::ViewAlreadyExists(name) => {
                ExecutorError::Other(format!("View '{}' already exists", name))
            }
            catalog::CatalogError::ViewNotFound(name) => {
                ExecutorError::Other(format!("View '{}' not found", name))
            }
            catalog::CatalogError::TriggerAlreadyExists(name) => {
                ExecutorError::Other(format!("Trigger '{}' already exists", name))
            }
            catalog::CatalogError::TriggerNotFound(name) => {
                ExecutorError::Other(format!("Trigger '{}' not found", name))
            }
            catalog::CatalogError::AssertionAlreadyExists(name) => {
                ExecutorError::Other(format!("Assertion '{}' already exists", name))
            }
            catalog::CatalogError::AssertionNotFound(name) => {
                ExecutorError::Other(format!("Assertion '{}' not found", name))
            }
            catalog::CatalogError::FunctionAlreadyExists(name) => {
                ExecutorError::Other(format!("Function '{}' already exists", name))
            }
            catalog::CatalogError::FunctionNotFound(name) => {
                ExecutorError::Other(format!("Function '{}' not found", name))
            }
            catalog::CatalogError::ProcedureAlreadyExists(name) => {
                ExecutorError::Other(format!("Procedure '{}' already exists", name))
            }
            catalog::CatalogError::ProcedureNotFound(name) => {
                ExecutorError::Other(format!("Procedure '{}' not found", name))
            }
        }
    }
}
