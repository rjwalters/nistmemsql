#[derive(Debug, Clone, PartialEq)]
pub enum ExecutorError {
    TableNotFound(String),
    TableAlreadyExists(String),
    ColumnNotFound {
        column_name: String,
        table_name: String,
        searched_tables: Vec<String>,
        available_columns: Vec<String>,
    },
    InvalidTableQualifier {
        qualifier: String,
        column: String,
        available_tables: Vec<String>,
    },
    ColumnAlreadyExists(String),
    IndexNotFound(String),
    IndexAlreadyExists(String),
    InvalidIndexDefinition(String),
    TriggerNotFound(String),
    TriggerAlreadyExists(String),
    SchemaNotFound(String),
    SchemaAlreadyExists(String),
    SchemaNotEmpty(String),
    RoleNotFound(String),
    TypeNotFound(String),
    TypeAlreadyExists(String),
    TypeInUse(String),
    DependentPrivilegesExist(String),
    PermissionDenied {
        role: String,
        privilege: String,
        object: String,
    },
    ColumnIndexOutOfBounds {
        index: usize,
    },
    TypeMismatch {
        left: vibesql_types::SqlValue,
        op: String,
        right: vibesql_types::SqlValue,
    },
    DivisionByZero,
    InvalidWhereClause(String),
    UnsupportedExpression(String),
    UnsupportedFeature(String),
    StorageError(String),
    SubqueryReturnedMultipleRows {
        expected: usize,
        actual: usize,
    },
    SubqueryColumnCountMismatch {
        expected: usize,
        actual: usize,
    },
    ColumnCountMismatch {
        expected: usize,
        provided: usize,
    },
    CastError {
        from_type: String,
        to_type: String,
    },
    TypeConversionError {
        from: String,
        to: String,
    },
    ConstraintViolation(String),
    MultiplePrimaryKeys,
    CannotDropColumn(String),
    ConstraintNotFound {
        constraint_name: String,
        table_name: String,
    },
    /// Expression evaluation exceeded maximum recursion depth
    /// This prevents stack overflow from deeply nested expressions or subqueries
    ExpressionDepthExceeded {
        depth: usize,
        max_depth: usize,
    },
    /// Query exceeded maximum execution time
    QueryTimeoutExceeded {
        elapsed_seconds: u64,
        max_seconds: u64,
    },
    /// Query exceeded maximum row processing limit
    RowLimitExceeded {
        rows_processed: usize,
        max_rows: usize,
    },
    /// Query exceeded maximum memory limit
    MemoryLimitExceeded {
        used_bytes: usize,
        max_bytes: usize,
    },
    /// Variable not found in procedural context
    VariableNotFound(String),
    /// Label not found in procedural context
    LabelNotFound(String),
    /// Type error in expression evaluation
    TypeError(String),
    Other(String),
}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutorError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
            ExecutorError::TableAlreadyExists(name) => write!(f, "Table '{}' already exists", name),
            ExecutorError::ColumnNotFound {
                column_name,
                table_name,
                searched_tables,
                available_columns,
            } => {
                if searched_tables.is_empty() {
                    write!(f, "Column '{}' not found in table '{}'", column_name, table_name)
                } else if available_columns.is_empty() {
                    write!(
                        f,
                        "Column '{}' not found (searched tables: {})",
                        column_name,
                        searched_tables.join(", ")
                    )
                } else {
                    write!(
                        f,
                        "Column '{}' not found (searched tables: {}). Available columns: {}",
                        column_name,
                        searched_tables.join(", "),
                        available_columns.join(", ")
                    )
                }
            }
            ExecutorError::InvalidTableQualifier { qualifier, column, available_tables } => {
                write!(
                    f,
                    "Invalid table qualifier '{}' for column '{}'. Available tables: {}",
                    qualifier,
                    column,
                    available_tables.join(", ")
                )
            }
            ExecutorError::ColumnAlreadyExists(name) => {
                write!(f, "Column '{}' already exists", name)
            }
            ExecutorError::IndexNotFound(name) => write!(f, "Index '{}' not found", name),
            ExecutorError::IndexAlreadyExists(name) => write!(f, "Index '{}' already exists", name),
            ExecutorError::InvalidIndexDefinition(msg) => write!(f, "Invalid index definition: {}", msg),
            ExecutorError::TriggerNotFound(name) => write!(f, "Trigger '{}' not found", name),
            ExecutorError::TriggerAlreadyExists(name) => write!(f, "Trigger '{}' already exists", name),
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
            ExecutorError::ColumnCountMismatch { expected, provided } => {
                write!(
                    f,
                    "Derived column list has {} columns but query produces {} columns",
                    provided, expected
                )
            }
            ExecutorError::CastError { from_type, to_type } => {
                write!(f, "Cannot cast {} to {}", from_type, to_type)
            }
            ExecutorError::TypeConversionError { from, to } => {
                write!(f, "Cannot convert {} to {}", from, to)
            }
            ExecutorError::ConstraintViolation(msg) => {
                write!(f, "Constraint violation: {}", msg)
            }
            ExecutorError::MultiplePrimaryKeys => {
                write!(f, "Multiple PRIMARY KEY constraints are not allowed")
            }
            ExecutorError::CannotDropColumn(msg) => {
                write!(f, "Cannot drop column: {}", msg)
            }
            ExecutorError::ConstraintNotFound { constraint_name, table_name } => {
                write!(f, "Constraint '{}' not found in table '{}'", constraint_name, table_name)
            }
            ExecutorError::ExpressionDepthExceeded { depth, max_depth } => {
                write!(
                    f,
                    "Expression depth limit exceeded: {} > {} (prevents stack overflow)",
                    depth, max_depth
                )
            }
            ExecutorError::QueryTimeoutExceeded { elapsed_seconds, max_seconds } => {
                write!(f, "Query timeout exceeded: {}s > {}s", elapsed_seconds, max_seconds)
            }
            ExecutorError::RowLimitExceeded { rows_processed, max_rows } => {
                write!(f, "Row processing limit exceeded: {} > {}", rows_processed, max_rows)
            }
            ExecutorError::MemoryLimitExceeded { used_bytes, max_bytes } => {
                write!(
                    f,
                    "Memory limit exceeded: {:.2} GB > {:.2} GB",
                    *used_bytes as f64 / 1024.0 / 1024.0 / 1024.0,
                    *max_bytes as f64 / 1024.0 / 1024.0 / 1024.0
                )
            }
            ExecutorError::VariableNotFound(name) => {
                write!(f, "Variable '{}' not found", name)
            }
            ExecutorError::LabelNotFound(name) => {
                write!(f, "Label '{}' not found", name)
            }
            ExecutorError::TypeError(msg) => {
                write!(f, "Type error: {}", msg)
            }
            ExecutorError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for ExecutorError {}

impl From<vibesql_storage::StorageError> for ExecutorError {
    fn from(err: vibesql_storage::StorageError) -> Self {
        match err {
            vibesql_storage::StorageError::TableNotFound(name) => ExecutorError::TableNotFound(name),
            vibesql_storage::StorageError::IndexAlreadyExists(name) => {
                ExecutorError::IndexAlreadyExists(name)
            }
            vibesql_storage::StorageError::IndexNotFound(name) => ExecutorError::IndexNotFound(name),
            vibesql_storage::StorageError::ColumnCountMismatch { expected, actual } => {
                ExecutorError::ColumnCountMismatch { expected, provided: actual }
            }
            vibesql_storage::StorageError::ColumnIndexOutOfBounds { index } => {
                ExecutorError::ColumnIndexOutOfBounds { index }
            }
            vibesql_storage::StorageError::CatalogError(msg) => ExecutorError::StorageError(msg),
            vibesql_storage::StorageError::TransactionError(msg) => ExecutorError::StorageError(msg),
            vibesql_storage::StorageError::RowNotFound => {
                ExecutorError::StorageError("Row not found".to_string())
            }
            vibesql_storage::StorageError::NullConstraintViolation { column } => {
                ExecutorError::ConstraintViolation(format!(
                    "NOT NULL constraint violation: column '{}' cannot be NULL",
                    column
                ))
            }
            vibesql_storage::StorageError::TypeMismatch { column, expected, actual } => {
                ExecutorError::StorageError(format!(
                    "Type mismatch in column '{}': expected {}, got {}",
                    column, expected, actual
                ))
            }
            vibesql_storage::StorageError::ColumnNotFound { column_name, table_name } => {
                ExecutorError::StorageError(format!(
                    "Column '{}' not found in table '{}'",
                    column_name, table_name
                ))
            }
            vibesql_storage::StorageError::NotImplemented(msg) => {
                ExecutorError::StorageError(format!("Not implemented: {}", msg))
            }
            vibesql_storage::StorageError::IoError(msg) => {
                ExecutorError::StorageError(format!("I/O error: {}", msg))
            }
        }
    }
}

impl From<vibesql_catalog::CatalogError> for ExecutorError {
    fn from(err: vibesql_catalog::CatalogError) -> Self {
        match err {
            vibesql_catalog::CatalogError::TableAlreadyExists(name) => {
                ExecutorError::TableAlreadyExists(name)
            }
            vibesql_catalog::CatalogError::TableNotFound { table_name } => {
                ExecutorError::TableNotFound(table_name)
            }
            vibesql_catalog::CatalogError::ColumnAlreadyExists(name) => {
                ExecutorError::ColumnAlreadyExists(name)
            }
            vibesql_catalog::CatalogError::ColumnNotFound {
                column_name,
                table_name,
            } => ExecutorError::ColumnNotFound {
                column_name,
                table_name,
                searched_tables: vec![],
                available_columns: vec![],
            },
            vibesql_catalog::CatalogError::SchemaNotFound(name) => ExecutorError::SchemaNotFound(name),
            vibesql_catalog::CatalogError::SchemaAlreadyExists(name) => {
                ExecutorError::SchemaAlreadyExists(name)
            }
            vibesql_catalog::CatalogError::SchemaNotEmpty(name) => ExecutorError::SchemaNotEmpty(name),
            vibesql_catalog::CatalogError::RoleAlreadyExists(name) => {
                ExecutorError::StorageError(format!("Role '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::RoleNotFound(name) => ExecutorError::RoleNotFound(name),
            // Advanced SQL:1999 objects
            vibesql_catalog::CatalogError::DomainAlreadyExists(name) => {
                ExecutorError::Other(format!("Domain '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::DomainNotFound(name) => {
                ExecutorError::Other(format!("Domain '{}' not found", name))
            }
            vibesql_catalog::CatalogError::DomainInUse { domain_name, dependent_columns } => {
                ExecutorError::Other(format!(
                    "Domain '{}' is still in use by {} column(s): {}",
                    domain_name,
                    dependent_columns.len(),
                    dependent_columns
                        .iter()
                        .map(|(t, c)| format!("{}.{}", t, c))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            }
            vibesql_catalog::CatalogError::SequenceAlreadyExists(name) => {
                ExecutorError::Other(format!("Sequence '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::SequenceNotFound(name) => {
                ExecutorError::Other(format!("Sequence '{}' not found", name))
            }
            vibesql_catalog::CatalogError::SequenceInUse { sequence_name, dependent_columns } => {
                ExecutorError::Other(format!(
                    "Sequence '{}' is still in use by {} column(s): {}",
                    sequence_name,
                    dependent_columns.len(),
                    dependent_columns
                        .iter()
                        .map(|(t, c)| format!("{}.{}", t, c))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            }
            vibesql_catalog::CatalogError::TypeAlreadyExists(name) => {
                ExecutorError::TypeAlreadyExists(name)
            }
            vibesql_catalog::CatalogError::TypeNotFound(name) => ExecutorError::TypeNotFound(name),
            vibesql_catalog::CatalogError::TypeInUse(name) => ExecutorError::TypeInUse(name),
            vibesql_catalog::CatalogError::CollationAlreadyExists(name) => {
                ExecutorError::Other(format!("Collation '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::CollationNotFound(name) => {
                ExecutorError::Other(format!("Collation '{}' not found", name))
            }
            vibesql_catalog::CatalogError::CharacterSetAlreadyExists(name) => {
                ExecutorError::Other(format!("Character set '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::CharacterSetNotFound(name) => {
                ExecutorError::Other(format!("Character set '{}' not found", name))
            }
            vibesql_catalog::CatalogError::TranslationAlreadyExists(name) => {
                ExecutorError::Other(format!("Translation '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::TranslationNotFound(name) => {
                ExecutorError::Other(format!("Translation '{}' not found", name))
            }
            vibesql_catalog::CatalogError::ViewAlreadyExists(name) => {
                ExecutorError::Other(format!("View '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::ViewNotFound(name) => {
                ExecutorError::Other(format!("View '{}' not found", name))
            }
            vibesql_catalog::CatalogError::ViewInUse { view_name, dependent_views } => {
                ExecutorError::Other(format!(
                    "View or table '{}' is still in use by {} view(s): {}",
                    view_name,
                    dependent_views.len(),
                    dependent_views.join(", ")
                ))
            }
            vibesql_catalog::CatalogError::TriggerAlreadyExists(name) => {
                ExecutorError::TriggerAlreadyExists(name)
            }
            vibesql_catalog::CatalogError::TriggerNotFound(name) => {
                ExecutorError::TriggerNotFound(name)
            }
            vibesql_catalog::CatalogError::AssertionAlreadyExists(name) => {
                ExecutorError::Other(format!("Assertion '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::AssertionNotFound(name) => {
                ExecutorError::Other(format!("Assertion '{}' not found", name))
            }
            vibesql_catalog::CatalogError::FunctionAlreadyExists(name) => {
                ExecutorError::Other(format!("Function '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::FunctionNotFound(name) => {
                ExecutorError::Other(format!("Function '{}' not found", name))
            }
            vibesql_catalog::CatalogError::ProcedureAlreadyExists(name) => {
                ExecutorError::Other(format!("Procedure '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::ProcedureNotFound(name) => {
                ExecutorError::Other(format!("Procedure '{}' not found", name))
            }
            vibesql_catalog::CatalogError::ConstraintAlreadyExists(name) => {
                ExecutorError::ConstraintViolation(format!("Constraint '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::ConstraintNotFound(name) => ExecutorError::ConstraintNotFound {
                constraint_name: name,
                table_name: "unknown".to_string(),
            },
            vibesql_catalog::CatalogError::IndexAlreadyExists {
                index_name,
                table_name,
            } => ExecutorError::IndexAlreadyExists(format!("{} on table {}", index_name, table_name)),
            vibesql_catalog::CatalogError::IndexNotFound {
                index_name,
                table_name,
            } => ExecutorError::IndexNotFound(format!("{} on table {}", index_name, table_name)),
        }
    }
}
