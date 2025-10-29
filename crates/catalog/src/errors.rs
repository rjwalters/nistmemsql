/// Errors returned by catalog operations.
#[derive(Debug, Clone, PartialEq)]
pub enum CatalogError {
    TableAlreadyExists(String),
    TableNotFound(String),
    ColumnAlreadyExists(String),
    ColumnNotFound(String),
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::TableAlreadyExists(name) => {
                write!(f, "Table '{}' already exists", name)
            }
            CatalogError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
            CatalogError::ColumnAlreadyExists(name) => {
                write!(f, "Column '{}' already exists", name)
            }
            CatalogError::ColumnNotFound(name) => write!(f, "Column '{}' not found", name),
        }
    }
}

impl std::error::Error for CatalogError {}
