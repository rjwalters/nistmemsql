/// Errors returned by catalog operations.
#[derive(Debug, Clone, PartialEq)]
pub enum CatalogError {
    TableAlreadyExists(String),
    TableNotFound(String),
    SchemaAlreadyExists(String),
    SchemaNotFound(String),
    SchemaNotEmpty(String),
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogError::TableAlreadyExists(name) => {
                write!(f, "Table '{}' already exists", name)
            }
            CatalogError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
            CatalogError::SchemaAlreadyExists(name) => {
                write!(f, "Schema '{}' already exists", name)
            }
            CatalogError::SchemaNotFound(name) => write!(f, "Schema '{}' not found", name),
            CatalogError::SchemaNotEmpty(name) => {
                write!(f, "Schema '{}' is not empty", name)
            }
        }
    }
}

impl std::error::Error for CatalogError {}
