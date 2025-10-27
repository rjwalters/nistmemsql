// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum StorageError {
    TableNotFound(String),
    ColumnCountMismatch { expected: usize, actual: usize },
    ColumnIndexOutOfBounds { index: usize },
    CatalogError(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
            StorageError::ColumnCountMismatch { expected, actual } => {
                write!(f, "Column count mismatch: expected {}, got {}", expected, actual)
            }
            StorageError::ColumnIndexOutOfBounds { index } => {
                write!(f, "Column index {} out of bounds", index)
            }
            StorageError::CatalogError(msg) => write!(f, "Catalog error: {}", msg),
        }
    }
}

impl std::error::Error for StorageError {}
