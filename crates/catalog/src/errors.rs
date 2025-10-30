/// Errors returned by catalog operations.
#[derive(Debug, Clone, PartialEq)]
pub enum CatalogError {
    TableAlreadyExists(String),
    TableNotFound(String),
    ColumnAlreadyExists(String),
    ColumnNotFound(String),
    SchemaAlreadyExists(String),
    SchemaNotFound(String),
    SchemaNotEmpty(String),
    RoleAlreadyExists(String),
    RoleNotFound(String),
    // Advanced SQL:1999 objects
    DomainAlreadyExists(String),
    DomainNotFound(String),
    SequenceAlreadyExists(String),
    SequenceNotFound(String),
    TypeAlreadyExists(String),
    TypeNotFound(String),
    TypeInUse(String),
    CollationAlreadyExists(String),
    CollationNotFound(String),
    CharacterSetAlreadyExists(String),
    CharacterSetNotFound(String),
    TranslationAlreadyExists(String),
    TranslationNotFound(String),
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
            CatalogError::SchemaAlreadyExists(name) => {
                write!(f, "Schema '{}' already exists", name)
            }
            CatalogError::SchemaNotFound(name) => write!(f, "Schema '{}' not found", name),
            CatalogError::SchemaNotEmpty(name) => {
                write!(f, "Schema '{}' is not empty", name)
            }
            CatalogError::RoleAlreadyExists(name) => {
                write!(f, "Role '{}' already exists", name)
            }
            CatalogError::RoleNotFound(name) => write!(f, "Role '{}' not found", name),
            CatalogError::DomainAlreadyExists(name) => {
                write!(f, "Domain '{}' already exists", name)
            }
            CatalogError::DomainNotFound(name) => write!(f, "Domain '{}' not found", name),
            CatalogError::SequenceAlreadyExists(name) => {
                write!(f, "Sequence '{}' already exists", name)
            }
            CatalogError::SequenceNotFound(name) => write!(f, "Sequence '{}' not found", name),
            CatalogError::TypeAlreadyExists(name) => {
                write!(f, "Type '{}' already exists", name)
            }
            CatalogError::TypeNotFound(name) => write!(f, "Type '{}' not found", name),
            CatalogError::TypeInUse(name) => {
                write!(f, "Type '{}' is still in use by one or more tables", name)
            }
            CatalogError::CollationAlreadyExists(name) => {
                write!(f, "Collation '{}' already exists", name)
            }
            CatalogError::CollationNotFound(name) => {
                write!(f, "Collation '{}' not found", name)
            }
            CatalogError::CharacterSetAlreadyExists(name) => {
                write!(f, "Character set '{}' already exists", name)
            }
            CatalogError::CharacterSetNotFound(name) => {
                write!(f, "Character set '{}' not found", name)
            }
            CatalogError::TranslationAlreadyExists(name) => {
                write!(f, "Translation '{}' already exists", name)
            }
            CatalogError::TranslationNotFound(name) => {
                write!(f, "Translation '{}' not found", name)
            }
        }
    }
}

impl std::error::Error for CatalogError {}
