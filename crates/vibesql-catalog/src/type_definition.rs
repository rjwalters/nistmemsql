//! User-defined type definitions

use vibesql_types::DataType;

/// A user-defined type definition stored in the catalog
#[derive(Debug, Clone, PartialEq)]
pub struct TypeDefinition {
    pub name: String,
    pub definition: TypeDefinitionKind,
}

/// Kind of type definition
#[derive(Debug, Clone, PartialEq)]
pub enum TypeDefinitionKind {
    /// Distinct type: CREATE TYPE money AS DISTINCT DECIMAL(10,2)
    Distinct { base_type: DataType },
    /// Structured type: CREATE TYPE address AS (street VARCHAR(100), city VARCHAR(50))
    Structured { attributes: Vec<TypeAttribute> },
    /// Forward declaration: CREATE TYPE type_name;
    Forward,
}

/// Attribute in a structured type
#[derive(Debug, Clone, PartialEq)]
pub struct TypeAttribute {
    pub name: String,
    pub data_type: DataType,
}

impl TypeDefinition {
    /// Create a new distinct type definition
    pub fn distinct(name: String, base_type: DataType) -> Self {
        TypeDefinition { name, definition: TypeDefinitionKind::Distinct { base_type } }
    }

    /// Create a new structured type definition
    pub fn structured(name: String, attributes: Vec<TypeAttribute>) -> Self {
        TypeDefinition { name, definition: TypeDefinitionKind::Structured { attributes } }
    }

    /// Create a new forward declaration
    pub fn forward(name: String) -> Self {
        TypeDefinition { name, definition: TypeDefinitionKind::Forward }
    }
}
