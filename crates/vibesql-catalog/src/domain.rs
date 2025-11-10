//! Domain definition metadata

use vibesql_ast::Expression;
use vibesql_types::DataType;

/// Domain definition stored in the catalog
#[derive(Debug, Clone, PartialEq)]
pub struct DomainDefinition {
    /// Name of the domain
    pub name: String,
    /// Underlying data type
    pub data_type: DataType,
    /// Optional default value expression
    pub default: Option<Box<Expression>>,
    /// CHECK constraints on domain values
    pub constraints: Vec<DomainConstraintDef>,
}

/// Domain constraint definition
#[derive(Debug, Clone, PartialEq)]
pub struct DomainConstraintDef {
    /// Optional constraint name
    pub name: Option<String>,
    /// CHECK expression to validate domain values
    pub check: Box<Expression>,
}

impl DomainDefinition {
    /// Create a new domain definition
    pub fn new(
        name: String,
        data_type: DataType,
        default: Option<Box<Expression>>,
        constraints: Vec<DomainConstraintDef>,
    ) -> Self {
        DomainDefinition { name, data_type, default, constraints }
    }
}
