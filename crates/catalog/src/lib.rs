//! Catalog - Schema Metadata Storage
//!
//! Provides metadata structures for tables and columns along with the catalog
//! registry that tracks table schemas.

mod advanced_objects;
mod column;
mod domain;
pub mod errors;
mod foreign_key;
mod privilege;
mod schema;
mod store;
mod table;
mod trigger;
mod type_definition;
mod view;

pub use advanced_objects::{
    Assertion, CharacterSet, Collation, Domain, Sequence, Translation, UserDefinedType,
};
pub use column::ColumnSchema;
pub use domain::{DomainConstraintDef, DomainDefinition};
pub use errors::CatalogError;
pub use foreign_key::{ForeignKeyConstraint, ReferentialAction};
pub use privilege::PrivilegeGrant;
pub use schema::Schema;
pub use store::Catalog;
pub use table::TableSchema;
pub use trigger::TriggerDefinition;
pub use type_definition::{TypeAttribute, TypeDefinition, TypeDefinitionKind};
pub use view::ViewDefinition;

#[cfg(test)]
mod tests;
