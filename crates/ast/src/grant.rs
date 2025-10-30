//! GRANT statement AST structures
//!
//! This module defines AST structures for GRANT statements that assign privileges
//! to roles/users on database objects.

/// Privilege types that can be granted on database objects.
#[derive(Debug, Clone, PartialEq)]
pub enum PrivilegeType {
    /// SELECT privilege (read access)
    Select,
    /// INSERT privilege (write access)
    Insert,
    /// UPDATE privilege (modify access)
    Update,
    /// DELETE privilege (delete access)
    Delete,
    /// REFERENCES privilege (foreign key access)
    References,
    /// USAGE privilege (schema/sequence usage)
    Usage,
    /// CREATE privilege (create objects in schema)
    Create,
    /// EXECUTE privilege (function/procedure execution)
    Execute,
    /// ALL PRIVILEGES (all applicable privileges for the object type)
    AllPrivileges,
}

/// Types of database objects that can have privileges granted on them.
#[derive(Debug, Clone, PartialEq)]
pub enum ObjectType {
    /// Table object
    Table,
    /// Schema object
    Schema,
}

/// GRANT statement - assigns privileges to roles/users.
///
/// Example SQL:
/// ```sql
/// GRANT SELECT ON TABLE users TO manager;
/// GRANT INSERT, UPDATE ON TABLE orders TO clerk;
/// GRANT ALL PRIVILEGES ON TABLE products TO admin WITH GRANT OPTION;
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct GrantStmt {
    /// List of privileges being granted
    pub privileges: Vec<PrivilegeType>,
    /// Type of object (TABLE, SCHEMA, etc.)
    pub object_type: ObjectType,
    /// Name of the object (table, schema, etc.) - supports qualified names like "schema.table"
    pub object_name: String,
    /// List of roles/users receiving the privileges
    pub grantees: Vec<String>,
    /// Whether grantees can grant these privileges to others
    pub with_grant_option: bool,
}
