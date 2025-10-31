//! REVOKE statement AST structures
//!
//! This module defines AST structures for REVOKE statements that remove privileges
//! from roles/users on database objects.

use crate::grant::{ObjectType, PrivilegeType};

/// CASCADE or RESTRICT option for REVOKE statement.
///
/// Controls behavior when revoking privileges that have been re-granted to others.
#[derive(Debug, Clone, PartialEq)]
pub enum CascadeOption {
    /// No explicit option specified (defaults to RESTRICT per SQL:1999)
    None,
    /// CASCADE: Recursively revoke from all dependent grants
    Cascade,
    /// RESTRICT: Error if dependent grants exist
    Restrict,
}

/// REVOKE statement - removes privileges from roles/users.
///
/// Example SQL:
/// ```sql
/// REVOKE SELECT ON TABLE users FROM manager;
/// REVOKE INSERT, UPDATE ON TABLE orders FROM clerk;
/// REVOKE ALL PRIVILEGES ON TABLE products FROM admin;
/// REVOKE SELECT ON TABLE data FROM analyst CASCADE;
/// REVOKE GRANT OPTION FOR SELECT ON TABLE reports FROM manager;
/// REVOKE EXECUTE ON METHOD calculate FOR address_type FROM app_role;
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct RevokeStmt {
    /// Whether this is GRANT OPTION FOR revocation (removes grant ability only)
    pub grant_option_for: bool,
    /// List of privileges being revoked
    pub privileges: Vec<PrivilegeType>,
    /// Type of object (TABLE, SCHEMA, etc.)
    pub object_type: ObjectType,
    /// Name of the object (table, schema, etc.) - supports qualified names like "schema.table"
    pub object_name: String,
    /// Optional type name for method/routine objects (e.g., "FOR address_type")
    pub for_type_name: Option<String>,
    /// List of roles/users losing the privileges
    pub grantees: Vec<String>,
    /// Optional grantor specification (GRANTED BY clause)
    pub granted_by: Option<String>,
    /// CASCADE or RESTRICT option
    pub cascade_option: CascadeOption,
}
