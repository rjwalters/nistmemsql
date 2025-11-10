//! Privilege grant storage for access control
//!
//! This module manages privilege grants that track which roles/users
//! have which privileges on which database objects.

use vibesql_ast::{ObjectType, PrivilegeType};

/// A privilege grant record tracking who has what privilege on which object.
#[derive(Debug, Clone)]
pub struct PrivilegeGrant {
    /// The database object name (table, schema, etc.) - supports qualified names like
    /// "schema.table"
    pub object: String,
    /// Type of object (TABLE, SCHEMA, etc.)
    pub object_type: ObjectType,
    /// The privilege being granted (SELECT, INSERT, etc.)
    pub privilege: PrivilegeType,
    /// The role/user receiving the privilege
    pub grantee: String,
    /// The role/user granting the privilege
    pub grantor: String,
    /// Whether the grantee can grant this privilege to others
    pub with_grant_option: bool,
}
