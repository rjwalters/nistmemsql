//! GRANT statement AST structures
//!
//! This module defines AST structures for GRANT statements that assign privileges
//! to roles/users on database objects.

/// Privilege types that can be granted on database objects.
#[derive(Debug, Clone, PartialEq)]
pub enum PrivilegeType {
    /// SELECT privilege (read access)
    /// Optional column list for column-level SELECT privileges (SQL:1999 Feature F031-03)
    Select(Option<Vec<String>>),
    /// INSERT privilege (write access)
    /// Optional column list for column-level INSERT privileges (SQL:1999 Feature F031-03)
    Insert(Option<Vec<String>>),
    /// UPDATE privilege (modify access)
    /// Optional column list for column-level UPDATE privileges (SQL:1999 Feature E081-05)
    Update(Option<Vec<String>>),
    /// DELETE privilege (delete access)
    Delete,
    /// REFERENCES privilege (foreign key access)
    /// Optional column list for column-level REFERENCES privileges (SQL:1999 Feature E081-07)
    References(Option<Vec<String>>),
    /// USAGE privilege (schema/sequence usage)
    Usage,
    /// CREATE privilege (create objects in schema)
    Create,
    /// EXECUTE privilege (function/procedure execution)
    Execute,
    /// TRIGGER privilege (create triggers on table)
    Trigger,
    /// UNDER privilege (create subtypes of user-defined type)
    Under,
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
    /// Domain object - user-defined data type with constraints (SQL:1999 Feature F031-03)
    Domain,
    /// Collation object - character comparison rules (SQL:1999 Feature F031-06)
    Collation,
    /// Character set object - character encoding definitions (SQL:1999 Feature F031-08)
    CharacterSet,
    /// Translation object - character set conversions (SQL:1999 Feature F031-09)
    Translation,
    /// Type object - user-defined types (SQL:1999 Feature F031-10)
    Type,
    /// Sequence object - auto-increment sequences (SQL:1999 Feature F031-11)
    Sequence,
    /// Function object (SQL:1999 Feature P001)
    Function,
    /// Procedure object (SQL:1999 Feature P001)
    Procedure,
    /// Routine object (generic term covering both functions and procedures)
    Routine,
    /// Method object (SQL:1999 Feature S091)
    Method,
    /// Constructor method for user-defined types
    ConstructorMethod,
    /// Static method for user-defined types
    StaticMethod,
    /// Instance method for user-defined types
    InstanceMethod,
    /// Specific function - function by signature (SQL:1999 Feature F031-15)
    SpecificFunction,
    /// Specific procedure - procedure by signature (SQL:1999 Feature F031-16)
    SpecificProcedure,
    /// Specific routine - routine by signature (SQL:1999 Feature F031-17)
    SpecificRoutine,
    /// Specific method - method by signature (SQL:1999 Feature F031-12)
    SpecificMethod,
    /// Specific constructor method (SQL:1999 Feature F031-12)
    SpecificConstructorMethod,
    /// Specific static method (SQL:1999 Feature F031-12)
    SpecificStaticMethod,
    /// Specific instance method (SQL:1999 Feature F031-12)
    SpecificInstanceMethod,
}

/// GRANT statement - assigns privileges to roles/users.
///
/// Example SQL:
/// ```sql
/// GRANT SELECT ON TABLE users TO manager;
/// GRANT INSERT, UPDATE ON TABLE orders TO clerk;
/// GRANT ALL PRIVILEGES ON TABLE products TO admin WITH GRANT OPTION;
/// GRANT EXECUTE ON METHOD calculate FOR address_type TO app_role;
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct GrantStmt {
    /// List of privileges being granted
    pub privileges: Vec<PrivilegeType>,
    /// Type of object (TABLE, SCHEMA, etc.)
    pub object_type: ObjectType,
    /// Name of the object (table, schema, etc.) - supports qualified names like "schema.table"
    pub object_name: String,
    /// Optional type name for method/routine objects (e.g., "FOR address_type")
    pub for_type_name: Option<String>,
    /// List of roles/users receiving the privileges
    pub grantees: Vec<String>,
    /// Whether grantees can grant these privileges to others
    pub with_grant_option: bool,
}
