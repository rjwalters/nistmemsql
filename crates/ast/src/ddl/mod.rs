//! Data Definition Language (DDL) AST nodes
//!
//! This module contains AST nodes for all DDL statements organized by category:
//!
//! - **table**: Table operations (CREATE/DROP/ALTER TABLE)
//! - **transaction**: Transaction control (BEGIN/COMMIT/ROLLBACK/SAVEPOINT)
//! - **schema**: Schema, view, index, role, and catalog operations
//! - **cursor**: Cursor operations (DECLARE/OPEN/FETCH/CLOSE)
//! - **advanced**: Advanced SQL:1999 objects (SEQUENCE, TYPE, DOMAIN, COLLATION, etc.)

// Declare modules
pub mod advanced;
pub mod cursor;
pub mod schema;
pub mod table;
pub mod transaction;

// Re-export all types to maintain backward compatibility
pub use advanced::*;
pub use cursor::*;
pub use schema::*;
pub use table::*;
pub use transaction::*;
