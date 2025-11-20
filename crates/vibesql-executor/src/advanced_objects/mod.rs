//! Executor for advanced SQL:1999 objects
//!
//! This module provides execution functions for advanced SQL objects including:
//! - **SEQUENCE**: Auto-incrementing numeric sequences
//! - **TYPE**: User-defined types (distinct and structured)
//! - **COLLATION**: Character collation definitions
//! - **CHARACTER SET**: Character set definitions
//! - **TRANSLATION**: Character set translation
//! - **VIEW**: Virtual tables based on queries
//! - **ASSERTION**: Database-level integrity constraints
//! - **PROCEDURE**: Stored procedures with parameter modes
//! - **FUNCTION**: User-defined functions
//! - **TRIGGER**: Event-driven actions
//!
//! Note: DOMAIN has a full implementation in the `domain_ddl` module.

// Module declarations
mod sequences;
mod types;
mod collations;
mod character_sets;
mod translations;
mod views;
mod assertions;
mod procedures;
mod functions;
mod triggers;

// Re-export public API to maintain backward compatibility
pub use sequences::{execute_alter_sequence, execute_create_sequence, execute_drop_sequence};
pub use types::{execute_create_type, execute_drop_type};
pub use collations::{execute_create_collation, execute_drop_collation};
pub use character_sets::{execute_create_character_set, execute_drop_character_set};
pub use translations::{execute_create_translation, execute_drop_translation};
pub use views::{execute_create_view, execute_drop_view};
pub use assertions::{execute_create_assertion, execute_drop_assertion};
pub use procedures::{execute_call, execute_create_procedure, execute_drop_procedure};
pub use functions::{execute_create_function, execute_drop_function};
pub use triggers::{execute_alter_trigger, execute_create_trigger, execute_drop_trigger};
