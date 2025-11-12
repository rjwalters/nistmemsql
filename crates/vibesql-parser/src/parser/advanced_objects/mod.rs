//! Advanced SQL object DDL parsing (SQL:1999)
//!
//! This module provides parsers for advanced SQL:1999 objects including:
//! - SEQUENCE (CREATE/DROP/ALTER)
//! - TYPE (CREATE/DROP - distinct and structured types)
//! - COLLATION (CREATE/DROP)
//! - CHARACTER SET (CREATE/DROP)
//! - TRANSLATION (CREATE/DROP)
//! - ASSERTION (CREATE/DROP)
//! - STORED PROCEDURES AND FUNCTIONS (CREATE/DROP/CALL)
//!
//! Note: DOMAIN has a full implementation in the separate domain module
//! (including data types, defaults, and CHECK constraints)

// Submodules
mod assertion;
mod collation;
mod routines;
mod sequence;
mod translation;
mod type_objects;

// Re-export all public parser functions for backward compatibility
pub use assertion::{parse_create_assertion, parse_drop_assertion};
pub use collation::{
    parse_create_character_set, parse_create_collation, parse_drop_character_set,
    parse_drop_collation,
};
pub use sequence::{parse_alter_sequence, parse_create_sequence, parse_drop_sequence};
pub use translation::{parse_create_translation, parse_drop_translation};
pub use type_objects::{parse_create_type, parse_drop_type};
// Note: routines are parsed directly via Parser methods, not standalone functions
// because they require complex procedural statement parsing
