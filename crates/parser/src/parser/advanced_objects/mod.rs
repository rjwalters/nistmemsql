//! Advanced SQL object DDL parsing (SQL:1999)
//!
//! This module provides parsers for advanced SQL:1999 objects including:
//! - SEQUENCE (CREATE/DROP/ALTER)
//! - TYPE (CREATE/DROP - distinct and structured types)
//! - COLLATION (CREATE/DROP)
//! - CHARACTER SET (CREATE/DROP)
//! - TRANSLATION (CREATE/DROP)
//! - ASSERTION (CREATE/DROP)
//!
//! Note: DOMAIN has a full implementation in the separate domain module
//! (including data types, defaults, and CHECK constraints)

// Submodules
mod sequence;
mod type_objects;
mod collation;
mod translation;
mod assertion;

// Re-export all public parser functions for backward compatibility
pub use sequence::{parse_create_sequence, parse_drop_sequence, parse_alter_sequence};
pub use type_objects::{parse_create_type, parse_drop_type};
pub use collation::{
    parse_create_collation,
    parse_drop_collation,
    parse_create_character_set,
    parse_drop_character_set,
};
pub use translation::{parse_create_translation, parse_drop_translation};
pub use assertion::{parse_create_assertion, parse_drop_assertion};
