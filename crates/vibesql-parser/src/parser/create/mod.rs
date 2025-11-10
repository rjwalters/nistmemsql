//! CREATE TABLE parsing
//!
//! This module handles parsing of CREATE TABLE statements, organized into:
//! - `table` - Main CREATE TABLE parsing logic
//! - `types` - Data type parsing (INTEGER, VARCHAR, NUMERIC, etc.)
//! - `constraints` - Constraint parsing (PRIMARY KEY, FOREIGN KEY, etc.)

mod constraints;
mod table;
mod types;
