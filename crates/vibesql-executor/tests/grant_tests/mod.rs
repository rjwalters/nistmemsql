//! Tests for GRANT statement execution
//!
//! This module contains comprehensive tests for the GRANT statement implementation,
//! organized by privilege type and operation scenarios.
//!
//! ## Module Organization
//!
//! - `table_privileges`: Tests for table-level privileges (SELECT, INSERT, UPDATE, DELETE, REFERENCES, ALL PRIVILEGES)
//! - `schema_privileges`: Tests for schema-level privileges (USAGE, CREATE, ALL PRIVILEGES)
//! - `grant_option`: Tests for WITH GRANT OPTION privilege propagation
//! - `edge_cases`: Tests for error conditions and validation scenarios
//!
//! ## SQL Standard Compliance
//!
//! These tests verify compliance with SQL:1999 Feature T321 (Basic privilege support).

mod edge_cases;
mod grant_option;
mod schema_privileges;
mod table_privileges;
