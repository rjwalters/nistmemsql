//! Test utilities and validation tests for WASM bindings
//!
//! This module provides a comprehensive test suite for the WASM API, organized by functional area.
//! Tests are split into focused modules for better maintainability and organization.
//!
//! ## Module Organization
//!
//! Tests are organized into the following categories:
//!
//! - **basic_operations** - Database creation and fundamental WASM API operations
//! - **transactions** - Transaction control (BEGIN, COMMIT, ROLLBACK, savepoints)
//! - **schema_operations** - DDL operations (CREATE/DROP for tables, roles, domains, sequences)
//! - **query_tests** - Query execution (SELECT, WHERE clauses, data type conversion)
//! - **schema_introspection** - Schema inspection (list tables, describe tables, column metadata)
//! - **examples_validation** - Sample database loading and automated validation of web-demo example queries
//! - **helpers** - Shared test utilities and helper functions
//!
//! ## Test Coverage
//!
//! This test suite provides comprehensive coverage of the WASM API:
//!
//! ### Database Operations
//! - Database creation and version checking
//! - Table management (CREATE, DROP)
//! - Data manipulation (INSERT, SELECT, UPDATE, DELETE)
//! - Schema introspection (listing tables, describing schemas)
//!
//! ### Advanced Features
//! - Transaction management (BEGIN, COMMIT, ROLLBACK)
//! - Savepoint operations (SAVEPOINT, ROLLBACK TO, RELEASE)
//! - Database roles (CREATE ROLE, DROP ROLE)
//! - User-defined domains (CREATE DOMAIN, DROP DOMAIN)
//! - Sequences (CREATE, ALTER, DROP SEQUENCE)
//!
//! ### Query Capabilities
//! - Basic SELECT queries
//! - WHERE clause filtering
//! - Data type handling and conversion
//! - Query result formatting
//!
//! ### Example Validation
//! - Loading sample databases (northwind, employees)
//! - Validating web-demo SQL examples
//! - SQL statement parsing and comment handling
//!
//! ## Browser Compatibility
//!
//! These tests are designed to validate the WASM API that will be used in browser environments.
//! Some tests require `wasm-pack test --node` to run properly, as they involve JsValue
//! conversions that need a JavaScript runtime.
//!
//! ## Running Tests
//!
//! ```bash
//! # Run all WASM binding tests
//! cargo test --package wasm-bindings
//!
//! # Run specific test module
//! cargo test --package wasm-bindings basic_operations
//!
//! # Run with wasm-pack (for browser compatibility tests)
//! wasm-pack test --node
//! ```

pub mod basic_operations;
pub mod examples_validation;
pub mod helpers;
pub mod query_tests;
pub mod schema_introspection;
pub mod schema_operations;
pub mod transactions;
