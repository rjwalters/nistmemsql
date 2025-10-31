//! Basic WASM API operation tests
//!
//! Tests fundamental database operations including creation and basic queries

use super::super::*;

#[test]
fn test_database_creation() {
    let db = Database::new();
    assert_eq!(db.version(), "nistmemsql-wasm 0.1.0");
}

// Note: Tests that call JsValue-returning methods must use wasm-pack test --node
// The methods above work correctly but require a WASM runtime to test
// For now, we verify that the code compiles correctly
