//! Database struct and constructor

use wasm_bindgen::prelude::*;

/// In-memory SQL database with WASM bindings
#[wasm_bindgen]
pub struct Database {
    pub(super) db: storage::Database,
}

#[wasm_bindgen]
impl Database {
    /// Creates a new empty database instance
    #[wasm_bindgen(constructor)]
    pub fn new() -> Database {
        Database { db: storage::Database::new() }
    }

    /// Returns the version string
    pub fn version(&self) -> String {
        "nistmemsql-wasm 0.1.0".to_string()
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}
