use wasm_bindgen::prelude::*;

/// Initializes the WASM module and sets up panic hooks
#[wasm_bindgen(start)]
pub fn init_wasm() {
    // Better error messages in browser console
    console_error_panic_hook::set_once();
}

/// Placeholder Database struct for initial testing
#[wasm_bindgen]
#[derive(Default)]
pub struct Database {
    // Will be populated in future issues
}

#[wasm_bindgen]
impl Database {
    /// Creates a new database instance
    #[wasm_bindgen(constructor)]
    pub fn new() -> Database {
        Database {}
    }

    /// Placeholder method to verify WASM compilation works
    pub fn version(&self) -> String {
        "nistmemsql-wasm 0.1.0".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_creation() {
        let db = Database::new();
        assert_eq!(db.version(), "nistmemsql-wasm 0.1.0");
    }
}
