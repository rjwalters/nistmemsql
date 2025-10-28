use wasm_bindgen::prelude::*;

mod modules;

pub use modules::{ColumnInfo, Database, ExecuteResult, QueryResult, TableSchema};

/// Initializes the WASM module and sets up panic hooks
#[wasm_bindgen(start)]
pub fn init_wasm() {
    // Better error messages in browser console
    console_error_panic_hook::set_once();
}
