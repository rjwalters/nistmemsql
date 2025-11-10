// ============================================================================
// Database Persistence Module
// ============================================================================
//
// Provides multiple formats for database persistence:
//
// - **SQL dump format** (human-readable, portable SQL statements)
//   - `save`: SQL dump generation and serialization
//   - `load`: SQL dump parsing and deserialization utilities
//
// - **JSON format** (structured, tool-friendly JSON)
//   - `json`: JSON serialization/deserialization
//
// The `save_sql_dump`, `save_json`, and `load_json` methods are implemented
// directly on the `Database` type via impl blocks in their respective modules.
//
// Load utilities are exported for use by the CLI layer to parse and execute
// dump files.

pub mod json;
pub mod load;
mod save;

#[cfg(test)]
mod tests;
