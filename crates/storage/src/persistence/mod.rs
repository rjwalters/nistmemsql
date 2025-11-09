// ============================================================================
// Database Persistence Module
// ============================================================================
//
// Provides SQL dump format (human-readable, portable) for database persistence.
// Organized into separate modules for better maintainability:
//
// - `save`: SQL dump generation and serialization
// - `load`: SQL dump parsing and deserialization utilities
//
// The `save_sql_dump` method is implemented directly on the `Database` type
// via an impl block in the `save` module.
//
// Load utilities are exported for use by the CLI layer to parse and execute
// SQL dump files.

pub mod load;
mod save;

#[cfg(test)]
mod tests;
