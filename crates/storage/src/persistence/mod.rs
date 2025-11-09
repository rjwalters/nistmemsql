// ============================================================================
// Database Persistence Module
// ============================================================================
//
// Provides SQL dump format (human-readable, portable) for database persistence.
// Organized into separate modules for better maintainability:
//
// - `save`: SQL dump generation and serialization (implemented on Database)
// - `load`: Utility functions for reading and splitting SQL dump files
//
// The `save_sql_dump` method is implemented directly on the `Database` type.
// Loading is done at a higher level (CLI) using the exported utility functions
// to avoid circular dependencies with parser and executor crates.

mod save;
mod load;

pub use load::{read_sql_dump, split_sql_statements};

#[cfg(test)]
mod tests;
