// ============================================================================
// Database Persistence Module
// ============================================================================
//
// Provides SQL dump format (human-readable, portable) for database persistence.
// Organized into separate modules for better maintainability:
//
// - `save`: SQL dump generation and serialization
// - `load`: SQL dump parsing and deserialization (TODO: future implementation)
//
// The `save_sql_dump` method is implemented directly on the `Database` type
// via an impl block in the `save` module, so no explicit re-exports are needed.

mod save;

#[cfg(test)]
mod tests;
