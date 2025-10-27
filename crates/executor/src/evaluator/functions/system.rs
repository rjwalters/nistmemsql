//! System information functions
//!
//! This module provides SQL functions that return system-level information:
//! - VERSION() - Database version information
//! - DATABASE() / SCHEMA() - Current database name
//! - USER() / CURRENT_USER() - Current user information

use crate::errors::ExecutorError;

/// VERSION() - Return database version
pub(super) fn version(args: &[types::SqlValue]) -> Result<types::SqlValue, ExecutorError> {
    if !args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "VERSION takes no arguments".to_string(),
        ));
    }

    // Get version from Cargo.toml at compile time
    let version = env!("CARGO_PKG_VERSION");
    Ok(types::SqlValue::Varchar(format!("NistMemSQL {}", version)))
}

/// DATABASE() / SCHEMA() - Return current database name
pub(super) fn database(args: &[types::SqlValue], name: &str) -> Result<types::SqlValue, ExecutorError> {
    if !args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            format!("{} takes no arguments", name),
        ));
    }

    // In current implementation, return default database name
    // In future with connection context, return actual database name
    Ok(types::SqlValue::Varchar("default".to_string()))
}

/// USER() / CURRENT_USER() - Return current user
pub(super) fn user(args: &[types::SqlValue], name: &str) -> Result<types::SqlValue, ExecutorError> {
    if !args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            format!("{} takes no arguments", name),
        ));
    }

    // In current implementation, return default user
    // In future with authentication, return actual username
    Ok(types::SqlValue::Varchar("anonymous".to_string()))
}
